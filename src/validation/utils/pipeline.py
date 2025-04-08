import threading
import time
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lag
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

from utils.cdc2iceberg import validate_cdc_to_iceberg
from utils.realtime import continuous_validation
from utils.integrity import recover_data_integrity

def setup_validation_pipeline(config: dict, cdc_schema: dict) -> dict:
    """
    CDC + Kafka + PySpark + Iceberg 데이터 파이프라인의 종합적인 무결성 검증 파이프라인을 구축합니다.
    
    이 함수는 실시간 및 배치 검증을 모두 설정하고, 검증 결과에 따라 알림 및 복구 메커니즘을 활성화합니다.
    
    Parameters:
    -----------
    config : dict
        검증 파이프라인 구성 정보:
        - kafka_brokers: Kafka 브로커 주소
        - cdc_topics: CDC 이벤트가 저장된 Kafka 토픽 목록
        - iceberg_tables: 검증할 Iceberg 테이블 정보 (테이블명, 스키마, 기본 키 등)
        - validation_interval: 배치 검증 주기 (초)
        - notification_config: 알림 설정 (이메일, Slack 등)
        - recovery_enabled: 자동 복구 활성화 여부
    cdc_schema : dict
        CDC 이벤트의 JSON 스키마 정의 (예: {"type": "struct", "fields": [...]})
    
    Returns:
    --------
    dict
        파이프라인 컴포넌트에 대한 참조:
        - streaming_queries: 활성화된 스트리밍 쿼리 목록
        - batch_jobs: 예약된 배치 작업 목록
        - validation_metrics: 검증 메트릭 테이블 참조
    
    Notes:
    ------
    이 함수는 Spark 클러스터에서 지속적으로 실행되는 작업들을 시작합니다.
    자원 사용량과 성능 영향을 고려하여 적절한 클러스터 사이징이 필요합니다.
    
    Examples:
    ---------
    >>> # 검증 파이프라인 구성 및 시작
    >>> validation_config = {
    >>>     "kafka_brokers": "broker1:9092,broker2:9092",
    >>>     "cdc_topics": ["postgres.public.users", "postgres.public.orders"],
    >>>     "iceberg_tables": [
    >>>         {
    >>>             "name": "users",
    >>>             "catalog": "iceberg",
    >>>             "schema": "db",
    >>>             "pk_columns": ["id"]
    >>>         },
    >>>         {
    >>>             "name": "orders",
    >>>             "catalog": "iceberg",
    >>>             "schema": "db",
    >>>             "pk_columns": ["order_id"]
    >>>         }
    >>>     ],
    >>>     "validation_interval": 600,
    >>>     "notification_config": {
    >>>         "email": ["alerts@example.com"],
    >>>         "slack_webhook": "https://hooks.slack.com/..."
    >>>     },
    >>>     "recovery_enabled": True
    >>> }
    >>> 
    >>> pipeline = setup_validation_pipeline(validation_config)
    """
    # Spark 세션 초기화
    spark = SparkSession.builder \
        .appName("CDC-Iceberg Integrity Validation") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3://your-bucket/iceberg/warehouse") \
        .getOrCreate()
    
    # 결과 저장용 컨테이너
    streaming_queries = []
    batch_jobs = []
    
    # 1. 실시간 CDC 스트림 모니터링 설정
    for topic in config["cdc_topics"]:
        # Kafka 스트림 설정
        cdc_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config["kafka_brokers"]) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        # CDC 메시지 파싱
        parsed_stream = cdc_stream \
            .select(
                from_json(col("value").cast("string"), cdc_schema).alias("cdc"),
                col("timestamp").alias("kafka_timestamp"),
                col("offset"),
                col("partition")
            ) \
            .select(
                "kafka_timestamp",
                "offset",
                "partition",
                "cdc.before",
                "cdc.after",
                "cdc.source.ts_ms",
                "cdc.op"
            )
        
        # 1.1 메시지 순서 검증 (오프셋 및 LSN)
        offset_validation = parsed_stream \
            .withColumn("prev_offset", 
                      lag("offset", 1).over(
                          Window.partitionBy("partition").orderBy("offset")
                      )) \
            .withColumn("offset_diff", 
                      col("offset") - col("prev_offset")) \
            .filter(
                (col("prev_offset").isNotNull()) & 
                (col("offset_diff") > 1)
            )
        
        offset_query = offset_validation \
            .writeStream \
            .format("memory") \
            .queryName(f"{topic}_offset_gaps") \
            .outputMode("append") \
            .start()
        
        streaming_queries.append(offset_query)
        
        # 1.2 이벤트 집계 메트릭
        metrics_query = continuous_validation(parsed_stream)
        streaming_queries.append(metrics_query)
    
    # 2. 배치 검증 작업 설정
    def run_batch_validation():
        while True:
            for table_config in config["iceberg_tables"]:
                table_name = table_config["name"]
                catalog = table_config["catalog"]
                schema = table_config["schema"]
                pk_columns = table_config["pk_columns"]
                
                # 2.1 Iceberg 테이블 현재 상태 읽기
                iceberg_table = spark.read \
                    .format("iceberg") \
                    .load(f"{catalog}.{schema}.{table_name}")
                
                # 2.2 최근 CDC 이벤트 상태 (임시 테이블에 저장된 처리된 이벤트)
                cdc_events = spark.table(f"cdc_events_{table_name}")
                
                # 2.3 종단 간 검증 수행
                mismatches = validate_cdc_to_iceberg(cdc_events, iceberg_table, pk_columns)
                
                # 검증 결과를 테이블에 저장
                mismatches.write \
                    .format("memory") \
                    .mode("overwrite") \
                    .saveAsTable(f"validation_mismatches_{table_name}")
                
                # 2.4 불일치 발견 시 알림 및 복구
                if mismatches.count() > 0:
                    # 알림 발송
                    send_validation_alert(
                        config["notification_config"],
                        f"데이터 불일치 발견: {table_name}, {mismatches.count()}개 레코드",
                        mismatches
                    )
                    
                    # 자동 복구 활성화된 경우
                    if config.get("recovery_enabled", False):
                        recovery_result = recover_data_integrity(mismatches)
                        
                        # 복구 결과 저장
                        spark.createDataFrame([recovery_result]) \
                            .write \
                            .format("memory") \
                            .mode("append") \
                            .saveAsTable(f"recovery_results_{table_name}")
                        
                        # 복구 결과 알림
                        send_validation_alert(
                            config["notification_config"],
                            f"데이터 복구 결과: {table_name}, {recovery_result['recovered_count']}개 성공, {recovery_result['failed_count']}개 실패",
                            recovery_result
                        )
            
            # 설정된 간격만큼 대기
            time.sleep(config["validation_interval"])
    
    # 배치 검증 작업 시작
    batch_thread = threading.Thread(target=run_batch_validation)
    batch_thread.daemon = True
    batch_thread.start()
    
    batch_jobs.append(batch_thread)
    
    # 3. 알림 발송 헬퍼 함수
    def send_validation_alert(notification_config, message, details=None):
        """
        검증 결과에 따른 알림을 발송합니다.
        """
        # 이메일 알림
        if "email" in notification_config:
            for email in notification_config["email"]:
                # 이메일 발송 로직 구현
                print(f"[알림 이메일] 수신자: {email}, 메시지: {message}")
        
        # Slack 알림
        if "slack_webhook" in notification_config:
            webhook_url = notification_config["slack_webhook"]
            # Slack 웹훅 호출 로직 구현
            print(f"[알림 Slack] 웹훅: {webhook_url}, 메시지: {message}")
        
        # 세부 정보 로깅
        if details is not None:
            if isinstance(details, DataFrame):
                details_str = details.limit(10).toPandas().to_json(orient="records")
            else:
                details_str = json.dumps(details)
            
            print(f"[알림 세부정보] {details_str}")
    
    return {
        "streaming_queries": streaming_queries,
        "batch_jobs": batch_jobs,
        "validation_metrics": "validation_metrics"  # 메모리 테이블 이름
    }