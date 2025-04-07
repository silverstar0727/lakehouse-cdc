from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
import argparse
import json
import logging
from datetime import datetime, timedelta

# 로컬 모듈 임포트
from utils.source_val import validate_cdc_sequence, deduplicate_cdc_events
from utils.kafka_val import validate_kafka_completeness, validate_cdc_schema
from utils.pyspark_val import validate_record_counts, validate_content_checksum
from utils.cdc2iceberg import validate_cdc_to_iceberg, validate_transaction_integrity
from utils.realtime import continuous_validation, check_and_alert
from utils.integrity import reprocess_missing_data, recover_data_integrity
from utils.pipeline import setup_validation_pipeline

# 로거 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("lakehouse-cdc-validation")

def init_spark():
    """Spark 세션을 초기화하고 반환합니다."""
    return (
        SparkSession.builder
        .appName("Lakehouse CDC Validation")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "/tmp/iceberg/warehouse")
        .getOrCreate()
    )

def load_cdc_data(spark, config):
    """
    구성에 따라 CDC 데이터를 로드합니다.
    배치 모드 또는 스트리밍 모드로 로드할 수 있습니다.
    """
    source_type = config.get("source_type", "batch")
    
    if source_type == "stream":
        # 스트리밍 모드로 Kafka에서 데이터 로드
        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", config["kafka_brokers"])
            .option("subscribe", config["cdc_topic"])
            .option("startingOffsets", "earliest")
            .load()
        )
        
        # CDC 스키마 로드 및 파싱
        with open(config["schema_path"]) as f:
            cdc_schema = json.load(f)
        
        # JSON 파싱
        parsed_stream = (
            raw_stream
            .select(
                col("key").cast("string").alias("key"),
                from_json(col("value").cast("string"), cdc_schema).alias("data"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            )
            .select(
                "key", 
                "data.*", 
                "topic", 
                "partition", 
                "offset",
                to_timestamp(col("kafka_timestamp").cast("long") / 1000).alias("event_time")
            )
        )
        
        return parsed_stream
    else:
        # 배치 모드로 데이터 로드 (예: 기존 Kafka 메시지 또는 파일)
        if config.get("source_format", "kafka") == "kafka":
            # Kafka 소스에서 배치로 데이터 로드
            raw_data = (
                spark.read
                .format("kafka")
                .option("kafka.bootstrap.servers", config["kafka_brokers"])
                .option("subscribe", config["cdc_topic"])
                .option("startingOffsets", config.get("starting_offsets", "earliest"))
                .option("endingOffsets", config.get("ending_offsets", "latest"))
                .load()
            )
            
            # CDC 스키마 로드 및 파싱
            with open(config["schema_path"]) as f:
                cdc_schema = json.load(f)
            
            # JSON 파싱
            parsed_data = (
                raw_data
                .select(
                    col("key").cast("string").alias("key"),
                    from_json(col("value").cast("string"), cdc_schema).alias("data"),
                    col("topic"),
                    col("partition"),
                    col("offset"),
                    col("timestamp").alias("kafka_timestamp")
                )
                .select(
                    "key", 
                    "data.*", 
                    "topic", 
                    "partition", 
                    "offset",
                    to_timestamp(col("kafka_timestamp").cast("long") / 1000).alias("event_time")
                )
            )
            
            return parsed_data
        else:
            # 파일에서 데이터 로드
            return spark.read.format(config["source_format"]).load(config["source_path"])

def load_iceberg_data(spark, config):
    """Iceberg 테이블에서 데이터를 로드합니다."""
    iceberg_path = f"{config['iceberg_catalog']}.{config['iceberg_schema']}.{config['iceberg_table']}"
    return spark.read.format("iceberg").load(iceberg_path)

def run_validation_checks(args, config):
    """지정된 유형의 검증을 실행합니다."""
    spark = init_spark()
    
    # 데이터 로드
    cdc_data = load_cdc_data(spark, config)
    
    # 검증 유형에 따라 적절한 함수 호출
    if args.validation_type == "sequence":
        # CDC 이벤트 순서 검증
        logger.info("CDC 이벤트 순서 검증 실행 중...")
        is_sequence_valid = validate_cdc_sequence(cdc_data)
        logger.info(f"CDC 시퀀스 검증 결과: {'유효함' if is_sequence_valid else '유효하지 않음'}")
        
        if not is_sequence_valid:
            logger.warning("CDC 이벤트 누락 발견! 추가 조치가 필요합니다.")
    
    elif args.validation_type == "deduplicate":
        # CDC 이벤트 중복 제거
        logger.info("CDC 이벤트 중복 제거 실행 중...")
        original_count = cdc_data.count()
        deduplicated_data = deduplicate_cdc_events(cdc_data)
        deduplicated_count = deduplicated_data.count()
        
        logger.info(f"중복 제거 전 레코드 수: {original_count}")
        logger.info(f"중복 제거 후 레코드 수: {deduplicated_count}")
        logger.info(f"제거된 중복 레코드 수: {original_count - deduplicated_count}")
    
    elif args.validation_type == "kafka_completeness":
        # Kafka 메시지 완전성 검증
        logger.info("Kafka 메시지 완전성 검증 실행 중...")
        missing_messages = validate_kafka_completeness(cdc_data)
        
        if missing_messages.count() > 0:
            logger.warning(f"누락된 Kafka 메시지 발견: {missing_messages.count()}개")
            logger.warning("샘플 누락 메시지:")
            missing_messages.show(5, truncate=False)
        else:
            logger.info("Kafka 메시지 완전성 검증 통과")
    
    elif args.validation_type == "cdc_to_iceberg":
        # CDC와 Iceberg 데이터 일치 검증
        logger.info("CDC와 Iceberg 데이터 일치 검증 실행 중...")
        iceberg_data = load_iceberg_data(spark, config)
        
        # 기본 키 컬럼 가져오기
        pk_cols = config["primary_key_columns"]
        
        # 기록 수 검증
        count_match = validate_record_counts(cdc_data, iceberg_data)
        logger.info(f"레코드 수 일치 여부: {'일치함' if count_match else '일치하지 않음'}")
        
        # 내용 일치 검증
        value_cols = config.get("value_columns", [c for c in cdc_data.columns if c not in pk_cols])
        mismatches = validate_content_checksum(cdc_data, iceberg_data, pk_cols, value_cols)
        
        if mismatches.count() > 0:
            logger.warning(f"데이터 불일치 발견: {mismatches.count()}개 레코드")
            logger.warning("샘플 불일치 레코드:")
            mismatches.show(5, truncate=False)
        else:
            logger.info("CDC와 Iceberg 데이터 일치 검증 통과")
    
    elif args.validation_type == "transaction_integrity":
        # 트랜잭션 무결성 검증
        logger.info("트랜잭션 무결성 검증 실행 중...")
        iceberg_data = load_iceberg_data(spark, config)
        
        # 기본 키 컬럼과 트랜잭션 ID 컬럼 가져오기
        pk_cols = config["primary_key_columns"]
        tx_id_col = config["transaction_id_column"]
        
        # 트랜잭션 무결성 검증
        tx_result = validate_transaction_integrity(
            spark,
            cdc_data, 
            iceberg_data, 
            tx_id_col, 
            pk_cols
        )
        
        if tx_result["integrity_maintained"]:
            logger.info("트랜잭션 무결성 검증 통과")
        else:
            logger.warning(f"트랜잭션 무결성 위반: {tx_result['incomplete_transactions']}개 트랜잭션")
            
            if tx_result["details"] is not None:
                logger.warning("불완전 트랜잭션 상세 정보:")
                tx_result["details"].show(5, truncate=False)
    
    elif args.validation_type == "streaming":
        # 실시간 검증 스트림 설정
        logger.info("실시간 검증 스트림 설정 중...")
        
        # 스트림 데이터 로드
        config["source_type"] = "stream"
        stream_data = load_cdc_data(spark, config)
        
        # 실시간 검증 시작
        validation_query = continuous_validation(stream_data)
        
        logger.info("실시간 검증 스트림이 시작되었습니다. 종료하려면 Ctrl+C를 누르세요.")
        
        try:
            # 매 5분마다 알림 확인
            while True:
                logger.info("검증 메트릭 확인 중...")
                alert_result = check_and_alert(spark, config.get("completeness_threshold", 0.99))
                
                if alert_result.get("alert_sent", False):
                    logger.warning("알림이 발송되었습니다!")
                    logger.warning(f"알림 세부 정보: {alert_result}")
                
                # 5분 대기
                time.sleep(300)
        except KeyboardInterrupt:
            logger.info("사용자에 의해 중단되었습니다.")
            validation_query.stop()
    
    elif args.validation_type == "recovery":
        # 데이터 복구
        logger.info("데이터 복구 작업 실행 중...")
        
        # CDC와 Iceberg 데이터 불일치 확인
        iceberg_data = load_iceberg_data(spark, config)
        pk_cols = config["primary_key_columns"]
        
        # CDC와 Iceberg 비교
        mismatches = validate_cdc_to_iceberg(cdc_data, iceberg_data, pk_cols)
        
        if mismatches.count() == 0:
            logger.info("불일치 레코드가 없습니다. 복구가 필요하지 않습니다.")
            return
        
        logger.info(f"불일치 레코드 발견: {mismatches.count()}개")
        logger.info("데이터 복구 시작...")
        
        # 데이터 복구 실행
        recovery_result = recover_data_integrity(spark, mismatches)
        
        logger.info(f"복구 결과: {recovery_result['recovered_count']}개 성공, {recovery_result['failed_count']}개 실패")
        if recovery_result['failed_count'] > 0:
            logger.warning(f"복구 실패 세부 정보: {recovery_result.get('details', {})}")
    
    elif args.validation_type == "pipeline":
        # 종합 검증 파이프라인 설정
        logger.info("종합 검증 파이프라인 설정 중...")
        
        # CDC 스키마 로드
        with open(config["schema_path"]) as f:
            cdc_schema = json.load(f)
        
        # 파이프라인 설정
        pipeline = setup_validation_pipeline(config, cdc_schema)
        
        logger.info("검증 파이프라인이 시작되었습니다. 종료하려면 Ctrl+C를 누르세요.")
        
        try:
            # 파이프라인 상태 모니터링
            while True:
                if spark.sql("SHOW TABLES").filter(col("tableName") == "validation_metrics").count() > 0:
                    logger.info("현재 검증 메트릭:")
                    spark.table("validation_metrics").show(5, truncate=False)
                
                # 1분 대기
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("사용자에 의해 중단되었습니다.")
            for query in pipeline["streaming_queries"]:
                query.stop()
    
    else:
        logger.error(f"지원하지 않는 검증 유형: {args.validation_type}")

def main():
    # 파라미터 파싱
    parser = argparse.ArgumentParser(description="Lakehouse CDC 검증 도구")
    parser.add_argument("--config", required=True, help="검증 설정 JSON 파일 경로")
    parser.add_argument(
        "--validation-type", 
        required=True,
        choices=[
            "sequence", 
            "deduplicate",
            "kafka_completeness",
            "cdc_to_iceberg",
            "transaction_integrity",
            "streaming",
            "recovery",
            "pipeline"
        ],
        help="실행할 검증 유형"
    )
    args = parser.parse_args()
    
    # 설정 파일 로드
    with open(args.config) as f:
        config = json.load(f)
    
    # 검증 실행
    run_validation_checks(args, config)

if __name__ == "__main__":
    import time
    main()
