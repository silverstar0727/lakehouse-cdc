from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, window, count, countDistinct, sum, when, min, max, desc
from datetime import datetime

def continuous_validation(kafka_stream: DataFrame) -> StreamingQuery:
    """
    Kafka 스트림을 실시간으로 모니터링하여 데이터 무결성 지표를 계산합니다.
    
    시간 윈도우 기반으로 CDC 이벤트를 집계하여 이벤트 수, 고유 레코드 수,
    이벤트 유형별 비율 등 다양한 메트릭을 계산합니다.
    
    Parameters:
    -----------
    kafka_stream : pyspark.sql.DataFrame
        Kafka 스트림의 DataFrame (Structured Streaming)
        다음 칼럼을 포함해야 함:
        - event_time: 이벤트 발생 시간 (timestamp)
        - table: 테이블 이름 (string)
        - primary_key: 레코드의 기본 키 (any type)
        - op: 작업 유형 (c/u/d)
    
    Returns:
    --------
    pyspark.sql.streaming.StreamingQuery
        실행 중인 스트리밍 쿼리 객체
    
    Notes:
    ------
    이 함수는 지속적으로 실행되는 스트리밍 작업을 시작합니다.
    결과는 메모리 테이블 "validation_metrics"에 저장되므로,
    spark.table("validation_metrics")를 통해 최신 메트릭에 접근할 수 있습니다.
    
    Examples:
    ---------
    >>> kafka_stream = spark.readStream.format("kafka").load(...)
    >>> parsed_stream = kafka_stream.select(...)  # 필요한 전처리 수행
    >>> validation_query = continuous_validation(parsed_stream)
    >>>
    >>> # 다른 쓰레드에서 메트릭 확인
    >>> metrics = spark.table("validation_metrics")
    >>> metrics.show()
    """
    return (
        kafka_stream
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("table")
        )
        .agg(
            count("*").alias("event_count"),
            countDistinct("primary_key").alias("unique_records"),
            sum(when(col("op") == "c", 1).otherwise(0)).alias("insert_count"),
            sum(when(col("op") == "u", 1).otherwise(0)).alias("update_count"),
            sum(when(col("op") == "d", 1).otherwise(0)).alias("delete_count"),
            min("event_time").alias("first_event_time"),
            max("event_time").alias("last_event_time")
        )
        .writeStream
        .format("memory")
        .queryName("validation_metrics")
        .outputMode("complete")
        .start()
    )


def check_and_alert(spark, threshold=0.99):
    """
    데이터 검증 메트릭을 확인하고 임계값을 초과하는 경우 알림을 발송합니다.
    
    지속적으로 생성되는 검증 메트릭 테이블의 최신 결과를 분석하여
    데이터 완전성, 지연 시간 등의 지표가 기준치를 충족하는지 확인합니다.
    
    Parameters:
    -----------
    spark : pyspark.sql.SparkSession
        활성 Spark 세션
    threshold : float, optional, default=0.99
        데이터 완전성 임계값 (0.0 ~ 1.0)
        지정된 값보다 낮은 완전성은 알림 발생
    
    Returns:
    --------
    dict
        검증 결과 요약 (임계값 초과 여부, 알림 발송 여부 등)
    
    Notes:
    ------
    이 함수는 주기적으로 실행되어야 하며, 외부 알림 서비스
    (이메일, Slack, PagerDuty 등)와 통합해야 합니다.
    
    Examples:
    ---------
    >>> # 10분마다 검증 수행
    >>> def scheduled_validation():
    >>>     while True:
    >>>         result = check_and_alert(spark, 0.98)
    >>>         if not result['all_passed']:
    >>>             print(f"검증 실패! 상세 정보: {result}")
    >>>         time.sleep(600)
    >>>
    >>> # 별도 쓰레드로 실행
    >>> import threading
    >>> validation_thread = threading.Thread(target=scheduled_validation)
    >>> validation_thread.daemon = True
    >>> validation_thread.start()
    """
    validation_df = spark.table("validation_metrics")
    if validation_df.count() == 0:
        return {"status": "no_data", "all_passed": False}
    
    latest = validation_df.orderBy(desc("window.end")).limit(1)
    
    # 검증 지표 확인
    completeness = latest.select(
        (col("event_count") / col("expected_count")).alias("completeness")
    ).collect()[0]["completeness"]
    
    # 지연 시간 확인 (현재 시간과 마지막 이벤트 시간의 차이)
    current_time = datetime.now()
    last_event_time = latest.select("last_event_time").collect()[0]["last_event_time"]
    latency_seconds = (current_time - last_event_time).total_seconds()
    
    result = {
        "completeness": completeness,
        "completeness_threshold": threshold,
        "latency_seconds": latency_seconds,
        "timestamp": current_time.isoformat(),
        "all_passed": completeness >= threshold and latency_seconds < 300  # 5분 이내
    }
    
    if completeness < threshold:
        # 알림 발송 로직
        send_alert(f"데이터 완전성 임계값 미달: {completeness:.2%} (기준: {threshold:.2%})")
        result["alert_sent"] = True
    
    if latency_seconds >= 300:
        # 알림 발송 로직
        send_alert(f"데이터 지연 발생: {latency_seconds/60:.1f}분 (기준: 5분)")
        result["alert_sent"] = True
    
    return result