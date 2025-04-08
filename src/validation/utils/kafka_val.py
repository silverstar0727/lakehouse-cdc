from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import col, lag, count, when


def validate_kafka_completeness(kafka_df: DataFrame) -> DataFrame:
    """
    Kafka 메시지 스트림의 완전성을 검증하여 메시지 누락 여부를 확인합니다.
    
    토픽과 파티션별로 오프셋 순서를 검사하여 오프셋 간 간격이 1보다 큰 경우
    메시지가 누락되었다고 판단합니다.
    
    Parameters:
    -----------
    kafka_df : pyspark.sql.DataFrame
        Kafka 메시지 DataFrame, 다음 칼럼을 포함해야 함:
        - topic: Kafka 토픽 (string)
        - partition: Kafka 파티션 번호 (int)
        - offset: Kafka 오프셋 (long)
    
    Returns:
    --------
    pyspark.sql.DataFrame
        누락된 오프셋을 식별하는 DataFrame, 비어있으면 누락 없음
    
    Notes:
    ------
    이 검증은 파티션 내 오프셋의 연속성만 검사합니다.
    파티션 간 메시지 순서는 보장되지 않으므로 별도 검증이 필요합니다.
    
    Examples:
    ---------
    >>> kafka_messages = spark.read.format("kafka").load(...)
    >>> missing_messages = validate_kafka_completeness(kafka_messages)
    >>> if missing_messages.count() > 0:
    >>>     print(f"누락된 메시지 발견: {missing_messages.count()}개")
    >>>     missing_messages.show()
    """
    # Kafka 오프셋 순서 확인
    window_spec = Window.partitionBy("topic", "partition").orderBy("offset")
    df_with_gaps = kafka_df.withColumn(
        "offset_diff", 
        col("offset") - lag("offset", 1).over(window_spec)
    )
    
    # 오프셋 간격이 1보다 큰 레코드 탐지
    return df_with_gaps.filter(
        (col("offset_diff").isNotNull()) & 
        (col("offset_diff") > 1)
    )

def validate_cdc_schema(df, expected_schema):
    """
    CDC 이벤트의 스키마가 예상된 스키마와 일치하는지 검증합니다.
    
    소스 시스템의 스키마 변경이나 CDC 구성 변경으로 인한 
    예상치 못한 스키마 변경을 감지합니다.
    
    Parameters:
    -----------
    df : pyspark.sql.DataFrame
        CDC 이벤트 DataFrame, Debezium과 같은 CDC 도구의 출력 형식을 따라야 함
    
    Returns:
    --------
    bool
        스키마가 예상과 일치하면 True, 그렇지 않으면 False 반환
    
    Notes:
    ------
    expected_schema 변수는 예상되는 CDC 스키마를 정의해야 합니다.
    이 함수는 새 필드 추가는 허용하지 않으므로, 유연한 스키마 검증이 필요한 경우
    스키마 호환성 검증 로직을 추가해야 합니다.
    
    Examples:
    ---------
    >>> cdc_events = spark.read.format("kafka").load(...)
    >>> parsed_events = cdc_events.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    >>> if not validate_cdc_schema(parsed_events):
    >>>     print("CDC 스키마가 예상과 다릅니다!")
    """
    actual_schema = df.schema
    return expected_schema == actual_schema