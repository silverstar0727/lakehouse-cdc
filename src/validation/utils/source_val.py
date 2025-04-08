from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import col, lag


def validate_cdc_sequence(df: DataFrame) -> bool:
    """
    CDC 이벤트의 LSN(Log Sequence Number) 순서를 검증하여 이벤트 누락 여부를 확인합니다.
    
    CDC 로그의 연속성을 검사하며, 연속되지 않은 LSN이 발견되면 데이터 누락을 의미합니다.
    테이블별로 그룹화하여 LSN 순서를 확인합니다.
    
    Parameters:
    -----------
    df : pyspark.sql.DataFrame
        검증할 CDC 이벤트 DataFrame, 다음 칼럼을 포함해야 함:
        - table: 테이블 이름 (string)
        - lsn: Log Sequence Number (long)
        - op: 작업 유형 (c=create, u=update, d=delete)
    
    Returns:
    --------
    bool
        모든 LSN이 연속적이면 True, 그렇지 않으면 False 반환
    
    Examples:
    ---------
    >>> cdc_events = spark.read.format("kafka").load(...)
    >>> is_sequence_valid = validate_cdc_sequence(cdc_events)
    >>> if not is_sequence_valid:
    >>>     print("CDC 이벤트 누락 발견!")
    """
    # LSN 순서 확인
    window_spec = Window.partitionBy("table").orderBy("lsn")
    df_with_prev = df.withColumn("prev_lsn", lag("lsn", 1).over(window_spec))
    
    # 누락된 LSN 탐지
    missing_lsns = df_with_prev.filter(
        (col("prev_lsn").isNotNull()) & 
        (col("lsn") - col("prev_lsn") > 1)
    )
    
    return missing_lsns.count() == 0

def deduplicate_cdc_events(df: DataFrame) -> DataFrame:
    """
    CDC 이벤트에서 중복된 이벤트를 제거합니다.
    
    네트워크 문제나 Kafka 재시도로 인해 CDC 이벤트가 중복 발생할 수 있습니다.
    이 함수는 LSN, 테이블, 작업 유형, 기본키를 기준으로 중복을 제거합니다.
    
    Parameters:
    -----------
    df : pyspark.sql.DataFrame
        중복이 포함될 수 있는 CDC 이벤트 DataFrame, 다음 칼럼을 포함해야 함:
        - lsn: Log Sequence Number (long)
        - table: 테이블 이름 (string)
        - operation: 작업 유형 (c/u/d)
        - primary_key: 레코드의 기본 키 값 (any type)
    
    Returns:
    --------
    pyspark.sql.DataFrame
        중복이 제거된 DataFrame
    
    Examples:
    ---------
    >>> cdc_events = spark.read.format("kafka").load(...)
    >>> deduplicated_events = deduplicate_cdc_events(cdc_events)
    >>> print(f"제거된 중복 이벤트 수: {cdc_events.count() - deduplicated_events.count()}")
    """
    # LSN과 operation_id로 중복 제거
    return df.dropDuplicates(["lsn", "table", "operation", "primary_key"])


