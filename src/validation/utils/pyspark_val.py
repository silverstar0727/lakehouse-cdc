from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat_ws

from typing import List, Optional

def validate_record_counts(source_df: DataFrame, target_df: DataFrame, group_by_cols : Optional[List[str]] = None) -> bool:
    """
    소스 DataFrame과 타겟 DataFrame 간의 레코드 수가 일치하는지 검증합니다.
    
    Parameters:
    -----------
    source_df : pyspark.sql.DataFrame
        소스 데이터를 포함하는 DataFrame
    target_df : pyspark.sql.DataFrame
        대상 데이터를 포함하는 DataFrame
    group_by_cols : list of str, optional
        그룹별로 레코드 수를 비교할 칼럼 목록
        지정하지 않으면 전체 레코드 수만 비교
    
    Returns:
    --------
    bool
        레코드 수가 일치하면 True, 그렇지 않으면 False 반환
        
    Notes:
    ------
    그룹별 비교를 수행하는 경우(group_by_cols 지정 시) 성능에 영향을 줄 수 있으므로,
    데이터 볼륨에 따라 적절한 방식을 선택해야 합니다.
    
    Examples:
    ---------
    >>> # 전체 레코드 수 비교
    >>> is_count_valid = validate_record_counts(cdc_df, iceberg_df)
    >>> 
    >>> # 날짜별 레코드 수 비교
    >>> is_daily_count_valid = validate_record_counts(cdc_df, iceberg_df, ["date"])
    """
    if group_by_cols:
        source_counts = source_df.groupBy(*group_by_cols).count()
        target_counts = target_df.groupBy(*group_by_cols).count()
        
        # 동일한 키에 대해 레코드 수 비교
        diff = source_counts.join(
            target_counts,
            group_by_cols,
            "full_outer"
        ).filter(
            (col("count") != col("count"))
        )
        
        return diff.count() == 0
    else:
        # 전체 레코드 수 비교
        return source_df.count() == target_df.count()
    
def validate_content_checksum(source_df: DataFrame, target_df: DataFrame, key_cols: List[str], value_cols: List[str]) -> DataFrame:
    """
    소스와 타겟 DataFrame 간의 데이터 내용이 일치하는지 체크섬을 통해 검증합니다.
    
    지정된 값 칼럼들의 내용을 연결하여 SHA-256 해시로 체크섬을 계산하고,
    키 칼럼으로 조인하여 체크섬이 일치하는지 검사합니다.
    
    Parameters:
    -----------
    source_df : pyspark.sql.DataFrame
        소스 데이터를 포함하는 DataFrame
    target_df : pyspark.sql.DataFrame
        대상 데이터를 포함하는 DataFrame
    key_cols : list of str
        소스와 타겟을 연결할 키 칼럼 목록 (예: 기본 키)
    value_cols : list of str
        내용 비교에 사용할 값 칼럼 목록
    
    Returns:
    --------
    pyspark.sql.DataFrame
        체크섬이 일치하지 않는 레코드를 포함하는 DataFrame
        비어있으면 모든 레코드가 일치함을 의미
    
    Notes:
    ------
    이 함수는 데이터 유형에 관계없이 문자열로 변환하여 비교합니다.
    따라서 날짜/시간과 같은 특수 데이터 유형에 대해서는 추가 처리가 필요할 수 있습니다.
    
    Examples:
    ---------
    >>> # 사용자 테이블의 데이터 내용 비교
    >>> mismatched = validate_content_checksum(
    >>>     source_df, 
    >>>     target_df, 
    >>>     key_cols=["user_id"], 
    >>>     value_cols=["name", "email", "status"]
    >>> )
    >>> if mismatched.count() > 0:
    >>>     print(f"불일치 레코드 수: {mismatched.count()}")
    >>>     mismatched.show()
    """
    # 검증에 사용할 필드들의 체크섬 계산
    source_with_checksum = source_df.withColumn(
        "checksum", 
        sha2(concat_ws("|", *[col(c) for c in value_cols]), 256)
    )
    
    target_with_checksum = target_df.withColumn(
        "checksum", 
        sha2(concat_ws("|", *[col(c) for c in value_cols]), 256)
    )
    
    # 체크섬이 다른 레코드 찾기
    diff = source_with_checksum.join(
        target_with_checksum,
        key_cols,
        "inner"
    ).filter(
        col("source_with_checksum.checksum") != col("target_with_checksum.checksum")
    )
    
    return diff