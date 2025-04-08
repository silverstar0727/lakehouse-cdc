from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, desc, expr, collect_set, struct, reduce

def validate_cdc_to_iceberg(cdc_df: DataFrame, iceberg_df: DataFrame, pk_cols: list) -> DataFrame:
    """
    CDC 이벤트의 최종 상태와 Iceberg 테이블의 상태가 일치하는지 검증합니다.
    
    각 기본 키에 대해 가장 최근의 CDC 이벤트를 선택하고,
    이를 Iceberg 테이블의 현재 상태와 비교합니다.
    
    Parameters:
    -----------
    cdc_df : pyspark.sql.DataFrame
        처리된 CDC 이벤트 DataFrame, 다음 칼럼을 포함해야 함:
        - lsn: Log Sequence Number (long)
        - op: 작업 유형 (c/u/d)
        - 기본 키 칼럼들
        - 데이터 칼럼들
    iceberg_df : pyspark.sql.DataFrame
        Iceberg 테이블 DataFrame
    pk_cols : list of str
        기본 키 칼럼 목록
    
    Returns:
    --------
    pyspark.sql.DataFrame
        불일치하는 레코드를 포함하는 DataFrame
        비어있으면 모든 레코드가 일치함을 의미
    
    Notes:
    ------
    이 함수는 삭제된 레코드(op='d')를 Iceberg에서 찾지 않아야 한다는 로직을 포함합니다.
    
    Examples:
    ---------
    >>> cdc_events = spark.table("cdc_events_table")
    >>> iceberg_table = spark.read.format("iceberg").load("iceberg.db.table")
    >>> mismatches = validate_cdc_to_iceberg(cdc_events, iceberg_table, ["id"])
    >>> if mismatches.count() > 0:
    >>>     print(f"CDC와 Iceberg 간 불일치 발견: {mismatches.count()}개")
    """
    # CDC 최종 상태 집계 (최신 작업만 고려)
    latest_cdc = cdc_df.withColumn(
        "row_num", 
        row_number().over(
            Window.partitionBy(*pk_cols).orderBy(desc("lsn"))
        )
    ).filter(col("row_num") == 1).drop("row_num")
    
    # 삭제된 레코드 제외
    active_records = latest_cdc.filter(col("op") != "d")
    
    # 키 기준 조인 및 불일치 레코드 찾기
    comparison = active_records.join(
        iceberg_df, 
        pk_cols,
        "full_outer"
    )
    
    # 불일치 레코드 반환 (한쪽에만 존재하거나 값이 다른 레코드)
    mismatches = comparison.filter(
        # CDC에는 존재하지만 Iceberg에는 없는 경우
        col(f"iceberg_df.{pk_cols[0]}").isNull() |
        # Iceberg에는 존재하지만 CDC 최종 상태에는 없는 경우
        col(f"active_records.{pk_cols[0]}").isNull() |
        # 값이 다른 경우 (공통 칼럼 비교)
        expr(" OR ".join([f"active_records.{col_name} != iceberg_df.{col_name}" 
                         for col_name in set(active_records.columns) & set(iceberg_df.columns) 
                         if col_name not in pk_cols]))
    )
    
    return mismatches

def validate_transaction_integrity(spark: SparkSession,cdc_df: DataFrame, iceberg_df: DataFrame, tx_id_col: str, pk_cols: list) -> dict:
    """
    트랜잭션 단위로 CDC 이벤트가 Iceberg 테이블에 올바르게 반영되었는지 검증합니다.
    
    동일한 트랜잭션 ID를 가진 CDC 이벤트들이 모두 함께 처리되었는지 확인합니다.
    이를 통해 트랜잭션의 원자성 보장을 검증합니다.
    
    Parameters:
    -----------
    cdc_df : pyspark.sql.DataFrame
        트랜잭션 ID가 포함된 CDC 이벤트 DataFrame
    iceberg_df : pyspark.sql.DataFrame
        검증할 Iceberg 테이블 DataFrame
    tx_id_col : str
        CDC 이벤트의 트랜잭션 ID 칼럼 이름
    pk_cols : list of str
        기본 키 칼럼 목록
    
    Returns:
    --------
    dict
        검증 결과를 포함하는 딕셔너리:
        - 'integrity_maintained': 트랜잭션 무결성 유지 여부 (bool)
        - 'incomplete_transactions': 완전히 반영되지 않은 트랜잭션 수 (int)
        - 'details': 불완전 트랜잭션의 상세 정보 (DataFrame 또는 None)
    
    Notes:
    ------
    이 함수는 트랜잭션 특성에 따라 로직을 조정해야 할 수 있습니다.
    특히, 대규모 트랜잭션이나 복잡한 스키마 변경을 포함하는 트랜잭션의 경우
    추가적인 검증 로직이 필요할 수 있습니다.
    
    Examples:
    ---------
    >>> cdc_events = spark.table("cdc_events_table")
    >>> iceberg_table = spark.read.format("iceberg").load("iceberg.db.table")
    >>> tx_result = validate_transaction_integrity(
    >>>     cdc_events, 
    >>>     iceberg_table, 
    >>>     "txid", 
    >>>     ["id"]
    >>> )
    >>> if not tx_result['integrity_maintained']:
    >>>     print(f"트랜잭션 무결성 위반: {tx_result['incomplete_transactions']}개 트랜잭션")
    """
    # 트랜잭션별 작업 그룹화
    tx_groups = cdc_df.groupBy(tx_id_col).agg(
        collect_set("op").alias("operations"),
        collect_set(struct(*pk_cols)).alias("affected_keys")
    )
    
    # 트랜잭션별 영향 받은 키 목록
    affected_keys_by_tx = {}
    for row in tx_groups.collect():
        tx_id = row[tx_id_col]
        affected_keys_by_tx[tx_id] = [key.asDict() for key in row["affected_keys"]]
    
    # 트랜잭션별 Iceberg 반영 여부 검증
    incomplete_tx = []
    
    for tx_id, keys in affected_keys_by_tx.items():
        tx_operations = tx_groups.filter(col(tx_id_col) == tx_id).first()["operations"]
        
        # 트랜잭션 유형에 따른 검증 로직
        # 삭제 작업이 포함된 경우
        if "d" in tx_operations:
            for key_dict in keys:
                key_filters = [col(k) == v for k, v in key_dict.items()]
                key_exists = iceberg_df.filter(reduce(lambda x, y: x & y, key_filters)).count() > 0
                
                # 삭제된 레코드가 여전히 존재하면 불완전 트랜잭션
                if key_exists:
                    incomplete_tx.append({
                        "tx_id": tx_id, 
                        "key": key_dict, 
                        "reason": "deleted record still exists"
                    })
        
        # 생성/수정 작업이 포함된 경우
        if "c" in tx_operations or "u" in tx_operations:
            # 트랜잭션에 포함된 최신 CDC 상태
            latest_tx_state = cdc_df.filter(col(tx_id_col) == tx_id).withColumn(
                "row_num", 
                row_number().over(
                    Window.partitionBy(*pk_cols).orderBy(desc("lsn"))
                )
            ).filter(col("row_num") == 1).drop("row_num")
            
            # 삭제 작업이 아닌 레코드만 선택
            active_tx_records = latest_tx_state.filter(col("op") != "d")
            
            # Iceberg와 비교
            for key_dict in keys:
                key_filters = [col(k) == v for k, v in key_dict.items()]
                
                # CDC의 최신 상태
                cdc_record = active_tx_records.filter(reduce(lambda x, y: x & y, key_filters))
                
                # 생성/수정된 레코드가 있지만 Iceberg에 없는 경우
                if cdc_record.count() > 0 and iceberg_df.filter(reduce(lambda x, y: x & y, key_filters)).count() == 0:
                    incomplete_tx.append({
                        "tx_id": tx_id, 
                        "key": key_dict, 
                        "reason": "created/updated record missing"
                    })
    
    # 결과 생성
    validation_result = {
        "integrity_maintained": len(incomplete_tx) == 0,
        "incomplete_transactions": len(incomplete_tx),
        "details": spark.createDataFrame(incomplete_tx) if incomplete_tx else None
    }
    
    return validation_result