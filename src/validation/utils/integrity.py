from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, array_contains, lit, array


def reprocess_missing_data(spark: SparkSession, missing_keys: list, from_timestamp: int, cdc_schema: dict) -> dict:
    """
    누락되거나 불일치하는 데이터를 재처리하는 함수입니다.
    
    특정 시점부터 Kafka의 CDC 이벤트를 재처리하여 누락된 데이터나
    불일치하는 데이터를 복구합니다.
    
    Parameters:
    -----------
    spark : pyspark.sql.SparkSession
    missing_keys : list of dict
        재처리할 레코드의 키 목록. 각 딕셔너리는 기본 키 필드와 값 쌍을 포함.
        예: [{"id": 123}, {"id": 456}]
    from_timestamp : int or str
        재처리를 시작할 타임스탬프 (에포크 밀리초 또는 ISO 형식 문자열)
    cdc_schema : dict
        CDC 이벤트의 스키마 정의. JSON 형식으로 제공되어야 함.
        예: {"type": "struct", "fields": [{"name": "id", "type": "long"}, ...]}
    
    Returns:
    --------
    dict
        재처리 결과 요약:
        - processed_count: 처리된 이벤트 수
        - recovered_keys: 복구된 키 목록
        - failed_keys: 복구 실패한 키 목록
    
    Notes:
    ------
    이 함수는 대량의 CDC 이벤트를 재처리할 수 있으므로,
    성능 영향을 고려하여 필요한 키만 선택적으로 처리해야 합니다.
    또한, 중복 처리로 인한 부작용이 없도록 멱등성을 보장해야 합니다.
    
    Examples:
    ---------
    >>> # 특정 레코드에 대해 지난 24시간 데이터 재처리
    >>> from datetime import datetime, timedelta
    >>> yesterday = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)
    >>> result = reprocess_missing_data([{"id": 123, "id": 456}], yesterday)
    >>> print(f"처리 결과: {result['processed_count']}개 이벤트, {len(result['recovered_keys'])}개 키 복구")
    """
    # 1. Kafka에서 지정 시간부터 CDC 이벤트 재처리
    reprocess_df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "cdc_topic")
        .option("startingOffsets", """{"cdc_topic":{"0":earliest}}""")
        .load()
        .filter(col("timestamp") >= from_timestamp)
    )
    
    # 타임스탬프가 문자열인 경우 변환
    if isinstance(from_timestamp, str):
        from_timestamp = datetime.fromisoformat(from_timestamp).timestamp() * 1000
    
    # CDC 메시지 파싱
    parsed_events = reprocess_df.select(
        from_json(col("value").cast("string"), cdc_schema).alias("cdc"),
        col("timestamp").alias("kafka_timestamp")
    ).select("cdc.*", "kafka_timestamp")
    
    # 2. 필요한 키만 선택적으로 처리
    missing_key_values = [k.values() for k in missing_keys]
    
    # 기본 키 필드 추출 (첫 번째 딕셔너리의 키 사용)
    pk_fields = list(missing_keys[0].keys()) if missing_keys else []
    
    # 필터링된 CDC 이벤트
    filtered_events = parsed_events.filter(
        # 기본 키 필드의 값이 missing_keys에 포함된 경우만 선택
        array_contains(lit(missing_key_values), 
                     array(*[col(f"cdc.{pk}") for pk in pk_fields]))
    )
    
    # 3. 재처리 및 Iceberg에 병합
    def process_batch(batch_df, batch_id):
        if batch_df.count() > 0:
            # 이벤트 타입별로 데이터 분리
            inserts = batch_df.filter(col("op") == "c").select("after.*")
            updates = batch_df.filter(col("op") == "u").select("after.*")
            deletes = batch_df.filter(col("op") == "d").select("before.*")
            
            # Iceberg 테이블에 반영
            if inserts.count() > 0:
                inserts.write.format("iceberg").mode("append").save("path/to/iceberg_table")
            
            if updates.count() > 0:
                updates.createOrReplaceTempView("updates_view")
                spark.sql("""
                    MERGE INTO path/to/iceberg_table t
                    USING updates_view u
                    ON t.id = u.id
                    WHEN MATCHED THEN UPDATE SET *
                """)
            
            if deletes.count() > 0:
                deletes.createOrReplaceTempView("deletes_view")
                spark.sql("""
                    DELETE FROM path/to/iceberg_table t
                    WHERE EXISTS (
                        SELECT 1 FROM deletes_view d WHERE t.id = d.id
                    )
                """)
            
            # 처리된 키 추적
            processed_keys = batch_df.select(pk_fields).distinct().collect()
            return {
                "batch_id": batch_id,
                "processed_count": batch_df.count(),
                "processed_keys": [row.asDict() for row in processed_keys]
            }
        else:
            return {"batch_id": batch_id, "processed_count": 0, "processed_keys": []}
    
    # 스트리밍 쿼리 실행
    query_results = []
    
    streaming_query = (
        filtered_events.writeStream
        .foreachBatch(lambda df, id: query_results.append(process_batch(df, id)))
        .outputMode("update")
        .trigger(once=True)  # 한 번만 처리
        .start()
    )
    
    # 쿼리 완료 대기
    streaming_query.awaitTermination()
    
    # 결과 집계
    processed_keys = set()
    total_processed = 0
    
    for result in query_results:
        total_processed += result["processed_count"]
        for key in result["processed_keys"]:
            processed_keys.add(tuple(sorted(key.items())))
    
    # 복구되지 않은 키 식별
    missing_key_tuples = {tuple(sorted(k.items())) for k in missing_keys}
    recovered_keys = processed_keys.intersection(missing_key_tuples)
    failed_keys = missing_key_tuples - recovered_keys
    
    return {
        "processed_count": total_processed,
        "recovered_keys": [dict(k) for k in recovered_keys],
        "failed_keys": [dict(k) for k in failed_keys]
    }

def recover_data_integrity(spark: SparkSession, validation_results: DataFrame) -> dict:
    """
    데이터 무결성 검증 결과를 기반으로 복구 작업을 수행합니다.
    
    데이터 불일치가 발견된 경우, 소스 데이터베이스에서 직접 데이터를 추출하여
    Iceberg 테이블에 병합함으로써 데이터 정합성을 복구합니다.
    
    Parameters:
    -----------
    spark : pyspark.sql.SparkSession
    validation_results : pyspark.sql.DataFrame or dict
        데이터 검증 결과. 다음 정보를 포함해야 함:
        - 불일치 레코드의 기본 키 정보
        - 불일치 유형 (누락, 값 불일치 등)
        - 관련 테이블 정보
    
    Returns:
    --------
    dict
        복구 작업 결과 요약:
        - recovered_count: 복구된 레코드 수
        - failed_count: 복구 실패한 레코드 수
        - details: 각 레코드별 복구 상태
    
    Notes:
    ------
    이 함수는 데이터베이스 직접 접근 권한이 필요하며,
    대량의 불일치가 발생한 경우 소스 데이터베이스에 성능 영향을 줄 수 있습니다.
    
    Examples:
    ---------
    >>> # 검증 결과를 기반으로 복구 수행
    >>> mismatches = validate_cdc_to_iceberg(cdc_events, iceberg_table, ["id"])
    >>> if mismatches.count() > 0:
    >>>     recovery_result = recover_data_integrity(mismatches)
    >>>     print(f"복구 결과: {recovery_result['recovered_count']}개 성공, {recovery_result['failed_count']}개 실패")
    """
    # 불일치 유형 분석
    if isinstance(validation_results, DataFrame):
        # DataFrame인 경우
        mismatches = validation_results
        missing_in_iceberg = mismatches.filter(col("iceberg_id").isNull()).select("cdc.*")
        value_mismatches = mismatches.filter((col("iceberg_id").isNotNull()) & (col("cdc_id").isNotNull()))
        
        # 테이블 이름 추출
        table_name = mismatches.select("table_name").first()["table_name"]
        
        # 기본 키 칼럼 이름 추출
        pk_columns = [c for c in mismatches.columns if c.endswith("_pk")]
        
    else:
        # 딕셔너리인 경우
        missing_in_iceberg = validation_results.get("missing_in_iceberg", [])
        value_mismatches = validation_results.get("value_mismatches", [])
        table_name = validation_results.get("table_name", "unknown_table")
        pk_columns = validation_results.get("pk_columns", ["id"])
    
    # 복구해야 할 키 목록 준비
    if isinstance(missing_in_iceberg, DataFrame):
        missing_keys = [row.asDict() for row in missing_in_iceberg.select(pk_columns).collect()]
    else:
        missing_keys = missing_in_iceberg
    
    if isinstance(value_mismatches, DataFrame):
        mismatch_keys = [row.asDict() for row in value_mismatches.select(pk_columns).collect()]
    else:
        mismatch_keys = value_mismatches
    
    all_keys_to_recover = missing_keys + mismatch_keys
    
    # 중복 제거
    unique_keys_to_recover = []
    seen = set()
    for k in all_keys_to_recover:
        k_tuple = tuple(sorted(k.items()))
        if k_tuple not in seen:
            seen.add(k_tuple)
            unique_keys_to_recover.append(k)
    
    # 키가 없으면 조기 반환
    if not unique_keys_to_recover:
        return {
            "recovered_count": 0,
            "failed_count": 0,
            "details": "No records to recover"
        }
    
    # 1. 소스 데이터베이스에서 직접 추출
    # 키 목록으로 IN 절 구성
    pk_column = pk_columns[0]  # 단순화를 위해 첫 번째 PK만 사용
    key_values = [str(k[pk_column]) for k in unique_keys_to_recover]
    keys_str = ", ".join(key_values)
    
    query = f"(SELECT * FROM {table_name} WHERE {pk_column} IN ({keys_str})) AS t"
    
    missing_records = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host:port/db") \
        .option("dbtable", query) \
        .option("user", "username") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    # 2. Iceberg 테이블에 병합
    # 레코드가 없으면 조기 반환
    if missing_records.count() == 0:
        return {
            "recovered_count": 0,
            "failed_count": len(unique_keys_to_recover),
            "details": "No records found in source database"
        }
    
    try:
        # MERGE 문 사용하여 병합
        missing_records.createOrReplaceTempView("records_to_recover")
        
        merge_sql = f"""
        MERGE INTO iceberg.db.{table_name} t
        USING records_to_recover s
        ON t.{pk_column} = s.{pk_column}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        spark.sql(merge_sql)
        
        # 복구된 키 확인
        recovered_keys = missing_records.select(pk_column).collect()
        recovered_values = [row[pk_column] for row in recovered_keys]
        
        # 복구 실패한 키 식별
        failed_keys = [k for k in unique_keys_to_recover 
                       if k[pk_column] not in recovered_values]
        
        return {
            "recovered_count": len(recovered_values),
            "failed_count": len(failed_keys),
            "details": {
                "recovered_keys": recovered_values,
                "failed_keys": [k[pk_column] for k in failed_keys]
            }
        }
        
    except Exception as e:
        # 오류 발생 시
        return {
            "recovered_count": 0,
            "failed_count": len(unique_keys_to_recover),
            "error": str(e),
            "details": "Error during recovery process"
        }