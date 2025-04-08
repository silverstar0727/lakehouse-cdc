import os
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, DoubleType, TimestampType, BooleanType
)
from pyiceberg.transforms import identity, day
from pyiceberg.partitioning import PartitionSpec

# REST 카탈로그 연결 설정
catalog_config = {
    "uri": "http://iceberg-rest-catalog-service.default.svc.cluster.local:8181",
    "warehouse": "s3a://iceberg-warehouse/",
    "s3.endpoint": "http://rook-ceph-rgw-my-store.rook-ceph.svc.cluster.local:80",
    "s3.access-key": os.environ.get("S3_ACCESS_KEY"),
    "s3.secret-key": os.environ.get("S3_SECRET_KEY"),
    "type": "rest"
}

# 카탈로그 로드
catalog = load_catalog("rest_catalog", **catalog_config)

# 소스 미러 테이블 정보
source_namespace = "mirror"
source_table_name = "users_mirror"
source_table = catalog.load_table(f"{source_namespace}.{source_table_name}")

# 대상 테이블 정보
target_namespace = "analytics"
target_table_name = "users_analytical"
full_target_name = f"{target_namespace}.{target_table_name}"

# 카탈로그에 네임스페이스가 없으면 생성
if target_namespace not in catalog.list_namespaces():
    catalog.create_namespace(target_namespace)

# 기존 테이블이 있는지 확인하고 있으면 삭제 (선택적)
if catalog.table_exists(full_target_name):
    catalog.drop_table(full_target_name)

# 새 스키마 정의 (소스 테이블에서 필요한 필드만 선택하거나 스키마 변경 가능)
# 여기서는 예시로 몇 가지 필드를 정의합니다
schema = Schema(
    NestedField.required(1, "user_id", LongType()),
    NestedField.required(2, "username", StringType()),
    NestedField.required(3, "email", StringType()),
    NestedField.optional(4, "created_at", TimestampType()),
    NestedField.optional(5, "last_login", TimestampType()),
    NestedField.optional(6, "is_active", BooleanType())
)

# 파티션 명세 정의 (예: created_at 필드로 일별 파티션)
partition_spec = PartitionSpec(
    day("created_at")
)

# 테이블 속성 정의
properties = {
    "description": "Analytical user table created from mirror table",
    "format-version": "2",  # Iceberg 형식 버전 2 사용
    "write.delete.mode": "merge-on-read",  # 삭제 모드 설정
    "write.update.mode": "merge-on-read",  # 업데이트 모드 설정
    "write.merge.mode": "merge-on-read"    # 병합 모드 설정
}

# 새 테이블 생성
new_table = catalog.create_table(
    identifier=full_target_name,
    schema=schema,
    partition_spec=partition_spec,
    properties=properties
)

# 테이블 데이터 로드 및 변환
def transform_and_load_data():
    # 소스 테이블에서 데이터 로드
    source_df = source_table.scan().to_pandas()
    
    # 필요한 변환 작업 수행
    # 예: 불필요한 컬럼 제거, 데이터 정제 등
    
    # 변환된 데이터를 새 테이블에 삽입
    with new_table.new_append() as append:
        append.append_pandas(source_df)
        append.commit()
    
    print(f"데이터가 {full_target_name} 테이블에 성공적으로 로드되었습니다.")

# 데이터 변환 및 로드 실행
transform_and_load_data()

# 더 복잡한 변환이 필요한 경우 Spark를 사용할 수도 있습니다
def transform_with_spark():
    from pyspark.sql import SparkSession
    
    # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("Iceberg Mirror to Target") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", catalog_config["uri"]) \
        .config("spark.sql.catalog.iceberg.warehouse", catalog_config["warehouse"]) \
        .config("spark.hadoop.fs.s3a.endpoint", catalog_config["s3.endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", catalog_config["s3.access-key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", catalog_config["s3.secret-key"]) \
        .getOrCreate()
    
    # 소스 테이블 로드
    source_df = spark.table(f"iceberg.{source_namespace}.{source_table_name}")
    
    # 필요한 변환 수행
    # 예: SQL 변환, 데이터 정제 등
    transformed_df = source_df.selectExpr(
        "user_id", 
        "username", 
        "email", 
        "created_at", 
        "last_login",
        "is_active"
    )
    
    # 변환된 데이터를 대상 테이블에 저장
    transformed_df.writeTo(f"iceberg.{target_namespace}.{target_table_name}") \
        .using("iceberg") \
        .append()
    
    print(f"Spark를 사용하여 데이터가 {full_target_name} 테이블에 성공적으로 로드되었습니다.")

# 필요에 따라 Spark 변환 실행
# transform_with_spark()