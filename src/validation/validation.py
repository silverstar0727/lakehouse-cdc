
import json
import requests
import logging
import os
import sys
from datetime import datetime, timedelta

import psycopg2

from confluent_kafka import Consumer, KafkaException

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_validation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("data_validation")

class DataValidation:
    def __init__(self, pg_config, kafka_config, iceberg_config):
        """
        Initialize class for data pipeline integrity validation
        
        Args:
            pg_config (dict): PostgreSQL connection information
            kafka_config (dict): Kafka connection information
            iceberg_config (dict): Iceberg/Spark connection information
        """
        self.pg_config = pg_config
        self.kafka_config = kafka_config
        self.iceberg_config = iceberg_config
        
        # Results repository
        self.validation_results = {}
        self.validation_metadata = {
            "start_time": None,
            "end_time": None,
            "status": None
        }
    
    def connect_postgres(self):
        """
        Connect to PostgreSQL database
        
        Returns:
            connection: PostgreSQL connection object
        """
        try:
            conn = psycopg2.connect(
                host=self.pg_config["host"],
                database=self.pg_config["database"],
                user=self.pg_config["user"],
                password=self.pg_config["password"],
                port=self.pg_config["port"]
            )
            return conn
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            raise
    
    def connect_spark(self) -> SparkSession:
        external_ip = self.iceberg_config["external_ip"]
        s3_access_key = self.iceberg_config["s3_access_key"]
        s3_secret_key = self.iceberg_config["s3_secret_key"]
        iceberg_warehouse_path = self.iceberg_config["iceberg_warehouse_path"]

        try:
            # Spark configuration
            conf = SparkConf()
            
            # Set up packages with AWS SDK v2
            conf.set("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," +
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2," +
                "org.apache.hadoop:hadoop-aws:3.3.4," +
                "software.amazon.awssdk:bundle:2.20.18," +
                "software.amazon.awssdk:s3:2.20.18," +
                "software.amazon.awssdk:url-connection-client:2.20.18," +
                "org.slf4j:slf4j-api:1.7.36," +
                "org.slf4j:slf4j-simple:1.7.36"
            )

            # Configure Iceberg
            conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            conf.set("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
            conf.set("spark.sql.catalog.iceberg.uri", f"http://iceberg-rest-catalog.{external_ip}.nip.io")
            
            # Configure Iceberg with S3 (AWS SDK v2)
            conf.set("spark.sql.catalog.iceberg.warehouse", iceberg_warehouse_path)
            conf.set("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            conf.set("spark.sql.catalog.iceberg.s3.endpoint", f"http://ceph.{external_ip}.nip.io")
            conf.set("spark.sql.catalog.iceberg.s3.access-key-id", s3_access_key)
            conf.set("spark.sql.catalog.iceberg.s3.secret-access-key", s3_secret_key)
            conf.set("spark.sql.catalog.iceberg.s3.path-style-access", "true")
            
            # Important: Explicit region setting for AWS SDK v2
            conf.set("spark.sql.catalog.iceberg.s3.region", "us-east-1")
            
            # Add region to Hadoop settings
            conf.set("spark.hadoop.fs.s3a.region", "us-east-1")
            
            # Set region as a system property (JVM level)
            os.environ["AWS_REGION"] = "us-east-1"
            
            # General Spark SQL settings
            conf.set("spark.sql.warehouse.dir", iceberg_warehouse_path)

            # S3/Ceph configuration (HDFS-compatible access)
            conf.set("spark.hadoop.fs.s3a.endpoint", f"http://ceph.{external_ip}.nip.io")
            conf.set("spark.hadoop.fs.s3a.access.key", s3_access_key)
            conf.set("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
            conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
            conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            
            # Set default file system explicitly
            conf.set("spark.hadoop.fs.defaultFS", iceberg_warehouse_path)
            
            # S3A performance settings
            conf.set("spark.hadoop.fs.s3a.connection.establish.timeout", "10000")
            conf.set("spark.hadoop.fs.s3a.connection.timeout", "300000")
            conf.set("spark.hadoop.fs.s3a.attempts.maximum", "20")
            conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
            
            # Explicitly disable signing for Ceph S3 compatibility
            conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            conf.set("spark.hadoop.fs.s3a.session.token", "")
            
            # AWS SDK v2 specific properties for S3 client
            conf.set("spark.sql.catalog.iceberg.s3.signing-enabled", "false")  # Disable signing for Ceph
            
            # SDK v2 setting: Use URL connection client
            conf.set("spark.sql.catalog.iceberg.s3.client.factory", "software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient$Builder")
            
            # Merge conflict strategy
            conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            conf.set("spark.sql.iceberg.handle-merge-cardinality-violation", "true")

            # Create Spark session
            spark = SparkSession.builder \
                .appName("Kafka Iceberg Streaming Integration") \
                .config(conf=conf) \
                .master("local[*]") \
                .getOrCreate()
            
            print(f"S3 connection info: {conf.get('spark.sql.catalog.iceberg.s3.endpoint')}")
            print(f"S3 access key: {conf.get('spark.sql.catalog.iceberg.s3.access-key-id')}")
            print(f"S3 secret key: {conf.get('spark.sql.catalog.iceberg.s3.secret-access-key')}")
            print(f"AWS region: {conf.get('spark.sql.catalog.iceberg.s3.region')}")
            print(f"S3 default filesystem: {conf.get('spark.hadoop.fs.defaultFS')}")
            print(f"Iceberg warehouse path: {conf.get('spark.sql.catalog.iceberg.warehouse')}")
            
            # S3 test
            try:
                spark.sql("SELECT 1").show()
                print("Spark session created successfully.")
                
                # Test Iceberg catalog connection
                spark.sql("SHOW NAMESPACES IN iceberg").show()
                print("Iceberg catalog connection successful.")

                # # Test Iceberg table access
                # spark.sql(f"SELECT * FROM {self.iceberg_config["table"]} LIMIT 1").show()
                # print("Iceberg table access successful.")
            except Exception as session_e:
                print(f"Error during Spark session test: {session_e}")
                
            return spark
        except Exception as e:
            print(f"Critical error: Failed to create Spark session: {e}")
            sys.exit(1)
            
    def connect_kafka(self):
        """
        Create Confluent Kafka Consumer object
        
        Returns:
            consumer: Configured Confluent Kafka Consumer object
        """
        try:
            # Confluent Kafka Consumer settings
            config = {
                'bootstrap.servers': self.kafka_config["bootstrap_servers"],
                'group.id': self.kafka_config.get("group_id", "data_validation_group"),
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            }
            
            # Merge additional settings if any
            for key, value in self.kafka_config.items():
                if key not in ['bootstrap_servers', 'group_id']:
                    config[key] = value
                    
            consumer = Consumer(config)
            return consumer
        except KafkaException as e:
            logger.error(f"Kafka connection failed: {e}")
            raise

    def validate_row_count(self, table_name, iceberg_table, timestamp_column="__ts_ms"):
        """
        Validate row count between source and target systems using timestamp_column
        """
        try:
            # PostgreSQL row count
            pg_conn = self.connect_postgres()
            pg_cursor = pg_conn.cursor()
            pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            pg_count = pg_cursor.fetchone()[0]
            pg_cursor.close()
            pg_conn.close()
            
            # Iceberg row count with improved filtering
            spark = self.connect_spark()
            
            iceberg_df = spark.read.format("iceberg").load(iceberg_table)
            iceberg_df.show(30)
            
            window_spec = Window.partitionBy("id").orderBy(F.col("__ts_ms").desc())

            latest_record_count = iceberg_df.withColumn("row_num", F.row_number().over(window_spec)) \
                .filter((F.col("row_num") == 1) & F.col("name").isNotNull()) \
                .drop("row_num").count()
            
            # 오차 범위 설정
            tolerance = 0.01  # 1% 허용 오차
            is_valid = abs(pg_count - latest_record_count) <= (tolerance * pg_count)
            
            return {
                "validation_type": "row_count",
                "source_row_count": pg_count,
                "target_row_count": latest_record_count,
                "is_valid": is_valid,
                "timestamp": datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Row count 검증 중 오류 발생: {e}")
            return {
                "validation_type": "row_count",
                "error": str(e),
                "is_valid": False,
                "timestamp": datetime.now().isoformat()
            }
        
    def validate_checksum(self, table_name, iceberg_table, key_columns=["id"], columns_to_check=None, timestamp_column="__ts_ms"):
        """
        PostgreSQL과 Iceberg 테이블 간의 데이터 일치성을 체크섬으로 검증합니다.
        
        Args:
            table_name (str): PostgreSQL 테이블 이름
            iceberg_table (str): Iceberg 테이블 경로
            key_columns (list): 레코드를 식별하는 키 컬럼 목록 (기본값: ["id"])
            columns_to_check (list): 체크섬을 계산할 컬럼 목록 (None인 경우 전체 컬럼 사용)
            timestamp_column (str): 타임스탬프 컬럼 이름 (기본값: "__ts_ms")
        
        Returns:
            dict: 검증 결과를 포함하는 딕셔너리
        """
        try:
            # Spark 연결
            spark = self.connect_spark()
            
            iceberg_df = spark.read.format("iceberg").load(iceberg_table)
            
            # 필요한 컬럼 목록 가져오기
            all_columns = iceberg_df.columns
            
            # 체크섬에 사용할 컬럼 결정 (명시적으로 지정하지 않으면 전체 컬럼 사용)
            if columns_to_check is None:
                # 타임스탬프 컬럼과 내부 관리 컬럼 제외
                columns_to_check = [col for col in all_columns 
                                if col != timestamp_column 
                                and not col.startswith("_") 
                                and col != "is_iceberg_deleted"
                                and col != "row_num"]
            
            # Iceberg에서 최신 레코드만 선택
            window_spec = Window.partitionBy("id").orderBy(F.col(timestamp_column).desc())
            
            latest_records = iceberg_df.withColumn("row_num", F.row_number().over(window_spec)) \
                .filter((F.col("row_num") == 1) & F.col("name").isNotNull()) \
                .drop("row_num")
            
            # Iceberg 체크섬 계산 - 각 컬럼에 접두사 추가로 고유하게 만듦
            checksum_expr = F.md5(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in columns_to_check]))
            iceberg_checksums = latest_records.select(*key_columns, checksum_expr.alias("iceberg_checksum"))
            
            # PostgreSQL 연결
            pg_conn = self.connect_postgres()
            pg_cursor = pg_conn.cursor()
            
            # PostgreSQL에서 체크섬 계산을 위한 쿼리 구성
            pg_columns_str = ", ".join([f"COALESCE({c}::text, 'NULL')" for c in columns_to_check])
            pg_key_columns_str = ", ".join(key_columns)
            
            pg_query = f"""
            SELECT {pg_key_columns_str}, 
                MD5(CONCAT_WS('|', {pg_columns_str})) as pg_checksum
            FROM {table_name}
            """
            
            pg_cursor.execute(pg_query)
            pg_results = pg_cursor.fetchall()
            pg_cursor.close()
            pg_conn.close()
            
            # PostgreSQL 결과를 Spark DataFrame으로 변환 - 이름도 다르게 지정
            pg_schema = StructType([
                StructField(key, StringType()) for key in key_columns
            ] + [StructField("pg_checksum", StringType())])
            
            pg_checksums = spark.createDataFrame(pg_results, schema=pg_schema)
            
            # Iceberg DataFrame과 PostgreSQL DataFrame 조인하여 체크섬 비교
            joined_df = iceberg_checksums.join(
                pg_checksums, 
                on=key_columns,
                how="full_outer"
            )
            
            # 불일치 레코드 찾기 - 이제 명확한 컬럼 이름 사용
            mismatch_df = joined_df.filter(
                (F.col("iceberg_checksum") != F.col("pg_checksum")) | 
                F.col("iceberg_checksum").isNull() | 
                F.col("pg_checksum").isNull()
            )
            
            mismatch_count = mismatch_df.count()
            total_count = joined_df.count()
            
            # 불일치 샘플 수집 (최대 10개)
            mismatch_samples = []
            if mismatch_count > 0:
                sample_rows = mismatch_df.limit(10).collect()
                for row in sample_rows:
                    sample_dict = {}
                    for key in key_columns:
                        sample_dict[key] = row[key]
                    sample_dict["iceberg_checksum"] = row["iceberg_checksum"]
                    sample_dict["pg_checksum"] = row["pg_checksum"]
                    mismatch_samples.append(sample_dict)
            
            # 결과 계산
            match_percentage = ((total_count - mismatch_count) / total_count * 100) if total_count > 0 else 0
            is_valid = match_percentage >= 99.9  # 99.9% 이상 일치하면 유효하다고 판단
            
            result = {
                "validation_type": "checksum",
                "table_name": table_name,
                "iceberg_path": iceberg_table,
                "total_records": total_count,
                "matching_records": total_count - mismatch_count,
                "mismatching_records": mismatch_count,
                "match_percentage": match_percentage,
                "mismatch_samples": mismatch_samples if mismatch_count > 0 else None,
                "columns_checked": columns_to_check,
                "is_valid": is_valid,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Checksum 검증 결과: {result}")
            return result
        
        except Exception as e:
            logger.error(f"Checksum 검증 중 오류 발생: {e}")
            return {
                "validation_type": "checksum",
                "error": str(e),
                "is_valid": False,
                "timestamp": datetime.now().isoformat()
            }
        
    def sample_data_validation(self, table_name, iceberg_table, sample_size=10, key_columns=["id"], columns_to_compare=None, timestamp_column="__ts_ms"):
        """
        PostgreSQL과 Iceberg 테이블에서 랜덤 샘플 n개를 추출하여 데이터를 직접 비교합니다.
        
        Args:
            table_name (str): PostgreSQL 테이블 이름
            iceberg_table (str): Iceberg 테이블 경로
            sample_size (int): 추출할 샘플 수 (기본값: 10)
            key_columns (list): 레코드를 식별하는 키 컬럼 목록 (기본값: ["id"])
            columns_to_compare (list): 비교할 컬럼 목록 (None인 경우 전체 컬럼 사용)
            timestamp_column (str): 타임스탬프 컬럼 이름 (기본값: "__ts_ms")
        
        Returns:
            dict: 검증 결과를 포함하는 딕셔너리
        """
        try:
            # Spark 연결
            spark = self.connect_spark()
            
            # Iceberg 테이블 로드
            iceberg_df = spark.read.format("iceberg").load(iceberg_table)
            
            # 필요한 컬럼 목록 가져오기
            all_columns = iceberg_df.columns
            
            # 비교할 컬럼 결정 (명시적으로 지정하지 않으면 전체 컬럼 사용)
            if columns_to_compare is None:
                # 타임스탬프 컬럼과 내부 관리 컬럼 제외
                columns_to_compare = [col for col in all_columns 
                                if col != timestamp_column 
                                and not col.startswith("_") 
                                and col != "is_iceberg_deleted"
                                and col != "row_num"]
            
            # Iceberg에서 최신 레코드만 선택
            window_spec = Window.partitionBy("id").orderBy(F.col(timestamp_column).desc())
            
            latest_records = iceberg_df.withColumn("row_num", F.row_number().over(window_spec)) \
                .filter((F.col("row_num") == 1) & F.col("name").isNotNull()) \
                .drop("row_num")
            
            # 랜덤 샘플 추출을 위한 키 목록 준비
            key_values_df = latest_records.select(*key_columns).distinct()
            
            # 샘플 크기가 실제 데이터 수보다 큰 경우 조정
            total_records = key_values_df.count()
            sample_size = min(sample_size, total_records)
            
            # 랜덤 샘플 추출 (PySpark의 sample 함수 사용)
            sampled_keys_df = key_values_df.sample(False, fraction=sample_size/total_records, seed=42)
            
            # 최소 샘플 수 보장
            if sampled_keys_df.count() < sample_size:
                # 부족한 만큼 랜덤 시드를 변경하여 추가 샘플링
                additional_samples_needed = sample_size - sampled_keys_df.count()
                additional_fraction = additional_samples_needed / total_records
                additional_keys_df = key_values_df.sample(False, fraction=additional_fraction, seed=43)
                sampled_keys_df = sampled_keys_df.union(additional_keys_df).distinct().limit(sample_size)
            
            # 최종 샘플 수 확인
            final_sample_size = sampled_keys_df.count()
            sampled_keys = sampled_keys_df.collect()
            
            # 키 값으로 조건 생성
            if len(key_columns) == 1:
                # 단일 키 컬럼인 경우
                key_name = key_columns[0]
                key_values = [row[key_name] for row in sampled_keys]
                
                # 문자열 타입 처리를 위한 인용부호 추가
                key_values_str = [f"'{val}'" if isinstance(val, str) else str(val) for val in key_values]
                id_condition = f"{key_name} IN ({', '.join(key_values_str)})"
            else:
                # 복합 키인 경우
                conditions = []
                for row in sampled_keys:
                    row_condition = " AND ".join([f"{key} = {repr(row[key])}" for key in key_columns])
                    conditions.append(f"({row_condition})")
                id_condition = " OR ".join(conditions)
            
            # Iceberg 샘플 추출
            iceberg_samples = latest_records.filter(id_condition).select(*key_columns, *columns_to_compare)
            
            # PostgreSQL 연결 및 샘플 추출
            pg_conn = self.connect_postgres()
            pg_cursor = pg_conn.cursor()
            
            # PG 에서 샘플 데이터 추출
            pg_columns_str = ", ".join(key_columns + columns_to_compare)
            pg_query = f"""
            SELECT {pg_columns_str}
            FROM {table_name}
            WHERE {id_condition}
            """
            
            pg_cursor.execute(pg_query)
            pg_results = pg_cursor.fetchall()
            
            # 컬럼 이름 가져오기
            pg_column_names = [desc[0] for desc in pg_cursor.description]
            pg_cursor.close()
            pg_conn.close()
            
            # 비교 결과 저장
            comparison_results = []
            mismatched_fields = {}
            
            # PG 결과를 딕셔너리로 변환
            pg_data_by_key = {}
            for row in pg_results:
                # 딕셔너리로 변환
                row_dict = dict(zip(pg_column_names, row))
                
                # 키 생성
                key_tuple = tuple(row_dict[key] for key in key_columns)
                pg_data_by_key[key_tuple] = row_dict
            
            # Iceberg 샘플 비교
            for iceberg_row in iceberg_samples.collect():
                # 키 생성
                key_tuple = tuple(iceberg_row[key] for key in key_columns)
                
                # PG에 같은 키를 가진 레코드가 있는지 확인
                if key_tuple in pg_data_by_key:
                    pg_row = pg_data_by_key[key_tuple]
                    
                    # 레코드 비교 결과
                    record_result = {
                        "keys": {key: iceberg_row[key] for key in key_columns},
                        "fields": {}
                    }
                    
                    all_match = True
                    
                    # 각 필드 비교
                    for field in columns_to_compare:
                        iceberg_value = iceberg_row[field]
                        pg_value = pg_row.get(field)
                        
                        # 값 비교 (None/NULL 처리)
                        values_match = (
                            (iceberg_value == pg_value) or 
                            (iceberg_value is None and pg_value is None)
                        )
                        
                        record_result["fields"][field] = {
                            "iceberg_value": iceberg_value,
                            "pg_value": pg_value,
                            "match": values_match
                        }
                        
                        if not values_match:
                            all_match = False
                            # 불일치 필드 카운트
                            mismatched_fields[field] = mismatched_fields.get(field, 0) + 1
                    
                    record_result["all_match"] = all_match
                    comparison_results.append(record_result)
                else:
                    # PG에 존재하지 않는 레코드
                    comparison_results.append({
                        "keys": {key: iceberg_row[key] for key in key_columns},
                        "exists_in_pg": False,
                        "all_match": False
                    })
            
            # PG에만 존재하는 키 확인
            iceberg_keys = set(tuple(row[key] for key in key_columns) for row in iceberg_samples.collect())
            for key_tuple, pg_row in pg_data_by_key.items():
                if key_tuple not in iceberg_keys:
                    # Iceberg에 존재하지 않는 레코드
                    comparison_results.append({
                        "keys": {key: pg_row[key] for key in key_columns},
                        "exists_in_iceberg": False,
                        "all_match": False
                    })
            
            # 일치율 계산
            matching_records = sum(1 for record in comparison_results if record.get("all_match", False))
            total_compared = len(comparison_results)
            match_percentage = (matching_records / total_compared * 100) if total_compared > 0 else 0
            
            # 결과 구성
            result = {
                "validation_type": "sample_comparison",
                "table_name": table_name,
                "iceberg_path": iceberg_table,
                "sample_size": total_compared,
                "matching_records": matching_records,
                "mismatching_records": total_compared - matching_records,
                "match_percentage": match_percentage,
                "mismatched_fields": mismatched_fields,
                "sample_results": comparison_results,
                "is_valid": match_percentage == 100,  # 모든 샘플이 일치해야 유효
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"샘플 비교 검증 결과: 일치율 {match_percentage:.2f}% ({matching_records}/{total_compared})")
            return result
        
        except Exception as e:
            logger.error(f"샘플 비교 검증 중 오류 발생: {e}")
            return {
                "validation_type": "sample_comparison",
                "error": str(e),
                "is_valid": False,
                "timestamp": datetime.now().isoformat()
            }
        
if __name__ == "__main__":    
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Data validation tool')
    parser.add_argument('--test', choices=[
        'row_count', 'checksum', 'sample_data', 'replication_lag', 
        'combined_lag', 'connector_status', 'iceberg_health', 'validation_suite'
    ], required=True, help='Validation test to run')

    parser.add_argument('--pg-host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--pg-port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--pg-db', default='postgres', help='PostgreSQL database')
    parser.add_argument('--pg-user', default='postgres', help='PostgreSQL user')
    parser.add_argument('--pg-password', default='postgres', help='PostgreSQL password')
    parser.add_argument('--pg-table', help='PostgreSQL table name')

    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--kafka-topic', help='Kafka topic')
    parser.add_argument('--kafka-group', default='validation-group', help='Kafka consumer group')

    parser.add_argument('--iceberg-warehouse', default='s3a://warehouse/', help='Iceberg warehouse path')
    parser.add_argument('--iceberg-table', help='Iceberg table name (catalog.database.table)')
    parser.add_argument('--s3-access-key', default='minioadmin', help='S3/Minio access key')
    parser.add_argument('--s3-secret-key', default='minioadmin', help='S3/Minio secret key')
    parser.add_argument('--external-ip', default='localhost', help='External IP for service access')

    parser.add_argument('--debezium-url', default='http://localhost:8083', help='Debezium REST API URL')
    parser.add_argument('--connector-name', help='Debezium connector name')
    parser.add_argument('--kafka-connect-url', default='http://localhost:8083', help='Kafka Connect REST API URL')

    parser.add_argument('--timestamp-column', help='Timestamp column')
    parser.add_argument('--columns', help='Columns to use for checksum validation (comma-separated)')
    parser.add_argument('--sample-size', type=int, default=1000, help='Sample size')

    args = parser.parse_args()

    # Create configuration dictionaries
    pg_config = {
        "host": args.pg_host,
        "port": args.pg_port,
        "database": args.pg_db,
        "user": args.pg_user,
        "password": args.pg_password,
        "table": args.pg_table
    }

    kafka_config = {
        "bootstrap_servers": args.kafka_servers,
        "group_id": args.kafka_group
    }

    iceberg_config = {
        "iceberg_warehouse_path": args.iceberg_warehouse,
        "external_ip": args.external_ip,
        "s3_access_key": args.s3_access_key,
        "s3_secret_key": args.s3_secret_key,
        "table": args.iceberg_table
    }

    # Create DataValidation instance
    validator = DataValidation(pg_config, kafka_config, iceberg_config)
    
    # Run selected test
    if args.test == 'row_count':
        if not pg_config["table"] or not args.iceberg_table:
            print("Error: Both PostgreSQL table and Iceberg table are required.")
            sys.exit(1)
        
        result = validator.validate_row_count(args.pg_table, args.iceberg_table, args.timestamp_column)
        print(f"Row count validation result: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'checksum':
        if not args.pg_table or not args.iceberg_table or not args.columns:
            print("Error: PostgreSQL table, Iceberg table, and column list are required.")
            sys.exit(1)
        
        columns = [col.strip() for col in args.columns.split(',')]
        result = validator.validate_checksum(table_name=args.pg_table, iceberg_table=args.iceberg_table, columns_to_check=columns)
        print(f"Checksum validation result: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'sample_data':
        if not args.pg_table or not args.iceberg_table:
            print("Error: Both PostgreSQL table and Iceberg table are required.")
            sys.exit(1)
        
        columns = [col.strip() for col in args.columns.split(',')]
        result = validator.sample_data_validation(table_name=args.pg_table, iceberg_table=args.iceberg_table, sample_size=args.sample_size, columns_to_compare=columns)
        print(f"Data sampling validation result: {json.dumps(result, indent=2, default=str)}")