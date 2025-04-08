import psycopg2
from datetime import datetime, timedelta
import json
import requests
import statistics
import sys
from confluent_kafka import Consumer, KafkaException, KafkaError
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, count, min, max, avg, stddev, lit, when, isnan, isnull
import logging
import os

# 로깅 설정
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
        데이터 파이프라인 무결성 검증을 위한 클래스 초기화
        
        Args:
            pg_config (dict): PostgreSQL 연결 정보
            kafka_config (dict): Kafka 연결 정보
            iceberg_config (dict): Iceberg/Spark 연결 정보
        """
        self.pg_config = pg_config
        self.kafka_config = kafka_config
        self.iceberg_config = iceberg_config
        
        # 결과 저장소
        self.validation_results = {}
        self.validation_metadata = {
            "start_time": None,
            "end_time": None,
            "status": None
        }
    
    def connect_postgres(self):
        """
        PostgreSQL 데이터베이스에 연결
        
        Returns:
            connection: PostgreSQL 연결 객체
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
            logger.error(f"PostgreSQL 연결 실패: {e}")
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
            
            # 중요: AWS SDK v2에 대한 명시적 리전 설정
            conf.set("spark.sql.catalog.iceberg.s3.region", "us-east-1")
            
            # Hadoop 설정에도 리전 추가
            conf.set("spark.hadoop.fs.s3a.region", "us-east-1")
            
            # 시스템 프로퍼티로 리전 설정 (JVM 레벨)
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
            
            # SDK v2 설정: URL 접속 클라이언트 사용
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
            except Exception as session_e:
                print(f"Error during Spark session test: {session_e}")
                
            return spark
        except Exception as e:
            print(f"Critical error: Failed to create Spark session: {e}")
            sys.exit(1)
            
    def connect_kafka(self):
        """
        Confluent Kafka Consumer 객체 생성
        
        Returns:
            consumer: 구성된 Confluent Kafka Consumer 객체
        """
        try:
            # Confluent Kafka Consumer 설정
            config = {
                'bootstrap.servers': self.kafka_config["bootstrap_servers"],
                'group.id': self.kafka_config.get("group_id", "data_validation_group"),
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            }
            
            # 추가 설정이 있으면 병합
            for key, value in self.kafka_config.items():
                if key not in ['bootstrap_servers', 'group_id']:
                    config[key] = value
                    
            consumer = Consumer(config)
            return consumer
        except KafkaException as e:
            logger.error(f"Kafka 연결 실패: {e}")
            raise

    def validate_row_count(self, table_name, iceberg_table):
        """
        소스 시스템과 타겟 시스템 간 행 수 비교 검증
        
        Args:
            table_name (str): PostgreSQL 테이블 이름
            iceberg_table (str): Iceberg 테이블 이름(catalog.database.table 형식)
            
        Returns:
            dict: 검증 결과 및 메타데이터
        """
        try:
            # PostgreSQL 행 수 조회
            pg_conn = self.connect_postgres()
            pg_cursor = pg_conn.cursor()
            pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            pg_count = pg_cursor.fetchone()[0]
            pg_cursor.close()
            pg_conn.close()
            
            # Iceberg 행 수 조회
            spark = self.connect_spark()
            iceberg_count = spark.read.format("iceberg").load(iceberg_table).count()
            
            # 결과 계산
            count_diff = abs(pg_count - iceberg_count)
            count_diff_pct = (count_diff / pg_count * 100) if pg_count > 0 else 0
            is_valid = count_diff_pct <= 0.1  # 0.1% 이내 차이는 허용
            
            result = {
                "validation_type": "row_count",
                "source_count": pg_count,
                "target_count": iceberg_count,
                "difference": count_diff,
                "difference_percentage": count_diff_pct,
                "is_valid": is_valid,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"행 수 검증 결과: {result}")
            return result
        
        except Exception as e:
            logger.error(f"행 수 검증 중 오류 발생: {e}")
            return {
                "validation_type": "row_count",
                "error": str(e),
                "is_valid": False,
                "timestamp": datetime.now().isoformat()
            }

    def validate_checksum(self, table_name, columns, iceberg_table):
        """
        체크섬을 이용한 데이터 내용 검증
        
        Args:
            table_name (str): PostgreSQL 테이블 이름
            columns (list): 체크섬 계산에 사용할 컬럼 목록
            iceberg_table (str): Iceberg 테이블 이름
            
        Returns:
            dict: 검증 결과 및 메타데이터
        """
        try:
            # PostgreSQL 체크섬 계산
            pg_conn = self.connect_postgres()
            pg_cursor = pg_conn.cursor()
            
            columns_concat = "||'#'||".join([f"COALESCE(CAST({col} AS VARCHAR), '')" for col in columns])
            pg_cursor.execute(f"SELECT MD5(STRING_AGG({columns_concat}, ',')) FROM {table_name}")
            pg_checksum = pg_cursor.fetchone()[0]
            pg_cursor.close()
            pg_conn.close()
            
            # Iceberg 체크섬 계산 (Spark SQL 사용)
            spark = self.connect_spark()
            
            # 컬럼을 문자열로 변환하고 결합하는 표현식 생성
            concat_expr = "concat_ws('#', " + ", ".join([f"coalesce(cast({col} as string), '')" for col in columns]) + ")"
            
            # Spark DataFrame을 생성하고 체크섬 계산
            df = spark.read.format("iceberg").load(iceberg_table)
            
            from pyspark.sql.functions import expr, concat_ws, hash, collect_list, md5
            
            # 모든 행을 하나의 문자열로 결합하여 MD5 해시 계산
            iceberg_checksum = df.selectExpr(concat_expr).orderBy(concat_expr).select(
                md5(concat_ws(",", collect_list(concat_expr)))).collect()[0][0]
            
            # 결과 비교
            is_valid = pg_checksum == iceberg_checksum
            
            result = {
                "validation_type": "checksum",
                "source_checksum": pg_checksum,
                "target_checksum": iceberg_checksum,
                "is_valid": is_valid,
                "columns_validated": columns,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"체크섬 검증 결과: {result}")
            return result
            
        except Exception as e:
            logger.error(f"체크섬 검증 중 오류 발생: {e}")
            return {
                "validation_type": "checksum",
                "error": str(e),
                "is_valid": False,
                "timestamp": datetime.now().isoformat()
            }

    def sample_data_validation(self, table_name, iceberg_table, sample_size=1000):
        """
        데이터 샘플링을 통한 소스와 타겟 시스템의 데이터 일치 검증
        
        Args:
            table_name (str): PostgreSQL 테이블 이름
            iceberg_table (str): Iceberg 테이블 이름
            sample_size (int): 샘플링할 레코드 수
            
        Returns:
            dict: 샘플링 검증 결과
        """
        try:
            # PostgreSQL에서 랜덤 샘플 추출
            pg_conn = self.connect_postgres()
            pg_cursor = pg_conn.cursor()
            
            # 키 컬럼 식별 (주요 키 또는 인덱스)
            pg_cursor.execute(f"""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary
            """)
            key_columns = [row[0] for row in pg_cursor.fetchall()]
            
            if not key_columns:
                logger.warning(f"테이블 {table_name}의 기본 키를 찾을 수 없습니다. 첫 번째 컬럼을 사용합니다.")
                pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position LIMIT 1")
                key_columns = [pg_cursor.fetchone()[0]]
            
            # 모든 컬럼 가져오기
            pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            all_columns = [row[0] for row in pg_cursor.fetchall()]
            
            # 랜덤 샘플 쿼리 실행
            columns_str = ", ".join(all_columns)
            key_columns_str = ", ".join(key_columns)
            
            pg_cursor.execute(f"SELECT {columns_str} FROM {table_name} ORDER BY RANDOM() LIMIT {sample_size}")
            pg_samples = pg_cursor.fetchall()
            
            # 키 값 추출
            key_values = []
            key_indices = [all_columns.index(col) for col in key_columns]
            
            for row in pg_samples:
                key_dict = {key_columns[i]: row[key_indices[i]] for i in range(len(key_columns))}
                key_values.append(key_dict)
            
            pg_cursor.close()
            pg_conn.close()
            
            # Iceberg에서 동일한 키를 가진 레코드 검색
            spark = self.connect_spark()
            iceberg_df = spark.read.format("iceberg").load(iceberg_table)
            
            match_count = 0
            mismatch_details = []
            
            for key_dict in key_values:
                # 키 조건 생성
                filter_conditions = []
                for col, val in key_dict.items():
                    if val is None:
                        filter_conditions.append(f"{col} IS NULL")
                    else:
                        filter_conditions.append(f"{col} = '{val}'")
                
                filter_expr = " AND ".join(filter_conditions)
                
                # 일치하는 레코드 검색
                matching_rows = iceberg_df.filter(filter_expr).collect()
                
                if matching_rows and len(matching_rows) == 1:
                    match_count += 1
                else:
                    mismatch_details.append({
                        "key": key_dict,
                        "found_in_target": len(matching_rows) > 0,
                        "multiple_matches": len(matching_rows) > 1
                    })
            
            match_rate = match_count / len(key_values) if key_values else 0
            
            result = {
                "validation_type": "data_sampling",
                "sample_size": len(pg_samples),
                "matched_records": match_count,
                "match_rate": match_rate,
                "is_valid": match_rate >= 0.99,  # 99% 이상 일치 시 유효
                "mismatch_count": len(mismatch_details),
                "timestamp": datetime.now().isoformat()
            }
            
            if len(mismatch_details) > 0:
                result["mismatch_examples"] = mismatch_details[:5]  # 처음 5개만 예시로 포함
            
            logger.info(f"데이터 샘플링 검증 결과: {result}")
            return result
            
        except Exception as e:
            logger.error(f"데이터 샘플링 검증 중 오류 발생: {e}")
            return {
                "validation_type": "data_sampling",
                "error": str(e),
                "is_valid": False,
                "timestamp": datetime.now().isoformat()
            }
        
    def measure_replication_lag(self, debezium_url, connector_name):
        """
        복제 지연(Replication Lag) 측정 - Debezium 커넥터의 복제 지연 시간 확인
        
        Args:
            debezium_url (str): Debezium REST API URL
            connector_name (str): Debezium 커넥터 이름
            
        Returns:
            dict: 복제 지연 측정 결과
        """
        try:
            # Debezium 커넥터 상태 확인 API 호출
            response = requests.get(f"{debezium_url}/connectors/{connector_name}/status")
            response.raise_for_status()
            
            connector_status = response.json()
            
            # 커넥터가 실행 중인지 확인
            if connector_status.get("connector", {}).get("state") != "RUNNING":
                return {
                    "validation_type": "replication_lag",
                    "error": f"Connector is not running. Current state: {connector_status.get('connector', {}).get('state')}",
                    "is_valid": False,
                    "timestamp": datetime.now().isoformat()
                }
            
            # Debezium 커넥터의 복제 지연 메트릭 확인
            response = requests.get(f"{debezium_url}/connectors/{connector_name}/metrics")
            response.raise_for_status()
            
            metrics = response.json()
            
            # 복제 지연 관련 메트릭 추출
            lag_metrics = {}
            
            if "tasks" in metrics:
                for task in metrics["tasks"]:
                    if "metrics" in task and "source-record-lag" in task["metrics"]:
                        lag_metrics["source_record_lag"] = task["metrics"]["source-record-lag"]
                    if "metrics" in task and "lag-in-millis" in task["metrics"]:
                        lag_metrics["lag_millis"] = task["metrics"]["lag-in-millis"]
            
            is_acceptable_lag = True
            if "lag_millis" in lag_metrics and lag_metrics["lag_millis"] > 60000:  # 1분 이상 지연은 경고
                is_acceptable_lag = False
            
            result = {
                "validation_type": "replication_lag",
                "connector_name": connector_name,
                "lag_metrics": lag_metrics,
                "is_acceptable": is_acceptable_lag,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"복제 지연 측정 결과: {result}")
            return result
            
        except Exception as e:
            logger.error(f"복제 지연 측정 중 오류 발생: {e}")
            return {
                "validation_type": "replication_lag",
                "error": str(e),
                "is_acceptable": False,
                "timestamp": datetime.now().isoformat()
            }
    
    def measure_combined_lag(self, table_name, iceberg_table, event_time_column):
        """
        전체 파이프라인 지연(Combined Lag) 측정 - 소스 생성 시점부터 타겟 적재까지의 총 지연 시간
        
        Args:
            table_name (str): PostgreSQL 테이블 이름
            iceberg_table (str): Iceberg 테이블 이름
            event_time_column (str): 이벤트 생성 시간을 저장하는 컬럼 이름
            
        Returns:
            dict: 전체 지연 측정 결과
        """
        try:
            # 현재 시간 기준 최근 30분 내의 데이터만 조회
            thirty_mins_ago = datetime.now() - timedelta(minutes=30)
            
            # PostgreSQL에서 최근 데이터의 이벤트 시간과 시스템 시간 조회
            pg_conn = self.connect_postgres()
            pg_cursor = pg_conn.cursor()
            
            pg_cursor.execute(f"""
                SELECT 
                    {event_time_column}, 
                    NOW() AS system_time
                FROM 
                    {table_name}
                WHERE 
                    {event_time_column} > %s
                ORDER BY 
                    {event_time_column} DESC
                LIMIT 100
            """, (thirty_mins_ago,))
            
            pg_times = pg_cursor.fetchall()
            pg_cursor.close()
            pg_conn.close()
            
            # Iceberg에서 동일 데이터 조회
            spark = self.connect_spark()
            
            from pyspark.sql.functions import current_timestamp, col
            
            # Spark SQL을 사용하여 최근 데이터 조회
            iceberg_df = spark.read.format("iceberg").load(iceberg_table)
            iceberg_times = iceberg_df.filter(col(event_time_column) > thirty_mins_ago.isoformat()) \
                                     .select(event_time_column, current_timestamp().alias("system_time")) \
                                     .orderBy(col(event_time_column).desc()) \
                                     .limit(100) \
                                     .collect()
            
            # 소스 생성 시간과 타겟 적재 시간 간의 지연 계산
            if pg_times and iceberg_times:
                # 평균 지연 시간 계산 (초 단위)
                pg_event_times = [row[0] for row in pg_times]
                iceberg_event_times = [row[0] for row in iceberg_times]
                
                # 소스와 타겟 모두에 있는 이벤트 시간 찾기
                common_times = set(pg_event_times).intersection(set(iceberg_event_times))
                
                if common_times:
                    # 각 이벤트의 시스템 시간 매핑
                    pg_time_map = {row[0]: row[1] for row in pg_times if row[0] in common_times}
                    iceberg_time_map = {row[0]: row[1] for row in iceberg_times if row[0] in common_times}
                    
                    # 지연 시간 계산
                    lag_times = []
                    for event_time in common_times:
                        # 이벤트가 타겟에 기록된 시간 - 소스에 기록된 시간
                        lag_seconds = (iceberg_time_map[event_time] - pg_time_map[event_time]).total_seconds()
                        lag_times.append(lag_seconds)
                    
                    avg_lag = sum(lag_times) / len(lag_times)
                    min_lag = min(lag_times)
                    max_lag = max(lag_times)
                    
                    # 5분(300초) 이내 지연은 허용
                    is_acceptable = avg_lag <= 300
                    
                    result = {
                        "validation_type": "combined_lag",
                        "avg_lag_seconds": avg_lag,
                        "min_lag_seconds": min_lag,
                        "max_lag_seconds": max_lag,
                        "samples_count": len(common_times),
                        "is_acceptable": is_acceptable,
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    result = {
                        "validation_type": "combined_lag",
                        "error": "No common event times found in source and target systems",
                        "is_acceptable": False,
                        "timestamp": datetime.now().isoformat()
                    }
            else:
                result = {
                    "validation_type": "combined_lag",
                    "error": "Insufficient recent data for lag calculation",
                    "is_acceptable": False,
                    "timestamp": datetime.now().isoformat()
                }
            
            logger.info(f"전체 지연 측정 결과: {result}")
            return result
            
        except Exception as e:
            logger.error(f"전체 지연 측정 중 오류 발생: {e}")
            return {
                "validation_type": "combined_lag",
                "error": str(e),
                "is_acceptable": False,
                "timestamp": datetime.now().isoformat()
            }
        
    def check_connector_status(self, debezium_url, kafka_connect_url):
        """
        커넥터 상태 확인 - Debezium과 Kafka Connect 커넥터의 상태 모니터링
        
        Args:
            debezium_url (str): Debezium REST API URL
            kafka_connect_url (str): Kafka Connect REST API URL
            
        Returns:
            dict: 커넥터 상태 확인 결과
        """
        try:
            # Debezium 커넥터 상태 확인
            debezium_status = {}
            try:
                response = requests.get(f"{debezium_url}/connectors")
                response.raise_for_status()
                
                debezium_connectors = response.json()
                
                for connector in debezium_connectors:
                    conn_response = requests.get(f"{debezium_url}/connectors/{connector}/status")
                    conn_response.raise_for_status()
                    
                    status = conn_response.json()
                    connector_state = status.get("connector", {}).get("state", "UNKNOWN")
                    task_states = [task.get("state", "UNKNOWN") for task in status.get("tasks", [])]
                    
                    debezium_status[connector] = {
                        "connector_state": connector_state,
                        "task_states": task_states,
                        "is_running": connector_state == "RUNNING" and all(state == "RUNNING" for state in task_states)
                    }
            except Exception as debezium_error:
                logger.error(f"Debezium 상태 확인 중 오류: {debezium_error}")
                debezium_status = {"error": str(debezium_error)}
            
            # Kafka Connect 상태 확인
            kafka_connect_status = {}
            try:
                response = requests.get(f"{kafka_connect_url}/connectors")
                response.raise_for_status()
                
                kafka_connect_connectors = response.json()
                
                for connector in kafka_connect_connectors:
                    conn_response = requests.get(f"{kafka_connect_url}/connectors/{connector}/status")
                    conn_response.raise_for_status()
                    
                    status = conn_response.json()
                    connector_state = status.get("connector", {}).get("state", "UNKNOWN")
                    task_states = [task.get("state", "UNKNOWN") for task in status.get("tasks", [])]
                    
                    kafka_connect_status[connector] = {
                        "connector_state": connector_state,
                        "task_states": task_states,
                        "is_running": connector_state == "RUNNING" and all(state == "RUNNING" for state in task_states)
                    }
            except Exception as kafka_connect_error:
                logger.error(f"Kafka Connect 상태 확인 중 오류: {kafka_connect_error}")
                kafka_connect_status = {"error": str(kafka_connect_error)}
            
            # 전체 상태 평가
            all_running = True
            
            for connector, status in debezium_status.items():
                if isinstance(status, dict) and not status.get("is_running", False):
                    all_running = False
                    break
            
            if all_running:  # Debezium이 모두 실행 중인 경우에만 Kafka Connect 확인
                for connector, status in kafka_connect_status.items():
                    if isinstance(status, dict) and not status.get("is_running", False):
                        all_running = False
                        break
            
            result = {
                "validation_type": "connector_status",
                "debezium_status": debezium_status,
                "kafka_connect_status": kafka_connect_status,
                "all_connectors_running": all_running,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"커넥터 상태 확인 결과: {result}")
            return result
            
        except Exception as e:
            logger.error(f"커넥터 상태 확인 중 오류 발생: {e}")
            return {
                "validation_type": "connector_status",
                "error": str(e),
                "all_connectors_running": False,
                "timestamp": datetime.now().isoformat()
            }
    
    def check_iceberg_table_health(self, iceberg_table):
        """
        Iceberg 테이블 건강도 확인 - 파티션, 스냅샷 상태 등 검사
        
        Args:
            iceberg_table (str): Iceberg 테이블 이름
            
        Returns:
            dict: Iceberg 테이블 건강도 검사 결과
        """
        try:
            spark = self.connect_spark()
            
            # 테이블 메타데이터 확인
            # 1. 스냅샷 히스토리
            history_df = spark.read.format("iceberg").load(f"{iceberg_table}.history")
            snapshots_count = history_df.count()
            last_snapshot = history_df.orderBy(col("made_current_at").desc()).first() if snapshots_count > 0 else None
            
            # 2. 매니페스트 확인
            manifests_df = spark.read.format("iceberg").load(f"{iceberg_table}.manifests")
            manifests_count = manifests_df.count()
            
            # 3. 파티션 확인
            metadata_df = spark.read.format("iceberg").load(f"{iceberg_table}.metadata")
            
            # 건강도 확인
            # 1. 오래된 스냅샷 존재 여부 (7일 이상)
            old_snapshots = 0
            if snapshots_count > 0:
                seven_days_ago = (datetime.now() - timedelta(days=7)).timestamp() * 1000
                old_snapshots = history_df.filter(col("made_current_at") < seven_days_ago).count()
            
            # 2. 매니페스트 파일 수가 너무 많은지 확인 (성능 저하 가능성)
            too_many_manifests = manifests_count > 100  # 임의의 임계값
            
            # 결과 구성
            health_issues = []
            health_score = 100  # 100점 만점에서 시작
            
            if old_snapshots > 5:
                health_issues.append(f"오래된 스냅샷 {old_snapshots}개 발견 (7일 이상)")
                health_score -= min(20, old_snapshots * 2)  # 최대 20점 감점
            
            if too_many_manifests:
                health_issues.append(f"매니페스트 파일이 너무 많음 ({manifests_count}개)")
                health_score -= min(20, (manifests_count - 100) // 10)  # 10개당 1점 감점, 최대 20점
            
            # 테이블 데이터 통계 확인
            table_df = spark.read.format("iceberg").load(iceberg_table)
            row_count = table_df.count()
            
            # 파티션 건강도 확인 (파티션 균형)
            partition_issues = []
            is_partitioned = False
            
            try:
                partitions_info = metadata_df.select("partition_spec").collect()
                if partitions_info and len(partitions_info) > 0 and partitions_info[0]["partition_spec"]:
                    is_partitioned = True
                    # 파티션 별 행 수 확인
                    partition_columns = []  # 실제 애플리케이션에서는 여기에 파티션 컬럼 식별 로직 추가
                    
                    if partition_columns:
                        partitions_df = table_df.groupBy(*partition_columns).count()
                        partitions_stats = partitions_df.agg(
                            min("count").alias("min_rows"),
                            max("count").alias("max_rows"),
                            avg("count").alias("avg_rows")
                        ).collect()[0]
                        
                        # 불균형 파티션 확인 (평균 대비 10배 이상 차이)
                        if partitions_stats["max_rows"] > partitions_stats["avg_rows"] * 10:
                            partition_issues.append("파티션 간 심각한 불균형 발견")
                            health_score -= 15
                        
                        # 작은 파티션이 너무 많은지 확인
                        small_partitions = partitions_df.filter(col("count") < 1000).count()  # 임의의 임계값
                        if small_partitions > 10:
                            partition_issues.append(f"작은 파티션이 너무 많음 ({small_partitions}개)")
                            health_score -= min(10, small_partitions // 2)
            except Exception as partition_error:
                logger.warning(f"파티션 분석 중 오류: {partition_error}")
            
            # 최종 건강도 평가
            health_level = "양호"
            if health_score < 70:
                health_level = "불량"
            elif health_score < 90:
                health_level = "주의"
            
            result = {
                "validation_type": "iceberg_table_health",
                "table": iceberg_table,
                "snapshots_count": snapshots_count,
                "manifests_count": manifests_count,
                "row_count": row_count,
                "is_partitioned": is_partitioned,
                "health_score": health_score,
                "health_level": health_level,
                "health_issues": health_issues,
                "partition_issues": partition_issues if is_partitioned else ["파티션 없음"],
                "last_modified": last_snapshot["made_current_at"] if last_snapshot else None,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Iceberg 테이블 건강도 확인 결과: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Iceberg 테이블 건강도 확인 중 오류 발생: {e}")
            return {
                "validation_type": "iceberg_table_health",
                "error": str(e),
                "health_level": "오류",
                "timestamp": datetime.now().isoformat()
            }
    
    def run_validation_suite(self, table_name, iceberg_table, kafka_topic=None, timestamp_column=None):
        """
        종합 검증 스위트 실행 - 여러 검증 함수를 한 번에 실행하고 결과 종합
        
        Args:
            table_name (str): PostgreSQL 테이블 이름
            iceberg_table (str): Iceberg 테이블 이름
            kafka_topic (str, optional): 관련 Kafka 토픽 이름
            timestamp_column (str, optional): 타임스탬프 컬럼 이름
            
        Returns:
            dict: 종합 검증 결과
        """
        self.validation_metadata["start_time"] = datetime.now().isoformat()
        
        try:
            # 기본 검증 지표
            row_count_result = self.validate_row_count(table_name, iceberg_table)
            
            # 데이터 샘플링
            sampling_result = self.sample_data_validation(table_name, iceberg_table)

            # Iceberg 테이블 건강도
            iceberg_health_result = self.check_iceberg_table_health(iceberg_table)
            
            # 시간 관련 지표 (타임스탬프 컬럼이 제공된 경우)
            time_metrics = {}
            if timestamp_column:
                freshness_result = self.check_data_freshness(table_name, iceberg_table, timestamp_column)
                combined_lag_result = self.measure_combined_lag(table_name, iceberg_table, timestamp_column)
                time_metrics = {
                    "freshness": freshness_result,
                    "combined_lag": combined_lag_result
                }
            
            # Kafka/CDC 관련 지표 (토픽이 제공된 경우)
            cdc_metrics = {}
            if kafka_topic:
                cdc_tracking_result = self.track_cdc_messages(kafka_topic)
                event_type_result = self.track_cdc_event_types(kafka_topic)
                cdc_metrics = {
                    "cdc_tracking": cdc_tracking_result,
                    "event_types": event_type_result
                }
            
            # 검증 성공률 계산
            validation_results = [
                row_count_result.get("is_valid", False),
                sampling_result.get("is_valid", False),
            ]
            
            if timestamp_column:
                validation_results.extend([
                    freshness_result.get("is_fresh", False),
                    combined_lag_result.get("is_acceptable", False)
                ])
            
            success_count = sum(1 for result in validation_results if result)
            total_validations = len(validation_results)
            success_rate = (success_count / total_validations * 100) if total_validations > 0 else 0
            
            # 종합 결과
            result = {
                "table_name": table_name,
                "iceberg_table": iceberg_table,
                "validation_timestamp": datetime.now().isoformat(),
                "validation_success_rate": success_rate,
                "basic_metrics": {
                    "row_count": row_count_result,
                    "sampling": sampling_result,
                },
                "time_metrics": time_metrics,
                "cdc_metrics": cdc_metrics,
                "system_health": {
                    "iceberg_table": iceberg_health_result
                }
            }
            
            # 검증 상태 평가
            if success_rate >= 95:
                result["overall_status"] = "양호"
            elif success_rate >= 80:
                result["overall_status"] = "주의"
            else:
                result["overall_status"] = "불량"
            
            self.validation_metadata["end_time"] = datetime.now().isoformat()
            self.validation_metadata["status"] = "completed"
            
            logger.info(f"종합 검증 스위트 실행 완료: {result['overall_status']}")
            return result
            
        except Exception as e:
            logger.error(f"종합 검증 스위트 실행 중 오류 발생: {e}")
            self.validation_metadata["end_time"] = datetime.now().isoformat()
            self.validation_metadata["status"] = "error"
            
            return {
                "table_name": table_name,
                "iceberg_table": iceberg_table,
                "validation_timestamp": datetime.now().isoformat(),
                "error": str(e),
                "overall_status": "오류"
            }
    
    def save_validation_results(self, results, output_dir="validation_results"):
        """
        검증 결과를 파일로 저장
        
        Args:
            results (dict): 저장할 검증 결과
            output_dir (str): 결과 저장 디렉토리
            
        Returns:
            str: 저장된 파일 경로
        """
        try:
            # 디렉토리 생성
            os.makedirs(output_dir, exist_ok=True)
            
            # 결과 파일명 구성
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            table_name = results.get("table_name", "unknown_table")
            filename = f"{table_name}_validation_{timestamp}.json"
            file_path = os.path.join(output_dir, filename)
            
            # 결과 저장
            with open(file_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info(f"검증 결과 저장 완료: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"검증 결과 저장 중 오류 발생: {e}")
            return None

if __name__ == "__main__":    
    import argparse

    # 명령행 인자 파싱
    parser = argparse.ArgumentParser(description='데이터 검증 툴')
    parser.add_argument('--test', choices=[
        'row_count', 'checksum', 'sample_data', 'replication_lag', 
        'combined_lag', 'connector_status', 'iceberg_health', 'validation_suite'
    ], required=True, help='실행할 검증 테스트')

    parser.add_argument('--pg-host', default='localhost', help='PostgreSQL 호스트')
    parser.add_argument('--pg-port', type=int, default=5432, help='PostgreSQL 포트')
    parser.add_argument('--pg-db', default='postgres', help='PostgreSQL 데이터베이스')
    parser.add_argument('--pg-user', default='postgres', help='PostgreSQL 사용자')
    parser.add_argument('--pg-password', default='postgres', help='PostgreSQL 비밀번호')
    parser.add_argument('--pg-table', help='PostgreSQL 테이블 이름')

    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka 부트스트랩 서버')
    parser.add_argument('--kafka-topic', help='Kafka 토픽')
    parser.add_argument('--kafka-group', default='validation-group', help='Kafka 컨슈머 그룹')

    parser.add_argument('--iceberg-warehouse', default='s3a://warehouse/', help='Iceberg 웨어하우스 경로')
    parser.add_argument('--iceberg-table', help='Iceberg 테이블 이름 (catalog.database.table)')
    parser.add_argument('--s3-access-key', default='minioadmin', help='S3/Minio 액세스 키')
    parser.add_argument('--s3-secret-key', default='minioadmin', help='S3/Minio 시크릿 키')
    parser.add_argument('--external-ip', default='localhost', help='서비스 접근용 외부 IP')

    parser.add_argument('--debezium-url', default='http://localhost:8083', help='Debezium REST API URL')
    parser.add_argument('--connector-name', help='Debezium 커넥터 이름')
    parser.add_argument('--kafka-connect-url', default='http://localhost:8083', help='Kafka Connect REST API URL')

    parser.add_argument('--timestamp-column', help='타임스탬프 컬럼')
    parser.add_argument('--columns', help='체크섬 검증에 사용할 컬럼 (콤마 구분)')
    parser.add_argument('--sample-size', type=int, default=1000, help='샘플링 크기')

    args = parser.parse_args()

    # 설정 딕셔너리 생성
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

    # DataValidation 인스턴스 생성
    validator = DataValidation(pg_config, kafka_config, iceberg_config)
    
    # 선택한 테스트 실행
    if args.test == 'row_count':
        if not pg_config["table"] or not args.iceberg_table:
            print("Error: PostgreSQL 테이블과 Iceberg 테이블 모두 필요합니다.")
            sys.exit(1)
        
        result = validator.validate_row_count(args.pg_table, args.iceberg_table)
        print(f"행 수 검증 결과: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'checksum':
        if not args.pg_table or not args.iceberg_table or not args.columns:
            print("Error: PostgreSQL 테이블, Iceberg 테이블, 컬럼 목록이 필요합니다.")
            sys.exit(1)
        
        columns = [col.strip() for col in args.columns.split(',')]
        result = validator.validate_checksum(args.pg_table, columns, args.iceberg_table)
        print(f"체크섬 검증 결과: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'sample_data':
        if not args.pg_table or not args.iceberg_table:
            print("Error: PostgreSQL 테이블과 Iceberg 테이블 모두 필요합니다.")
            sys.exit(1)
        
        result = validator.sample_data_validation(args.pg_table, args.iceberg_table, args.sample_size)
        print(f"데이터 샘플링 검증 결과: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'replication_lag':
        if not args.debezium_url or not args.connector_name:
            print("Error: Debezium URL과 커넥터 이름이 필요합니다.")
            sys.exit(1)
        
        result = validator.measure_replication_lag(args.debezium_url, args.connector_name)
        print(f"복제 지연 측정 결과: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'combined_lag':
        if not args.pg_table or not args.iceberg_table or not args.timestamp_column:
            print("Error: PostgreSQL 테이블, Iceberg 테이블, 타임스탬프 컬럼이 필요합니다.")
            sys.exit(1)
        
        result = validator.measure_combined_lag(args.pg_table, args.iceberg_table, args.timestamp_column)
        print(f"전체 지연 측정 결과: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'connector_status':
        if not args.debezium_url or not args.kafka_connect_url:
            print("Error: Debezium URL과 Kafka Connect URL이 필요합니다.")
            sys.exit(1)
        
        result = validator.check_connector_status(args.debezium_url, args.kafka_connect_url)
        print(f"커넥터 상태 확인 결과: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'iceberg_health':
        if not args.iceberg_table:
            print("Error: Iceberg 테이블이 필요합니다.")
            sys.exit(1)
        
        result = validator.check_iceberg_table_health(args.iceberg_table)
        print(f"Iceberg 테이블 건강도 확인 결과: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'validation_suite':
        if not args.pg_table or not args.iceberg_table:
            print("Error: PostgreSQL 테이블과 Iceberg 테이블 모두 필요합니다.")
            sys.exit(1)
        
        result = validator.run_validation_suite(
            args.pg_table, 
            args.iceberg_table, 
            args.kafka_topic, 
            args.timestamp_column
        )
        
        # 결과 저장
        output_path = validator.save_validation_results(result)
        print(f"종합 검증 스위트 결과: {json.dumps(result, indent=2, default=str)}")
        print(f"결과가 다음 경로에 저장되었습니다: {output_path}")
