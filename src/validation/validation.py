
import json
import requests
import logging
import os
import sys
from datetime import datetime, timedelta

import psycopg2

from confluent_kafka import Consumer, KafkaException, KafkaError

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, min, max, avg, lit, coalesce, concat_ws, collect_list, md5

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

    def validate_row_count(self, table_name, iceberg_table):
        """
        Validate row count between source and target systems
        
        Args:
            table_name (str): PostgreSQL table name
            iceberg_table (str): Iceberg table name (format: catalog.database.table)
            
        Returns:
            dict: Validation results and metadata
        """
        try:
            # Get row count from PostgreSQL
            pg_conn = self.connect_postgres()
            pg_cursor = pg_conn.cursor()
            pg_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            pg_count = pg_cursor.fetchone()[0]
            pg_cursor.close()
            pg_conn.close()
            
            # Get row count from Iceberg
            spark = self.connect_spark()
            
            iceberg_df = spark.read.format("iceberg").load(iceberg_table)
            
            if "is_iceberg_deleted" in iceberg_df.columns:
                iceberg_count = iceberg_df.filter("is_iceberg_deleted = false OR is_iceberg_deleted IS NULL").count()
            else:
                iceberg_count = iceberg_df.count()
                logger.warning("is_iceberg_deleted 필드를 찾을 수 없어 전체 레코드를 카운트합니다.")
            
            # Calculate results
            count_diff = abs(pg_count - iceberg_count)
            count_diff_pct = (count_diff / pg_count * 100) if pg_count > 0 else 0
            is_valid = count_diff_pct <= 0.1  # Allow difference within 0.1%
            
            result = {
                "validation_type": "row_count",
                "source_count": pg_count,
                "target_count": iceberg_count,
                "difference": count_diff,
                "difference_percentage": count_diff_pct,
                "is_valid": is_valid,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Row count validation result: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error during row count validation: {e}")
            return {
                "validation_type": "row_count",
                "error": str(e),
                "is_valid": False,
                "timestamp": datetime.now().isoformat()
            }
        
    def validate_checksum(self, table_name, columns, iceberg_table):
        """
        Validate data content using checksum
        
        Args:
            table_name (str): PostgreSQL table name
            columns (list): List of columns to use for checksum calculation
            iceberg_table (str): Iceberg table name
            
        Returns:
            dict: Validation results and metadata
        """
        try:
            # Calculate checksum from PostgreSQL
            pg_conn = self.connect_postgres()
            pg_cursor = pg_conn.cursor()
            
            # Create PostgreSQL concatenation expression with # as delimiter
            columns_concat = "||'#'||".join([f"COALESCE(CAST({col} AS VARCHAR), '')" for col in columns])
            pg_cursor.execute(f"SELECT MD5(STRING_AGG({columns_concat}, ',')) FROM {table_name}")
            pg_checksum = pg_cursor.fetchone()[0]
            pg_cursor.close()
            pg_conn.close()
            
            # Calculate checksum from Iceberg using Spark DataFrame operations
            spark = self.connect_spark()
            
            # Load the Iceberg table data
            df = spark.read.format("iceberg").load(iceberg_table)
            
            # Create column expressions for each column in the list
            col_exprs = [coalesce(col(c).cast("string"), lit("")) for c in columns]
            
            # Create a combined string for each row using concat_ws with # delimiter
            df_with_concat = df.withColumn("combined_cols", concat_ws("#", *col_exprs))
            
            # Order the data consistently to ensure matching with PostgreSQL
            df_ordered = df_with_concat.orderBy("combined_cols")
            
            # Collect all combined strings and join with comma, then calculate MD5
            iceberg_checksum = df_ordered.agg(
                md5(concat_ws(",", collect_list("combined_cols")))
            ).collect()[0][0]
            
            # Compare checksums to determine validity
            is_valid = pg_checksum == iceberg_checksum
            
            # Prepare result object
            result = {
                "validation_type": "checksum",
                "source_checksum": pg_checksum,
                "target_checksum": iceberg_checksum,
                "is_valid": is_valid,
                "columns_validated": columns,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Checksum validation result: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error during checksum validation: {e}")
            return {
                "validation_type": "checksum",
                "error": str(e),
                "is_valid": False,
                "timestamp": datetime.now().isoformat()
            }
    
    def sample_data_validation(self, table_name, iceberg_table, sample_size=1000):
        """
        Validate data consistency between source and target systems using data sampling
        
        Args:
            table_name (str): PostgreSQL table name
            iceberg_table (str): Iceberg table name
            sample_size (int): Number of records to sample
            
        Returns:
            dict: Sampling validation results
        """
        try:
            # Extract random sample from PostgreSQL
            pg_conn = self.connect_postgres()
            pg_cursor = pg_conn.cursor()
            
            # Identify key columns (primary key or index)
            pg_cursor.execute(f"""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary
            """)
            key_columns = [row[0] for row in pg_cursor.fetchall()]
            
            if not key_columns:
                logger.warning(f"Primary key not found for table {table_name}. Using first column.")
                pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position LIMIT 1")
                key_columns = [pg_cursor.fetchone()[0]]
            
            # Get all columns
            pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            all_columns = [row[0] for row in pg_cursor.fetchall()]
            
            # Execute random sample query
            columns_str = ", ".join(all_columns)
            key_columns_str = ", ".join(key_columns)
            
            pg_cursor.execute(f"SELECT {columns_str} FROM {table_name} ORDER BY RANDOM() LIMIT {sample_size}")
            pg_samples = pg_cursor.fetchall()
            
            # Extract key values
            key_values = []
            key_indices = [all_columns.index(col) for col in key_columns]
            
            for row in pg_samples:
                key_dict = {key_columns[i]: row[key_indices[i]] for i in range(len(key_columns))}
                key_values.append(key_dict)
            
            pg_cursor.close()
            pg_conn.close()
            
            # Search for records with the same keys in Iceberg
            spark = self.connect_spark()
            iceberg_df = spark.read.format("iceberg").load(iceberg_table)
            
            match_count = 0
            mismatch_details = []
            
            for key_dict in key_values:
                # Create key conditions
                filter_conditions = []
                for col, val in key_dict.items():
                    if val is None:
                        filter_conditions.append(f"{col} IS NULL")
                    else:
                        filter_conditions.append(f"{col} = '{val}'")
                
                filter_expr = " AND ".join(filter_conditions)
                
                # Search for matching records
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
                "is_valid": match_rate >= 0.99,  # Valid if match rate is 99% or higher
                "mismatch_count": len(mismatch_details),
                "timestamp": datetime.now().isoformat()
            }
            
            if len(mismatch_details) > 0:
                result["mismatch_examples"] = mismatch_details[:5]  # Include first 5 examples
            
            logger.info(f"Data sampling validation result: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error during data sampling validation: {e}")
            return {
                "validation_type": "data_sampling",
                "error": str(e),
                "is_valid": False,
                "timestamp": datetime.now().isoformat()
            }
        
    def measure_replication_lag(self, debezium_url, connector_name):
        """
        Measure replication lag - Check replication lag time of Debezium connector
        
        Args:
            debezium_url (str): Debezium REST API URL
            connector_name (str): Debezium connector name
            
        Returns:
            dict: Replication lag measurement results
        """
        try:
            # Call Debezium connector status check API
            response = requests.get(f"{debezium_url}/connectors/{connector_name}/status")
            response.raise_for_status()
            
            connector_status = response.json()
            
            # Check if connector is running
            if connector_status.get("connector", {}).get("state") != "RUNNING":
                return {
                    "validation_type": "replication_lag",
                    "error": f"Connector is not running. Current state: {connector_status.get('connector', {}).get('state')}",
                    "is_valid": False,
                    "timestamp": datetime.now().isoformat()
                }
            
            # Check replication lag metrics of Debezium connector
            response = requests.get(f"{debezium_url}/connectors/{connector_name}")
            response.raise_for_status()
            
            metrics = response.json()
            
            # Extract replication lag related metrics
            lag_metrics = {}
            
            if "tasks" in metrics:
                for task in metrics["tasks"]:
                    if "metrics" in task and "source-record-lag" in task["metrics"]:
                        lag_metrics["source_record_lag"] = task["metrics"]["source-record-lag"]
                    if "metrics" in task and "lag-in-millis" in task["metrics"]:
                        lag_metrics["lag_millis"] = task["metrics"]["lag-in-millis"]
            
            is_acceptable_lag = True
            if "lag_millis" in lag_metrics and lag_metrics["lag_millis"] > 60000:  # Warning for lag over 1 minute
                is_acceptable_lag = False
            
            result = {
                "validation_type": "replication_lag",
                "connector_name": connector_name,
                "lag_metrics": lag_metrics,
                "is_acceptable": is_acceptable_lag,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Replication lag measurement result: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error during replication lag measurement: {e}")
            return {
                "validation_type": "replication_lag",
                "error": str(e),
                "is_acceptable": False,
                "timestamp": datetime.now().isoformat()
            }
    
    def measure_combined_lag(self, table_name, iceberg_table, event_time_column):
        """
        Measure combined lag - Total lag time from source creation to target loading
        
        Args:
            table_name (str): PostgreSQL table name
            iceberg_table (str): Iceberg table name
            event_time_column (str): Column name storing event creation time
            
        Returns:
            dict: Combined lag measurement results
        """
        try:
            # Query data within the last 30 minutes from current time
            thirty_mins_ago = datetime.now() - timedelta(minutes=30)
            
            # Query event time and system time of recent data from PostgreSQL
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
            
            # Query the same data from Iceberg
            spark = self.connect_spark()
            
            from pyspark.sql.functions import current_timestamp, col
            
            # Query recent data using Spark SQL
            iceberg_df = spark.read.format("iceberg").load(iceberg_table)
            iceberg_times = iceberg_df.filter(col(event_time_column) > thirty_mins_ago.isoformat()) \
                                     .select(event_time_column, current_timestamp().alias("system_time")) \
                                     .orderBy(col(event_time_column).desc()) \
                                     .limit(100) \
                                     .collect()
            
            # Calculate lag between source creation time and target loading time
            if pg_times and iceberg_times:
                # Calculate average lag time (in seconds)
                pg_event_times = [row[0] for row in pg_times]
                iceberg_event_times = [row[0] for row in iceberg_times]
                
                # Find event times present in both source and target
                common_times = set(pg_event_times).intersection(set(iceberg_event_times))
                
                if common_times:
                    # Map system time for each event
                    pg_time_map = {row[0]: row[1] for row in pg_times if row[0] in common_times}
                    iceberg_time_map = {row[0]: row[1] for row in iceberg_times if row[0] in common_times}
                    
                    # Calculate lag time
                    lag_times = []
                    for event_time in common_times:
                        # Time recorded in target - Time recorded in source
                        lag_seconds = (iceberg_time_map[event_time] - pg_time_map[event_time]).total_seconds()
                        lag_times.append(lag_seconds)
                    
                    avg_lag = sum(lag_times) / len(lag_times)
                    min_lag = min(lag_times)
                    max_lag = max(lag_times)
                    
                    # Allow lag within 5 minutes (300 seconds)
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
            
            logger.info(f"Combined lag measurement result: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error during combined lag measurement: {e}")
            return {
                "validation_type": "combined_lag",
                "error": str(e),
                "is_acceptable": False,
                "timestamp": datetime.now().isoformat()
            }
        
    def check_connector_status(self, debezium_url, kafka_connect_url):
        """
        Check connector status - Monitor status of Debezium and Kafka Connect connectors
        
        Args:
            debezium_url (str): Debezium REST API URL
            kafka_connect_url (str): Kafka Connect REST API URL
            
        Returns:
            dict: Connector status check results
        """
        try:
            # Check Debezium connector status
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
                logger.error(f"Error during Debezium status check: {debezium_error}")
                debezium_status = {"error": str(debezium_error)}
            
            # Check Kafka Connect status
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
                logger.error(f"Error during Kafka Connect status check: {kafka_connect_error}")
                kafka_connect_status = {"error": str(kafka_connect_error)}
            
            # Evaluate overall status
            all_running = True
            
            for connector, status in debezium_status.items():
                if isinstance(status, dict) and not status.get("is_running", False):
                    all_running = False
                    break
            
            if all_running:  # Check Kafka Connect only if all Debezium connectors are running
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
            
            logger.info(f"Connector status check result: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error during connector status check: {e}")
            return {
                "validation_type": "connector_status",
                "error": str(e),
                "all_connectors_running": False,
                "timestamp": datetime.now().isoformat()
            }
    
    def check_iceberg_table_health(self, iceberg_table):
        """
        Check Iceberg table health - Examine partitions, snapshot status, etc.
        
        Args:
            iceberg_table (str): Iceberg table name
            
        Returns:
            dict: Iceberg table health check results
        """
        try:
            spark = self.connect_spark()
            
            # Check table metadata
            # 1. Snapshot history
            history_df = spark.read.format("iceberg").load(f"{iceberg_table}.history")
            snapshots_count = history_df.count()
            last_snapshot = history_df.orderBy(col("made_current_at").desc()).first() if snapshots_count > 0 else None
            
            # 2. Check manifests
            manifests_df = spark.read.format("iceberg").load(f"{iceberg_table}.manifests")
            manifests_count = manifests_df.count()
            
            # 3. Check partitions
            metadata_df = spark.read.format("iceberg").load(f"{iceberg_table}.metadata")
            
            # Health check
            # 1. Check for old snapshots (more than 7 days)
            old_snapshots = 0
            if snapshots_count > 0:
                seven_days_ago = (datetime.now() - timedelta(days=7)).timestamp() * 1000
                old_snapshots = history_df.filter(col("made_current_at") < seven_days_ago).count()
            
            # 2. Check if there are too many manifest files (potential performance issue)
            too_many_manifests = manifests_count > 100  # arbitrary threshold
            
            # Compose results
            health_issues = []
            health_score = 100  # Starting from 100 points
            
            if old_snapshots > 5:
                health_issues.append(f"Found {old_snapshots} old snapshots (more than 7 days)")
                health_score -= min(20, old_snapshots * 2)  # Maximum 20 point deduction
            
            if too_many_manifests:
                health_issues.append(f"Too many manifest files ({manifests_count})")
                health_score -= min(20, (manifests_count - 100) // 10)  # 1 point deduction per 10 files, max 20 points
            
            # Check table data statistics
            table_df = spark.read.format("iceberg").load(iceberg_table)
            row_count = table_df.count()
            
            # Check partition health (partition balance)
            partition_issues = []
            is_partitioned = False
            
            try:
                partitions_info = metadata_df.select("partition_spec").collect()
                if partitions_info and len(partitions_info) > 0 and partitions_info[0]["partition_spec"]:
                    is_partitioned = True
                    # Check row count by partition
                    partition_columns = []  # Add partition column identification logic here in actual application
                    
                    if partition_columns:
                        partitions_df = table_df.groupBy(*partition_columns).count()
                        partitions_stats = partitions_df.agg(
                            min("count").alias("min_rows"),
                            max("count").alias("max_rows"),
                            avg("count").alias("avg_rows")
                        ).collect()[0]
                        
                        # Check for unbalanced partitions (more than 10x difference from average)
                        if partitions_stats["max_rows"] > partitions_stats["avg_rows"] * 10:
                            partition_issues.append("Severe imbalance found between partitions")
                            health_score -= 15
                        
                        # Check if there are too many small partitions
                        small_partitions = partitions_df.filter(col("count") < 1000).count()  # arbitrary threshold
                        if small_partitions > 10:
                            partition_issues.append(f"Too many small partitions ({small_partitions})")
                            health_score -= min(10, small_partitions // 2)
            except Exception as partition_error:
                logger.warning(f"Error during partition analysis: {partition_error}")
            
            # Final health assessment
            health_level = "Good"
            if health_score < 70:
                health_level = "Poor"
            elif health_score < 90:
                health_level = "Warning"
            
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
                "partition_issues": partition_issues if is_partitioned else ["No partitions"],
                "last_modified": last_snapshot["made_current_at"] if last_snapshot else None,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Iceberg table health check result: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error during Iceberg table health check: {e}")
            return {
                "validation_type": "iceberg_table_health",
                "error": str(e),
                "health_level": "Error",
                "timestamp": datetime.now().isoformat()
            }
    
    def run_validation_suite(self, table_name, iceberg_table, kafka_topic=None, timestamp_column=None):
        """
        Run comprehensive validation suite - Execute multiple validation functions at once and summarize results
        
        Args:
            table_name (str): PostgreSQL table name
            iceberg_table (str): Iceberg table name
            kafka_topic (str, optional): Related Kafka topic name
            timestamp_column (str, optional): Timestamp column name
            
        Returns:
            dict: Comprehensive validation results
        """
        self.validation_metadata["start_time"] = datetime.now().isoformat()
        
        try:
            # Basic validation metrics
            row_count_result = self.validate_row_count(table_name, iceberg_table)
            
            # Data sampling
            sampling_result = self.sample_data_validation(table_name, iceberg_table)

            # Iceberg table health
            iceberg_health_result = self.check_iceberg_table_health(iceberg_table)
            
            # Time-related metrics (if timestamp column is provided)
            time_metrics = {}
            if timestamp_column:
                freshness_result = self.check_data_freshness(table_name, iceberg_table, timestamp_column)
                combined_lag_result = self.measure_combined_lag(table_name, iceberg_table, timestamp_column)
                time_metrics = {
                    "freshness": freshness_result,
                    "combined_lag": combined_lag_result
                }
            
            # Kafka/CDC-related metrics (if topic is provided)
            cdc_metrics = {}
            if kafka_topic:
                cdc_tracking_result = self.track_cdc_messages(kafka_topic)
                event_type_result = self.track_cdc_event_types(kafka_topic)
                cdc_metrics = {
                    "cdc_tracking": cdc_tracking_result,
                    "event_types": event_type_result
                }
            
            # Calculate validation success rate
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
            
            # Comprehensive results
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
            
            # Evaluate validation status
            if success_rate >= 95:
                result["overall_status"] = "Good"
            elif success_rate >= 80:
                result["overall_status"] = "Warning"
            else:
                result["overall_status"] = "Poor"
            
            self.validation_metadata["end_time"] = datetime.now().isoformat()
            self.validation_metadata["status"] = "completed"
            
            logger.info(f"Comprehensive validation suite completed: {result['overall_status']}")
            return result
            
        except Exception as e:
            logger.error(f"Error during comprehensive validation suite: {e}")
            self.validation_metadata["end_time"] = datetime.now().isoformat()
            self.validation_metadata["status"] = "error"
            
            return {
                "table_name": table_name,
                "iceberg_table": iceberg_table,
                "validation_timestamp": datetime.now().isoformat(),
                "error": str(e),
                "overall_status": "Error"
            }
    
    def save_validation_results(self, results, output_dir="validation_results"):
        """
        Save validation results to a file
        
        Args:
            results (dict): Validation results to save
            output_dir (str): Directory to save results
            
        Returns:
            str: Path to saved file
        """
        try:
            # Create directory
            os.makedirs(output_dir, exist_ok=True)
            
            # Compose result file name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            table_name = results.get("table_name", "unknown_table")
            filename = f"{table_name}_validation_{timestamp}.json"
            file_path = os.path.join(output_dir, filename)
            
            # Save results
            with open(file_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info(f"Validation results saved: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error during validation results save: {e}")
            return None

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
        
        result = validator.validate_row_count(args.pg_table, args.iceberg_table)
        print(f"Row count validation result: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'checksum':
        if not args.pg_table or not args.iceberg_table or not args.columns:
            print("Error: PostgreSQL table, Iceberg table, and column list are required.")
            sys.exit(1)
        
        columns = [col.strip() for col in args.columns.split(',')]
        result = validator.validate_checksum(args.pg_table, columns, args.iceberg_table)
        print(f"Checksum validation result: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'sample_data':
        if not args.pg_table or not args.iceberg_table:
            print("Error: Both PostgreSQL table and Iceberg table are required.")
            sys.exit(1)
        
        result = validator.sample_data_validation(args.pg_table, args.iceberg_table, args.sample_size)
        print(f"Data sampling validation result: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'replication_lag':
        if not args.debezium_url or not args.connector_name:
            print("Error: Debezium URL and connector name are required.")
            sys.exit(1)
        
        result = validator.measure_replication_lag(args.debezium_url, args.connector_name)
        print(f"Replication lag measurement result: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'combined_lag':
        if not args.pg_table or not args.iceberg_table or not args.timestamp_column:
            print("Error: PostgreSQL table, Iceberg table, and timestamp column are required.")
            sys.exit(1)
        
        result = validator.measure_combined_lag(args.pg_table, args.iceberg_table, args.timestamp_column)
        print(f"Combined lag measurement result: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'connector_status':
        if not args.debezium_url or not args.kafka_connect_url:
            print("Error: Debezium URL and Kafka Connect URL are required.")
            sys.exit(1)
        
        result = validator.check_connector_status(args.debezium_url, args.kafka_connect_url)
        print(f"Connector status check result: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'iceberg_health':
        if not args.iceberg_table:
            print("Error: Iceberg table is required.")
            sys.exit(1)
        
        result = validator.check_iceberg_table_health(args.iceberg_table)
        print(f"Iceberg table health check result: {json.dumps(result, indent=2, default=str)}")
    
    elif args.test == 'validation_suite':
        if not args.pg_table or not args.iceberg_table:
            print("Error: Both PostgreSQL table and Iceberg table are required.")
            sys.exit(1)
        
        result = validator.run_validation_suite(
            args.pg_table, 
            args.iceberg_table, 
            args.kafka_topic, 
            args.timestamp_column
        )
        
        # Save results
        output_path = validator.save_validation_results(result)
        print(f"Comprehensive validation suite result: {json.dumps(result, indent=2, default=str)}")
        print(f"Results saved at: {output_path}")
