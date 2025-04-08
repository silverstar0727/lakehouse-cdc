import sys

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Window

import logging

logger = logging.getLogger(__name__)

def create_iceberg_spark_session(external_ip: str, s3_access_key: str, s3_secret_key: str, iceberg_warehouse_path: str) -> SparkSession:
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
        
        # AWS SDK v2 specific properties
        conf.set("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        
        # Hadoop region
        conf.set("spark.hadoop.fs.s3a.region", "us-east-1")
        
        # JVM level region setting
        import os
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
        
        # SDK v2
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
        print(f"S3 secret key: <hidden>")
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

def debug_iceberg_data(spark, iceberg_table):
    logger.info("Running Iceberg data debug checks...")
    
    # Load the Iceberg table
    iceberg_df = spark.read.format("iceberg").load(iceberg_table)
    
    # 전체 레코드 수 확인
    total_count = iceberg_df.count()
    logger.info(f"Total records in Iceberg table: {total_count}")
    
    # Check column types
    logger.info("Iceberg table schema:")
    iceberg_df.printSchema()
    
    # Count records with different is_iceberg_deleted values (string type 확인)
    if "is_iceberg_deleted" in iceberg_df.columns:
        # 먼저 distinct value 확인
        logger.info("Distinct values for is_iceberg_deleted:")
        iceberg_df.select("is_iceberg_deleted").distinct().show(truncate=False)
        
        # 각 값별 카운트
        str_true_count = iceberg_df.filter("is_iceberg_deleted = 'true'").count()
        str_false_count = iceberg_df.filter("is_iceberg_deleted = 'false'").count()
        bool_true_count = iceberg_df.filter("is_iceberg_deleted = true").count()
        bool_false_count = iceberg_df.filter("is_iceberg_deleted = false").count()
        null_count = iceberg_df.filter("is_iceberg_deleted IS NULL").count()
        
        logger.info(f"Records with is_iceberg_deleted = 'true' (string): {str_true_count}")
        logger.info(f"Records with is_iceberg_deleted = 'false' (string): {str_false_count}")
        logger.info(f"Records with is_iceberg_deleted = true (boolean): {bool_true_count}")
        logger.info(f"Records with is_iceberg_deleted = false (boolean): {bool_false_count}")
        logger.info(f"Records with is_iceberg_deleted = NULL: {null_count}")
        
        # 유효한 레코드 수 (삭제되지 않은 레코드)
        valid_records = iceberg_df.filter(
            "is_iceberg_deleted = 'false' OR is_iceberg_deleted = false OR is_iceberg_deleted IS NULL"
        ).count()
        logger.info(f"Valid records (not deleted): {valid_records}")
    
    # Check for duplicate IDs
    window_spec = Window.partitionBy("id")
    dupes_df = iceberg_df.withColumn("count", F.count("id").over(window_spec)) \
                         .filter("count > 1")
    
    dupe_count = dupes_df.select("id").distinct().count()
    logger.info(f"Number of records with duplicate IDs: {dupes_df.count()}")
    logger.info(f"Number of unique IDs with duplicates: {dupe_count}")
    
    if dupe_count > 0:
        logger.info("Sample of duplicate IDs:")
        dupes_df.select("id", "count", "is_iceberg_deleted").distinct().show(10)
        
        # 첫 번째 중복 ID에 대한 모든 레코드 확인
        first_dupe_id = dupes_df.select("id").first().id
        logger.info(f"Details of all records for duplicate ID {first_dupe_id}:")
        iceberg_df.filter(f"id = {first_dupe_id}").show(truncate=False)
    
    # 스냅샷 히스토리 확인
    try:
        logger.info("Iceberg table history:")
        history_df = spark.read.format("iceberg").load(f"{iceberg_table}.history")
        history_df.show(truncate=False)
    except Exception as e:
        logger.info(f"Could not read history: {e}")


    
if __name__ == "__main__":
    import os
    spark = create_iceberg_spark_session(
        external_ip=os.getenv("EXTERNAL_IP"),
        s3_access_key=os.getenv("OBC_ACCESS_KEY"),
        s3_secret_key=os.getenv("OBC_SECRET_KEY"),
        iceberg_warehouse_path="s3a://iceberg-warehouse/"
    )
    
    # 네임스페이스 확인 (선택사항)
    table_name = "iceberg.fastapi_db.items"

    debug_iceberg_data(spark, table_name)