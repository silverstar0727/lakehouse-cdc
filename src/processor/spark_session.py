import sys

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def create_iceberg_spark_session(external_ip: str, s3_access_key: str, s3_secret_key: str, iceberg_warehouse_path: str) -> SparkSession:
    try:
        # Spark configuration
        conf = SparkConf()
        
        # Set up packages
        conf.set("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," +
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2," +
            "org.apache.hadoop:hadoop-aws:3.3.4," +
            "com.amazonaws:aws-java-sdk-bundle:1.12.262")

        # Configure Iceberg
        conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        conf.set("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        conf.set("spark.sql.catalog.iceberg.uri", f"http://iceberg-rest-catalog.{external_ip}.nip.io")
        conf.set("spark.sql.catalog.iceberg.warehouse", iceberg_warehouse_path)
        conf.set("spark.sql.warehouse.dir", iceberg_warehouse_path)

        # S3/Ceph configuration
        conf.set("spark.hadoop.fs.s3a.endpoint", f"http://ceph.{external_ip}.nip.io")
        conf.set("spark.hadoop.fs.s3a.access.key", s3_access_key)
        conf.set("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        # Add: Set default file system explicitly
        conf.set("spark.hadoop.fs.defaultFS", iceberg_warehouse_path)
        
        # # S3A performance settings
        # conf.set("spark.hadoop.fs.s3a.connection.establish.timeout", "10000")
        # conf.set("spark.hadoop.fs.s3a.connection.timeout", "300000")
        # conf.set("spark.hadoop.fs.s3a.attempts.maximum", "20")
        # conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
        
        # Merge conflict strategy
        conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        conf.set("spark.sql.iceberg.handle-merge-cardinality-violation", "true")

        # Create Spark session
        spark = SparkSession.builder \
            .appName("Kafka Iceberg Streaming Integration") \
            .config(conf=conf) \
            .master("local[*]") \
            .getOrCreate()
        
        print(f"S3 connection info: {conf.get('spark.hadoop.fs.s3a.endpoint')}")
        print(f"S3 access key: {conf.get('spark.hadoop.fs.s3a.access.key')}")
        print(f"S3 secret key: {conf.get('spark.hadoop.fs.s3a.secret.key')}")
        print(f"S3 default filesystem: {conf.get('spark.hadoop.fs.defaultFS')}")
        print(f"Iceberg warehouse path: {conf.get('spark.sql.catalog.iceberg.warehouse')}")
        
        # S3 test
        try:
            spark.sql("SELECT 1").show()
            print("Spark session created successfully.")
        except Exception as session_e:
            print(f"Error during Spark session test: {session_e}")
            
        return spark
    except Exception as e:
        print(f"Critical error: Failed to create Spark session: {e}")
        sys.exit(1)
