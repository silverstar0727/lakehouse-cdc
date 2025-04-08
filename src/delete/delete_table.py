
import sys

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

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

import os
spark = create_iceberg_spark_session(
    external_ip=os.getenv("EXTERNAL_IP"),
    s3_access_key=os.getenv("OBC_ACCESS_KEY"),
    s3_secret_key=os.getenv("OBC_SECRET_KEY"),
    iceberg_warehouse_path="s3a://iceberg-warehouse/"
)


# 네임스페이스 확인 (선택사항)
spark.sql("SHOW NAMESPACES IN iceberg").show()

# 테이블 삭제 (PURGE를 사용하여 물리적 데이터도 함께 삭제)
for table in ["items_spark", "items"]:
    try:
        spark.sql(f"DROP TABLE iceberg.fastapi_db.{table} PURGE")
    except Exception as e:
        print(f"테이블 삭제 중 오류 발생: {e}")

# 종료
spark.stop()