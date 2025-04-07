import os
import time
import sys
from datetime import datetime
from typing import Dict, Any, List

from confluent_kafka import Consumer
import json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType, BooleanType
from pyspark.sql.functions import lit, current_timestamp, col, date_format, to_date, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

from spark_session import create_iceberg_spark_session


EXTERNAL_IP = os.getenv("EXTERNAL_IP")
OBC_ACCESS_KEY = os.getenv("OBC_ACCESS_KEY")
OBC_SECRET_KEY = os.getenv("OBC_SECRET_KEY")
ICEBERG_WAREHOUSE_PATH = os.getenv("ICEBERG_WAREHOUSE_PATH")

BOOTSTRAP_SERVER_URL = os.getenv("BOOTSTRAP_SERVER_URL")

# Kafka configuration
kafka_config = {
    'bootstrap.servers': f"{BOOTSTRAP_SERVER_URL}:9094",
    'group.id': 'spark-iceberg-consumer',
    'auto.offset.reset': 'latest'
}

def define_schema_for_items():
    """
    Defines the Spark schema to match the Item model from the FastAPI application.
    """
    try:
        # Define item table schema (including partitioning columns)
        return StructType([
            StructField("id", IntegerType(), True),  # Allow NULL
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("price", IntegerType(), True),
            StructField("on_offer", BooleanType(), True),
            StructField("operation", StringType(), True),  # Allow NULL
            StructField("processing_time", TimestampType(), True),  # Allow NULL
            StructField("year", StringType(), True),
            StructField("month", StringType(), True)
        ])
    except Exception as e:
        print(f"Fatal error: Schema definition failed: {e}")
        sys.exit(1)  # Exit on error

def create_iceberg_table_if_not_exists(spark, namespace, table_name, schema_fields):
    """
    Creates an Iceberg table if it doesn't exist.
    """
    full_table_name = f"iceberg.{namespace}.{table_name}"
    
    try:
        # Check if table exists
        spark.sql(f"SELECT 1 FROM {full_table_name} LIMIT 1")
        print(f"Table {full_table_name} already exists")
        table_exists = True
    except Exception:
        table_exists = False
    
    if not table_exists:
        try:
            print(f"Creating Iceberg table: {full_table_name}")
            
            # Create table directly with SQL
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                id INT,
                name STRING,
                description STRING,
                price INT,
                on_offer BOOLEAN,
                operation STRING,
                processing_time TIMESTAMP,
                year STRING,
                month STRING
            )
            USING iceberg
            PARTITIONED BY (year, month)
            """
            
            spark.sql(create_table_sql)
            print(f"Table {full_table_name} creation completed")
            
            # Set table properties
            spark.sql(f"""
            ALTER TABLE {full_table_name} SET TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
            """)
        except Exception as e:
            print(f"Fatal error: Table creation failed: {e}")
            sys.exit(1)  # Exit on error

def determine_operation(payload):
    """
    Determines the operation type from a Kafka message (insert, update, delete).
    Maps to the CRUD operations in the FastAPI application.
    """
    try:
        deleted = payload.get('__deleted', 'false').lower() == 'true'
        if deleted:
            return 'd'  # Delete (DELETE)
        elif payload.get('id') is None:
            return 'c'  # Create (POST)
        else:
            return 'u'  # Update (PUT)
    except Exception as e:
        print(f"Fatal error: Failed to determine operation type: {e}")
        sys.exit(1)  # Exit on error

def process_kafka_message(message):
    """
    Parses a Kafka message to extract valid payload.
    Handles messages in Debezium format.
    Added logic to handle empty messages.
    """
    try:
        # Handle empty messages
        if message is None:
            print("Warning: Received empty message (None), ignoring.")
            return None
            
        # Check for empty strings (bytes or str)
        if isinstance(message, bytes) and len(message) == 0:
            print("Warning: Received empty byte message, ignoring.")
            return None
        elif isinstance(message, str) and message.strip() == "":
            print("Warning: Received empty string message, ignoring.")
            return None
        elif isinstance(message, bytes) and message.decode('utf-8', errors='replace').strip() == "empty":
            print("Warning: Received 'empty' message, ignoring.")
            return None
        elif isinstance(message, str) and message.strip() == "empty":
            print("Warning: Received 'empty' message, ignoring.")
            return None
            
        # Parse the full message structure
        if isinstance(message, bytes):
            message_str = message.decode('utf-8')
            # Check for 'empty' string
            if message_str.strip() == "empty":
                print("Warning: Received 'empty' message, ignoring.")
                return None
            # print(f"Raw message (first 100 chars): {message_str[:100]}...")
            message_json = json.loads(message_str)
        elif isinstance(message, str):
            # print(f"Raw message (first 100 chars): {message[:100]}...")
            message_json = json.loads(message)
        else:
            message_json = message
            
        # Handle case with no payload
        if not message_json.get('payload'):
            print("Warning: Received message without payload, ignoring.")
            return None
            
        # Extract payload
        payload = message_json.get('payload', {})
        
        # Debug log
        # print(f"Parsed message payload: {json.dumps(payload, indent=2)[:200]}...")
        
        return payload
    except json.JSONDecodeError as e:
        print(f"Warning: JSON parsing failed, ignoring message: {e}")
        if isinstance(message, bytes):
            try:
                # Convert bytes to string for output
                decoded = message.decode('utf-8', errors='replace')
                print(f"Failed to parse message (first 200 chars): {decoded[:200]}")
            except:
                print(f"Cannot decode message bytes")
        return None
    except Exception as e:
        print(f"Warning: Failed to parse Kafka message, ignoring: {e}")
        if isinstance(message, bytes):
            try:
                # Convert bytes to string for output
                decoded = message.decode('utf-8', errors='replace')
                print(f"Failed to parse message (first 200 chars): {decoded[:200]}")
            except:
                print(f"Cannot decode message bytes")
        return None

def process_cdc_records(spark, records, namespace, table_name, try_count=0):
    """
    Processes CDC records and stores them in the Iceberg table.
    Handles the records according to the Item model from FastAPI application.
    Added deduplication logic to keep only the latest record for each ID.
    """
    if not records:
        return 0
    
    max_try_count = 3
    try:
        # Get table schema
        schema = define_schema_for_items()
        
        # Process records
        processed_records = []
        invalid_records = 0
        
        for record in records:
            try:
                payload = process_kafka_message(record)
                if not payload:
                    invalid_records += 1
                    continue
                    
                # Determine operation type
                operation = determine_operation(payload)
                
                # Set processing time to current time
                now = datetime.now()
                
                # Extract fields (only those defined in schema)
                processed_record = {
                    "id": payload.get("id"),
                    "name": payload.get("name"),
                    "description": payload.get("description"),
                    "price": payload.get("price"),
                    "on_offer": payload.get("on_offer", False),
                    "operation": operation,
                    "processing_time": now,
                    "year": now.strftime("%Y"),
                    "month": now.strftime("%m")
                }
                
                processed_records.append(processed_record)
                    
            except Exception as e:
                invalid_records += 1
                print(f"Warning: Failed to process individual record, skipping: {e}")
                try:
                    print(f"Problematic record: {record[:200] if isinstance(record, (str, bytes)) else str(record)[:200]}...")
                except:
                    print("Cannot display record")
        
        if invalid_records > 0:
            print(f"Warning: {invalid_records} records were not processed.")
            
        if processed_records:
            # Create DataFrame
            df = spark.createDataFrame(processed_records, schema)
            
            # Important: Keep only the most recent record for each ID (deduplication)
            # Define window by ID and order by processing time (sort within each ID group)
            window_spec = Window.partitionBy("id").orderBy(desc("processing_time"))
            
            # Select only the latest record for each ID
            deduplicated_df = df.withColumn("row_num", row_number().over(window_spec)) \
                               .filter("row_num = 1") \
                               .drop("row_num")
                               
            # Create temporary view
            deduplicated_df.createOrReplaceTempView("source_data")
            
            # Add data to Iceberg table
            full_table_name = f"iceberg.{namespace}.{table_name}"
            
            # Condition for merge operation (based on ID)
            merge_condition = "target.id = source.id"
            
            try:
                # Execute Iceberg merge into SQL
                spark.sql(f"""
                    MERGE INTO {full_table_name} target
                    USING source_data source
                    ON {merge_condition}
                    WHEN MATCHED AND source.operation = 'd' THEN DELETE
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
                
                print(f"MERGE operation successful: {len(processed_records)} records processed, unique ID count: {deduplicated_df.select('id').distinct().count()}")
                
            except Exception as merge_error:
                # Try alternative method if MERGE operation fails
                print(f"MERGE operation failed: {merge_error}")
                print(f"Retrying... (Attempt: {try_count + 1})")

                if try_count < max_try_count:
                    process_cdc_records(spark, records, namespace, table_name, try_count+1)
                    sys.exit(1)  # Exit on error
                else:
                    print(f"Maximum retry attempts exceeded: {max_try_count}")
                    print(f"Warning: MERGE operation failed, stopping record processing")
                    return 0
                
            return len(processed_records)
        
        return 0
    except Exception as e:
        print(f"Fatal error: Error occurred during CDC record processing: {e}")
        sys.exit(1)  # Exit on error

def consume_kafka_messages(spark, topic, namespace, table_name):
    """
    Consumes messages from Kafka topic and stores them in an Iceberg table.
    Captures CRUD events from the FastAPI application.
    """
    try:
        consumer = Consumer(kafka_config)
        consumer.subscribe([topic])
    except Exception as e:
        print(f"Fatal error: Failed to create Kafka Consumer: {e}")
        sys.exit(1)  # Exit on error
    
    try:
        # Get table schema
        schema = define_schema_for_items()
        
        # Create Iceberg table (if it doesn't exist)
        create_iceberg_table_if_not_exists(spark, namespace, table_name, schema)
        
        # Store records for batch processing
        batch_size = 100
        records = []
        last_commit_time = time.time()
        commit_interval = 60  # Commit every 60 seconds
        
        print(f"Starting consumer for topic: {topic}")
        
        while True:
            try:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    # Process records on timeout if there are any
                    current_time = time.time()
                    if records and (current_time - last_commit_time > commit_interval):
                        processed = process_cdc_records(spark, records, namespace, table_name)
                        print(f"Timeout commit: {processed} records processed")
                        records = []
                        consumer.commit()
                        last_commit_time = current_time
                    continue
                
                if msg.error():
                    print(f"Fatal error: Consumer error: {msg.error()}")
                    sys.exit(1)  # Exit on error
                
                # Get message value
                value = msg.value()
                
                # Print message for debugging
                print(f"Message received: Partition: {msg.partition()} Offset: {msg.offset()}")
                
                # Check for empty messages
                if value is None or (isinstance(value, bytes) and len(value) == 0):
                    print(f"Warning: Received empty message (offset: {msg.offset()}), ignoring.")
                    consumer.commit()  # Commit empty messages too
                    continue
                
                # Check for "empty" string
                if isinstance(value, bytes) and value.decode('utf-8', errors='replace').strip() == "empty":
                    print(f"Warning: Received 'empty' message (offset: {msg.offset()}), ignoring.")
                    consumer.commit()  # Commit 'empty' messages too
                    continue
                
                # Store message
                records.append(value)
                
                # Process when batch size is reached
                if len(records) >= batch_size:
                    processed = process_cdc_records(spark, records, namespace, table_name)
                    print(f"Batch commit: {processed} records processed")
                    records = []
                    consumer.commit()
                    last_commit_time = time.time()
                    
            except KeyboardInterrupt:
                print("Interrupted by user...")
                break
            except Exception as e:
                print(f"Warning: Error processing message: {e}")
                # Commit offset on error (prevent infinite loop)
                try:
                    consumer.commit()
                    print("Offset committed after error")
                except Exception as commit_err:
                    print(f"Failed to commit offset: {commit_err}")
                
    except KeyboardInterrupt:
        print("Stopping consumer...")
    except Exception as e:
        print(f"Fatal error: Error consuming Kafka messages: {e}")
        sys.exit(1)  # Exit on error
    finally:
        # Process remaining records
        if records:
            try:
                processed = process_cdc_records(spark, records, namespace, table_name)
                print(f"Final commit: {processed} records processed")
                consumer.commit()
            except Exception as e:
                print(f"Error processing final records: {e}")
                
        consumer.close()

def optimize_iceberg_table(spark, namespace, table_name, days_to_retain=30):
    """
    Optimizes the Iceberg table:
    - Compacts small files
    - Cleans up expired snapshots
    - Removes orphaned files
    """
    full_table_name = f"iceberg.{namespace}.{table_name}"
    
    print(f"Optimizing table: {full_table_name}")
    
    try:
        # Run compaction
        spark.sql(f"CALL iceberg.system.rewrite_data_files(table => '{full_table_name}', options => map('min-input-files','5'))")
        
        # Expire old snapshots
        spark.sql(f"CALL iceberg.system.expire_snapshots(table => '{full_table_name}', older_than => TIMESTAMP '{days_to_retain} days', retain_last => 5)")
        
        # Remove orphaned files
        spark.sql(f"CALL iceberg.system.remove_orphan_files(table => '{full_table_name}')")
        
        print(f"Table optimization completed: {full_table_name}")
    except Exception as e:
        print(f"Fatal error: Error during table optimization: {e}")
        sys.exit(1)  # Exit on error

def main():
    """
    Main execution function
    """
    try:
        # Create Spark session
        spark = create_iceberg_spark_session(EXTERNAL_IP, OBC_ACCESS_KEY, OBC_SECRET_KEY, ICEBERG_WAREHOUSE_PATH)
        
        # Create namespace (if it doesn't exist)
        namespace = "fastapi_db"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{namespace}")
        
        # Map FastAPI application tables
        # Map to topics created by Debezium
        # Adjust topic names based on actual message analysis
        table_mappings = {
            "items": "postgres-connector.public.items"  # Adjust to match actual topic name
        }
        
        # Start consumer for each table
        for table_name, topic in table_mappings.items():
            print(f"Setting up consumer for table: {table_name}, topic: {topic}")
            consume_kafka_messages(spark, topic, namespace, table_name)
            
            # Perform periodic table optimization
            optimize_iceberg_table(spark, namespace, table_name, days_to_retain=30)
        
        # Stop Spark session
        spark.stop()
    except Exception as e:
        print(f"Fatal error: Error in main function: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()