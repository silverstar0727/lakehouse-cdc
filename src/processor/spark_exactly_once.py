import os
import time
import sys
import uuid
import json
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, TopicPartition
import json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, BooleanType
from pyspark.sql.functions import desc, row_number
from pyspark.sql.window import Window

from spark_session import create_iceberg_spark_session


EXTERNAL_IP = os.getenv("EXTERNAL_IP")
OBC_ACCESS_KEY = os.getenv("OBC_ACCESS_KEY")
OBC_SECRET_KEY = os.getenv("OBC_SECRET_KEY")
ICEBERG_WAREHOUSE_PATH = os.getenv("ICEBERG_WAREHOUSE_PATH")

BOOTSTRAP_SERVER_URL = os.getenv("BOOTSTRAP_SERVER_URL")

# Kafka configuration for exactly-once semantics
kafka_config = {
    'bootstrap.servers': f"{BOOTSTRAP_SERVER_URL}:9094",
    'group.id': 'spark-iceberg-consumer',
    'enable.auto.commit': 'false',  # Disable auto commit for manual control
    'isolation.level': 'read_committed',  # Only read committed transactions
    'auto.offset.reset': 'earliest'  # Start from earliest to avoid message loss
}

# Global batch tracking
processed_batches = set()

def define_schema_for_items():
    """
    Defines the Spark schema to match the Item model from the FastAPI application.
    """
    try:
        # Define item table schema (including partitioning columns)
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("price", IntegerType(), True),
            StructField("on_offer", BooleanType(), True),
            StructField("operation", StringType(), True),
            StructField("processing_time", TimestampType(), True),
            StructField("year", StringType(), True),
            StructField("month", StringType(), True),
            StructField("batch_id", StringType(), True)  # Track batch ID for idempotency
        ])
    except Exception as e:
        print(f"Fatal error: Schema definition failed: {e}")
        sys.exit(1)  # Exit on error

def define_schema_for_offsets():
    """
    Defines the schema for storing Kafka offset information.
    """
    try:
        return StructType([
            StructField("commit_time", TimestampType(), False),
            StructField("topic", StringType(), False),
            StructField("partition", IntegerType(), False),
            StructField("offset", IntegerType(), False),
            StructField("batch_id", StringType(), False)
        ])
    except Exception as e:
        print(f"Fatal error: Offset schema definition failed: {e}")
        sys.exit(1)  # Exit on error

def create_iceberg_tables(spark, namespace):
    """
    Creates necessary Iceberg tables if they don't exist.
    """
    # Create items table
    create_iceberg_table_if_not_exists(
        spark, 
        namespace, 
        "items", 
        define_schema_for_items()
    )
    
    # Create offsets tracking table
    full_table_name = f"iceberg.{namespace}.kafka_offsets"
    
    try:
        # Check if table exists
        spark.sql(f"SELECT 1 FROM {full_table_name} LIMIT 1")
        print(f"Table {full_table_name} already exists")
    except Exception:
        try:
            print(f"Creating Kafka offsets table: {full_table_name}")
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                commit_time TIMESTAMP,
                topic STRING,
                partition INT,
                offset INT,
                batch_id STRING
            )
            USING iceberg
            """
            
            spark.sql(create_table_sql)
            print(f"Kafka offsets table created: {full_table_name}")
            
            # Set table properties
            spark.sql(f"""
            ALTER TABLE {full_table_name} SET TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
            """)
        except Exception as e:
            print(f"Fatal error: Kafka offsets table creation failed: {e}")
            sys.exit(1)
            
    # Create processed batches table
    full_table_name = f"iceberg.{namespace}.processed_batches"
    
    try:
        # Check if table exists
        spark.sql(f"SELECT 1 FROM {full_table_name} LIMIT 1")
        print(f"Table {full_table_name} already exists")
    except Exception:
        try:
            print(f"Creating processed batches table: {full_table_name}")
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                batch_id STRING,
                processed_time TIMESTAMP,
                record_count INT
            )
            USING iceberg
            """
            
            spark.sql(create_table_sql)
            print(f"Processed batches table created: {full_table_name}")
            
            # Set table properties
            spark.sql(f"""
            ALTER TABLE {full_table_name} SET TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
            """)
        except Exception as e:
            print(f"Fatal error: Processed batches table creation failed: {e}")
            sys.exit(1)

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
            
            # Create table directly with SQL - now includes batch_id
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
                month STRING,
                batch_id STRING
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
            message_json = json.loads(message_str)
        elif isinstance(message, str):
            message_json = json.loads(message)
        else:
            message_json = message
            
        # Handle case with no payload
        if not message_json.get('payload'):
            print("Warning: Received message without payload, ignoring.")
            return None
            
        # Extract payload
        payload = message_json.get('payload', {})
        
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

def is_batch_processed(spark, namespace, batch_id):
    """
    Checks if a batch has already been processed by looking up the batch_id
    in the processed_batches table.
    """
    if batch_id in processed_batches:
        return True
        
    full_table_name = f"iceberg.{namespace}.processed_batches"
    
    try:
        result = spark.sql(f"""
            SELECT COUNT(*) as count 
            FROM {full_table_name} 
            WHERE batch_id = '{batch_id}'
        """)
        
        count = result.first()["count"]
        
        if count > 0:
            # Add to in-memory cache
            processed_batches.add(batch_id)
            return True
        return False
    except Exception as e:
        print(f"Warning: Error checking processed batch: {e}")
        return False  # Assume not processed on error

def record_processed_batch(spark, namespace, batch_id, record_count):
    """
    Records that a batch has been processed successfully.
    """
    full_table_name = f"iceberg.{namespace}.processed_batches"
    
    try:
        spark.sql(f"""
            INSERT INTO {full_table_name}
            VALUES (
                '{batch_id}',
                CURRENT_TIMESTAMP(),
                {record_count}
            )
        """)
        
        # Add to in-memory cache
        processed_batches.add(batch_id)
        
        print(f"Recorded processed batch: {batch_id}")
    except Exception as e:
        print(f"Warning: Failed to record processed batch: {e}")

def save_offsets(spark, namespace, batch_id, topic, partition_offsets):
    """
    Saves Kafka offsets to Iceberg table for exactly-once processing.
    """
    full_table_name = f"iceberg.{namespace}.kafka_offsets"
    
    try:
        for partition, offset in partition_offsets.items():
            spark.sql(f"""
                INSERT INTO {full_table_name}
                VALUES (
                    CURRENT_TIMESTAMP(),
                    '{topic}',
                    {partition},
                    {offset},
                    '{batch_id}'
                )
            """)
        
        print(f"Saved offsets for batch: {batch_id}")
    except Exception as e:
        print(f"Warning: Failed to save offsets: {e}")
        raise e

def get_last_committed_offsets(spark, namespace, topic):
    """
    Retrieves the last committed offsets for a topic.
    """
    full_table_name = f"iceberg.{namespace}.kafka_offsets"
    
    offsets = {}
    
    try:
        # Get latest offset for each partition
        offset_df = spark.sql(f"""
            WITH ranked_offsets AS (
                SELECT 
                    partition, 
                    offset,
                    ROW_NUMBER() OVER (PARTITION BY partition ORDER BY commit_time DESC) as rn
                FROM {full_table_name}
                WHERE topic = '{topic}'
            )
            SELECT partition, offset
            FROM ranked_offsets
            WHERE rn = 1
        """)
        
        for row in offset_df.collect():
            offsets[row['partition']] = row['offset']
            
        return offsets
    except Exception as e:
        print(f"Warning: Error retrieving last committed offsets: {e}")
        return {}

def process_cdc_records(spark, records, namespace, table_name, consumer, topic, try_count=0):
    """
    Processes CDC records with exactly-once semantics.
    """
    if not records:
        return 0
    
    # Generate unique batch ID
    batch_id = str(uuid.uuid4())
    
    # Check if batch already processed (idempotency check)
    if is_batch_processed(spark, namespace, batch_id):
        print(f"Batch {batch_id} already processed, skipping")
        return 0
    
    max_try_count = 3
    
    # Get current partition offsets before processing
    partition_offsets = {}
    for tp in consumer.assignment():
        if tp.topic == topic:
            partition_offsets[tp.partition] = tp.offset
    
    # Start transaction
    print(f"Starting transaction for batch: {batch_id}")
    
    try:
        spark.sql("START TRANSACTION")
        
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
                    "month": now.strftime("%m"),
                    "batch_id": batch_id  # Include batch ID for idempotence
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
            
            # Keep only the most recent record for each ID (deduplication)
            window_spec = Window.partitionBy("id").orderBy(desc("processing_time"))
            
            # Select only the latest record for each ID
            deduplicated_df = df.withColumn("row_num", row_number().over(window_spec)) \
                               .filter("row_num = 1") \
                               .drop("row_num")
                               
            # Create temporary view
            deduplicated_df.createOrReplaceTempView("source_data")
            
            # Add data to Iceberg table
            full_table_name = f"iceberg.{namespace}.{table_name}"
            
            # Condition for merge operation (based on ID and different batch)
            merge_condition = "target.id = source.id AND target.batch_id <> source.batch_id"
            
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
                
                # Save Kafka offsets
                save_offsets(spark, namespace, batch_id, topic, partition_offsets)
                
                # Record batch as processed (for idempotency)
                record_processed_batch(spark, namespace, batch_id, len(processed_records))
                
                # Commit the transaction
                spark.sql("COMMIT")
                
                print(f"Transaction committed for batch: {batch_id}")
                print(f"MERGE operation successful: {len(processed_records)} records processed, unique ID count: {deduplicated_df.select('id').distinct().count()}")
                
                # Now commit Kafka offsets after successful transaction
                consumer.commit()
                print(f"Kafka offsets committed for batch: {batch_id}")
                
            except Exception as merge_error:
                # Rollback
                spark.sql("ROLLBACK")
                print(f"Transaction rolled back: {merge_error}")
                
                # Retry with backoff
                if try_count < max_try_count:
                    retry_wait = (2 ** try_count) * 1000  # Exponential backoff
                    print(f"Retrying... (Attempt: {try_count + 1}) after {retry_wait}ms")
                    time.sleep(retry_wait / 1000)  # Convert to seconds
                    return process_cdc_records(spark, records, namespace, table_name, consumer, topic, try_count+1)
                else:
                    print(f"Maximum retry attempts exceeded: {max_try_count}")
                    print(f"Warning: MERGE operation failed, stopping record processing")
                    return 0
                
            return len(processed_records)
        else:
            # Rollback empty transaction
            spark.sql("ROLLBACK")
            return 0
            
    except Exception as e:
        # Rollback on any exception
        try:
            spark.sql("ROLLBACK")
            print(f"Transaction rolled back due to error: {e}")
        except:
            pass
        
        print(f"Fatal error: Error occurred during CDC record processing: {e}")
        
        # Retry with backoff
        if try_count < max_try_count:
            retry_wait = (2 ** try_count) * 1000  # Exponential backoff
            print(f"Retrying... (Attempt: {try_count + 1}) after {retry_wait}ms")
            time.sleep(retry_wait / 1000)  # Convert to seconds
            return process_cdc_records(spark, records, namespace, table_name, consumer, topic, try_count+1)
        else:
            raise e

def init_consumer(spark, topic, namespace):
    """
    Initializes a Kafka consumer with exactly-once semantics by
    positioning it at the correct offset based on previous processing.
    """
    try:
        consumer = Consumer(kafka_config)
        
        # Get last committed offsets
        offsets = get_last_committed_offsets(spark, namespace, topic)
        
        if offsets:
            print(f"Found stored offsets for topic {topic}: {offsets}")
            
            # Create TopicPartition objects for assignment
            tps = []
            for partition, offset in offsets.items():
                tp = TopicPartition(topic, partition)
                # Resume from next message (offset + 1)
                tp.offset = offset + 1
                tps.append(tp)
                
            # Manually assign consumer to partitions with specific offsets
            consumer.assign(tps)
            print(f"Consumer assigned to partitions with stored offsets")
        else:
            # No stored offsets, subscribe to topic
            consumer.subscribe([topic])
            print(f"Consumer subscribed to topic: {topic}")
            
        return consumer
    except Exception as e:
        print(f"Fatal error: Failed to initialize Kafka consumer: {e}")
        raise e

def consume_kafka_messages(spark, topic, namespace, table_name):
    """
    Consumes messages from Kafka topic with exactly-once semantics.
    """
    try:
        # Initialize Kafka consumer with correct offset positioning
        consumer = init_consumer(spark, topic, namespace)
    except Exception as e:
        print(f"Fatal error: Failed to create Kafka Consumer: {e}")
        sys.exit(1)
    
    try:
        # Load processed batches into memory for faster lookups
        load_processed_batches(spark, namespace)
        
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
                        try:
                            processed = process_cdc_records(spark, records, namespace, table_name, consumer, topic)
                            print(f"Timeout commit: {processed} records processed")
                            records = []
                            last_commit_time = current_time
                        except Exception as e:
                            print(f"Error processing batch on timeout: {e}")
                            # Don't clear records on error
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        print(f"Reached end of partition {msg.partition()}")
                        continue
                    else:
                        print(f"Consumer error: {msg.error()}")
                        # Handle recoverable errors
                        if msg.error().retriable():
                            print("Retryable error, continuing...")
                            continue
                        else:
                            print(f"Fatal error: {msg.error()}")
                            break
                
                # Get message value
                value = msg.value()
                
                # Print message for debugging
                print(f"Message received: Partition: {msg.partition()} Offset: {msg.offset()}")
                
                # Check for empty messages
                if value is None or (isinstance(value, bytes) and len(value) == 0):
                    print(f"Warning: Received empty message (offset: {msg.offset()}), ignoring.")
                    # Don't commit here, will be part of batch commit
                    continue
                
                # Check for "empty" string
                if isinstance(value, bytes) and value.decode('utf-8', errors='replace').strip() == "empty":
                    print(f"Warning: Received 'empty' message (offset: {msg.offset()}), ignoring.")
                    # Don't commit here, will be part of batch commit
                    continue
                
                # Store message
                records.append(value)
                
                # Process when batch size is reached
                if len(records) >= batch_size:
                    try:
                        processed = process_cdc_records(spark, records, namespace, table_name, consumer, topic)
                        print(f"Batch commit: {processed} records processed")
                        records = []
                        last_commit_time = time.time()
                    except Exception as e:
                        print(f"Error processing batch: {e}")
                        # Don't clear records on error
                    
            except KeyboardInterrupt:
                print("Interrupted by user...")
                break
            except Exception as e:
                print(f"Warning: Error processing message: {e}")
                # Don't commit offset on error to prevent message loss
                
    except KeyboardInterrupt:
        print("Stopping consumer...")
    except Exception as e:
        print(f"Fatal error: Error consuming Kafka messages: {e}")
        sys.exit(1)
    finally:
        # Process remaining records
        if records:
            try:
                processed = process_cdc_records(spark, records, namespace, table_name, consumer, topic)
                print(f"Final commit: {processed} records processed")
            except Exception as e:
                print(f"Error processing final records: {e}")
                
        consumer.close()

def load_processed_batches(spark, namespace):
    """
    Loads processed batch IDs into memory for faster lookups.
    """
    global processed_batches
    
    try:
        full_table_name = f"iceberg.{namespace}.processed_batches"
        
        # Check if table exists first
        try:
            df = spark.sql(f"SELECT batch_id FROM {full_table_name}")
            
            # Load all batch IDs into memory
            for row in df.collect():
                processed_batches.add(row['batch_id'])
                
            print(f"Loaded {len(processed_batches)} processed batch IDs into memory")
        except:
            print("Processed batches table not yet available")
    except Exception as e:
        print(f"Warning: Error loading processed batches: {e}")

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
        print(f"Warning: Error during table optimization: {e}")

def main():
    """
    Main execution function with exactly-once semantics
    """
    try:
        # Create Spark session
        spark = create_iceberg_spark_session(EXTERNAL_IP, OBC_ACCESS_KEY, OBC_SECRET_KEY, ICEBERG_WAREHOUSE_PATH)
        
        # Create namespace (if it doesn't exist)
        namespace = "fastapi_db"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{namespace}")
        
        # Create all necessary tables
        create_iceberg_tables(spark, namespace)
        
        # Map FastAPI application tables to Kafka topics
        table_mappings = {
            "items": "postgres-connector.public.items"  # Adjust to match actual topic name
        }
        
        # Start consumer for each table
        for table_name, topic in table_mappings.items():
            print(f"Setting up consumer for table: {table_name}, topic: {topic}")
            try:
                consume_kafka_messages(spark, topic, namespace, table_name)
                
                # Perform periodic table optimization
                optimize_iceberg_table(spark, namespace, table_name, days_to_retain=30)
            except Exception as e:
                print(f"Fatal error processing table {table_name}: {e}")
        
        # Stop Spark session
        spark.stop()
    except Exception as e:
        print(f"Fatal error: Error in main function: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()