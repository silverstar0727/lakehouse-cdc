# Lakehouse CDC Processor

This module processes Change Data Capture (CDC) events from Kafka and writes them to an Apache Iceberg data lake.

## Architecture

The processor consists of the following components:

1. **Spark Session Manager** (`spark_session.py`): Configures and creates a Spark session with Iceberg and S3/Ceph support.
2. **CDC Batch Processor** (`spark_batch.py`): Consumes CDC events from Kafka and writes them to Iceberg tables.

## How It Works

1. **Data Flow**:
   - Debezium captures changes from PostgreSQL database
   - Changes are published to Kafka topics
   - This processor consumes those events and writes them to Iceberg tables
   - Data is stored in S3-compatible storage (Ceph)

2. **Processing Logic**:
   - Consumes messages in batches
   - Determines operation type (create, update, delete)
   - Handles deduplication using window functions
   - Uses Iceberg MERGE INTO for upserts and deletes
   - Partitions data by year and month

3. **Optimization**:
   - Periodically compacts small files
   - Expires old snapshots
   - Removes orphaned files

## Configuration

Environment variables:
- `EXTERNAL_IP`: External IP for accessing services
- `S3_ACCESS_KEY`: S3/Ceph access key
- `S3_SECRET_KEY`: S3/Ceph secret key

Kafka configuration:
- Bootstrap servers: Configured in `kafka_config` 
- Consumer group: `spark-iceberg-consumer`
- Auto offset reset: `latest`

## Usage

### Running with Python

To run the processor directly with Python:

```bash
python spark_batch.py
```

### Running with Docker

This processor can be containerized using Docker for easier deployment.

1. **Build the Docker image:**

```bash
docker build -t lakehouse-cdc-processor:latest .
```

2. **Run the container:**

```bash
docker run -d \
  --name cdc-processor \
  -e EXTERNAL_IP=<your-external-ip> \
  -e S3_ACCESS_KEY=<your-s3-access-key> \
  -e S3_SECRET_KEY=<your-s3-secret-key> \
  -e BOOTSTRAP_SERVER_URL=<your-bootstrap-lb-ip> \
  lakehouse-cdc-processor:latest
```

## Table Structure

The processor creates Iceberg tables with the following schema:

- `id`: Integer - Primary key
- `name`: String
- `description`: String
- `price`: Integer
- `on_offer`: Boolean
- `operation`: String - CDC operation type (c=create, u=update, d=delete)
- `processing_time`: Timestamp - When the record was processed
- `year`: String - Partition column
- `month`: String - Partition column

## Error Handling

- Robust error handling for connection failures
- Message parsing errors are logged but won't crash the application
- Failed batches are retried up to 3 times
- Offset commits ensure no messages are lost
