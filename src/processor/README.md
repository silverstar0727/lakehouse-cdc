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
cd ../service
docker build -t lakehouse-cdc-processor:latest .
```

2. **Run the container:**

```bash
export EXTERNAL_IP=$(kubectl get service -n ingress-nginx nginx-ingress-ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

export OBC_ACCESS_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 --decode)
export OBC_SECRET_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 --decode)

export BOOTSTRAP_SERVER_URL=$(kubectl get service -n strimzi-kafka kraft-cluster-kafka-external-bootstrap -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

docker run -d \
   --name cdc-processor \
   -e EXTERNAL_IP=$EXTERNAL_IP \
   -e S3_ACCESS_KEY=$OBC_ACCESS_KEY \
   -e S3_SECRET_KEY=$OBC_SECRET_KEY \
   -e BOOTSTRAP_SERVER_URL=$BOOTSTRAP_SERVER_URL \
   -e ICEBERG_WAREHOUSE_PATH=s3a://iceberg-warehouse \
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
