# Data Pipeline Validation Tools

This directory contains tools for validating CDC (Change Data Capture) based data pipelines connecting PostgreSQL databases to Apache Iceberg tables via Kafka and Debezium.

## Overview

The validation tools help ensure data integrity, monitoring, and troubleshooting across the entire pipeline:

- **PostgreSQL Source** → **Debezium CDC** → **Kafka** → **Spark/Iceberg**

These tools can validate data consistency, measure replication lag, check system health, and provide comprehensive insights into the pipeline's reliability.

## Prerequisites

- Python 3.6+
- Required Python packages:
  - psycopg2
  - confluent_kafka
  - pyspark
  - requests
- Running Kubernetes cluster with:
  - PostgreSQL deployment
  - Kafka (Strimzi operator)
  - Debezium Connect
  - Apache Iceberg with REST catalog
  - S3-compatible storage (Ceph/MinIO)

## Available Validation Tests

The toolkit provides several validation tests:

1. **Row Count Validation** (`row_count`): Compares the number of rows between source PostgreSQL and target Iceberg tables
2. **Checksum Validation** (`checksum`): Validates data content integrity by comparing checksums
3. **Sample Data Validation** (`sample_data`): Samples records from both systems and verifies their consistency
4. **Replication Lag** (`replication_lag`): Measures CDC replication delay using Debezium metrics
5. **Combined Lag** (`combined_lag`): Measures end-to-end latency from source to target
6. **Connector Status** (`connector_status`): Verifies Debezium and Kafka Connect connectors health
7. **Iceberg Table Health** (`iceberg_health`): Evaluates Iceberg table metadata, snapshot health, and partition balance
8. **Validation Suite** (`validation_suite`): Runs multiple validation tests and provides a comprehensive report

## Usage

You can run the validation tools using the provided `run.sh` script which sets up the necessary environment variables:

```bash
./run.sh
```

### Customizing Tests

Edit the `run.sh` script to uncomment the desired test:

```bash
# Select test type (uncomment one to use)
TEST_TYPE="row_count"
# TEST_TYPE="checksum"
# TEST_TYPE="sample_data"
# TEST_TYPE="replication_lag"
# TEST_TYPE="combined_lag"
# TEST_TYPE="connector_status"
# TEST_TYPE="iceberg_health"
# TEST_TYPE="validation_suite"
```

### Manual Execution

You can also run the validation script directly:

```bash
python3 validation.py --test row_count \
  --pg-host <HOST> \
  --pg-port <PORT> \
  --pg-db <DATABASE> \
  --pg-user <USERNAME> \
  --pg-password <PASSWORD> \
  --pg-table <TABLE> \
  --kafka-servers <BOOTSTRAP_SERVERS> \
  --iceberg-warehouse <WAREHOUSE_PATH> \
  --iceberg-table <TABLE_NAME> \
  --s3-access-key <KEY> \
  --s3-secret-key <SECRET>
```

## Configuration Options

| Parameter | Description |
|-----------|-------------|
| `--test` | Validation test to run |
| `--pg-host` | PostgreSQL host |
| `--pg-port` | PostgreSQL port |
| `--pg-db` | PostgreSQL database |
| `--pg-user` | PostgreSQL username |
| `--pg-password` | PostgreSQL password |
| `--pg-table` | PostgreSQL table name |
| `--kafka-servers` | Kafka bootstrap servers |
| `--kafka-topic` | Kafka topic name |
| `--kafka-group` | Kafka consumer group |
| `--iceberg-warehouse` | Iceberg warehouse path |
| `--iceberg-table` | Iceberg table name (catalog.database.table) |
| `--s3-access-key` | S3 access key |
| `--s3-secret-key` | S3 secret key |
| `--external-ip` | External IP for services |
| `--debezium-url` | Debezium REST API URL |
| `--connector-name` | Debezium connector name |
| `--kafka-connect-url` | Kafka Connect REST API URL |
| `--timestamp-column` | Timestamp column name |
| `--columns` | Columns for checksum (comma-separated) |
| `--sample-size` | Sample size for data validation |

## Output

Validation results are output in JSON format and logged to `data_validation.log`. The `validation_suite` test also saves results to the `validation_results` directory.

Example output:

```json
{
  "validation_type": "row_count",
  "source_count": 1000,
  "target_count": 1000,
  "difference": 0,
  "difference_percentage": 0.0,
  "is_valid": true,
  "timestamp": "2023-06-15T12:34:56.789012"
}
```

## Troubleshooting

If you encounter issues:

1. Check the `data_validation.log` file for detailed error messages
2. Ensure all connection parameters are correct
3. Verify that services are running in the Kubernetes cluster
4. Check network connectivity between the validation script and services

## Extending

To add new validation tests, extend the `DataValidation` class in `validation.py` with additional methods and update the command-line argument handling.
