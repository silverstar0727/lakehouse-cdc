# Lakehouse CDC Validation

This directory contains scripts for validating data integrity and consistency in a Lakehouse architecture using PostgreSQL, Kafka, and Iceberg.

## Overview

The validation scripts are designed to ensure that data is correctly replicated and synchronized across the components of the Lakehouse architecture. The key components include:
- **PostgreSQL**: Source database.
- **Kafka**: Message broker for CDC (Change Data Capture) events.
- **Iceberg**: Data lake table format for analytical queries.

## Files

### `debug.py`
Contains utility functions for debugging Iceberg tables and Spark configurations. It includes:
- Spark session creation with Iceberg and S3 configurations.
- Debugging Iceberg table data (e.g., schema, record counts, duplicates).

### `validation.py`
Implements various validation methods, including:
- **Row Count Validation**: Compares row counts between PostgreSQL and Iceberg.
- **Checksum Validation**: Verifies data consistency using checksums.
- **Sample Data Validation**: Compares random samples of data between PostgreSQL and Iceberg.

### `run.sh`
A shell script to automate the execution of validation tests. It sets up environment variables and runs the `validation.py` script with the appropriate arguments.

## Setup

### Prerequisites
- Python 3.8 or higher
- Required Python packages:
  - `confluent-kafka`
  - `requests`
  - `psycopg2-binary`
  - `py4j`
  - `pandas`
- Spark 3.5.2 with Iceberg and Kafka dependencies
- Access to PostgreSQL, Kafka, and Iceberg environments

### Installation
1. Install Python dependencies:
   ```bash
   pip install confluent-kafka requests psycopg2-binary py4j==0.10.9.7 pandas
   ```
2. Ensure Spark is installed and accessible.

## Usage

### Use Docker
```bash
export BOOTSTRAP_SERVER_URL=$(kubectl get service -n strimzi-kafka kraft-cluster-kafka-external-bootstrap -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

export OBC_ACCESS_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 --decode)
export OBC_SECRET_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 --decode)

export EXTERNAL_IP=$(kubectl get service -n ingress-nginx nginx-ingress-ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

export PG_HOST=$(kubectl get service -n default backend-postgres -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

 docker run -it -d -v ~/Desktop/lakehouse-cdc/src:/workspace \
      -e EXTERNAL_IP=$EXTERNAL_IP \
      -e OBC_ACCESS_KEY=$OBC_ACCESS_KEY \
      -e OBC_SECRET_KEY=$OBC_SECRET_KEY \
      -e BOOTSTRAP_SERVER_URL=$BOOTSTRAP_SERVER_URL \
      -e ICEBERG_WAREHOUSE_PATH=$ICEBERG_WAREHOUSE_PATH -e PG_HOST=$PG_HOST \
      -n pyspark bitnami/spark:3.5.2

# docker exec
docker exec -it pyspark /bin/bash
cd /workspace
pip install confluent-kafka requests psycopg2-binary py4j==0.10.9.7 pandas
```

### Running Validation Tests
1. Configure the environment variables in `run.sh`:
   - PostgreSQL connection details (`PG_HOST`, `PG_PORT`, etc.).
   - Kafka settings (`BOOTSTRAP_SERVER_URL`, `KAFKA_TOPIC`, etc.).
   - Iceberg settings (`ICEBERG_WAREHOUSE`, `ICEBERG_TABLE`, etc.).
2. Choose a validation test by setting the `TEST_TYPE` variable in `run.sh`:
   - `row_count`: Validate row counts.
   - `checksum`: Validate data consistency using checksums.
   - `sample_data`: Validate data using random samples.
3. Run the script:
   ```bash
   bash run.sh
   ```

### Running Individual Scripts
- **Debugging Iceberg Tables**:
  ```bash
  python3 debug.py
  ```
- **Validation Tests**:
  ```bash
  python3 validation.py --test <test_type> --pg-host <host> --pg-port <port> --pg-db <db> \
    --pg-user <user> --pg-password <password> --pg-table <table> \
    --kafka-servers <servers> --kafka-topic <topic> --kafka-group <group> \
    --iceberg-warehouse <warehouse> --iceberg-table <table> \
    --s3-access-key <access_key> --s3-secret-key <secret_key> \
    --external-ip <external_ip> --timestamp-column <timestamp_column> \
    --columns <columns> --sample-size <sample_size>
  ```

## Validation Methods

### Row Count Validation
Compares the total number of rows in PostgreSQL and Iceberg tables. Ensures that the data volume matches within a specified tolerance.

### Checksum Validation
Calculates and compares checksums for specified columns in PostgreSQL and Iceberg tables to ensure data consistency.

### Sample Data Validation
Randomly samples records from PostgreSQL and Iceberg tables and compares their values for specified columns.

## Logs
Validation results and errors are logged to `data_validation.log`.

## Troubleshooting
- Ensure all environment variables are correctly set.
- Verify connectivity to PostgreSQL, Kafka, and Iceberg.
- Check the logs for detailed error messages.

## License
This project is licensed under the MIT License.
