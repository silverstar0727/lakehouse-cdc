#!/bin/bash


export BOOTSTRAP_SERVER_URL=$(kubectl get service -n strimzi-kafka kraft-cluster-kafka-external-bootstrap -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

export OBC_ACCESS_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 --decode)
export OBC_SECRET_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 --decode)

export EXTERNAL_IP=$(kubectl get service -n ingress-nginx nginx-ingress-ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

export PG_HOST=$(kubectl get service -n default backend-postgres -o jsonpath='{.status.loadBalancer.ingress[0].ip}')


# PostgreSQL 정보 설정
PG_HOST="$PG_HOST"
PG_PORT="5432"
PG_DB="postgres"
PG_USER="postgres"
PG_PASSWORD="12341234" 
PG_TABLE="items"

# Kafka 설정
KAFKA_SERVERS="$BOOTSTRAP_SERVER_URL:9094"
KAFKA_TOPIC="postgres-connector.public.items"
KAFKA_GROUP="validation-group"

# Iceberg 설정
ICEBERG_WAREHOUSE="s3a://iceberg-warehouse/"
ICEBERG_TABLE="fastapi_db.items"
S3_ACCESS_KEY="$OBC_ACCESS_KEY"
S3_SECRET_KEY="$OBC_SECRET_KEY"
EXTERNAL_IP="$EXTERNAL_IP"

# Debezium 및 Kafka Connect 설정
DEBEZIUM_URL="http://kafka-connect.$EXTERNAL_IP.nip.io"
CONNECTOR_NAME="postgres-connector"
KAFKA_CONNECT_URL="http://kafka-connect.$EXTERNAL_IP.nip.io"

# 검증 설정
TIMESTAMP_COLUMN="created_at"
COLUMNS="id,name,description,price,on_offer"
SAMPLE_SIZE="100"

# 테스트 유형 선택 (주석 해제하여 사용)
TEST_TYPE="row_count"
# TEST_TYPE="checksum"
# TEST_TYPE="sample_data"
# TEST_TYPE="replication_lag"
# TEST_TYPE="combined_lag"
# TEST_TYPE="connector_status"
# TEST_TYPE="iceberg_health"
# TEST_TYPE="validation_suite"

# 데이터 검증 스크립트 실행
python3 validation.py --test $TEST_TYPE \
  --pg-host $PG_HOST \
  --pg-port $PG_PORT \
  --pg-db $PG_DB \
  --pg-user $PG_USER \
  --pg-password $PG_PASSWORD \
  --pg-table $PG_TABLE \
  --kafka-servers $KAFKA_SERVERS \
  --kafka-topic $KAFKA_TOPIC \
  --kafka-group $KAFKA_GROUP \
  --iceberg-warehouse $ICEBERG_WAREHOUSE \
  --iceberg-table $ICEBERG_TABLE \
  --s3-access-key $S3_ACCESS_KEY \
  --s3-secret-key $S3_SECRET_KEY \
  --external-ip $EXTERNAL_IP \
  --debezium-url $DEBEZIUM_URL \
  --connector-name $CONNECTOR_NAME \
  --kafka-connect-url $KAFKA_CONNECT_URL \
  --timestamp-column $TIMESTAMP_COLUMN \
  --columns $COLUMNS \
  --sample-size $SAMPLE_SIZE