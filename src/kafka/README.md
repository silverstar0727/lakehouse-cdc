# Kafka CDC (Change Data Capture) Demo

This project demonstrates how to set up a Change Data Capture (CDC) pipeline using Kafka, Debezium, and Strimzi Kafka Operator on Kubernetes.

## Architecture Overview

The setup consists of the following components:
- Apache Kafka cluster deployed with Strimzi Operator in KRaft mode
- Kafka Connect with Debezium for CDC
- Kafdrop for Kafka topic monitoring and management

## Folder Structure

```
src/kafka/
├── deployment-connector/
│   ├── create_source_connector.py
│   ├── create_sink_connector.py
│   ├── kafka-deployment.yaml
│   └── run.sh
├── kafdrop.yaml
├── strimzi.yaml
└── README.md
```

## Prerequisites

- Kubernetes cluster (GKE recommended)
- kubectl CLI
- Helm v3
- Python 3.x with `requests` library installed

## Setup Instructions

### 1. Deploy Strimzi Kafka Operator

```bash
# Add Strimzi Helm repository
helm repo add strimzi https://strimzi.io/charts
helm repo update

# Install Strimzi Kafka Operator
helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator --version 0.44.0 \
  --namespace strimzi-kafka --create-namespace
```

### 2. Deploy Kafka Cluster

The Kafka cluster is deployed in KRaft mode using Strimzi's node pools feature:

```bash
# Deploy Kafka cluster configuration
kubectl apply -f strimzi.yaml
```

This will create a 3-node Kafka cluster with the following characteristics:
- KRaft mode (no Zookeeper)
- 3 replicas for high availability
- Persistent storage using standard storage class
- Internal and external listeners

### 3. Deploy Kafka Connect and Kafdrop

#### Deploy Kafka Connect

To deploy Kafka Connect, update the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the `kafka-deployment.yaml` file dynamically using the following command:

```bash
export S3_ACCESS_KEY_B64=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.AccessKey}')
export S3_SECRET_KEY_B64=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.SecretKey}')

kubectl apply -f <(sed -e 's|AWS_ACCESS_KEY_ID:.*|AWS_ACCESS_KEY_ID: '"$S3_ACCESS_KEY_B64"'|' \
-e 's|AWS_SECRET_ACCESS_KEY:.*|AWS_SECRET_ACCESS_KEY: '"$S3_SECRET_KEY_B64"'|' deployment-connector/kafka-deployment.yaml)
```

#### Deploy Kafdrop

```bash
kubectl apply -f kafdrop.yaml
```

### 4. Set Environment Variables for AWS Credentials

Retrieve and decode the AWS credentials for the Iceberg warehouse:

```bash
export AWS_ACCESS_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 --decode)
export AWS_SECRET_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 --decode)
```

### 5. Create Connectors

#### PostgreSQL Source Connector

Run the following command to create the PostgreSQL source connector:

```bash
export EXTERNAL_IP=$(kubectl get service -n ingress-nginx nginx-ingress-ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
python deployment-connector/create_source_connector.py
```

#### Iceberg Sink Connector

Run the following command to create the Iceberg sink connector:

```bash
export EXTERNAL_IP=$(kubectl get service -n ingress-nginx nginx-ingress-ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
python deployment-connector/create_sink_connector.py
```

### 6. Check Sink Connector Status

Verify the status of the Iceberg sink connector:

```bash
curl -X GET http://kafka-connect.${EXTERNAL_IP}.nip.io/connectors/iceberg-sink/status
```

## Access the Services

### Kafdrop UI

To access Kafdrop UI, use the configured ingress:

```bash
# Get your external IP or domain
export EXTERNAL_IP=$(kubectl get service -n ingress-nginx nginx-ingress-ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "Access Kafdrop at http://kafdrop.${EXTERNAL_IP}.nip.io"
```

### Kafka Connect REST API

Access Kafka Connect using the configured ingress:

```bash
# Get your external IP or domain
export EXTERNAL_IP=$(kubectl get service -n ingress-nginx nginx-ingress-ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "Access Kafka Connect at http://kafka-connect.${EXTERNAL_IP}.nip.io"
```

## Monitoring & Management

### Using Kafdrop

Access Kafdrop through the ingress URL to:
- View Kafka topics, brokers, and consumers
- Examine messages in topics
- View topic configurations

```bash
# Get your external IP or domain for Kafdrop
echo "Open your browser at: http://kafdrop.${EXTERNAL_IP}.nip.io"
```

### Check Kafka Connect Logs

```bash
kubectl logs -f -n strimzi-kafka deployment/kafka-connect
```

## Cleanup

```bash
# Delete all resources
kubectl delete -f kafdrop.yaml
kubectl delete -f deployment-connector/kafka-deployment.yaml
kubectl delete -f strimzi.yaml

# Delete Strimzi operator
helm uninstall strimzi-kafka-operator -n strimzi-kafka
```
