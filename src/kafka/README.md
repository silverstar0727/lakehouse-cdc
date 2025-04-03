# Kafka CDC (Change Data Capture) Demo

This project demonstrates how to set up a Change Data Capture (CDC) pipeline using Kafka, Debezium, and Strimzi Kafka Operator on Kubernetes.

## Architecture Overview

The setup consists of the following components:
- Apache Kafka cluster deployed with Strimzi Operator in KRaft mode
- Kafka Connect with Debezium for CDC
- Kafdrop for Kafka topic monitoring and management

## Prerequisites

- Kubernetes cluster (GKE recommended)
- kubectl CLI
- Helm v3

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

```bash
# Deploy Kafka Connect with Debezium
kubectl apply -f kafka-deployment.yaml

# Deploy Kafdrop for Kafka monitoring
kubectl apply -f kafdrop.yaml
```

### 4. Access the Services

#### Kafdrop UI

To access Kafdrop UI, use the configured ingress:

```bash
# Get your external IP or domain
export URL=$(kubectl get service -n ingress-nginx ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "Access Kafdrop at http://kafdrop.${URL}.nip.io"
```

#### Kafka Connect REST API

Access Kafka Connect using the configured ingress:

```bash
# Get your external IP or domain
export URL=$(kubectl get service -n ingress-nginx ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "Access Kafka Connect at http://kafka-connect.${URL}.nip.io"
```

## Setting Up Connectors

### List installed connectors

```bash
export EXTERNAL_IP=$(kubectl get service -n ingress-nginx ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl -X GET http://kafka-connect.${EXTERNAL_IP}.nip.io/connector-plugins | jq
```

### Create PostgreSQL CDC Connector

```bash
export EXTERNAL_IP=$(kubectl get service -n ingress-nginx ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

pip install requests
python create_connector.py
```

### List active connectors

```bash
# Using ingress
curl -X GET http://kafka-connect.${EXTERNAL_IP}.nip.io/connectors
```

### Check connector status

```bash
# Using ingress
curl -X GET http://kafka-connect.${EXTERNAL_IP}.nip.io/connectors/postgres-connector/status
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

### Check Kafka Connect status

```bash
kubectl logs -f -n strimzi-kafka deployment/kafka-connect
```

## Troubleshooting

### Common Issues

1. **Connector not starting**:
   - Check connector logs: `kubectl logs -f -n strimzi-kafka deployment/kafka-connect`
   - Verify database credentials and connectivity
   - Ensure proper permissions for the database user

2. **Topics not appearing**:
   - Verify Kafka broker connectivity
   - Check connector status and configuration

3. **Kafka Connect fails to start**:
   - Ensure Kafka cluster is healthy: `kubectl get pods -n strimzi-kafka`
   - Check that storage configuration is correct
   - Verify resource allocations are sufficient

### Restart components

```bash
# Restart Kafka Connect
kubectl rollout restart deployment/kafka-connect -n strimzi-kafka

# Check Kafka cluster health
kubectl get kafkas -n strimzi-kafka
kubectl get pods -n strimzi-kafka
```

## Cleanup

```bash
# Delete all resources
kubectl delete -f kafdrop.yaml
kubectl delete -f kafka-deployment.yaml
kubectl delete -f strimzi.yaml

# Delete Strimzi operator
helm uninstall strimzi-kafka-operator -n strimzi-kafka
```
