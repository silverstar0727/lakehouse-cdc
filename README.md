# Lakehouse CDC (Change Data Capture) Demo
> A Kubernetes-based data pipeline that captures PostgreSQL database changes in real-time and streams them to Apache Iceberg via Kafka and S3.

This project demonstrates a complete end-to-end Change Data Capture (CDC) pipeline for a data lakehouse architecture deployed on Kubernetes. The architecture captures database changes in real-time and stores them in an Apache Iceberg data lakehouse.

## Architecture Overview
The setup consists of the following components:

1. **Data Source**: FastAPI application with PostgreSQL database
2. **Change Data Capture**: Kafka and Debezium for CDC
3. **Data Storage**: Rook-Ceph providing S3-compatible object storage
4. **Data Lakehouse**: Apache Iceberg with PostgreSQL catalog
5. **Ingress**: NGINX Ingress Controller for external access
6. **Load Testing**: Locust for generating test load

## Prerequisites

- Kubernetes cluster (GKE recommended)
- kubectl configured to access your cluster
- Helm v3
- Docker
- Skaffold (for development workflow)

## Deployment Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/silverstar0727/lakehouse-cdc.git
cd lakehouse-cdc-demo
```

### 2. Deploy Rook-Ceph Object Storage

First, deploy Ceph as the underlying S3-compatible storage:

```bash
cd src/ceph

# Create required roles
kubectl create clusterrole rook-endpointslice-role --verb=get,list,watch,create,update,delete --resource=endpointslices.discovery.k8s.io
kubectl create clusterrolebinding rook-endpointslice-binding --clusterrole=rook-endpointslice-role --serviceaccount=rook-ceph:rook-ceph-system

# Deploy Rook operator and Ceph cluster
kubectl apply -f crds.yaml -f common.yaml -f operator.yaml
kubectl apply -f cluster-gke.yaml
kubectl apply -f object-gke.yaml

# Wait for deployment to complete
kubectl -n rook-ceph get cephclusters -w
```

### 3. Deploy Ingress Controller

```bash
cd ../ingress

# Add and install NGINX Ingress Controller
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update
helm install nginx-ingress ingress-nginx/ingress-nginx --namespace ingress-nginx --create-namespace

# Get the external IP
export EXTERNAL_IP=$(kubectl get svc nginx-ingress-ingress-nginx-controller -n ingress-nginx -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# Apply Ingress configuration
sed "s/\${EXTERNAL_IP}/$EXTERNAL_IP/g" nginx-ingress.yaml | kubectl apply -f -
```

### 4. Deploy Kafka and Kafka Connect with Debezium

```bash
cd ../kafka

# Add and install Strimzi Kafka Operator
helm repo add strimzi https://strimzi.io/charts
helm repo update
helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator --version 0.44.0 \
  --namespace strimzi-kafka --create-namespace

# Deploy Kafka cluster, Kafka Connect, and Kafdrop
kubectl apply -f strimzi.yaml
kubectl apply -f kafka-deployment.yaml
kubectl apply -f kafdrop.yaml
```

### 5. Deploy Iceberg Data Lakehouse

```bash
cd ../iceberg

# Get S3 credentials from Ceph
export S3_ACCESS_KEY=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.AccessKey}' | base64 --decode)
export S3_SECRET_KEY=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.SecretKey}' | base64 --decode)

# Create base64 encoded versions for the secret
export S3_ACCESS_KEY_B64=$(echo -n "$S3_ACCESS_KEY" | base64)
export S3_SECRET_KEY_B64=$(echo -n "$S3_SECRET_KEY" | base64)

# Deploy PostgreSQL and Iceberg REST catalog
kubectl apply -f postgres.yaml
kubectl apply -f <(sed -e 's|s3AccessKey:.*|s3AccessKey: '"$S3_ACCESS_KEY_B64"'|' -e 's|s3SecretKey:.*|s3SecretKey: '"$S3_SECRET_KEY_B64"'|' rest-catalog.yaml)
```

### 6. Deploy FastAPI Service and PostgreSQL Database

```bash
cd ../service

# Set your Docker image
export IMAGE_REGISTRY=<yourusername>

# Deploy with Skaffold
skaffold run --default-repo $IMAGE_REGISTRY
```

### 7. Set Up Debezium Connector

```bash
# Create PostgreSQL CDC Connector
export EXTERNAL_IP=$(kubectl get service -n ingress-nginx ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

cd ../kafka
pip install requests
python create_connector.py
```

### 8. Deploy Load Testing Framework (Optional)

```bash
cd ../locust

# Deploy with Skaffold
skaffold run --default-repo $IMAGE_REGISTRY
```

## Accessing the Services

After deployment, the following services will be accessible:

| Service | EXTERNAL_IP | Description |
|---------|-----|-------------|
| FastAPI Application | http://app.${EXTERNAL_IP}.nip.io | Main application API |
| Locust | http://locust.${EXTERNAL_IP}.nip.io | Load testing web interface |
| Kafdrop | http://kafdrop.${EXTERNAL_IP}.nip.io | Kafka cluster management UI |
| Kafka Connect | http://kafka-connect.${UEXTERNAL_IPRL}.nip.io | Kafka Connect REST API |
| Ceph Object Storage | http://ceph.${EXTERNAL_IP}.nip.io | S3-compatible storage interface |
| Iceberg REST Catalog | http://iceberg-rest-catalog.${EXTERNAL_IP}.nip.io | Iceberg catalog REST interface |

Replace `${EXTERNAL_IP}` with the actual IP address assigned to your Ingress Controller.

## Using the Demo

1. **Generate Data**: Use the FastAPI service to create, update, and delete items, or use Locust to generate load
2. **Monitor CDC Pipeline**: Use Kafdrop to observe the CDC events flowing through Kafka
3. **View Iceberg Tables**: Connect to the Iceberg REST Catalog to access the CDC data in the lakehouse

### Example API Operations

```bash
# Set the API URL
export API_URL=http://app.${EXTERNAL_IP}.nip.io

# Create an item
curl -X POST $API_URL/items -H "Content-Type: application/json" -d '{"name":"Test Item","description":"This is a test item","price":29.99}'

# Get all items
curl -X GET $API_URL/items

# Update an item
curl -X PUT $API_URL/item/1 -H "Content-Type: application/json" -d '{"name":"Updated Item","description":"This is an updated item","price":39.99}'

# Delete an item
curl -X DELETE $API_URL/item/1
```

## Component Details

### Rook-Ceph Object Storage

- S3-compatible object storage for the data lakehouse
- Configuration details in [src/ceph/README.md](src/ceph/README.md)

### NGINX Ingress Controller

- External access to services via HTTP/HTTPS
- Configuration details in [src/ingress/README.md](src/ingress/README.md)

### Kafka and Debezium

- Kafka deployed with Strimzi in KRaft mode
- Debezium for PostgreSQL CDC
- Configuration details in [src/kafka/README.md](src/kafka/README.md)

### Iceberg Data Lakehouse

- Apache Iceberg with PostgreSQL as catalog backend
- REST interface for table operations
- Configuration details in [src/iceberg/README.md](src/iceberg/README.md)

### FastAPI Service

- Sample application with PostgreSQL database
- CRUD operations for items
- Configuration details in [src/service/README.md](src/service/README.md)

### Locust Load Testing

- Generate realistic loads on the FastAPI service
- Web interface for test control and monitoring
- Configuration details in [src/locust/README.md](src/locust/README.md)

## Monitoring and Troubleshooting

### Monitoring Components

- Use Kafdrop for Kafka topic monitoring
- Ceph dashboard for storage monitoring
- Kubernetes standard tools for pod and service monitoring

### Common Issues and Solutions

1. **CDC connector not starting**:
   - Check connector logs: `kubectl logs -f -n strimzi-kafka deployment/kafka-connect`
   - Verify PostgreSQL config: `kubectl exec -it <postgres-pod> -- psql -U postgres -c "SHOW wal_level;"`

2. **Object storage issues**:
   - Check Ceph status: `kubectl -n rook-ceph get cephclusters`
   - Examine object store: `kubectl -n rook-ceph get cephobjectstore`

3. **Ingress problems**:
   - Check Ingress status: `kubectl get ingress --all-namespaces`
   - Examine Ingress controller logs: `kubectl logs -n ingress-nginx deployment/nginx-ingress-ingress-nginx-controller`

4. **Iceberg catalog issues**:
   - Verify REST catalog: `kubectl logs -n iceberg statefulset/iceberg-rest-catalog`
   - Check PostgreSQL connectivity: `kubectl -n iceberg exec -it iceberg-postgres-0 -- psql -U iceberg -d iceberg`

## Cleanup

To remove the entire setup:

```bash
# Clean up Locust
cd src/locust
skaffold delete

# Clean up FastAPI service
cd ../service
skaffold delete

# Clean up Iceberg
cd ../iceberg
kubectl delete -f rest-catalog.yaml
kubectl delete -f postgres.yaml

# Clean up Kafka
cd ../kafka
kubectl delete -f kafdrop.yaml
kubectl delete -f kafka-deployment.yaml
kubectl delete -f strimzi.yaml
helm uninstall strimzi-kafka-operator -n strimzi-kafka

# Clean up Ingress
cd ../ingress
kubectl delete -f nginx-ingress.yaml
helm uninstall nginx-ingress -n ingress-nginx

# Clean up Rook-Ceph
cd ../ceph
kubectl delete -f lb-gke.yaml
kubectl delete -f object-gke.yaml
kubectl delete -f cluster-gke.yaml
kubectl delete -f operator.yaml -f common.yaml -f crds.yaml
```

## Security Considerations

Note: This demo uses default credentials and configuration suitable for development environments only. For production deployments:

- Change all default passwords
- Configure TLS/SSL for service communication
- Implement proper authentication and authorization
- Restrict access to management interfaces
- Consider network policies for inter-service communication

## Additional Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Debezium Documentation](https://debezium.io/documentation/)
- [Strimzi Kafka Operator Documentation](https://strimzi.io/docs/)
- [Rook Ceph Documentation](https://rook.io/docs/rook/latest/)
