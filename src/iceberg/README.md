# Iceberg CDC Environment Setup

This guide provides detailed instructions for setting up the Iceberg CDC environment, including PostgreSQL, S3 bucket, and Iceberg REST catalog configuration.

## Overview

The Iceberg CDC environment enables efficient data lake operations with support for Change Data Capture (CDC). It integrates with Kubernetes, Rook-Ceph for S3-compatible storage, and PostgreSQL for metadata management.

## Prerequisites

- Kubernetes cluster with at least 3 nodes
- `kubectl` CLI installed and configured
- Rook-Ceph storage configured in the cluster
- Storage class `standard-rwo` available in your Kubernetes cluster

## Architecture

- **PostgreSQL**: Metadata store for Iceberg tables
- **S3 Bucket**: Storage backend for Iceberg tables
- **Iceberg REST Catalog**: REST API for managing Iceberg tables

## Setup Instructions

### 1. Deploy PostgreSQL

Deploy a PostgreSQL instance for Iceberg metadata:

```bash
kubectl apply -f postgres.yaml
```

- Namespace: `iceberg`
- Database: `iceberg`
- User: `iceberg`
- Password: `icebergpassword`

Verify the deployment:

```bash
kubectl -n iceberg get pods
kubectl -n iceberg get svc postgres
```

### 2. Create S3 Bucket

Create an S3-compatible bucket using Rook-Ceph:

```bash
kubectl apply -f bucket.yaml
```

Retrieve the bucket details:

```bash
kubectl -n iceberg get obc
```

Extract the S3 credentials:

```bash
export S3_ACCESS_KEY_B64=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_ACCESS_KEY_ID}')
export S3_SECRET_KEY_B64=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}')
```

### 3. Configure Iceberg REST Catalog

Update and Deploy the `rest-catalog.yaml` file with the S3 credentials:

```bash
kubectl apply -f <(sed -e 's|AWS_ACCESS_KEY_ID:.*|AWS_ACCESS_KEY_ID: '"$S3_ACCESS_KEY_B64"'|' -e 's|AWS_SECRET_ACCESS_KEY:.*|AWS_SECRET_ACCESS_KEY: '"$S3_SECRET_KEY_B64"'|' rest-catalog.yaml)
```

Verify the deployment:

```bash
kubectl -n iceberg get pods
kubectl -n iceberg get svc iceberg-rest-catalog
```

### 4. Test Iceberg REST Catalog

Retrieve the external IP of the REST catalog service:

```bash
export EXTERNAL_IP=$(kubectl get svc nginx-ingress-ingress-nginx-controller -n ingress-nginx -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
```

Use the following commands to test the Iceberg REST catalog:

#### Create a Namespace

```bash
curl -X POST -H "Content-Type: application/json" \
  http://$EXTERNAL_IP/v1/namespaces \
  -d '{"namespace": ["example_namespace"]}'
```

#### Create a Table

```bash
curl -X POST -H "Content-Type: application/json" \
  http://$EXTERNAL_IP/v1/namespaces/example_namespace/tables \
  -d '{
    "name": "example_table",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "id", "type": "int", "required": true},
        {"id": 2, "name": "name", "type": "string", "required": false}
      ]
    },
    "properties": {
      "write.format.default": "parquet"
    }
  }'
```

### 5. Monitor the Environment

Monitor the status of the deployed components:

```bash
kubectl -n iceberg get all
```

## Configuration Details

### PostgreSQL Configuration

- Persistent storage: 10Gi
- Storage class: `standard-rwo`
- Service: Exposed on port `5432`

### S3 Bucket Configuration

- Bucket name: `iceberg-warehouse`
- Storage class: `rook-ceph-delete-bucket`

### Iceberg REST Catalog Configuration

- Image: `tabulario/iceberg-rest:1.6.0`
- REST port: `8181`
- S3 endpoint: `http://rook-ceph-rgw-my-store.rook-ceph.svc:80`
- Metadata store: PostgreSQL

## Cleanup

To delete all resources:

```bash
kubectl delete namespace iceberg
```

## Troubleshooting

### Common Issues

1. **PostgreSQL not starting**: Check the PVC and storage class configuration.
   ```bash
   kubectl -n iceberg describe pvc postgres-pvc
   kubectl -n iceberg logs -l app=postgres
   ```

2. **S3 bucket not created**: Verify the Rook-Ceph configuration.
   ```bash
   kubectl -n rook-ceph get cephobjectstore
   kubectl -n rook-ceph logs -l app=rook-ceph-rgw
   ```

3. **REST catalog not accessible**: Check the service and ingress configuration.
   ```bash
   kubectl -n iceberg get svc iceberg-rest-catalog
   kubectl -n ingress-nginx get svc
   ```

## Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Rook Documentation](https://rook.io/docs/rook/latest/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
