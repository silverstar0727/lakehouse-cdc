# Apache Iceberg Lakehouse with Change Data Capture (CDC)

This project sets up an Apache Iceberg data lakehouse environment with PostgreSQL as the catalog backend and Rook-Ceph providing S3 object storage. The setup is containerized using Kubernetes.

## Architecture Overview

The setup consists of the following components:

- **PostgreSQL**: Serves as the catalog database for Iceberg tables metadata
- **Iceberg REST Catalog**: Provides a REST API for Iceberg table operations
- **Rook-Ceph S3**: Object storage for the actual data in the lakehouse

## Prerequisites

- Kubernetes cluster
- Rook-Ceph installed and configured with S3 capabilities
- `kubectl` configured to access your cluster
- Storage class `standard-rwo` available in your cluster

## Setup Instructions

1. **Create the namespace and deploy PostgreSQL**

   The PostgreSQL instance will store Iceberg metadata:

   ```bash
   kubectl apply -f postgres.yaml
   ```

2. **Deploy the Iceberg REST Catalog service**

   This will set up the REST API endpoint for Iceberg operations:

   ```bash
   # Get current S3 credentials from Ceph
   export S3_ACCESS_KEY=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.AccessKey}' | base64 --decode)
   export S3_SECRET_KEY=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.SecretKey}' | base64 --decode)

   # Create base64 encoded versions for the secret
   export S3_ACCESS_KEY_B64=$(echo -n "$S3_ACCESS_KEY" | base64)
   export S3_SECRET_KEY_B64=$(echo -n "$S3_SECRET_KEY" | base64)

   kubectl apply -f <(sed -e 's|s3AccessKey:.*|s3AccessKey: '"$S3_ACCESS_KEY_B64"'|' -e 's|s3SecretKey:.*|s3SecretKey: '"$S3_SECRET_KEY_B64"'|' rest-catalog.yaml)
   ```

3. **Verify the deployment**

   ```bash
   kubectl -n iceberg get all
   ```

   You can monitor the pod startup process with:

   ```bash
   kubectl -n iceberg get all -w
   ```

## Component Details

### PostgreSQL Database

- **Service**: Available at `iceberg-postgres.iceberg.svc.cluster.local:5432`
- **Database name**: `iceberg`
- **User**: `iceberg`
- **Password**: `12341234` (stored as a Kubernetes secret)
- **Persistence**: 10Gi volume claim for database storage

### Iceberg REST Catalog

- **Image**: `tabulario/iceberg-rest:0.5.0`
- **Service**: Available at `iceberg-rest-catalog.iceberg.svc.cluster.local:8181`
- **Resources**: Requests 500m CPU and 512Mi memory, with limits of 2 CPU and 2Gi memory
- **JVM settings**: -Xmx1024m -Xms512m
- **Persistence**: 5Gi volume claim for cache storage

### S3 Storage with Rook-Ceph

- **Bucket name**: Automatically generated with prefix `iceberg-warehouse`
- **Access method**: Uses S3 endpoint, access key, and secret key from Rook-Ceph
- **Configuration**: Path style access with region `us-east-1`

## Managing S3 Credentials

If you need to update the S3 credentials (for example after Ceph credentials rotation):

```bash
# Get current S3 credentials from Ceph
export S3_ACCESS_KEY=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.AccessKey}' | base64 --decode)
export S3_SECRET_KEY=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.SecretKey}' | base64 --decode)

# Create base64 encoded versions for the secret
export S3_ACCESS_KEY_B64=$(echo -n "$S3_ACCESS_KEY" | base64)
export S3_SECRET_KEY_B64=$(echo -n "$S3_SECRET_KEY" | base64)

kubectl apply -f <(sed -e 's|s3AccessKey:.*|s3AccessKey: '"$S3_ACCESS_KEY_B64"'|' -e 's|s3SecretKey:.*|s3SecretKey: '"$S3_SECRET_KEY_B64"'|' rest-catalog.yaml)

# Restart the Iceberg REST Catalog pod for changes to take effect
kubectl -n iceberg rollout restart statefulset iceberg-rest-catalog
```

## Usage

To connect to the Iceberg REST Catalog from external applications, configure your client with:

- **Catalog URI**: `http://[CLUSTER-IP]:8181` (or use NodePort if accessing externally)
- **Warehouse location**: `s3://iceberg-warehouse`

## Security Notes

- Default credentials are used in this demo setup and should be changed in production
- The S3 access keys are stored as Kubernetes secrets
- The PostgreSQL password is stored as a Kubernetes secret

## Troubleshooting

- Check pod logs: `kubectl -n iceberg logs -f <pod-name>`
- Verify services are running: `kubectl -n iceberg get services`
- Ensure S3 bucket is properly created: `kubectl -n iceberg get objectbucketclaims`
