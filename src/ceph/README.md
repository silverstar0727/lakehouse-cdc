# Ceph Object Storage on GKE

This guide explains how to set up a Rook Ceph cluster on Google Kubernetes Engine (GKE) to provide S3-compatible object storage for data lake applications.

## Overview

The setup creates a Ceph cluster with S3-compatible object storage using Rook operator on GKE. This provides a self-contained storage solution for applications that require S3 API access, such as data lake architectures, CDC (Change Data Capture) pipelines, or analytics workloads.

## Architecture

- **Rook Operator**: Manages the Ceph cluster deployment and lifecycle
- **Ceph Cluster**: Configured for GKE with SSD-based storage
- **Object Storage**: S3-compatible interface via Ceph RGW (RADOS Gateway)
- **Storage**: Uses GKE's persistent volumes for Ceph OSDs (Object Storage Daemons)

## Prerequisites

- GKE cluster with at least 3 nodes
- `kubectl` configured to access your GKE cluster
- Node sizes with sufficient resources (recommended: at least 4 CPU, 16GB RAM per node)
- Storage class `standard-rwo` available in your GKE cluster

## Installation

### 1. Deploy Rook Operator

First, install the Rook CRDs, common resources, and the Rook operator:

```bash
kubectl create clusterrole rook-endpointslice-role --verb=get,list,watch,create,update,delete --resource=endpointslices.discovery.k8s.io
kubectl create clusterrolebinding rook-endpointslice-binding --clusterrole=rook-endpointslice-role --serviceaccount=rook-ceph:rook-ceph-system

kubectl apply -f crds.yaml -f common.yaml -f operator.yaml
```

### 2. Deploy Ceph Cluster

Deploy the Ceph cluster configured for GKE:

```bash
kubectl apply -f cluster-gke.yaml
```

Monitor the deployment:

```bash
kubectl -n rook-ceph get po -w
kubectl -n rook-ceph get cephclusters -w
```

### 3. Deploy Object Storage Service

Deploy the Ceph Object Store (S3-compatible storage):

```bash
kubectl apply -f object-gke.yaml
```

Monitor the object store creation:

```bash
kubectl -n rook-ceph get cephobjectstore -w
```

### 4. Get S3 Access Keys

Retrieve the S3 access credentials:

```bash
export S3_ACCESS_KEY=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.AccessKey}' | base64 --decode)
export S3_SECRET_KEY=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.SecretKey}' | base64 --decode)
echo "ACCESS_KEY: $S3_ACCESS_KEY"
echo "SECRET_KEY: $S3_SECRET_KEY"
```

## Configuration Details

### Ceph Cluster Configuration

The Ceph cluster is configured specifically for GKE:

- Using `standard-rwo` storage class for persistent storage
- 1 monitor with 10Gi storage
- 2 managers for high availability
- 3 OSDs with 20Gi storage each
- Dashboard enabled for monitoring

### Object Store Configuration

The object store is configured with:

- Simplified setup with replication factor of 1 for development environments
- Standard metadata and data pools
- S3-compatible API through RADOS Gateway
- Exposed via NodePort service for external access

## Usage

### Connecting with S3 Clients

Use the following parameters to connect to your S3-compatible storage:

- **Endpoint**: Get the endpoint URL with:
  ```bash
  kubectl -n rook-ceph get svc rook-ceph-rgw-my-store-external -o jsonpath='{.status.loadBalancer.ingress[0].ip}:{.spec.ports[0].port}'
  ```
- **Access Key**: Use the ACCESS_KEY retrieved earlier
- **Secret Key**: Use the SECRET_KEY retrieved earlier
- **Region**: Leave empty or use `us-east-1`
- **Path Style**: Enable path-style S3 URLs (not virtual-hosted style)

### Python Example

```python
import boto3

s3_client = boto3.client(
    's3',
    endpoint_url='http://YOUR_ENDPOINT_IP:80',
    aws_access_key_id='YOUR_ACCESS_KEY',
    aws_secret_access_key='YOUR_SECRET_KEY',
    config=boto3.session.Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# Create a bucket
s3_client.create_bucket(Bucket='my-test-bucket')

# List buckets
response = s3_client.list_buckets()
for bucket in response['Buckets']:
    print(bucket['Name'])
```

## Monitoring

Access the Ceph dashboard to monitor the health of your cluster:

1. Get the dashboard URL:
   ```bash
   kubectl -n rook-ceph get svc
   ```

2. For dashboard credentials:
   ```bash
   kubectl -n rook-ceph get secret rook-ceph-dashboard-password -o jsonpath="{['data']['password']}" | base64 --decode
   ```

## Cleanup

To remove the entire Ceph deployment:

```bash
kubectl delete -f lb-gke.yaml
kubectl delete -f object-gke.yaml
kubectl delete -f cluster-gke.yaml
kubectl delete -f operator.yaml -f common.yaml -f crds.yaml
```

## Troubleshooting

### Common Issues

1. **OSDs not starting**: Check if the storage class exists and has proper permissions
   ```bash
   kubectl get sc
   kubectl -n rook-ceph describe pods -l app=rook-ceph-osd
   ```

2. **Object store not ready**: Verify the status and logs
   ```bash
   kubectl -n rook-ceph get cephobjectstore my-store -o yaml
   kubectl -n rook-ceph logs -l app=rook-ceph-rgw
   ```

3. **Connection issues**: Ensure the service is exposed properly
   ```bash
   kubectl -n rook-ceph get svc rook-ceph-rgw-my-store-external
   ```

## Resources

- [Rook Documentation](https://rook.io/docs/rook/latest/)
- [Ceph Documentation](https://docs.ceph.com/)
- [S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)
