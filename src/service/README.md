# Lakehouse CDC Demo - FastAPI Service

This project demonstrates a Change Data Capture (CDC) pattern with a PostgreSQL database and a FastAPI application deployed on Kubernetes.

## Prerequisites

- Docker
- Kubernetes cluster (Minikube, Docker Desktop, or a cloud provider)
- kubectl
- Skaffold

## Project Structure

```
lakehouse-cdc-demo/src/service/
├── app/
│   ├── Dockerfile
│   ├── database.py
│   ├── main.py
│   ├── models.py
│   └── requirements.txt
├── k8s/
│   ├── deployment.yaml
│   ├── postgres.yaml
│   └── service.yaml
└── skaffold.yaml
```

## Quick Start

### 1. Set your Docker image

You must set your own Docker image using the `IMAGE_NAME` environment variable:

```bash
# Set your custom image
export IMAGE_NAME=yourusername/yourimage
```

### 2. Deploy with Skaffold

```bash
# Run skaffold with your image
skaffold run
```

## Accessing the API

The FastAPI application exposes the following endpoints:

- `GET /items`: List all items
- `GET /item/{item_id}`: Get an item by ID
- `POST /items`: Create a new item
- `PUT /item/{item_id}`: Update an item
- `DELETE /item/{item_id}`: Delete an item

You can access the API through the Kubernetes NodePort service on port 8000.

## Database Configuration

The PostgreSQL database is configured with:

- Logical replication enabled (wal_level = logical)
- Credentials stored in Kubernetes secrets
- Data persisted using PersistentVolumeClaims

## Development

### Building the Docker image

```bash
cd app
docker build -t yourusername/cdc-app .
docker push yourusername/cdc-app
```

### Custom Configuration

You can modify database connection parameters in the deployment manifest:

- `POSTGRES_HOST`: PostgreSQL service hostname
- `POSTGRES_USER`: Database username
- `POSTGRES_PASSWORD`: Database password
- `POSTGRES_DB`: Database name

## Troubleshooting

If you encounter issues with the deployment:

1. Check pod status: `kubectl get pods`
2. View pod logs: `kubectl logs <pod-name>`
3. Check services: `kubectl get svc`
4. Ensure the database is ready: `kubectl exec -it <postgres-pod> -- pg_isready -U postgres`
