# Kubernetes Ingress Controller Setup

This directory contains configuration files for setting up the NGINX Ingress Controller in a Kubernetes cluster. The Ingress Controller provides external access to services within the cluster through HTTP/HTTPS routes.

## Overview

The setup includes:

- NGINX Ingress Controller installation via Helm
- Ingress resources for various services across different namespaces
- Automatic domain configuration using nip.io DNS service

## Manual Installation

Since the run.sh script is no longer available, follow these steps to install and configure the NGINX Ingress Controller:

1. Add the NGINX Ingress Helm repository:
```bash
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update
```

2. Install the NGINX Ingress Controller:
```bash
helm install nginx-ingress ingress-nginx/ingress-nginx --namespace ingress-nginx --create-namespace
```

3. View the installed services and wait for the external IP to be assigned:
```bash
kubectl get service -n ingress-nginx
kubectl get service nginx-ingress-ingress-nginx-controller -n ingress-nginx
```

4. Get the external IP address assigned to the controller:
```bash
export EXTERNAL_IP=$(kubectl get svc nginx-ingress-ingress-nginx-controller -n ingress-nginx -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
```

5. Apply the Ingress configuration with the correct URL:
```bash
sed "s/\${EXTERNAL_IP}/$EXTERNAL_IP/g" nginx-ingress.yaml | kubectl apply -f -
```

## Available Services

After installation, the following services will be accessible:

| Service | EXTERNAL_IP | Description |
|---------|-----|-------------|
| FastAPI Application | http://app.${EXTERNAL_IP}.nip.io | Main application API |
| Locust | http://locust.${EXTERNAL_IP}.nip.io | Load testing web interface |
| Kafdrop | http://kafdrop.${EXTERNAL_IP}.nip.io | Kafka cluster management UI |
| Kafka Connect | http://kafka-connect.${EXTERNAL_IP}.nip.io | Kafka Connect REST API |
| Ceph Object Storage | http://ceph.${EXTERNAL_IP}.nip.io | S3-compatible storage interface |
| Iceberg REST Catalog | http://iceberg-rest-catalog.${EXTERNAL_IP}.nip.io | Iceberg catalog REST interface |

Replace `${EXTERNAL_IP}` with the actual IP address assigned to your Ingress Controller.

## Configuration

The Ingress resources are defined in the `nginx-ingress.yaml` file, which is processed to substitute the external IP address. The Ingress resources are configured in the following namespaces:

- `default`: FastAPI application and Locust
- `strimzi-kafka`: Kafdrop and Kafka Connect
- `rook-ceph`: Ceph Object Storage
- `iceberg`: Iceberg REST Catalog

## Verification

To verify the installation and configuration, run:

```bash
kubectl get ingress --all-namespaces
```

This will show all configured Ingress resources and their associated hostnames.
