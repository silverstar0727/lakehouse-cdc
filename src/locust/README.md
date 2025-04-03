# Locust Load Testing for Lakehouse CDC Demo

This directory contains a Locust-based load testing framework to test the performance and behavior of the API under various traffic conditions.

## Overview

The setup uses:
- **Locust**: An open-source load testing tool
- **Kubernetes**: For distributed test execution
- **Skaffold**: For easy deployment and management of the load testing environment

## Prerequisites

- Docker installed and configured
- Kubernetes cluster (local like Minikube/Docker Desktop or remote)
- kubectl configured to work with your cluster
- Skaffold installed (`brew install skaffold` on macOS)

## Configuration

### Docker Image Name

The Docker image name is configurable via environment variables:

- `DOCKER_REGISTRY`: Your Docker registry (default: empty)
- `DOCKER_USERNAME`: Your Docker username (default: 'default')
- `DOCKER_IMAGE`: The Docker image name (default: 'locust-test')

## Load Testing Scenarios

The locustfile.py includes various test scenarios for the items API:

1. **GET /items**: Retrieve all items
2. **GET /item/{id}**: Retrieve a single item
3. **POST /items**: Create new items
4. **PUT /item/{id}**: Update existing items
5. **DELETE /item/{id}**: Delete items

The test simulates realistic user behavior with varied wait times between actions.

## Usage

### Starting the Load Test

Run the following commands directly in your terminal:

```bash
# Set your custom image
export IMAGE_REGISTRY=<yourusername>

# Deploy with Skaffold
skaffold run --default-repo $IMAGE_REGISTRY
```

### Configuring the Load Test

From the web UI:
1. Enter the number of users to simulate
2. Enter the spawn rate (users started per second)
3. Enter the host URL to test against (e.g., http://your-api-endpoint)
4. Click "Start swarming" to begin

### Scaling Workers

The setup includes a Horizontal Pod Autoscaler that will automatically scale the number of worker pods based on CPU utilization.

## Monitoring Results

The Locust web interface provides:
- Real-time statistics
- Request distribution charts
- Response time graphs
- Error rates
- Download options for test results

## Stopping the Load Test

Press CTRL+C in the terminal if skaffold is still running in the foreground, or run:

```bash
skaffold delete
```

## Kubernetes Resources

- **Master Deployment**: Controls the test and provides the web UI
- **Worker Deployment**: Executes the load tests (auto-scales based on demand)
- **Services**: Expose the master for web UI access and worker communication
