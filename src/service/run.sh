#!/bin/bash

# Check if IMAGE_NAME is set
if [ -z "${IMAGE_NAME}" ]; then
  echo "ERROR: IMAGE_NAME environment variable is not set"
  echo "Please set your image name: export IMAGE_NAME=yourusername/yourimage"
  exit 1
fi

echo "Using image: ${IMAGE_NAME}"

# Run skaffold
skaffold run