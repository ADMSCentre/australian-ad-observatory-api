#!/bin/bash

# Bash script to build and deploy AAO Enrichment Ingestion service

set -e

version=$(cat version | tr -d '\n')
registry="registry.rc.nectar.org.au/aio2024-private"
image_name="aao-enrichment-ingestion"

echo "Building and deploying $image_name version $version"

# Build the image
docker build . -t "$image_name:$version"

# Tag the images
docker tag "$image_name:$version" "$registry/$image_name:$version"
docker tag "$image_name:$version" "$registry/$image_name:latest"

# Push the images
docker push "$registry/$image_name:$version"
docker push "$registry/$image_name:latest"

echo "Successfully pushed $image_name version $version"