#!/bin/bash

ORA_VERSION=$1
BINARY_NAME=$2
IMAGE_NAME=$3
DOCKERFILE_SUFFIX=$4

if [[ "$ORA_VERSION" == "" ]]; then
  echo "Missing ORA_VERSION as \$1"
  exit 1
fi

SCRIPT_DIR=$(dirname $0)
#EXISTING_IMAGE_TAG=`docker images --filter=reference=${IMAGE_NAME}* --format '{{.Tag}}' | egrep -v 'latest|<none>' | sort | tail -1`
CURRENT_TAG=$(git describe --tags --abbrev=0)

if [[ "$CURRENT_TAG" == "" ]]; then # if we're building for the very first time...
  CURRENT_TAG=v0.0.1
fi

if [[ -n "$DOCKERFILE_SUFFIX" ]]; then # if there is a non zero suffix...
  DOCKERFILE_SUFFIX="-$DOCKERFILE_SUFFIX" # prefix with a dash
fi

echo "Building image ${IMAGE_NAME}:${CURRENT_TAG}"

cmd="docker build \\
    -t ${IMAGE_NAME}:${CURRENT_TAG} \\
    -t ${IMAGE_NAME}:latest \\
    --build-arg ORA_VERSION=${ORA_VERSION} \\
    --build-arg BINARY_NAME=${BINARY_NAME} \\
    -f ${SCRIPT_DIR}/../image/Dockerfile${DOCKERFILE_SUFFIX} ${SCRIPT_DIR}/.."

echo "$cmd"
eval "$cmd"
