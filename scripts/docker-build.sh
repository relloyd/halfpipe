#!/bin/bash

ORA_VERSION=${1:?"Missing ORA_VERSION as \$1"}
HP_VERSION=${2:?"Missing HP_VERSION as \$2"}
IMAGE_NAME=${4:?"Missing IMAGE_NAME as \$3"}
DOCKERFILE_SUFFIX=$5

SCRIPT_DIR=$(dirname "$0")
CURRENT_TAG=$(git describe --tags --abbrev=0)
#EXISTING_IMAGE_TAG=`docker images --filter=reference=${IMAGE_NAME}* --format '{{.Tag}}' | grep -Ev 'latest|<none>' | sort | tail -1`

if [[ -z "$CURRENT_TAG" ]]; then # if we're building for the very first time...
  CURRENT_TAG=v0.0.1
fi

if [[ -n "$DOCKERFILE_SUFFIX" ]]; then # if there is a non-zero suffix...
  DOCKERFILE_SUFFIX="-$DOCKERFILE_SUFFIX" # prefix with a dash
fi

echo "Building image ${IMAGE_NAME}:${CURRENT_TAG}"

cmd="docker buildx build --platform linux/amd64 \\
    -t ${IMAGE_NAME}:${CURRENT_TAG} \\
    -t ${IMAGE_NAME}:latest \\
    --build-arg ORA_VERSION=${ORA_VERSION} \\
    --build-arg HP_VERSION=${HP_VERSION} \\
    -f ${SCRIPT_DIR}/../image/Dockerfile${DOCKERFILE_SUFFIX} ${SCRIPT_DIR}/.."

echo "$cmd"
eval "$cmd"
