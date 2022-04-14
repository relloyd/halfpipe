#!/bin/bash

set -e

ORA_VERSION=$1

if [[ "$ORA_VERSION" == "" ]]; then
    echo "Missing ORA_VERSION as \$1"
    exit 1
fi

SCRIPT_DIR=`dirname $0`
IMAGE_NAME="relloyd/halfpipe-oracle-${ORA_VERSION}-no-oci"
CURRENT_TAG=`docker images --filter=reference=${IMAGE_NAME}* --format '{{.Tag}}' | egrep -v 'latest|<none>' | sort | tail -1`

echo "Pushing image ${IMAGE_NAME}:${CURRENT_TAG}"

docker login \
	&& docker push ${IMAGE_NAME}:${CURRENT_TAG} \
	&& docker push ${IMAGE_NAME}:latest
