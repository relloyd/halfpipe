#! /bin/bash

set -e

echo ""
echo "---"
echo ""

kubectl create secret generic halfpipe-connections \
    --from-file=$HOME/.halfpipe/connections.yaml \
    --dry-run \
    -o yaml

echo ""
echo "---"
echo ""

kubectl create secret generic aws-access-keys \
    --from-literal=`env | grep AWS_ACCESS_KEY_ID` \
    --from-literal=`env | grep AWS_SECRET_ACCESS_KEY` \
    --dry-run \
    -o yaml
