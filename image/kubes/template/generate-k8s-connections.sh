#! /bin/bash

set -e

echo ""
echo "---"
echo ""

kubectl create secret generic halfpipe-config \
    --from-file="$HOME"/.halfpipe/connections.yaml \
    --from-file="$HOME"/.halfpipe/session.token \
    --dry-run \
    -o yaml

echo ""
echo "---"
echo ""

if [[ "${AWS_ACCESS_KEY_ID}" != "" ]]; then
  kubectl create secret generic aws-access-keys \
      --from-literal="$(env | grep AWS_ACCESS_KEY_ID)" \
      --from-literal="$(env | grep AWS_SECRET_ACCESS_KEY)" \
      --dry-run \
      -o yaml
fi
