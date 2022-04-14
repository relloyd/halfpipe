aws lambda delete-layer-version --layer-name my-layer --version 1

aws lambda publish-layer-version --layer-name halfpipe --description "Halfpipe" \
  --content S3Bucket=lambda.halfpipe.sh,S3Key=halfpipe-lambda-layer.zip --compatible-runtimes go1.x

