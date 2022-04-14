#!/usr/bin/env bash

# Values for S3 client defaults...
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile halfpipe)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile halfpipe)

# Values for testing uetl demo switch...
export SNOWFLAKE_TARGET_S3KEY=$(aws configure get aws_access_key_id --profile halfpipe)
export SNOWFLAKE_TARGET_S3SECRET=$(aws configure get aws_secret_access_key --profile halfpipe)
export SNOWFLAKE_TARGET_S3URL=s3://test.halfpipe.sh

exec $SHELL
