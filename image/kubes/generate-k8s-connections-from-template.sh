#! /bin/bash

# Script to replace '{{ connections }}' in file ${k8s_yaml_file}
# with the base64 encoded connection details found in ${hp_connections}

set -e

script_dir=`dirname $0`
k8s_yaml_file=${script_dir}/template/template-secret-connections.yaml

# Get connections base64 encoded
# Use a good version of base64 to avoid wrapping lines.
os=`uname`
if [[ "$os" == "Darwin" ]]; then
    b64_connections=`base64 ~/.halfpipe/connections.yaml`
elif [[ "$os" == "Linux" ]]; then
    b64_connections=`base64 -w 0 ~/.halfpipe/connections.yaml`
fi

b64_aws_access_key_id=`echo "$AWS_ACCESS_KEY_ID" | base64`
b64_aws_secret_access_key=`echo "$AWS_SECRET_ACCESS_KEY" | base64`

cat "${k8s_yaml_file}" \
    | perl -e "\$a = '${b64_connections}'; while (<>) { s/\{\{ connections \}\}/\$a/; print }" \
    | perl -e "\$a = '${b64_aws_access_key_id}'; while (<>) { s/\{\{ b64_aws_access_key_id \}\}/\$a/; print }" \
    | perl -e "\$a = '${b64_aws_secret_access_key}'; while (<>) { s/\{\{ b64_aws_secret_access_key \}\}/\$a/; print }"

#    sed -e "s/{{ connections }}/${b64_connections}/" \
