#!/bin/bash

set -e  # exit after any failure

HP_VERSION=1
ORA_VERSION=19.5
K8S_VERSION=v1.13.1  # include k8s leading 'v'

image_name=halfpipe
script_dir=`dirname $0`
build_dir="${script_dir}/image"
image_tag=${HP_VERSION}
image_already_built=`docker images --filter=reference=${image_name}:${image_tag} -q | wc -l`

usage() {
    echo $1
    echo
    echo "Usage: $0 -p <profile name> [-b] [-f]" 1>&2
    echo ""
    echo "   where:"
    echo ""
    echo "   -p supplies a profile name found in ~/.aws/credentials, to set AWS access keys in Halfpipe"
    echo "   -b forces a Docker image build, else we will build once and run image, $image_name:$image_tag"
    echo "   -f starts HalfPipe without setting AWS access keys (you'll need them to stage Snowflake data)"
    echo
    exit 1
}

while getopts ":p:fb" o; do
    case "${o}" in
        p)
            profile=${OPTARG};;
        f)
            force=1;;
        b)
            build_requested=1;;
        *)
            usage;;
    esac
done
shift $((OPTIND-1))

# Set AWS access keys or ignore with -f.
if [[ "$profile" != "" ]]; then  # if we have a profile name to fetch AWS access keys...
    # Set access keys using the supplied profile name.
    echo "Setting AWS access keys using profile $profile"
    AWS_ACCESS_KEY_ID=`aws configure get aws_access_key_id --profile "$profile"`
    AWS_SECRET_ACCESS_KEY=`aws configure get aws_secret_access_key --profile "$profile"`
elif [[ "$force" -ne 1 ]]; then  # else if we have bad args...
    usage "Supply -p or -f to get going."
    # else continue...
fi

# Build Halfpipe.
if [[ "$build_requested" -eq 1 || "$image_already_built" -ne 1 ]]; then  # if we should build halfpipe...
    docker build \
        --build-arg HP_VERSION=${HP_VERSION} \
        --build-arg ORA_VERSION=${ORA_VERSION} \
        --build-arg K8S_VERSION=${K8S_VERSION} \
        -t ${image_name}:${image_tag} \
        "${build_dir}"
fi

# Create if not exists the halfpipe home dir.
mkdir -p ~/.halfpipe

# Add defaults for the first time.
if [[ ! -f "$HOME/.halfpipe/config.yaml" ]]; then  # if there are no existing config defaults...
    echo "Using default config file config.yaml"
    cp "${script_dir}/default.config.yaml" "$HOME/.halfpipe/config.yaml"
fi

# Start Halfpipe container.
echo Starting ${image_name}:${image_tag}...
docker run -ti --rm \
    -v ~/.halfpipe:/home/dataops/.halfpipe \
    -v ~/.aws:/home/dataops/.aws \
    -v ~/.kube:/home/dataops/.kube \
    -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    -p 8080:8080 \
    ${image_name}:${image_tag}
