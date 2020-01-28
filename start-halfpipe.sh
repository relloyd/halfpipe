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
default_port=8080

usage() {
  echo $1
  echo
  echo "Usage: $0 [ -a <profile name> | -f ] [-k] [-b] [-p <port>]" 1>&2
  echo ""
  echo "    Start Halfpipe in a Docker container preconfigured with Oracle Instant Client ${ORA_VERSION}"
  echo "    where:"
  echo ""
  echo "   -a  supplies a profile name found in ~/.aws/credentials, to set AWS access keys in the container"
  echo "   -f  starts the container without setting AWS access keys (but you'll need them to stage Snowflake data)"
  echo "   -k  mounts .kube/config into the container so you can launch Kubernetes jobs easily"
  echo "   -b  forces a Docker image build, else we will build once and run image, $image_name:$image_tag"
  echo "   -p  port to expose for Halfpipe's micro-service used by 'hp pipe' commands (default $default_port)"
  echo
  exit 1
}

while getopts ":a:fbkp:" o; do
  case "${o}" in
    a)
      profile=${OPTARG};;
    f)
      force=1;;
    b)
      build_requested=1;;
    k)
      kube=1;;
    p)
      port=${OPTARG};;
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
  aws_mount='-v ~/.aws:/home/dataops/.aws'
  aws_variables="-e AWS_ACCESS_KEY_ID=\"$AWS_ACCESS_KEY_ID\" -e AWS_SECRET_ACCESS_KEY=\"$AWS_SECRET_ACCESS_KEY\""
elif [[ "$force" -ne 1 ]]; then  # else if we have bad args...
  usage "Error: supply -a or -f to get going."
  # else continue...
fi

if [[ "$kube" -eq 1 ]]; then  # if we should mount .kube/config...
  kube_mount="-v ~/.kube:/home/dataops/.kube"
fi

if [[ "$port" == "" ]]; then  # if the port has NOT been set...
  port=$default_port
fi


# Build Halfpipe.
if [[ "$build_requested" -eq 1 || "$image_already_built" -ne 1 ]]; then  # if we should build halfpipe...
  cmd="docker build \\
    --build-arg HP_VERSION=${HP_VERSION} \\
    --build-arg ORA_VERSION=${ORA_VERSION} \\
    --build-arg K8S_VERSION=${K8S_VERSION} \\
    -t ${image_name}:${image_tag} \\
    \"${build_dir}\""
  echo "${cmd}"
  eval "${cmd}"
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
cmd="docker run -ti --rm \\
    -v ~/.halfpipe:/home/dataops/.halfpipe \\
    ${kube_mount} \\
    ${aws_mount} \\
    ${aws_variables} \\
    -p ${port}:8080 \\
    ${image_name}:${image_tag}"
eval "$cmd"
