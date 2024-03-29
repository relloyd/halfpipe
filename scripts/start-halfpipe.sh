#!/bin/bash

set -e

image=${1:?"Supply the halfpipe Docker image:tag as \$1"}
script_dir=$(dirname "$0")
default_aws_profile=halfpipe
default_port=8080

usage() {
  cat <<EOF 1>&2

Usage: $0 [ -a <AWS profile name> ] [-p <port>]"

    Start Halfpipe in a Docker container preconfigured with Oracle Instant Client
    where:

   -a  supplies a profile name found in '$HOME/.aws/credentials'
       to set AWS access keys in the container (default: ${default_aws_profile})
   -p  port to expose for Halfpipe's microservice used by 'hp pipe' commands
       (default: $default_port)

EOF
  exit 1
}

while getopts ":a:p:" o; do
  case "${o}" in
    a)
      aws_profile=${OPTARG};;
    p)
      port=${OPTARG};;
    *)
      usage;;
  esac
done
shift $((OPTIND-1))

if [[ -z "${aws_profile}" ]]; then  # if the profile has NOT been set...
  aws_profile="${default_aws_profile}"  # use the default.
fi

if [[ -z "$port" ]]; then  # if the port has NOT been set...
  port=$default_port
fi

# Create if not exists the halfpipe home dir.

mkdir -p ~/.halfpipe

# Add defaults for the first time.

if [[ ! -f "$HOME/.halfpipe/config.yaml" ]]; then  # if there are no existing config defaults...
  echo "Using default config file config.yaml"
  cp "${script_dir}/.default-config.yaml" "$HOME/.halfpipe/config.yaml"
fi

# Start Halfpipe container.

echo "Starting ${image}:latest using AWS profile \"${aws_profile}\"..."
cmd="docker run -ti --rm \\
    -v ~/.halfpipe:/home/dataops/.halfpipe \\
    -v ~/.aws:/home/dataops/.aws \\
    -e AWS_PROFILE=\"${aws_profile}\" \\
    -p ${port}:8080 \\
    ${image}"
eval "$cmd"
