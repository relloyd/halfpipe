#! /bin/bash

script_dir=`dirname $0`
target_dir="$HOME/go/src/github.com/relloyd/halfpipe/svg"
template="termtosvg-template-richard-1.svg"

if [[ "$1" == "" ]]; then
    echo "Supply file name as \$1 to save SVG in ${target_dir}"
    exit 1
fi

height=$2
if [[ "$height" == "" ]]; then
    height=25
fi

termtosvg "${target_dir}/$1" -t "${target_dir}/${template}" -g 120x${height} -M 2000 -D 3000
