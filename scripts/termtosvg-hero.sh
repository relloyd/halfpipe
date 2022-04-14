#! /bin/bash

script_dir=`dirname $0`
target_dir="$HOME/go/src/github.com/relloyd/halfpipe/svg"
template="termtosvg-template-richard-hero-1.svg"

if [[ "$1" == "" ]]; then
    echo "Supply file name as \$1 to save SVG in ${target_dir}"
    exit 1
fi

width=$2
height=$3
if [[ "$height" == "" || "$width" == "" ]]; then
    echo "Supply width and height as \$2 \$3"
    exit 1
fi

termtosvg "${target_dir}/$1" -t "${target_dir}/${template}" -g ${width}x${height} -M 2000 -D 3000
