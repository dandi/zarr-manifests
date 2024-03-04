#!/bin/bash

set -eu -o pipefail
set -x

p=s3://dandiarchive/zarr
aws --no-sign-request s3 ls $p/ \
    | sed -ne '/.*-.*-.*/s,.*PRE \(.*\)/,\1,gp' \
    | parallel -j 10 ./update_manifest.py -i dandi --mode api-check --manifests-root zarr-manifests-v2-sorted '{}'

rm -f STATS.md
./make_stats.sh  >| STATS.md

