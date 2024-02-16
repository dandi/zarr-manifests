#!/bin/bash

set -eu -o pipefail

set -x

#rm -rf  times manifests*
mkdir -p times manifests manifests-v2

# needs config
#s3cmd -c ~/.s3cfg-dandi-backup ls s3://dandiarchive/zarr/ \
#	| while read _ zarr; do
#	z=${zarr%*/}
#	z=${z##*/}

p=s3://dandiarchive/zarr
aws --no-sign-request s3 ls $p/ \
	| tac \
	| while read _ zarr; do
	z=${zarr%*/}
	if [ -s "manifests/$z.json" ]; then
		echo "Skipping $z - already present"
		continue
	fi
	/usr/bin/time -o times/$z.out ./list_bucket_prefix_versionids.py $p/$zarr >| manifests/$z.json 
	{ 
		set -eu -o pipefail
		./convert_schema_1to2.py manifests/$z.json manifests-v2/$z.json;
		./sort-v2.sh zarr-manifests-v2-sorted  manifests-v2/$z.json;
	} &
done
