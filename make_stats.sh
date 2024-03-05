#!/bin/bash
#
#
set -eu

cd "$(dirname "$0")"

echo "## Zarrs with multiple versions"
echo ""

for zf in zarr-manifests-v2-sorted/*/*/*; do 
	versions=( $(/bin/ls -1 "$zf"/*.json) ); 
	if [ ${#versions[@]} != 1 ]; then
	       z=${zf##*/}; 
	       echo "- [$z]($zf): ${#versions[@]} versions"
	       for v in "${versions[@]}"; do
		       lm=$(jq -r .statistics.lastModified $v)
		       echo "    - $lm [$(basename $v)](https://datasets.datalad.org/dandi/zarr-manifests/$v)"
	       done | sort
	fi
done
