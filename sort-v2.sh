#!/bin/bash
#
# Given v2 manifests - place them under corresponding hierarchy
#
topdir="$1"
shift
for m in "$@"; do
	if [ -d "$m" ]; then
		# if directory -- go through the json files there
		# It is a trick for whenever we cannot pass too many files due
		# hitting max cmdline size
		for f in "$m"/*.json; do
			"$0" "$topdir" $f
		done
		continue
	fi
	checksum=$(jq -r .statistics.zarrChecksum "$m" | sed -e 's, .*,,g')
	ext="${m#*.}"
	stem=$(basename $m | sed -e 's,\..*,,g')
	# Let's mimic what we have for blobs -- better be consistent
	# although current "zarr" is not consistent, but it is not even "zarrs"
	basedir="$topdir/${stem:0:3}/${stem:3:3}/$stem"
	mkdir -p "$basedir"
	cp --reflink=auto "$m" "$basedir/$checksum.$ext"
done
