#!/bin/bash

if [ "$#" -ne 2 ]; then
	echo "Usage: <image_dir> <existing_tables>"
	exit
fi

inputdir=$1
current_tables=$2

new=0
existing=0

for filename in $inputdir/*.jp2; do
	basename=$(basename $filename .jp2)
	if grep -i $basename $current_tables > /dev/null; then
		existing=$((existing + 1))
	else
		new=$((new + 1))
		echo "Adding $basename"
		raster2pgsql -I -t 120x120 -C $filename | psql >> postgis_import.log
	fi
done

echo "Existing $existing, new $new"
