#!/bin/bash
DBNAME=geospatial
inputdir="download"

current_tables="./current_tables.lst"

new=0
existing=0

for filename in $inputdir/*.jp2; do
	basename=$(basename $filename .jp2)
	if grep -i $basename $current_tables > /dev/null; then
		existing=$((existing + 1))
	else
		new=$((new + 1))
		echo "Adding $basename"
		raster2pgsql -I -t 120x120 -C $filename | psql -U postgres -h home.arsbrevis.de -p 31313 -d $DBNAME >> postgis_import.log
	fi
done

echo "Existing $existing, new $new"
