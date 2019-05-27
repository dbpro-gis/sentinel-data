#!/bin/bash
DBNAME=geospatial
inputdir="download-tif"

for filename in $inputdir/*.tif; do
	echo $filename
	fname=$(basename $filename .tif | awk '{print tolower($0)}')
	echo $fname
	if grep $fname loaded_files.txt > /dev/null; then
		echo "Loaded"
	else
		echo "Not loaded $fname"
	fi
	# raster2pgsql -s 32633 -I -t 500x500 -C $filename | psql -U postgres -h home.arsbrevis.de -p 31313 -d $DBNAME >> postgis_import.log
done
