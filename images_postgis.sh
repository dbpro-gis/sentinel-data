#!/bin/bash
DBNAME=geospatial
inputdir="download-tif"

for filename in $inputdir/*.tif; do
	echo $filename
	raster2pgsql -s 32633 -I -t 500x500 -C $filename | psql -U postgres -h home.arsbrevis.de -p 31313 -d $DBNAME >> postgis_import.log
done
