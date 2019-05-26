#!/bin/bash
BIN=../gis-database-docker/postgis.sh raster2pgsql

COORD_REF=32633
DBNAME=geospatial

$inputdir=download-tif

for filename in $inputdir/*.tif; do
	$BIN "-s $COORD_REF -I -t 500x500 -C | psql -U postgres -d $DBNAME"
done
