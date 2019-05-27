#!/bin/bash
BIN="../gis-database-docker/gdal.sh gdal_translate"
inputdir=$1
outputdir="download-tif"
mkdir -p $outputdir
for filename in $inputdir/*.jp2; do
	name=$(basename $filename .jp2)
	$BIN $filename $outputdir/$name.tif
	break
done
