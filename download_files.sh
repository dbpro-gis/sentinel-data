#!/bin/bash
outdir=download

mkdir -p $outdir
while read p; do
	filename=$(echo $p | grep -o '[[:alnum:]_]*.jp2')
	curl -v -o "$outdir/$filename" -u $COPERNICUS_USER:$COPERNICUS_PASS "https://scihub.copernicus.eu/dhus/odata/v1$p"
done <$1
