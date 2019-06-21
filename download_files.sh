#!/bin/bash
outdir=download

mkdir -p $outdir
while read p; do
	filename=$(echo $p | grep -o '[[:alnum:]_]*.jp2')
	filepath="$outdir/$filename"
	if [ -f $filepath ]; then
		echo "$filepath already downloaded"
	else
		curl -o $filepath -u $COPERNICUS_USER:$COPERNICUS_PASS "https://scihub.copernicus.eu/dhus/odata/v1$p"
	fi
done <$1
