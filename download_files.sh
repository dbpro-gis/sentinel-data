#!/bin/bash
if [ "$#" -ne 2 ]; then
	echo "Usage: <filepaths> <output_dir>"
	exit
fi
filepaths=$1
outdir=$2

mkdir -p $outdir
while read p; do
	filename=$(echo $p | grep -o '[[:alnum:]_]*.jp2')
	filepath="$outdir/$filename"
	if [ -f $filepath ]; then
		echo "$filepath already downloaded"
	else
		curl -o $filepath -u $COPERNICUS_USER:$COPERNICUS_PASS "https://scihub.copernicus.eu/dhus/odata/v1$p"
	fi
done <$filepaths
