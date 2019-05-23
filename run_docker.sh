#!/bin/bash
case $1 in
build)
	docker build -t sentinel-data .
	;;
*)
	docker run --name sentinel-data -v $(pwd):/app --env COPERNICUS_USER=$COPERNICUS_USER --env COPERNICUS_PASS=$COPERNICUS_PASS --rm sentinel-data
	;;
esac
