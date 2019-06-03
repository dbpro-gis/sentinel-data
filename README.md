# Sentinel data - Download and Processing

## Installation

Required dependencies are specified in `environment.yml`. Either install these
manually using the package manager of your choice or create a conda environment
based on the config.

```
conda env create -f environment.yml --name sentinel-data
```

## Data download

Usable files are queried using the `fetch_images.py` script. This file can
produce a list of URL fragments usable for data download. An example output
of that file is contained in `filepaths.txt`, which is the list of images
used in the project.

The download of the data itself is handled via curl in the script
`download_files.sh`.

The downloaded TCI (true color images, eg 3-band RGB images) are in jp2000,
which is not widely supported. The data is converted to TIFF using the script
`process_images.sh`. This depends on a binary for gdal_translate being
available.

## PostGIS import

The import of the data into a PostGIS database is handled via
`images_postgis.sh`. STDOUT should be redirected into a log file.

Example usage:

```
./images_postgis.sh download-tif >> images_postgis.log
```

## Tile export

Export of PNG image tiles is handled in `query_postgis.py`.

Based on a region query, all found rasters will be exported to 120px120p PNG
files. An accompanying CSV file will contain additional metadata for the PNG
files mapping filenames to geographical extent and additional classification
data.
