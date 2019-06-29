# Sentinel data - Download and Processing

This projects handles the filtering, download and import of Sentinel II
satellite imaging data into a PostGIS database.

## Installation

Required dependencies are specified in `environment.yml`. Either install these
manually using the package manager of your choice or create a conda environment
based on the config.

```
conda env create -f environment.yml --name sentinel-data
```

## Environment variables

All following commands will assume that postgres configuration options will be
set via environment variables.

The following will be necessary to connect to the main database:

Change values accordingly:

```
# Postgres options
export PGHOST=localhost  # set to postgres database hostname
export PGPORT=5432  # set to postgres database port, default is 5432
export PGDATABASE=geospatial  # set to database name
export PGUSER=postgres  # set username
export PGPASSWORD=postgres  # set to password

# Copernicus options  PLEASE CHANGE TO CORRECT ACCOUNT
export COPERNICUS_USER=testuser
export COPERNICUS_PASS=testpass
```

## Satellite image acquisition

### Image Link generation

Usable files are queried using the `copernicus_links.py` script. This file can
produce a list of URL fragments usable for data download. An example output
of that file is contained in `filepaths.txt`, which is the list of images
used in the project.

Example usage:

```
python3 copernicus_links.py --meta metadata.json --urls filepaths.txt
```

### Image download

The download of the data itself is handled via curl in the script
`download_files.sh`.

```
./download_files.sh filepaths.txt ./download
```

The downloaded TCI (true color images, eg 3-band RGB images) are in jp2000. This
format can be directly imported into postgis via raster2psql, as long as a
jp2000 driver is available.

## PostGIS import

### Generate list of available tables

A list of already existing satellite image tables in the database is generated
in order to avoid reimporting already existing data.

```
$ psql -c "SELECT r_table_name FROM raster_columns WHERE
 r_table_name LIKE '%tci_10m';" > table_names.txt
```

### Import images

The import of the data into a PostGIS database is handled via
`postgis_import.sh`. STDOUT should be redirected into a log file.

This step will already tile the images into 120x120px tiles, which will then be
directly used on export.

```
./postgis_import.sh ./download ./table_names.txt >> images_postgis.log
```

## Tile export

### Corine Shape Annotation

The tile export requires available corine classes. These have to be loaded into
the database prior to the tile export. We use a manually processed corine shape
file.

```
shp2pgsql -I -s 4326 corinagermanydata.shp | psql
```

### Processing of tiles

Tile export requires filtering all tiles in the PostGIS dataset for tiles of
120x120 size and containing information. Blank tiles will exist inside PostGIS,
since the whole rectangular image with black borders will have been imported in
the previous step.

```
python3 query_postgis.py --metadata metadata.json --corine corinagermanydata ./dataset
```

Export of PNG image tiles is handled in `query_postgis.py`.

Based on a region query, all found rasters will be exported to 120px120p PNG
files.

The directory structure will follow the major and year class.

```
final-dataset
├── 2018
│   ├── 111
│   ├── 112
│   ├── ...
│   ├── 522
│   └── 523
├── 2019
│   ├── 111
│   ├── 112
│   ├── ...
│   ├── 522
│   └── 523
```

A single filename will consist of `<table_name>_T<raster_id>_p<major_percentage>.png`.
