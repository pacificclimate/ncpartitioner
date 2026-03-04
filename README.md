# NCPartitioner

This container generates user-requested netCDF files using `ncks` and makes them available for download via THREDDS.

## Run for Development

The Unidata netCDF tools must be installed. This package can be installed with poetry:

```
apt get install nco
github clone http://github.com/pacificclimate/ncpartitioner
poetry install
```

To do end-to-end testing, you will also need a THREDDS instance running on your workstation, though the test suite does not need a working THREDDS instance. Set the environment variables:

* `OUTPUT_DIR` - file directory to put the partitioned files in. It should be accessible to THREDDS
* `THREDDS_HTTP_BASE` - the base URL for the THREDDS http server (probably ends /fileserver); a user will be redirected to download the completed file
* `THREDDS_DAP_BASE` - the base URL for the THREDDS openDAP server (probably ends /dodsC): used to fulfill metadata requests
* `DATA_ROOT` - directory under which all data is found; prevents files outside the directory from being served

Run with flask:
```
poetry run flask run
```

Run the test suite (environment variables will be provided by pytest and do not need to be set):
```
poetry run pytest
```

## Data assumptions

This server assumes all files to be downloaded are netCDF4 files with dimensions named `lat`, `lon`, and `time`, and that all variables one might wish to download have those dimensions. Timeless files or station-based geometries cannot be downloaded via this server. Only one variable may be downloaded at a time.

## Request format

Request format is indicated by concatenating an extension onto the `filepath` parameter. Some request formats require an additional `targets` parameter. Request attributes other than `targets` and `filepath` are ignored.

This server supports four request formats. Three of them are simply redirected to the THREDDS server:

### DDS request
`https://server/partition/?filepath=path/to/file.nc.dds&targets=time`

Redirects to a THREDDS page displaying metadata about the `time` dimension. This request accepts a single dimension.

### DAS request
`https://server/partition/?filepath=path/to/file.nc.das`

Redirects to a THREDDS page displaying metadata about all variables and attributes.`targets` attribute is ignored, if present.

### ASCII request
`https://server/partition/?filepath=path/to/file.nc.ascii&targets=lat,lon`

Redirects to a THREDDS page displaying values for the requested dimension variable(s) in ASCII format. This server will only display values for dimension variables (`lat`, `lon`, and `time`) via this request type. OpenDAP standards support requesting any variable in ASCII format this way, but since THREDDS has a 500MB maximum file size for DAP requests, this server only supports requesting the dimension variables, not multidimensional data variables.

### Partition request
`https://server/partition/?filepath=path/to/file.nc.nc&targets=time[0:10],lat[0:20],lon[0:30],tasmax[0:10][0:20][0:30]`

Creates a file with the requested dimensions using `ncks`, then redirects the user to the THREDDS page to download the newly created file. Note that the variable is always trimmed to the hyperslab specified in the dimensions portion of the `targets` attribute; if the variable portion of the `targets` attribute is different, it will be overruled.