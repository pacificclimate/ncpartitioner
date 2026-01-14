# NCPartitioner

This is an experimental container that generates user-requested netCDF files using ncks and makes them available for download via THREDDS.

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

## Data assumptions

This server assumes all files to be downloaded are netCDF 4 files with dimensions named `lat`, `lon`, and `time`, and that all variables one might wish to download have those dimensions. Timeless files or station-based geometries cannot be downloaded via this server. Only one variable may be downloaded at a time.