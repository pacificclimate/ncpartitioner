# NCPartitioner

This is an experimental container that generates user-requested netCDF files using ncks and makes them available for download via THREDDS. It is in its very early stages and has no verification, tests, or cleanup.

## Run for Development

The Unidata netCDF tools must be installed. This package can be installed with poetry:

```
apt get install nco
github clone http://github.com/pacificclimate/ncpartitioner
poetry install
```

To do end-to-end testing, you will also need a THREDDS instance running on your workstation, though the partitioning functionality can be tested without THREDDS. Set the environment variables:

* `OUTPUT_DIR` - file directory to put the partitioned files in. It should be accessible to THREDDS
* `THREDDS_HTTP_BASE` - the base URL for the THREDDS http server

Run with flask:
```
poetry run flask run
```