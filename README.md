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
* `NCPARTITIONER_CHUNK_BYTES` - optional target size, in bytes, for each time-window slice job; defaults to `314572800` (300 MiB). Smaller values reduce per-`ncks` memory pressure but create more chunks.
* `NCPARTITIONER_BYTES_PER_ELEMENT` - optional byte estimate used by the chunk planner; defaults to `8` to size conservatively for `Float64` datasets.
* `NCPARTITIONER_MAX_WORKERS` - optional maximum number of chunk extraction workers; defaults to `3`. Increase cautiously because each worker runs its own `ncks` process.
* `NCPARTITIONER_DEFLATE_LEVEL` - optional netCDF4 compression level passed to the final `ncrcat -L`; defaults to `1`.
* `NCPARTITIONER_INTERMEDIATE_DEFLATE_LEVEL` - optional netCDF4 compression level passed to intermediate `ncks -L`; defaults to `NCPARTITIONER_DEFLATE_LEVEL`. Setting this to `0` can speed extraction for small jobs, but can massively expand compressed source data on disk.
* `NCPARTITIONER_NCRCAT_THREADS` - optional thread count passed to final `ncrcat -t`; defaults to `1`. Increase only after measuring because higher values can increase CPU and IO contention.

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

Starts an asynchronous slice job. The initial response is `202 Accepted` with a JSON body containing:

* `status` - always `queued` for the initial response
* `job_id` - backend-generated identifier for the slice job
* `queue_position` - current queue position when available
* `status_url` - relative polling path in the form `partition/status/<job_id>`; intentionally no leading slash
* `download_url` - final THREDDS download URL for the output file
* `output_filename` - final output filename

The frontend should poll `status_url` until it receives a terminal job state. Current job states are:

* `running`
* `complete`
* `failed`

Running jobs may include progress fields:

* `phase` - current processing phase, currently `extracting` or `merging`
* `chunks_complete` - number of chunk extraction tasks completed
* `chunks_total` - total chunk extraction tasks for the request
* `chunk_bytes` - total bytes of extracted chunks, included during `merging`

Completed jobs keep the same `download_url` and `output_filename` values, so the frontend can start the download when status becomes `complete`.

Chunking notes:

* Large requests are split into multiple time windows based on `NCPARTITIONER_CHUNK_BYTES`
* Chunk extraction runs in parallel up to `NCPARTITIONER_MAX_WORKERS`
* Intermediate chunks are written to temporary files with `ncks --mk_rec_dmn time` and `NCPARTITIONER_INTERMEDIATE_DEFLATE_LEVEL`
* Completed chunks are concatenated in time order with a single final `ncrcat`, which applies `NCPARTITIONER_DEFLATE_LEVEL`
* NCO internal temp files are disabled with `--no_tmp_fl` because all chunk and final-merge output is already written under job-scoped `.jobs/<job_id>` paths before publication
* `NCPARTITIONER_CHUNK_BYTES` is a per-chunk target, not a per-time-index target
* Approximate in-flight slice memory is `NCPARTITIONER_CHUNK_BYTES * NCPARTITIONER_MAX_WORKERS`, plus process and netCDF/NCO overhead
* The chunk planner estimates bytes from `lat * lon * NCPARTITIONER_BYTES_PER_ELEMENT`; it does not account for metadata, coordinates, associated variables, or compression ratio

Note that the variable is always trimmed to the hyperslab specified in the dimensions portion of the `targets` attribute; if the variable portion of the `targets` attribute is different, it will be overruled.
