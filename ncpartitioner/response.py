"""Send responses to user requests.

DDS/DAS/ASCII requests redirect immediately to THREDDS. NetCDF slice requests
are enqueued onto a Dragonfly-backed queue and processed by a separate worker
process (see worker.py). The worker writes bounded-memory NetCDF4 subsets
directly into the THREDDS-visible output directory.
"""

import builtins
import json
import logging
import os
import shutil
import tempfile
import threading
import uuid
from datetime import datetime, timezone
from time import monotonic

import netCDF4
from flask import Response, redirect

from . import queue_client

logger = logging.getLogger(__name__)

TERMINAL_JOB_STATUSES = {"complete", "failed"}
_job_locks = {}
_job_locks_guard = threading.Lock()


def input_filepath(args):
    """Resolve the source file path for the current request."""
    return os.path.join(
        os.sep,
        args["dirname"],
        f"{args['basename']}.{args['extension']}",
    )


def output_filename(args):
    return f"{args['basename']}_{args['timestamp']}.{args['extension']}"


def output_filepath(args):
    return os.path.join(os.getenv("OUTPUT_DIR"), output_filename(args))


def output_url(args):
    thredds_base = os.getenv("THREDDS_HTTP_BASE")
    output_dir = os.getenv("OUTPUT_DIR")
    return f"{thredds_base}{output_dir}/{output_filename(args)}"


def jobs_dir():
    return os.path.join(os.getenv("OUTPUT_DIR"), ".jobs")


def status_filepath(job_id):
    return os.path.join(jobs_dir(), f"{job_id}.json")


def job_temp_dir(job_id):
    return os.path.join(jobs_dir(), job_id)


def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()


def ensure_jobs_dir():
    os.makedirs(jobs_dir(), exist_ok=True)


def ensure_job_temp_dir(job_id):
    os.makedirs(job_temp_dir(job_id), exist_ok=True)


def job_lock(job_id):
    with _job_locks_guard:
        lock = _job_locks.get(job_id)
        if lock is None:
            lock = threading.Lock()
            _job_locks[job_id] = lock
        return lock


def build_job_status(job_id, args, status, **extra):
    payload = {
        "job_id": job_id,
        "status": status,
        "status_url": status_url(job_id),
        "download_url": output_url(args),
        "output_filename": output_filename(args),
        "updated_at": utcnow_iso(),
    }
    payload.update(extra)
    return payload


def parse_iso8601(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def write_job_status(job_id, payload):
    ensure_jobs_dir()
    with job_lock(job_id):
        fd, temp_path = tempfile.mkstemp(
            dir=jobs_dir(),
            prefix=f"{job_id}.",
            suffix=".json.tmp",
            text=True,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
            os.replace(temp_path, status_filepath(job_id))
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


def _recover_status_payload(raw_status):
    decoder = json.JSONDecoder()
    index = 0
    payload = None
    payload_count = 0

    while index < len(raw_status):
        while index < len(raw_status) and raw_status[index].isspace():
            index += 1
        if index >= len(raw_status):
            break
        payload, index = decoder.raw_decode(raw_status, index)
        payload_count += 1

    if payload_count == 0 or not isinstance(payload, dict):
        return None, 0

    return payload, payload_count


def read_job_status(job_id):
    try:
        with open(status_filepath(job_id), encoding="utf-8") as handle:
            raw_status = handle.read()
    except FileNotFoundError:
        return None
    try:
        return json.loads(raw_status)
    except json.JSONDecodeError:
        payload, payload_count = _recover_status_payload(raw_status)
        if payload is None:
            logger.exception("Job %s status file is unreadable JSON", job_id)
            return None

        logger.warning(
            "Job %s status file contained %s concatenated JSON payloads; "
            "recovering the last payload",
            job_id,
            payload_count,
        )
        try:
            write_job_status(job_id, payload)
        except OSError:
            logger.exception(
                "Job %s status recovery succeeded in memory but failed to rewrite "
                "the status file",
                job_id,
            )
        return payload


def write_running_job_status(job_id, args, **extra):
    existing = read_job_status(job_id) or {}
    write_job_status(
        job_id,
        build_job_status(
            job_id,
            args,
            "running",
            started_at=existing.get("started_at") or utcnow_iso(),
            **extra,
        ),
    )


def queue_idle_ttl_seconds():
    return int(os.getenv("NCPARTITIONER_QUEUE_IDLE_TTL_SECONDS", 5 * 60))


def queued_job_stale_reason(payload, now=None):
    if payload.get("status") != "queued":
        return None

    now = now or datetime.now(timezone.utc)
    queued_at = parse_iso8601(payload.get("queued_at"))
    last_seen_at = parse_iso8601(payload.get("last_seen_at")) or queued_at
    if last_seen_at is not None:
        idle_seconds = (now - last_seen_at).total_seconds()
        if idle_seconds > queue_idle_ttl_seconds():
            return "Queued job was abandoned before processing"

    return None


def fail_job_from_status_payload(job_id, payload, error, *, returncode=None):
    write_job_status(
        job_id,
        {
            **payload,
            "status": "failed",
            "updated_at": utcnow_iso(),
            "completed_at": utcnow_iso(),
            "error": error,
            "returncode": returncode,
        },
    )
    cleanup_job_temp_dir(job_id)


def fail_and_discard_queued_job(job_id, payload, error):
    if not queue_client.discard_queued_job(job_id):
        return False
    fail_job_from_status_payload(job_id, payload, error)
    return True


def status_url(job_id):
    return f"partition/status/{job_id}"


def response_json(payload, status=200):
    return Response(json.dumps(payload), status=status, mimetype="application/json")


def time_windows(args, source_bytes, byte_budget):
    start, end = args["time"]
    lat0, lat1 = args["lat"]
    lon0, lon1 = args["lon"]
    n_lat, n_lon = lat1 - lat0 + 1, lon1 - lon0 + 1
    bytes_per_step = max(n_lat * n_lon * source_bytes, 1)
    window = max(1, byte_budget // bytes_per_step)
    return [(s, min(s + window - 1, end)) for s in range(start, end + 1, window)]


def final_temp_filepath(job_id, args):
    return os.path.join(job_temp_dir(job_id), output_filename(args))


def deflate_level():
    return int(os.getenv("NCPARTITIONER_DEFLATE_LEVEL", 1))


def compress_final_output():
    return os.getenv("NCPARTITIONER_COMPRESS_FINAL_OUTPUT") == "true"


def netcdf4_slab_byte_budget():
    """Maximum in-memory data slab used by the direct NetCDF4 writer."""
    return int(os.getenv("NCPARTITIONER_NETCDF4_SLAB_BYTES", 64 * 1024 * 1024))


def cleanup_job_temp_dir(job_id):
    shutil.rmtree(job_temp_dir(job_id), ignore_errors=True)


def fail_job(job_id, args, error, *, returncode=None):
    """Mark a job as failed and clean up its unpublished output file."""
    existing = read_job_status(job_id)
    write_job_status(
        job_id,
        build_job_status(
            job_id,
            args,
            "failed",
            started_at=existing.get("started_at") if existing else None,
            completed_at=utcnow_iso(),
            error=error,
            returncode=returncode,
        ),
    )
    cleanup_job_temp_dir(job_id)


def _copy_netcdf_attributes(source, destination, *, exclude=()):
    attributes = {
        name: source.getncattr(name) for name in source.ncattrs() if name not in exclude
    }
    if attributes:
        destination.setncatts(attributes)


def _output_chunksizes(source_variable, output_dimension_sizes):
    """Preserve source chunks, clamped to the smaller subset dimensions."""
    chunks = source_variable.chunking()
    if chunks == "contiguous":
        return None
    return tuple(
        max(1, min(int(chunk), output_dimension_sizes[dimension]))
        for dimension, chunk in zip(source_variable.dimensions, chunks)
    )


def _create_netcdf_variable(output, source_variable, output_dimension_sizes):
    fill_value = (
        source_variable.getncattr("_FillValue")
        if "_FillValue" in source_variable.ncattrs()
        else None
    )
    options = {}
    chunksizes = _output_chunksizes(source_variable, output_dimension_sizes)
    if chunksizes is not None:
        options["chunksizes"] = chunksizes
    if compress_final_output():
        options.update(zlib=True, complevel=deflate_level(), shuffle=True)

    destination = output.createVariable(
        source_variable.name,
        source_variable.datatype,
        source_variable.dimensions,
        fill_value=fill_value,
        **options,
    )
    _copy_netcdf_attributes(source_variable, destination, exclude={"_FillValue"})
    source_variable.set_auto_maskandscale(False)
    destination.set_auto_maskandscale(False)
    return destination


def execute_netcdf4_slice_job(job_id, args):
    """Write a bounded-memory NetCDF4 subset directly, without temp chunks."""
    source_path = input_filepath(args)
    temp_path = final_temp_filepath(job_id, args)
    final_path = output_filepath(args)
    time_start, time_end = args["time"]
    selected_sizes = {
        "time": time_end - time_start + 1,
        "lat": args["lat"][1] - args["lat"][0] + 1,
        "lon": args["lon"][1] - args["lon"][0] + 1,
    }
    started = monotonic()

    try:
        with netCDF4.Dataset(source_path) as source:
            source_variable = source.variables[args["variable"]]
            source_variable.set_auto_maskandscale(False)
            windows = time_windows(
                args,
                source_variable.dtype.itemsize,
                byte_budget=netcdf4_slab_byte_budget(),
            )
            write_running_job_status(
                job_id,
                args,
                phase="extracting",
                chunks_complete=0,
                chunks_total=len(windows),
            )

            with netCDF4.Dataset(temp_path, "w", format="NETCDF4") as output:
                _copy_netcdf_attributes(source, output, exclude={"_NCProperties"})
                for dimension in ("time", "lat", "lon"):
                    output.createDimension(
                        dimension,
                        None if dimension == "time" else selected_sizes[dimension],
                    )

                output_sizes = dict(selected_sizes)
                destination_variables = {}
                for name in ("time", "lat", "lon", args["variable"]):
                    if name not in source.variables or name in destination_variables:
                        continue
                    destination_variables[name] = _create_netcdf_variable(
                        output, source.variables[name], output_sizes
                    )

                for dimension in ("time", "lat", "lon"):
                    if dimension not in destination_variables:
                        continue
                    start, end = args[dimension]
                    destination_variables[dimension][:] = source.variables[dimension][
                        start : end + 1
                    ]

                destination_variable = destination_variables[args["variable"]]
                for completed, (slab_start, slab_end) in enumerate(windows, start=1):
                    source_slice = []
                    destination_slice = []
                    for dimension in source_variable.dimensions:
                        start, end = args[dimension]
                        if dimension == "time":
                            source_slice.append(
                                builtins.slice(slab_start, slab_end + 1)
                            )
                            output_start = slab_start - time_start
                            destination_slice.append(
                                builtins.slice(
                                    output_start,
                                    output_start + slab_end - slab_start + 1,
                                )
                            )
                        else:
                            source_slice.append(builtins.slice(start, end + 1))
                            destination_slice.append(builtins.slice(None))

                    data = source_variable[tuple(source_slice)]
                    destination_variable[tuple(destination_slice)] = data
                    del data
                    write_running_job_status(
                        job_id,
                        args,
                        phase="extracting",
                        chunks_complete=completed,
                        chunks_total=len(windows),
                    )

        os.replace(temp_path, final_path)
    except Exception:  # netCDF-C errors surface as several Python types
        logger.exception("NetCDF4 slice job %s failed", job_id)
        fail_job(
            job_id,
            args,
            "Direct NetCDF4 subset failed. Try a smaller time or spatial range.",
        )
        return

    logger.info(
        "NetCDF4 slice job %s wrote %s bytes in %.2fs using %s slabs",
        job_id,
        os.path.getsize(final_path),
        monotonic() - started,
        len(windows),
    )
    cleanup_job_temp_dir(job_id)
    payload = read_job_status(job_id)
    if payload is None:
        logger.warning("Slice job %s lost its status record", job_id)
        return
    write_job_status(
        job_id,
        build_job_status(
            job_id,
            args,
            "complete",
            started_at=payload.get("started_at"),
            completed_at=utcnow_iso(),
            operator_warnings=[],
        ),
    )


def execute_slice_job(job_id, args):
    """Run one queued bounded-memory NetCDF4 slice job."""
    return execute_netcdf4_slice_job(job_id, args)


def slice(args):
    """Enqueue a slice job onto the Dragonfly-backed queue and return 202
    immediately with the job's queue position. A separate worker process
    (worker.py) picks the job up and calls execute_slice_job.
    """
    job_id = uuid.uuid4().hex
    ensure_job_temp_dir(job_id)

    payload = build_job_status(
        job_id,
        args,
        "queued",
        started_at=None,
        queued_at=utcnow_iso(),
        last_seen_at=utcnow_iso(),
    )
    write_job_status(job_id, payload)

    position = queue_client.enqueue_slice_job(job_id, args)
    logger.info(
        "Slice job queued for %s -> %s (job_id=%s, position=%s)",
        input_filepath(args),
        output_filepath(args),
        job_id,
        position,
    )

    response = response_json(
        {
            "status": "queued",
            "job_id": job_id,
            "queue_position": position,
            "status_url": status_url(job_id),
            "download_url": output_url(args),
            "output_filename": output_filename(args),
        },
        status=202,
    )
    response.headers["Location"] = output_url(args)
    response.headers["X-Job-Id"] = job_id
    return response


def slice_status(job_id):
    payload = read_job_status(job_id)
    if payload is None:
        return response_json({"status": "not_found", "job_id": job_id}, status=404)

    if payload.get("status") == "queued":
        stale_reason = queued_job_stale_reason(payload)
        if stale_reason is not None:
            if fail_and_discard_queued_job(job_id, payload, stale_reason):
                return response_json(read_job_status(job_id), status=200)
            latest_payload = read_job_status(job_id)
            if latest_payload is not None:
                return response_json(latest_payload, status=200)
            return response_json({"status": "not_found", "job_id": job_id}, status=404)

        payload = {
            **payload,
            "last_seen_at": utcnow_iso(),
            "updated_at": utcnow_iso(),
        }
        write_job_status(job_id, payload)
        position = queue_client.queue_position(job_id)
        if position is not None:
            payload = {**payload, "queue_position": position}
        # If position is None here, a worker has already picked the job up
        # (popped it from the queue) but hasn't written "running" yet --
        # a brief, harmless window. The status will read "running" shortly.

    return response_json(payload, status=200)


def dap_filepath(args):
    """Construct the filepath for DDS/DAS requests."""
    thredds_base = os.getenv("THREDDS_DAP_BASE")
    return f"{thredds_base}/{args['dirname']}/{args['basename']}.{args['extension']}"


def dds(args):
    filepath = dap_filepath(args)
    logger.info("Received DDS request: filepath=%s", filepath)
    if "target" in args:
        return redirect(f"{filepath}.dds?{args['target']}")
    return redirect(f"{filepath}.dds")


def das(args):
    filepath = dap_filepath(args)
    logger.info("Received DAS request: filepath=%s", filepath)
    return redirect(f"{filepath}.das")


def asc(args):
    filepath = dap_filepath(args)
    dims = (
        args["target"] if isinstance(args["target"], str) else ",".join(args["target"])
    )
    logger.info("Received ASCII request: filepath=%s", filepath)
    return redirect(f"{filepath}.ascii?{dims}")
