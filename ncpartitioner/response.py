"""Send responses to user requests.

DDS/DAS/ASCII requests redirect immediately to THREDDS. NetCDF slice requests
run asynchronously and publish job status through local metadata stored under
OUTPUT_DIR/.jobs.
"""

import json
import logging
import os
import shutil
import subprocess
import threading
import uuid
from datetime import datetime, timezone

from flask import Response, redirect

logger = logging.getLogger(__name__)

TERMINAL_JOB_STATUSES = {"complete", "failed"}
DEFAULT_TIME_WINDOW_SIZE = 10


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


def write_job_status(job_id, payload):
    ensure_jobs_dir()
    temp_path = f"{status_filepath(job_id)}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    os.replace(temp_path, status_filepath(job_id))


def read_job_status(job_id):
    try:
        with open(status_filepath(job_id), encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return None


def status_url(job_id):
    return f"/partition/status/{job_id}"


def response_json(payload, status=200):
    return Response(json.dumps(payload), status=status, mimetype="application/json")


def time_window_size():
    return int(os.getenv("NCPARTITIONER_TIME_WINDOW_SIZE", DEFAULT_TIME_WINDOW_SIZE))


def time_windows(args):
    start, end = args["time"]
    window = max(1, time_window_size())
    return [
        (window_start, min(window_start + window - 1, end))
        for window_start in range(start, end + 1, window)
    ]


def chunk_output_filepath(job_id, index):
    return os.path.join(job_temp_dir(job_id), f"chunk_{index:04d}.nc")


def slice_command(args, source_filepath, destination, time_start, time_end):
    return [
        "ncks",
        "-4",
        "-v",
        f"{args['variable']}",
        "-d",
        f"time,{time_start},{time_end}",
        "-d",
        f"lat,{args['lat'][0]},{args['lat'][1]}",
        "-d",
        f"lon,{args['lon'][0]},{args['lon'][1]}",
        source_filepath,
        destination,
    ]


def cleanup_job_temp_dir(job_id):
    shutil.rmtree(job_temp_dir(job_id), ignore_errors=True)


def execute_slice_job(job_id, args):
    source_filepath = input_filepath(args)
    chunk_paths = []
    stderr_messages = []

    try:
        for index, (time_start, time_end) in enumerate(time_windows(args)):
            chunk_path = chunk_output_filepath(job_id, index)
            chunk_paths.append(chunk_path)
            result = subprocess.run(
                slice_command(args, source_filepath, chunk_path, time_start, time_end),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )
            if result.stderr:
                stderr_messages.append(result.stderr.strip())
    except (subprocess.CalledProcessError, OSError) as exc:
        error = getattr(exc, "stderr", None)
        error = error.strip() if error else str(exc)
        write_job_status(
            job_id,
            build_job_status(
                job_id,
                args,
                "failed",
                started_at=read_job_status(job_id).get("started_at") if read_job_status(job_id) else None,
                completed_at=utcnow_iso(),
                error=error,
                returncode=getattr(exc, "returncode", None),
            ),
        )
        cleanup_job_temp_dir(job_id)
        return

    if len(chunk_paths) == 1:
        os.replace(chunk_paths[0], output_filepath(args))
    else:
        try:
            subprocess.run(
                ["ncrcat", *chunk_paths, output_filepath(args)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )
        except (subprocess.CalledProcessError, OSError) as exc:
            error = getattr(exc, "stderr", None)
            error = error.strip() if error else str(exc)
            write_job_status(
                job_id,
                build_job_status(
                    job_id,
                    args,
                    "failed",
                    started_at=read_job_status(job_id).get("started_at") if read_job_status(job_id) else None,
                    completed_at=utcnow_iso(),
                    error=error,
                    returncode=getattr(exc, "returncode", None),
                ),
            )
            cleanup_job_temp_dir(job_id)
            return

    cleanup_job_temp_dir(job_id)
    payload = read_job_status(job_id)
    if payload is None:
        logger.warning("Slice job %s lost its status record", job_id)
        return

    if os.path.isfile(output_filepath(args)):
        write_job_status(
            job_id,
            build_job_status(
                job_id,
                args,
                "complete",
                started_at=payload.get("started_at"),
                completed_at=utcnow_iso(),
                operator_warnings=[msg for msg in stderr_messages if msg],
            ),
        )
        return
    write_job_status(
        job_id,
        build_job_status(
            job_id,
            args,
            "failed",
            started_at=payload.get("started_at"),
            completed_at=utcnow_iso(),
            error="Slice job did not create an output file",
        ),
    )


def slice(args):
    job_id = uuid.uuid4().hex
    ensure_job_temp_dir(job_id)

    logger.info("Starting background slice job")
    payload = build_job_status(
        job_id,
        args,
        "running",
        started_at=utcnow_iso(),
    )
    write_job_status(job_id, payload)

    threading.Thread(
        target=execute_slice_job,
        args=(job_id, args),
        daemon=True,
    ).start()

    logger.info(
        "Background slice job started for %s -> %s (job_id=%s)",
        input_filepath(args),
        output_filepath(args),
        job_id,
    )
    response = response_json(
        {
            "status": "accepted",
            "job_id": job_id,
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
