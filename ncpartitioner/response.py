"""Send responses to user requests.

DDS/DAS/ASCII requests redirect immediately to THREDDS. NetCDF slice requests
are enqueued onto a Dragonfly-backed queue and processed by a separate
worker process (see worker.py). Job status is published through local
metadata stored under OUTPUT_DIR/.jobs, same as before -- only how a job
gets *started* has changed; execute_slice_job itself is unmodified.
"""

import concurrent.futures
import json
import logging
import os
import shutil
import subprocess
import threading
import uuid
from datetime import datetime, timezone

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


def write_job_status(job_id, payload):
    ensure_jobs_dir()
    with job_lock(job_id):
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
    return f"partition/status/{job_id}"


def response_json(payload, status=200):
    return Response(json.dumps(payload), status=status, mimetype="application/json")


# Target chunk size in bytes, used to size time windows so that a single
# ncks slice has a roughly constant memory/IO footprint regardless of how
# large a spatial subset the request covers.
BYTES_PER_ELEMENT = 4  # adjust per dtype if you support more than float32


def chunk_byte_budget():
    return int(os.getenv("NCPARTITIONER_CHUNK_BYTES", 300 * 1024 * 1024))  # ~300MB


def time_windows(args):
    start, end = args["time"]
    lat0, lat1 = args["lat"]
    lon0, lon1 = args["lon"]
    n_lat, n_lon = lat1 - lat0 + 1, lon1 - lon0 + 1
    bytes_per_step = max(n_lat * n_lon * BYTES_PER_ELEMENT, 1)
    window = max(1, chunk_byte_budget() // bytes_per_step)
    return [(s, min(s + window - 1, end)) for s in range(start, end + 1, window)]


def chunk_output_filepath(job_id, index):
    return os.path.join(job_temp_dir(job_id), f"chunk_{index:04d}.nc")


def merge_round_output_filepath(job_id, round_index, batch_index):
    return os.path.join(
        job_temp_dir(job_id), f"merge_r{round_index:02d}_{batch_index:04d}.nc"
    )


def deflate_level():
    return int(os.getenv("NCPARTITIONER_DEFLATE_LEVEL", 1))


def slice_command(args, source_filepath, destination, time_start, time_end):
    return [
        "ncks",
        "-4",
        "-L",
        str(deflate_level()),
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


def merge_batch_size():
    return max(2, int(os.getenv("NCPARTITIONER_MERGE_BATCH_SIZE", 16)))


def subprocess_error_message(exc, cmd):
    if isinstance(exc, OSError):
        return "Subset request failed due to a processing error. Please try again."

    step = cmd[0] if cmd else "subprocess"
    if step == "ncrcat":
        return "Subset assembly failed. Try a smaller time or spatial range."
    if step == "ncks":
        return "Subset extraction failed. Try a smaller time or spatial range."
    return "Subset request failed. Please try again."


def fail_job(job_id, args, error, *, returncode=None):
    """Mark a job as failed, preserving its original started_at if known,
    and clean up any temp chunk files. Used for both per-step subprocess
    failures and post-hoc validation failures (e.g. missing output file).
    """
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


# Sentinel distinguishing "step failed, job already marked failed" from a
# real stderr string (which may be empty/None on a clean run).
_STEP_FAILED = object()


def run_subprocess_step(job_id, args, cmd):
    """Run a single ncks/ncrcat step. Returns the stripped stderr output (or
    None) on success. On failure, fails the job and returns the sentinel
    _STEP_FAILED so callers can short-circuit without raising.
    """
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, OSError) as exc:
        logger.exception(
            "Slice job %s subprocess failed: cmd=%s returncode=%s stderr=%r",
            job_id,
            cmd,
            getattr(exc, "returncode", None),
            getattr(exc, "stderr", None),
        )
        fail_job(
            job_id,
            args,
            subprocess_error_message(exc, cmd),
            returncode=getattr(exc, "returncode", None),
        )
        return _STEP_FAILED
    return result.stderr.strip() if result.stderr else None


DEFAULT_MAX_WORKERS = 3


def max_workers(num_windows):
    configured = int(os.getenv("NCPARTITIONER_MAX_WORKERS", DEFAULT_MAX_WORKERS))
    return max(1, min(configured, num_windows))


def merge_chunk_paths(job_id, args, chunk_paths, final_path):
    if len(chunk_paths) == 1:
        os.replace(chunk_paths[0], final_path)
        return []

    current_paths = list(chunk_paths)
    temporary_outputs = []
    round_index = 0

    while len(current_paths) > 1:
        next_paths = []
        batch_size = merge_batch_size()
        for batch_index, start in enumerate(range(0, len(current_paths), batch_size)):
            batch = current_paths[start : start + batch_size]
            if len(batch) == 1:
                next_paths.append(batch[0])
                continue

            is_final_batch = len(current_paths) <= batch_size
            destination = (
                final_path
                if is_final_batch
                else merge_round_output_filepath(job_id, round_index, batch_index)
            )
            stderr = run_subprocess_step(job_id, args, ["ncrcat", *batch, destination])
            if stderr is _STEP_FAILED:
                return _STEP_FAILED
            temporary_outputs.extend(path for path in batch if path != final_path)
            next_paths.append(destination)

        current_paths = next_paths
        round_index += 1

    return temporary_outputs


def execute_slice_job(job_id, args):
    """Run one slice job, invoked by the worker process."""
    source_filepath = input_filepath(args)
    windows = time_windows(args)
    final_path = output_filepath(args)
    workers = max_workers(len(windows))
    lookahead = workers
    completed_chunks, in_flight = {}, {}
    next_to_submit = 0
    stderr_messages = []

    def slice_one(index, time_start, time_end):
        chunk_path = chunk_output_filepath(job_id, index)
        stderr = run_subprocess_step(
            job_id,
            args,
            slice_command(args, source_filepath, chunk_path, time_start, time_end),
        )
        return index, chunk_path, stderr

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:

        def submit_more():
            nonlocal next_to_submit
            while (
                next_to_submit < len(windows)
                and next_to_submit - len(completed_chunks) < workers + lookahead
            ):
                time_start, time_end = windows[next_to_submit]
                future = pool.submit(slice_one, next_to_submit, time_start, time_end)
                in_flight[future] = next_to_submit
                next_to_submit += 1

        submit_more()
        while in_flight:
            done, _ = concurrent.futures.wait(
                in_flight, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                in_flight.pop(future)
                index, chunk_path, stderr = future.result()
                if stderr is _STEP_FAILED:
                    return
                if stderr:
                    stderr_messages.append(stderr)
                completed_chunks[index] = chunk_path

            submit_more()

    ordered_chunk_paths = [completed_chunks[index] for index in range(len(windows))]
    merged_inputs = merge_chunk_paths(job_id, args, ordered_chunk_paths, final_path)
    if merged_inputs is _STEP_FAILED:
        return
    for path in merged_inputs:
        if os.path.exists(path):
            os.remove(path)

    cleanup_job_temp_dir(job_id)
    payload = read_job_status(job_id)
    if payload is None:
        logger.warning("Slice job %s lost its status record", job_id)
        return

    if len(completed_chunks) == len(windows) and os.path.isfile(final_path):
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

    fail_job(job_id, args, "Slice job did not create an output file")


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
