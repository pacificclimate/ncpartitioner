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
import tempfile
import threading
import uuid
from datetime import datetime, timezone
from time import monotonic

from flask import Response, redirect

from . import queue_client

logger = logging.getLogger(__name__)

TERMINAL_JOB_STATUSES = {"complete", "failed"}
DEFAULT_BYTES_PER_ELEMENT = 4
NETCDF_TYPE_BYTES = {
    "byte": 1,
    "char": 1,
    "ubyte": 1,
    "short": 2,
    "ushort": 2,
    "int": 4,
    "uint": 4,
    "float": 4,
    "int64": 8,
    "uint64": 8,
    "double": 8,
}
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


def chunk_byte_budget():
    return int(os.getenv("NCPARTITIONER_CHUNK_BYTES", 1024 * 1024 * 1024))


def bytes_per_element(source_bytes=None):
    configured = os.getenv("NCPARTITIONER_BYTES_PER_ELEMENT")
    if configured is not None:
        return int(configured)
    if source_bytes is not None:
        return source_bytes
    return DEFAULT_BYTES_PER_ELEMENT


def time_windows(args, source_bytes=None):
    start, end = args["time"]
    lat0, lat1 = args["lat"]
    lon0, lon1 = args["lon"]
    n_lat, n_lon = lat1 - lat0 + 1, lon1 - lon0 + 1
    bytes_per_step = max(n_lat * n_lon * bytes_per_element(source_bytes), 1)
    window = max(1, chunk_byte_budget() // bytes_per_step)
    return [(s, min(s + window - 1, end)) for s in range(start, end + 1, window)]


def chunk_output_filepath(job_id, index):
    return os.path.join(job_temp_dir(job_id), f"chunk_{index:04d}.nc")


def final_temp_filepath(job_id, args):
    return os.path.join(job_temp_dir(job_id), output_filename(args))


def deflate_level():
    return int(os.getenv("NCPARTITIONER_DEFLATE_LEVEL", 1))


def compress_intermediate_chunks():
    return os.getenv("NCPARTITIONER_COMPRESS_INTERMEDIATE_CHUNKS") == "true"


def compress_final_output():
    return os.getenv("NCPARTITIONER_COMPRESS_FINAL_OUTPUT") == "true"


def ncrcat_threads():
    return max(1, int(os.getenv("NCPARTITIONER_NCRCAT_THREADS", 1)))


def chunk_cache_bytes():
    """HDF5 per-variable chunk cache size for generated netCDF4 files."""
    return int(os.getenv("NCPARTITIONER_CNK_CSH_BYTES", 64 * 1024 * 1024))


def chunk_cache_flags():
    """Set NCO's cache without changing the source file's chunk layout."""
    return [
        "--cnk_csh",
        str(chunk_cache_bytes()),
    ]


def source_variable_bytes_from_header(header, variable):
    declaration_prefix = f"{variable}("
    for line in header.splitlines():
        stripped = line.strip()
        if not stripped.endswith(";") or declaration_prefix not in stripped:
            continue
        left_side = stripped.split("(", 1)[0].strip()
        parts = left_side.split()
        if len(parts) != 2 or parts[1] != variable:
            continue
        return NETCDF_TYPE_BYTES.get(parts[0], DEFAULT_BYTES_PER_ELEMENT)
    return DEFAULT_BYTES_PER_ELEMENT


def inspect_source(source_filepath, variable):
    """Read the source file's header (a single `ncdump -hs`) to determine
    whether `time` is already the record (UNLIMITED) dimension, what
    deflate level the variable is already stored at, and the variable's
    storage width in bytes.
    """
    is_unlimited = False
    level = 0
    variable_bytes = DEFAULT_BYTES_PER_ELEMENT
    try:
        result = subprocess.run(
            ["ncdump", "-hs", source_filepath],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=True,
        )
        output = result.stdout or ""
        is_unlimited = "UNLIMITED" in output

        variable_bytes = source_variable_bytes_from_header(output, variable)

        marker = f"{variable}:_DeflateLevel"
        for line in output.splitlines():
            stripped = line.strip()
            if stripped.startswith(marker):
                try:
                    level = int(stripped.split("=")[1].strip().rstrip(" ;"))
                except (IndexError, ValueError):
                    level = 0
                break
    except (subprocess.CalledProcessError, OSError):
        is_unlimited = False
        level = 0
        variable_bytes = DEFAULT_BYTES_PER_ELEMENT

    return is_unlimited, level, variable_bytes


def chunk_deflate_level(source_deflate, target_deflate):
    """Compression level to force on every chunk during slicing.
    Preserves the source's existing level if it has one.
    """
    return source_deflate if source_deflate > 0 else target_deflate


def slice_command(
    args,
    source_filepath,
    destination,
    time_start,
    time_end,
    chunk_level,
    add_record_dimension=False,
):
    command = [
        "ncks",
        "-O",
        "-h",
        "--no_tmp_fl",
        "-4",
    ]
    command.extend(chunk_cache_flags())
    if chunk_level is not None:
        command.extend(["-L", str(chunk_level)])
    if add_record_dimension:
        command.extend(["--mk_rec_dmn", "time"])
    command.extend(
        [
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
    )
    return command


def record_chunk_output_filepath(job_id, index):
    return os.path.join(job_temp_dir(job_id), f"record_chunk_{index:04d}.nc")


def make_record_dimension_command(source, destination, args, chunk_level=None):
    command = [
        "ncks",
        "-O",
        "-h",
        "--no_tmp_fl",
        "-4",
        "--mk_rec_dmn",
        "time",
    ]
    command.extend(chunk_cache_flags())
    if chunk_level is not None:
        command.extend(["-L", str(chunk_level)])
    command.extend([source, destination])
    return command


def concat_command(chunk_paths, destination, args, final_level=None):
    command = [
        "ncrcat",
        "-O",
        "-h",
        "--no_tmp_fl",
        "-4",
    ]
    if final_level is not None:
        command.extend(["-L", str(final_level)])
    command.extend(chunk_cache_flags())

    threads = ncrcat_threads()
    if threads > 1:
        command.extend(["-t", str(threads)])
    command.extend([*chunk_paths, destination])
    return command


def cleanup_job_temp_dir(job_id):
    shutil.rmtree(job_temp_dir(job_id), ignore_errors=True)


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


def run_subprocess(cmd):
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=True,
    )
    return result.stdout.strip() if result.stdout else None


def log_subprocess_failure(job_id, cmd, exc, *, fallback=False):
    log = logger.warning if fallback else logger.exception
    log(
        "Slice job %s subprocess failed: cmd=%s returncode=%s output=%r",
        job_id,
        cmd,
        getattr(exc, "returncode", None),
        getattr(exc, "stdout", None) or getattr(exc, "stderr", None),
    )


def run_subprocess_step(job_id, args, cmd):
    """Run a subprocess step. Returns stripped output (or None) on success.
    On failure, fails the job and returns _STEP_FAILED.
    """
    try:
        return run_subprocess(cmd)
    except (subprocess.CalledProcessError, OSError) as exc:
        log_subprocess_failure(job_id, cmd, exc)
        fail_job(
            job_id,
            args,
            subprocess_error_message(exc, cmd),
            returncode=getattr(exc, "returncode", None),
        )
        return _STEP_FAILED


def try_subprocess_step(job_id, cmd):
    """Run a subprocess step without mutating job status on failure."""
    try:
        return True, run_subprocess(cmd), None
    except (subprocess.CalledProcessError, OSError) as exc:
        log_subprocess_failure(job_id, cmd, exc, fallback=True)
        return False, None, exc


def looks_like_missing_record_dimension(exc):
    output = (
        getattr(exc, "stdout", None) or getattr(exc, "stderr", None) or ""
    ).lower()
    return (
        "record" in output
        or "unlimited" in output
        or "no variables fit criteria" in output
    )


DEFAULT_MAX_WORKERS = 1


def max_workers(num_windows):
    configured = int(os.getenv("NCPARTITIONER_MAX_WORKERS", DEFAULT_MAX_WORKERS))
    return max(1, min(configured, num_windows))


def total_file_size(paths):
    return sum(os.path.getsize(path) for path in paths if os.path.exists(path))


def concat_chunks_with_fallback(
    job_id, args, chunk_paths, destination, chunk_level=None, final_level=None
):
    command = concat_command(chunk_paths, destination, args, final_level=final_level)
    success, stderr, exc = try_subprocess_step(job_id, command)
    if success:
        return [stderr] if stderr else []

    if not looks_like_missing_record_dimension(exc):
        fail_job(
            job_id,
            args,
            subprocess_error_message(exc, command),
            returncode=getattr(exc, "returncode", None),
        )
        return _STEP_FAILED

    logger.info(
        "Slice job %s ncrcat fallback triggered; converting chunks to record dimension",
        job_id,
    )
    write_running_job_status(
        job_id,
        args,
        phase="converting_record_dimension",
        chunks_complete=len(chunk_paths),
        chunks_total=len(chunk_paths),
        ncrcat_fallback=True,
    )

    converted_paths = [None] * len(chunk_paths)
    conversion_messages = []
    workers = max_workers(len(chunk_paths))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                run_subprocess_step,
                job_id,
                args,
                make_record_dimension_command(
                    chunk_path,
                    record_chunk_output_filepath(job_id, index),
                    args,
                    chunk_level,
                ),
            ): index
            for index, chunk_path in enumerate(chunk_paths)
        }
        for future in concurrent.futures.as_completed(futures):
            index = futures[future]
            stderr = future.result()
            if stderr is _STEP_FAILED:
                return _STEP_FAILED
            if stderr:
                conversion_messages.append(stderr)
            converted_paths[index] = record_chunk_output_filepath(job_id, index)

    retry_command = concat_command(
        converted_paths, destination, args, final_level=final_level
    )
    success, stderr, retry_exc = try_subprocess_step(job_id, retry_command)
    if not success:
        fail_job(
            job_id,
            args,
            subprocess_error_message(retry_exc, retry_command),
            returncode=getattr(retry_exc, "returncode", None),
        )
        return _STEP_FAILED

    messages = conversion_messages
    if stderr:
        messages.append(stderr)
    return messages


def execute_slice_job(job_id, args):
    """Run one slice job, invoked by the worker process."""
    source_filepath = input_filepath(args)
    final_path = output_filepath(args)
    source_is_unlimited, source_level, source_bytes = inspect_source(
        source_filepath, args["variable"]
    )
    windows = time_windows(args, source_bytes)
    workers = max_workers(len(windows))
    lookahead = workers
    completed_chunks, in_flight = {}, {}
    next_to_submit = 0
    stderr_messages = []
    job_started = monotonic()
    needs_record_dimension = not source_is_unlimited
    source_compression_level = chunk_deflate_level(source_level, deflate_level())
    chunk_level = source_compression_level if compress_intermediate_chunks() else None
    final_level = source_compression_level if compress_final_output() else None

    logger.info(
        "Slice job %s extracting %s chunks with %s workers; chunk_byte_budget=%s "
        "source_bytes_per_element=%s intermediate_chunk_deflate=%s "
        "final_deflate_level=%s needs_record_dimension=%s",
        job_id,
        len(windows),
        workers,
        chunk_byte_budget(),
        source_bytes,
        chunk_level,
        final_level,
        needs_record_dimension,
    )
    write_running_job_status(
        job_id,
        args,
        phase="extracting",
        chunks_complete=0,
        chunks_total=len(windows),
    )

    def slice_one(index, time_start, time_end):
        chunk_path = chunk_output_filepath(job_id, index)
        stderr = run_subprocess_step(
            job_id,
            args,
            slice_command(
                args,
                source_filepath,
                chunk_path,
                time_start,
                time_end,
                chunk_level,
                add_record_dimension=needs_record_dimension,
            ),
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

            write_running_job_status(
                job_id,
                args,
                phase="extracting",
                chunks_complete=len(completed_chunks),
                chunks_total=len(windows),
            )
            submit_more()

    extraction_finished = monotonic()
    ordered_chunk_paths = [completed_chunks[index] for index in range(len(windows))]
    temp_final_path = final_temp_filepath(job_id, args)
    chunk_bytes = total_file_size(ordered_chunk_paths)
    logger.info(
        "Slice job %s extracted %s chunks (%s bytes) in %.2fs; starting ncrcat",
        job_id,
        len(ordered_chunk_paths),
        chunk_bytes,
        extraction_finished - job_started,
    )
    write_running_job_status(
        job_id,
        args,
        phase="merging",
        chunks_complete=len(ordered_chunk_paths),
        chunks_total=len(windows),
        chunk_bytes=chunk_bytes,
    )
    if len(ordered_chunk_paths) == 1 and final_level is None:
        os.replace(ordered_chunk_paths[0], temp_final_path)
    else:
        merge_messages = concat_chunks_with_fallback(
            job_id,
            args,
            ordered_chunk_paths,
            temp_final_path,
            chunk_level=chunk_level,
            final_level=final_level,
        )
        if merge_messages is _STEP_FAILED:
            return
        stderr_messages.extend(msg for msg in merge_messages if msg)
    os.replace(temp_final_path, final_path)
    merge_finished = monotonic()
    logger.info(
        "Slice job %s finished ncrcat in %.2fs; final size=%s bytes",
        job_id,
        merge_finished - extraction_finished,
        os.path.getsize(final_path) if os.path.exists(final_path) else None,
    )

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
