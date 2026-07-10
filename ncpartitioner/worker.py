"""Standalone worker process for slice jobs.

Runs as its own container/process, entirely separate from the gunicorn
processes that handle HTTP requests. Pulls job ids off the Dragonfly queue
and runs them to completion via execute_slice_job (unchanged from the
request-handling module).

Concurrency model:
  - Each worker process runs jobs ONE AT A TIME from the queue, but within
    a job still uses the existing ThreadPoolExecutor (max_workers, default
    3) for parallel ncks calls.
  - Total system-wide ncks concurrency = (number of worker processes) x
    NCPARTITIONER_MAX_WORKERS. Run N copies of this worker (e.g. via
    `deploy: replicas: N` in compose) to size that independently of
    gunicorn's --workers, which now only governs HTTP-handling capacity.

Run with: python worker.py
"""

import logging
import os
import sys
import time

from . import queue_client, response

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [worker] %(message)s",
)
logger = logging.getLogger(__name__)

# How long to block on an empty queue before looping back around (lets the
# process notice signals / shut down cleanly rather than blocking forever).
POLL_TIMEOUT_SECONDS = 5


def run_one_job(job_id, args):
    existing = response.read_job_status(job_id) or {}
    stale_reason = response.queued_job_stale_reason(existing)
    if stale_reason is not None:
        logger.info("Dropping stale queued job %s: %s", job_id, stale_reason)
        response.fail_job_from_status_payload(job_id, existing, stale_reason)
        return

    if args is None:
        logger.warning(
            "Job %s popped from queue but its args were missing (TTL'd out?) "
            "-- marking failed without running.",
            job_id,
        )
        if existing:
            response.fail_job_from_status_payload(
                job_id, existing, "Job args expired before processing"
            )
        else:
            response.fail_job(job_id, {}, "Job args expired before processing")
        return

    logger.info("Picked up job %s", job_id)
    started_at = existing.get("started_at") or response.utcnow_iso()
    response.write_job_status(
        job_id,
        response.build_job_status(job_id, args, "running", started_at=started_at),
    )

    try:
        response.execute_slice_job(job_id, args)
    except Exception:  # noqa: BLE001 -- last-resort guard so one bad job
        # can't kill the worker loop; execute_slice_job already handles its
        # own expected failure modes via fail_job/run_subprocess_step.
        logger.exception("Unhandled exception while running job %s", job_id)
        try:
            response.fail_job(job_id, args, "Unhandled worker exception")
        except Exception:  # noqa: BLE001 -- keep the worker alive even if
            # the status file itself is corrupted or otherwise unwritable.
            logger.exception(
                "Unable to persist failure status for job %s after run error",
                job_id,
            )
    else:
        logger.info("Finished job %s", job_id)


def main():
    logger.info("Worker starting, waiting for jobs...")
    while True:
        try:
            job_id, args = queue_client.dequeue_slice_job(timeout=POLL_TIMEOUT_SECONDS)
        except Exception:  # noqa: BLE001 -- connection hiccups shouldn't
            # crash the worker; back off briefly and retry.
            logger.exception("Error polling queue, retrying shortly")
            time.sleep(POLL_TIMEOUT_SECONDS)
            continue

        if job_id is None:
            continue  # timed out with nothing queued, loop back around

        try:
            run_one_job(job_id, args)
        except Exception:  # noqa: BLE001 -- last-resort guard around the
            # entire dispatch path, including the initial status transition
            # to "running". Without this, one status-write race can kill the
            # whole worker process and strand the remaining queue.
            logger.exception("Unhandled exception while dispatching job %s", job_id)
            if args is None:
                continue
            try:
                response.fail_job(job_id, args, "Unhandled worker exception")
            except Exception:  # noqa: BLE001 -- log and keep serving
                logger.exception(
                    "Unable to persist failure status for job %s after dispatch error",
                    job_id,
                )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Worker shutting down")
        sys.exit(0)
