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
    if args is None:
        logger.warning(
            "Job %s popped from queue but its args were missing (TTL'd out?) "
            "-- marking failed without running.",
            job_id,
        )
        response.fail_job(job_id, {}, "Job args expired before processing")
        return

    logger.info("Picked up job %s", job_id)
    existing = response.read_job_status(job_id)
    started_at = (existing or {}).get("started_at") or response.utcnow_iso()
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
        response.fail_job(job_id, args, "Unhandled worker exception")
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

        run_one_job(job_id, args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Worker shutting down")
        sys.exit(0)
