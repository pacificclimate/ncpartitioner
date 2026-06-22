"""Thin wrapper around a Dragonfly (Redis-protocol) list used as a FIFO job
queue for slice jobs.

Design notes:
  - The list itself (`QUEUE_KEY`) holds only job_id strings, kept small and
    cheap to scan with LPOS for queue-position lookups.
  - Each job's actual `args` payload (dirname/basename/extension/etc.) is
    stored separately under a per-job key, since list elements shouldn't
    carry large/variable-shaped payloads.
  - Enqueue is RPUSH (append to tail); workers BLPOP from the head, giving
    strict FIFO order.
"""

import json
import os

import redis

QUEUE_KEY = "ncpartitioner:slice_queue"
JOB_ARGS_KEY_PREFIX = "ncpartitioner:slice_args:"
JOB_ARGS_TTL_SECONDS = 60 * 60 * 24  # 1 day; avoids orphaned args piling up
# forever if a job_id is enqueued but never picked up.

_client = None


def get_client():
    """Lazily build a single shared redis client for this process."""
    global _client
    if _client is None:
        password_file = os.getenv("DRAGONFLY_PASSWORD_FILE")
        password = None
        if password_file and os.path.isfile(password_file):
            with open(password_file, encoding="utf-8") as handle:
                password = handle.read().strip()
        _client = redis.Redis(
            host=os.getenv("DRAGONFLY_HOST", "dragonfly"),
            port=int(os.getenv("DRAGONFLY_PORT", 6379)),
            password=password,
            decode_responses=True,
        )
    return _client


def _job_args_key(job_id):
    return f"{JOB_ARGS_KEY_PREFIX}{job_id}"


def enqueue_slice_job(job_id, args):
    """Store the job's args and push its id onto the tail of the queue.
    Returns the 1-indexed position of this job in the queue at the moment
    of enqueue (i.e. how many jobs, including this one, are ahead of or at
    this job -- a freshly enqueued job with nothing ahead of it gets 1).
    """
    client = get_client()
    client.set(_job_args_key(job_id), json.dumps(args), ex=JOB_ARGS_TTL_SECONDS)
    client.rpush(QUEUE_KEY, job_id)
    return queue_position(job_id)


def queue_position(job_id):
    """Return the 1-indexed position of job_id in the queue, or None if it's
    not currently queued (already picked up by a worker, or never enqueued).
    """
    client = get_client()
    index = client.lpos(QUEUE_KEY, job_id)
    if index is None:
        return None
    return index + 1


def queue_length():
    return get_client().llen(QUEUE_KEY)


def dequeue_slice_job(timeout=0):
    """Blocking pop from the head of the queue. Returns (job_id, args) or
    (None, None) if `timeout` elapses with nothing queued (timeout=0 blocks
    forever, matching BLPOP semantics).
    """
    client = get_client()
    result = client.blpop([QUEUE_KEY], timeout=timeout)
    if result is None:
        return None, None
    _key, job_id = result
    raw_args = client.get(_job_args_key(job_id))
    if raw_args is None:
        # Args TTL'd out or were never written -- shouldn't happen under
        # normal operation, but don't crash the worker loop over it.
        return job_id, None
    client.delete(_job_args_key(job_id))
    return job_id, json.loads(raw_args)
