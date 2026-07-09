from unittest.mock import patch

from ncpartitioner.response import build_job_status, read_job_status, write_job_status
from ncpartitioner.worker import run_one_job


def test_run_one_job_drops_stale_queued_job(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("NCPARTITIONER_QUEUE_IDLE_TTL_SECONDS", "60")
    args = {
        "basename": "tasmax",
        "dirname": "tests/data",
        "extension": "nc",
        "timestamp": 1234567890,
    }
    job_id = "stale-worker-job"
    write_job_status(
        job_id,
        build_job_status(
            job_id,
            args,
            "queued",
            queued_at="2026-01-01T00:00:00+00:00",
            last_seen_at="2026-01-01T00:00:00+00:00",
        ),
    )

    with patch("ncpartitioner.worker.response.execute_slice_job") as execute_slice_job:
        run_one_job(job_id, args)

    payload = read_job_status(job_id)
    assert payload["status"] == "failed"
    assert payload["error"] == "Queued job was abandoned before processing"
    execute_slice_job.assert_not_called()
