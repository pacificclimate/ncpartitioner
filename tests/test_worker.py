import json
import pytest
from unittest.mock import patch

from ncpartitioner.response import build_job_status, read_job_status, write_job_status
from ncpartitioner.worker import main, run_one_job


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


def test_write_job_status_uses_unique_temp_paths(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    args = {
        "basename": "tasmax",
        "dirname": "tests/data",
        "extension": "nc",
        "timestamp": 1234567890,
    }
    job_id = "unique-temp-status-job"
    payload = build_job_status(job_id, args, "queued")

    with patch("ncpartitioner.response.os.replace") as replace:
        write_job_status(job_id, payload)
        write_job_status(job_id, payload)

    assert len(replace.call_args_list) == 2
    first_src, first_dst = replace.call_args_list[0].args
    second_src, second_dst = replace.call_args_list[1].args
    assert first_src != second_src
    assert first_dst == second_dst
    assert first_src.endswith(".json.tmp")
    assert second_src.endswith(".json.tmp")


def test_read_job_status_recovers_last_concatenated_payload(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    args = {
        "basename": "tasmax",
        "dirname": "tests/data",
        "extension": "nc",
        "timestamp": 1234567890,
    }
    job_id = "concatenated-status-job"
    jobs_dir = tmp_path / ".jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    status_path = jobs_dir / f"{job_id}.json"
    first_payload = build_job_status(job_id, args, "queued")
    second_payload = build_job_status(
        job_id,
        args,
        "running",
        started_at="2026-01-01T00:00:00+00:00",
    )

    status_path.write_text(
        json.dumps(first_payload) + json.dumps(second_payload),
        encoding="utf-8",
    )

    payload = read_job_status(job_id)

    assert payload["status"] == "running"
    with open(status_path, encoding="utf-8") as handle:
        assert json.load(handle)["status"] == "running"


def test_worker_main_survives_dispatch_error():
    args = {
        "basename": "tasmax",
        "dirname": "tests/data",
        "extension": "nc",
        "timestamp": 1234567890,
    }

    with patch(
        "ncpartitioner.worker.queue_client.dequeue_slice_job",
        side_effect=[("dispatch-error-job", args), KeyboardInterrupt()],
    ):
        with patch(
            "ncpartitioner.worker.run_one_job",
            side_effect=FileNotFoundError("status race"),
        ):
            with patch("ncpartitioner.worker.response.fail_job") as fail_job:
                with pytest.raises(KeyboardInterrupt):
                    main()

    fail_job.assert_called_once_with(
        "dispatch-error-job",
        args,
        "Unhandled worker exception",
    )
