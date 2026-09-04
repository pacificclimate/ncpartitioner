from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from ncpartitioner import create_app
from ncpartitioner.response import build_job_status, read_job_status, write_job_status


def test_partition_submits_slice_job():
    app = create_app()
    client = app.test_client()
    checked = {
        "request_format": "nc",
        "basename": "tasmax",
        "dirname": "tests/data",
        "extension": "nc",
        "timestamp": 1,
    }

    with (
        patch("ncpartitioner.routes.check_filepath", return_value=checked),
        patch(
            "ncpartitioner.routes.check_targets_slice",
            return_value={
                "variable": "tasmax",
                "time": (0, 1),
                "lat": (0, 1),
                "lon": (0, 1),
            },
        ),
        patch("ncpartitioner.routes.check_ranges"),
        patch("ncpartitioner.routes.slice", return_value=("queued", 202)) as enqueue,
    ):
        response = client.get("/partition/?filepath=data/tasmax.nc&targets=x")

    assert response.status_code == 202


def test_partition_status_not_found():
    app = create_app()
    client = app.test_client()

    response = client.get("/partition/status/missing-job")

    assert response.status_code == 404
    assert response.get_json() == {"status": "not_found", "job_id": "missing-job"}


def test_partition_status_returns_job_state(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    app = create_app()
    client = app.test_client()
    args = {
        "basename": "tasmax",
        "dirname": "tests/data",
        "extension": "nc",
        "timestamp": 1234567890,
    }
    job_id = "job-123"
    write_job_status(
        job_id,
        build_job_status(
            job_id,
            args,
            "complete",
            completed_at="2026-01-01T00:00:00+00:00",
        ),
    )

    response = client.get(f"partition/status/{job_id}")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["job_id"] == job_id
    assert payload["status"] == "complete"
    assert payload["status_url"] == f"partition/status/{job_id}"
    assert payload["download_url"] == (
        f"http://thredds.test/fileserver{tmp_path}/tasmax_1234567890.nc"
    )
    assert payload["output_filename"] == "tasmax_1234567890.nc"
    assert payload["completed_at"] == "2026-01-01T00:00:00+00:00"


def test_partition_status_refreshes_active_queued_job(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    app = create_app()
    client = app.test_client()
    args = {
        "basename": "tasmax",
        "dirname": "tests/data",
        "extension": "nc",
        "timestamp": 1234567890,
    }
    now = datetime.now(timezone.utc)
    job_id = "queued-job"
    write_job_status(
        job_id,
        build_job_status(
            job_id,
            args,
            "queued",
            queued_at=(now - timedelta(seconds=10)).isoformat(),
            last_seen_at=(now - timedelta(seconds=10)).isoformat(),
        ),
    )

    with patch("ncpartitioner.response.queue_client.queue_position", return_value=2):
        response = client.get(f"/partition/status/{job_id}")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "queued"
    assert payload["queue_position"] == 2
    refreshed = read_job_status(job_id)
    assert refreshed["last_seen_at"] != payload["queued_at"]


def test_partition_status_fails_stale_queued_job(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("NCPARTITIONER_QUEUE_IDLE_TTL_SECONDS", "60")
    app = create_app()
    client = app.test_client()
    args = {
        "basename": "tasmax",
        "dirname": "tests/data",
        "extension": "nc",
        "timestamp": 1234567890,
    }
    stale_time = "2026-01-01T00:00:00+00:00"
    job_id = "stale-queued-job"
    write_job_status(
        job_id,
        build_job_status(
            job_id,
            args,
            "queued",
            queued_at=stale_time,
            last_seen_at=stale_time,
        ),
    )

    with patch(
        "ncpartitioner.response.queue_client.discard_queued_job", return_value=True
    ) as discard:
        response = client.get(f"/partition/status/{job_id}")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "failed"
    assert payload["error"] == "Queued job was abandoned before processing"
    discard.assert_called_once_with(job_id)


def test_partition_status_does_not_fail_stale_job_after_worker_pop(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("NCPARTITIONER_QUEUE_IDLE_TTL_SECONDS", "60")
    app = create_app()
    client = app.test_client()
    args = {
        "basename": "tasmax",
        "dirname": "tests/data",
        "extension": "nc",
        "timestamp": 1234567890,
    }
    stale_time = "2026-01-01T00:00:00+00:00"
    job_id = "stale-worker-owned-job"
    write_job_status(
        job_id,
        build_job_status(
            job_id,
            args,
            "queued",
            queued_at=stale_time,
            last_seen_at=stale_time,
        ),
    )

    with patch(
        "ncpartitioner.response.queue_client.discard_queued_job", return_value=False
    ) as discard:
        response = client.get(f"/partition/status/{job_id}")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "queued"
    assert "error" not in payload
    discard.assert_called_once_with(job_id)
    persisted = read_job_status(job_id)
    assert persisted["status"] == "queued"
