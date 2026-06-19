from ncpartitioner import create_app
from ncpartitioner.response import build_job_status, write_job_status


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
