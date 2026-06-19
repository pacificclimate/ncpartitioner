import os
import re
import subprocess
import time
from unittest.mock import patch

import pytest

from ncpartitioner.response import execute_slice_job, read_job_status, slice, dds, das

args = {
    "basename": "tasmax",
    "dirname": "tests/data",
    "extension": "nc",
    "timestamp": 1234567890,
}


def wait_for_job_status(job_id, expected_status, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        payload = read_job_status(job_id)
        if payload and payload["status"] == expected_status:
            return payload
        time.sleep(0.05)
    raise AssertionError(
        f"Timed out waiting for job {job_id} to reach {expected_status}"
    )


def test_dds():
    response = dds(args)
    assert response.status_code == 302
    assert (
        response.location == f"{os.getenv('THREDDS_DAP_BASE')}/tests/data/tasmax.nc.dds"
    )


def test_das():
    response = das(args)
    assert response.status_code == 302
    assert (
        response.location == f"{os.getenv('THREDDS_DAP_BASE')}/tests/data/tasmax.nc.das"
    )


def test_slice_error(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    request_args = {
        "time": (0, 10),
        "lat": (0, 10),
        "lon": (0, 10),
        "variable": "tasmax",
        "timestamp": 1234567890,
        "dirname": "tests/data",
        "basename": "tasmin",
        "extension": "nc",
    }
    with patch("ncpartitioner.response.subprocess.run", side_effect=OSError("boom")):
        response = slice(request_args)

    assert response.status_code == 202
    payload = response.get_json()
    failed = wait_for_job_status(payload["job_id"], "failed")
    assert failed["error"] == "boom"


def test_execute_slice_job_marks_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    request_args = {
        "basename": "tasmax",
        "dirname": "tests/data",
        "extension": "nc",
        "timestamp": 99,
        "variable": "tasmax",
        "time": (0, 1),
        "lat": (0, 1),
        "lon": (0, 1),
    }
    job_id = "failed-job"
    status_path = os.path.join(str(tmp_path), ".jobs", f"{job_id}.json")
    os.makedirs(os.path.dirname(status_path), exist_ok=True)
    with open(status_path, "w", encoding="utf-8") as handle:
        handle.write(
            '{"job_id":"failed-job","status":"running","status_url":"partition/status/failed-job","download_url":"x","output_filename":"y","started_at":"2026-01-01T00:00:00+00:00"}'
        )

    with patch(
        "ncpartitioner.response.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, ["ncks"], stderr="broken"),
    ):
        execute_slice_job(job_id, request_args)

    payload = read_job_status(job_id)
    assert payload["status"] == "failed"
    assert payload["error"] == "broken"


@pytest.mark.parametrize(
    "targets,timestamp",
    [
        (
            {"time": (0, 10), "lat": (0, 10), "lon": (0, 10), "variable": "tasmax"},
            1,
        ),
        (
            {"time": (0, 50), "lat": (0, 10), "lon": (0, 10), "variable": "tasmax"},
            2,
        ),
        (
            {"time": (0, 50), "lat": (0, 50), "lon": (0, 99), "variable": "tasmax"},
            3,
        ),
        (
            {"time": (0, 1), "lat": (0, 1), "lon": (0, 1), "variable": "tasmax"},
            4,
        ),
    ],
)
def test_slice(targets, timestamp, tmp_path, monkeypatch):
    request_args = dict(args)
    request_args.update(targets)
    request_args["timestamp"] = timestamp
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))

    expected_location = (
        f"{os.getenv('THREDDS_HTTP_BASE')}{tmp_path}/tasmax_{timestamp}.nc"
    )
    response = slice(request_args)

    assert response.status_code == 202
    assert response.location == expected_location
    payload = response.get_json()
    assert payload["status"] == "accepted"
    assert payload["job_id"]
    assert payload["status_url"] == f"partition/status/{payload['job_id']}"
    assert payload["download_url"] == expected_location
    assert payload["output_filename"] == f"tasmax_{timestamp}.nc"

    outfile = os.path.join(str(tmp_path), f"tasmax_{timestamp}.nc")
    status_payload = wait_for_job_status(payload["job_id"], "complete")
    assert status_payload["download_url"] == expected_location
    assert status_payload["output_filename"] == f"tasmax_{timestamp}.nc"
    assert os.path.isfile(outfile)

    metadata = subprocess.check_output(["ncks", "-m", outfile]).decode("utf-8")

    # make sure file contains requested variable
    varreg = re.search(rf"{request_args['variable']}\((.+),(.+),(.+)\)", metadata)
    assert varreg is not None

    # make sure dimensions match requested ranges
    for dim in ["lat", "lon", "time"]:
        dim_size = -1
        dimreg = re.search(rf"    {dim} = (\d+) ;", metadata)
        if dimreg:
            dim_size = int(dimreg.group(1))
        else:  # for unlimited dimensions (normally time)
            dimreg = re.search(
                rf"    {dim} = UNLIMITED ; \/\/ \((\d+) currently\)", metadata
            )
            if dimreg:
                dim_size = int(dimreg.group(1))
        assert dim_size == request_args[dim][1] - request_args[dim][0] + 1

    os.remove(outfile)
