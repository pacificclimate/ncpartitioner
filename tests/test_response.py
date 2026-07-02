import os
import re
import subprocess
import time
from unittest.mock import patch

import netCDF4
import pytest

pytestmark = pytest.mark.filterwarnings(
    "ignore:Setting the shape on a NumPy array has been deprecated:DeprecationWarning"
)

from ncpartitioner.response import (
    concat_command,
    execute_slice_job,
    read_job_status,
    slice,
    slice_command,
    dds,
    das,
    time_windows,
)

args = {
    "basename": "tasmax",
    "dirname": "tests/data",
    "extension": "nc",
    "timestamp": 1234567890,
}


def run_job_inline(job_id, args):
    execute_slice_job(job_id, args)
    return 1


def wait_for_job_status(job_id, expected_status, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        payload = read_job_status(job_id)
        if payload:
            if payload["status"] == expected_status:
                return payload
            if payload["status"] == "failed":
                raise AssertionError(
                    f"Job {job_id} failed while waiting for {expected_status}: {payload}"
                )
        time.sleep(0.05)
    raise AssertionError(
        f"Timed out waiting for job {job_id} to reach {expected_status}; last payload={read_job_status(job_id)}"
    )


def make_source_netcdf(path, *, unlimited_time):
    with netCDF4.Dataset(path, "w", format="NETCDF4") as dataset:
        dataset.createDimension("time", None if unlimited_time else 3)
        dataset.createDimension("lat", 2)
        dataset.createDimension("lon", 2)
        time_var = dataset.createVariable("time", "i4", ("time",))
        lat_var = dataset.createVariable("lat", "f4", ("lat",))
        lon_var = dataset.createVariable("lon", "f4", ("lon",))
        data_var = dataset.createVariable("tasmax", "f4", ("time", "lat", "lon"))
        time_var[:] = [0, 1, 2]
        lat_var[:] = [0, 1]
        lon_var[:] = [0, 1]
        for time_index in range(3):
            for lat_index in range(2):
                for lon_index in range(2):
                    data_var[time_index, lat_index, lon_index] = (
                        time_index * 4 + lat_index * 2 + lon_index
                    )


def time_dimension_is_unlimited(path):
    with netCDF4.Dataset(path) as dataset:
        return dataset.dimensions["time"].isunlimited()


def write_running_status(output_dir, job_id):
    status_path = os.path.join(str(output_dir), ".jobs", f"{job_id}.json")
    os.makedirs(os.path.dirname(status_path), exist_ok=True)
    with open(status_path, "w", encoding="utf-8") as handle:
        handle.write(
            f'{{"job_id":"{job_id}","status":"running","status_url":"partition/status/{job_id}","download_url":"x","output_filename":"y","started_at":"2026-01-01T00:00:00+00:00"}}'
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
    with patch(
        "ncpartitioner.response.queue_client.enqueue_slice_job",
        side_effect=run_job_inline,
    ):
        with patch(
            "ncpartitioner.response.subprocess.run", side_effect=OSError("boom")
        ):
            response = slice(request_args)

    assert response.status_code == 202
    payload = response.get_json()
    failed = wait_for_job_status(payload["job_id"], "failed")
    assert (
        failed["error"]
        == "Subset request failed due to a processing error. Please try again."
    )


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
    assert (
        payload["error"]
        == "Subset extraction failed. Try a smaller time or spatial range."
    )


def test_execute_slice_job_sanitizes_merge_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("NCPARTITIONER_CHUNK_BYTES", "64")
    request_args = {
        "basename": "tasmax",
        "dirname": "tests/data",
        "extension": "nc",
        "timestamp": 100,
        "variable": "tasmax",
        "time": (0, 4),
        "lat": (0, 1),
        "lon": (0, 3),
    }
    job_id = "merge-failed-job"
    status_path = os.path.join(str(tmp_path), ".jobs", f"{job_id}.json")
    os.makedirs(os.path.dirname(status_path), exist_ok=True)
    os.makedirs(os.path.join(str(tmp_path), ".jobs", job_id), exist_ok=True)
    with open(status_path, "w", encoding="utf-8") as handle:
        handle.write(
            '{"job_id":"merge-failed-job","status":"running","status_url":"partition/status/merge-failed-job","download_url":"x","output_filename":"y","started_at":"2026-01-01T00:00:00+00:00"}'
        )

    def fake_run(cmd, **kwargs):
        if cmd[0] == "ncks":
            os.makedirs(os.path.dirname(cmd[-1]), exist_ok=True)
            with open(cmd[-1], "w", encoding="utf-8") as handle:
                handle.write("chunk")
            return subprocess.CompletedProcess(cmd, 0, "", "")
        raise subprocess.CalledProcessError(1, cmd)

    with patch("ncpartitioner.response.subprocess.run", side_effect=fake_run):
        execute_slice_job(job_id, request_args)

    payload = read_job_status(job_id)
    assert payload["status"] == "failed"
    assert (
        payload["error"]
        == "Subset assembly failed. Try a smaller time or spatial range."
    )


def test_time_windows_uses_byte_budget(monkeypatch):
    monkeypatch.setenv("NCPARTITIONER_CHUNK_BYTES", "64")
    request_args = {
        "time": (0, 10),
        "lat": (0, 1),
        "lon": (0, 3),
    }

    windows = time_windows(request_args)

    assert windows == [(0, 1), (2, 3), (4, 5), (6, 7), (8, 9), (10, 10)]


@pytest.mark.parametrize("unlimited_time", [False, True])
def test_slice_command_outputs_unlimited_time_dimension(tmp_path, unlimited_time):
    source = tmp_path / "source.nc"
    chunk = tmp_path / "chunk.nc"
    make_source_netcdf(source, unlimited_time=unlimited_time)
    request_args = {
        "variable": "tasmax",
        "time": (0, 2),
        "lat": (0, 1),
        "lon": (0, 1),
    }

    subprocess.run(
        slice_command(request_args, str(source), str(chunk), 0, 1),
        check=True,
    )

    assert time_dimension_is_unlimited(chunk)


def test_slice_command_uses_uncompressed_intermediate_chunks():
    request_args = {
        "variable": "tasmax",
        "time": (0, 2),
        "lat": (0, 1),
        "lon": (0, 1),
    }

    command = slice_command(request_args, "/input.nc", "/chunk.nc", 0, 1)

    assert command[:7] == ["ncks", "-O", "-h", "-4", "-L", "0", "--mk_rec_dmn"]
    assert command[-2:] == ["/input.nc", "/chunk.nc"]


def test_concat_command_applies_final_deflate_level(monkeypatch):
    monkeypatch.setenv("NCPARTITIONER_DEFLATE_LEVEL", "2")

    command = concat_command(["/chunk_0000.nc", "/chunk_0001.nc"], "/final.nc")

    assert command == [
        "ncrcat",
        "-O",
        "-h",
        "-4",
        "-L",
        "2",
        "/chunk_0000.nc",
        "/chunk_0001.nc",
        "/final.nc",
    ]


@pytest.mark.parametrize("unlimited_time", [False, True])
def test_execute_slice_job_merges_fixed_and_unlimited_time_sources(
    tmp_path, monkeypatch, unlimited_time
):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    source = input_dir / "tasmax.nc"
    make_source_netcdf(source, unlimited_time=unlimited_time)
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("NCPARTITIONER_CHUNK_BYTES", "16")
    request_args = {
        "basename": "tasmax",
        "dirname": str(input_dir),
        "extension": "nc",
        "timestamp": 101,
        "variable": "tasmax",
        "time": (0, 2),
        "lat": (0, 1),
        "lon": (0, 1),
    }
    job_id = f"merge-source-{unlimited_time}"
    os.makedirs(os.path.join(str(tmp_path), ".jobs", job_id), exist_ok=True)
    write_running_status(tmp_path, job_id)

    execute_slice_job(job_id, request_args)

    payload = read_job_status(job_id)
    output = tmp_path / "tasmax_101.nc"
    assert payload["status"] == "complete"
    assert output.is_file()
    assert not os.path.isdir(os.path.join(str(tmp_path), ".jobs", job_id))
    with netCDF4.Dataset(output) as dataset:
        assert dataset.dimensions["time"].isunlimited()
        assert len(dataset.dimensions["time"]) == 3
        assert dataset.variables["time"][:].tolist() == [0, 1, 2]
        assert dataset.variables["tasmax"][:, 0, 0].tolist() == [0, 4, 8]


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
    # Force a small chunk budget so these cases actually exercise multiple
    # parallel chunks plus a real final ncrcat merge, not just the
    # single-chunk rename shortcut.
    monkeypatch.setenv("NCPARTITIONER_CHUNK_BYTES", "64")

    expected_location = (
        f"{os.getenv('THREDDS_HTTP_BASE')}{tmp_path}/tasmax_{timestamp}.nc"
    )
    with patch(
        "ncpartitioner.response.queue_client.enqueue_slice_job",
        side_effect=run_job_inline,
    ):
        response = slice(request_args)

    assert response.status_code == 202
    assert response.location == expected_location
    payload = response.get_json()
    assert payload["status"] == "queued"
    assert payload["job_id"]
    assert payload["queue_position"] == 1
    assert payload["status_url"] == f"partition/status/{payload['job_id']}"
    assert payload["download_url"] == expected_location
    assert payload["output_filename"] == f"tasmax_{timestamp}.nc"

    outfile = os.path.join(str(tmp_path), f"tasmax_{timestamp}.nc")
    status_payload = wait_for_job_status(payload["job_id"], "complete")
    assert status_payload["status_url"] == f"partition/status/{payload['job_id']}"
    assert status_payload["download_url"] == expected_location
    assert status_payload["output_filename"] == f"tasmax_{timestamp}.nc"
    assert os.path.isfile(outfile)

    # temp chunk dir should be fully cleaned up after a successful job
    job_temp_dir = os.path.join(str(tmp_path), ".jobs", payload["job_id"])
    assert not os.path.isdir(job_temp_dir)

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
