import os
from unittest.mock import patch

import netCDF4

from ncpartitioner.response import (
    das,
    dds,
    execute_slice_job,
    read_job_status,
    slice,
    time_windows,
)


def make_source_netcdf(path, *, unlimited_time=False):
    with netCDF4.Dataset(path, "w", format="NETCDF4") as dataset:
        dataset.title = "source metadata"
        dataset.createDimension("time", None if unlimited_time else 3)
        dataset.createDimension("lat", 2)
        dataset.createDimension("lon", 2)
        time_var = dataset.createVariable("time", "i4", ("time",))
        lat_var = dataset.createVariable("lat", "f4", ("lat",))
        lon_var = dataset.createVariable("lon", "f4", ("lon",))
        data_var = dataset.createVariable(
            "tasmax", "f4", ("time", "lat", "lon"), fill_value=-9999.0
        )
        data_var.units = "K"
        time_var[:] = [0, 1, 2]
        lat_var[:] = [0, 1]
        lon_var[:] = [0, 1]
        for time_index in range(3):
            for lat_index in range(2):
                for lon_index in range(2):
                    data_var[time_index, lat_index, lon_index] = (
                        time_index * 4 + lat_index * 2 + lon_index
                    )


def request_args(tmp_path, *, timestamp=123, **extra):
    return {
        "basename": "source",
        "dirname": str(tmp_path),
        "extension": "nc",
        "timestamp": timestamp,
        "variable": "tasmax",
        "time": (1, 2),
        "lat": (0, 1),
        "lon": (1, 1),
        **extra,
    }


def write_running_status(output_dir, job_id):
    status_path = os.path.join(str(output_dir), ".jobs", f"{job_id}.json")
    os.makedirs(os.path.dirname(status_path), exist_ok=True)
    with open(status_path, "w", encoding="utf-8") as handle:
        handle.write(
            f'{{"job_id":"{job_id}","status":"running","status_url":"partition/status/{job_id}","download_url":"x","output_filename":"y","started_at":"2026-01-01T00:00:00+00:00"}}'
        )


def test_dds_and_das():
    args = {
        "basename": "tasmax",
        "dirname": "tests/data",
        "extension": "nc",
        "timestamp": 1234567890,
    }

    dds_response = dds(args)
    das_response = das(args)

    assert dds_response.status_code == 302
    assert dds_response.location == "http://thredds.test/dap//tests/data/tasmax.nc.dds"
    assert das_response.status_code == 302
    assert das_response.location == "http://thredds.test/dap//tests/data/tasmax.nc.das"


def test_time_windows_obey_explicit_slab_byte_budget():
    args = {"time": (0, 4), "lat": (0, 1), "lon": (0, 3)}

    assert time_windows(args, source_bytes=2, byte_budget=64) == [(0, 3), (4, 4)]


def test_execute_slice_job_writes_direct_netcdf4_subset(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("NCPARTITIONER_NETCDF4_SLAB_BYTES", "8")
    make_source_netcdf(tmp_path / "source.nc")
    args = request_args(tmp_path)
    job_id = "direct-netcdf4-job"
    os.makedirs(tmp_path / ".jobs" / job_id)
    write_running_status(tmp_path, job_id)

    execute_slice_job(job_id, args)

    payload = read_job_status(job_id)
    assert payload["status"] == "complete"
    assert payload["operator_warnings"] == []
    with netCDF4.Dataset(tmp_path / "source_123.nc") as output:
        assert output.data_model == "NETCDF4"
        assert output.title == "source metadata"
        assert output.dimensions["time"].isunlimited()
        assert output.variables["tasmax"].units == "K"
        assert output.variables["time"][:].tolist() == [1, 2]
        assert output.variables["tasmax"][:].tolist() == [
            [[5.0], [7.0]],
            [[9.0], [11.0]],
        ]


def test_direct_writer_can_compress_final_output(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("NCPARTITIONER_COMPRESS_FINAL_OUTPUT", "true")
    monkeypatch.setenv("NCPARTITIONER_DEFLATE_LEVEL", "2")
    make_source_netcdf(tmp_path / "source.nc")
    args = request_args(tmp_path, timestamp=124)
    job_id = "compressed-netcdf4-job"
    os.makedirs(tmp_path / ".jobs" / job_id)
    write_running_status(tmp_path, job_id)

    execute_slice_job(job_id, args)

    with netCDF4.Dataset(tmp_path / "source_124.nc") as output:
        filters = output.variables["tasmax"].filters()
        assert filters["zlib"]
        assert filters["complevel"] == 2


def test_direct_writer_marks_missing_source_as_failed(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    args = request_args(tmp_path)
    job_id = "missing-source-job"
    os.makedirs(tmp_path / ".jobs" / job_id)
    write_running_status(tmp_path, job_id)

    execute_slice_job(job_id, args)

    payload = read_job_status(job_id)
    assert payload["status"] == "failed"
    assert (
        payload["error"]
        == "Direct NetCDF4 subset failed. Try a smaller time or spatial range."
    )
    assert not (tmp_path / ".jobs" / job_id).exists()


def test_slice_enqueues_a_direct_writer_job(tmp_path, monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    args = request_args(tmp_path)

    with patch(
        "ncpartitioner.response.queue_client.enqueue_slice_job", return_value=3
    ) as enqueue:
        response = slice(args)

    assert response.status_code == 202
    assert response.get_json()["queue_position"] == 3
    assert enqueue.call_args.args[1] == args
