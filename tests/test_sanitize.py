import pytest
import os

from ncpartitioner.sanitize import check_filepath, check_targets, check_ranges


@pytest.mark.parametrize(
    "filepath, valid, error",
    [
        ("fake/tests/tasmax.nc", False, "Invalid filepath: must start with"),
        ("tests/data/tasmax.nc.nc", True, None),
        ("tests/data/tasmax.nc.das", True, None),
        ("tests/data/tasmax.nc.dds", True, None),
        ("tests/data/tasmax.nc.banana", False, "Invalid request format"),
        ("tests/data/missing.nc.nc", False, "Invalid filepath: file does not exist"),
        ("tests/data/tasmax.txt.nc", False, "Invalid filepath: must be a .nc file"),
    ],
)
def test_check_filepath(filepath, valid, error):
    filepath = os.path.abspath(filepath)
    if valid:
        args = check_filepath(filepath)
        # not checking result values, just make sure args is populated
        for att in ["request_format", "timestamp", "dirname", "basename", "extension"]:
            assert att in args
    else:
        with pytest.raises(ValueError) as excinfo:
            check_filepath(filepath)
        assert str(excinfo.value).startswith(error)


@pytest.mark.parametrize(
    "targets, valid, error",
    [
        ("time[0:100],lat[0:50],lon[0:100],tasmax[0:100][0:50][0:100]", True, None),
        (
            "time[0:100],lat[0:50],lon[0:100],banana[0:100][0:50][0:100],tasmax[0:100][0:50][0:100]",
            False,
            "Multiple variables",
        ),
        (
            "time[100:0],lat[0:50],lon[0:100],tasmax[0:100][0:50][0:100]",
            False,
            "Invalid range",
        ),
        (
            "time[0:bb][lat[0:50],lon[0:100],tasmax[0:100][0:50][0:100]",
            False,
            "Invalid target format",
        ),
        ("lat[0:50],lon[0:100],tasmax[0:100][0:50][0:100]", False, "Missing required"),
        (
            "time[0:100],lon[0:100],tasmax[0:100][0:50][0:100]",
            False,
            "Missing required",
        ),
        ("time[0:100],lat[0:50],tasmax[0:100][0:50][0:100]", False, "Missing required"),
        ("time[0:100],lat[0:50],lon[0:100]", False, "Missing required"),
    ],
)
def test_check_targets(targets, valid, error):
    if valid:
        args = check_targets(targets)
        # not checking result values, just make sure args is populated
        for att in ["time", "lat", "lon", "variable"]:
            assert att in args
    else:
        with pytest.raises(ValueError) as excinfo:
            check_targets(targets)
        assert str(excinfo.value).startswith(error)


@pytest.mark.parametrize(
    "args, valid, error",
    [
        (
            {"time": (0, 100), "lat": (0, 50), "lon": (0, 100), "variable": "tasmax"},
            True,
            None,
        ),
        (
            {"time": (0, 100), "lat": (0, 50), "lon": (0, 100), "variable": "pr"},
            False,
            "Variable pr not found in file",
        ),
        (
            {"time": (0, 200), "lat": (0, 50), "lon": (0, 100), "variable": "tasmax"},
            False,
            "Requested range for dimension time exceeds file size",
        ),
    ],
)
def test_check_ranges(args, valid, error):
    args["basename"] = "tasmax"
    dirname = "tests/data/"
    args["dirname"] = dirname
    args["extension"] = "nc"

    if valid:
        # currently no range checks implemented, so just call the function
        check_ranges(args)
    else:
        with pytest.raises(ValueError) as excinfo:
            check_ranges(args)
        assert str(excinfo.value).startswith(error)
