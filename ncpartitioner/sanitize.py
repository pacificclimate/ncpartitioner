import re
import os
import time


def check_filepath(filepath):
    """make sure user has requested a valid file that exists"""
    args = {}

    # filepaths have an extra suffix to indicate what format the user wants
    # the response in. Valid options are .nc (data request),.dds amd .das (metadata requests)
    (filepath, request_format) = os.path.splitext(filepath)
    if request_format not in [".nc", ".dds", ".das"]:
        raise ValueError(
            f"Invalid request format: must be .nc, .dds, or .das : {request_format}"
        )
    args["request_format"] = request_format.lstrip(".")

    # check requested filepath, and fetch metadata from THREDDS.
    # file path must start with /storage and be a file this container has access to
    # remaining filepath must end with .nc
    if not filepath.startswith("storage/"):
        raise ValueError(f"Invalid filepath: must start with storage/ : {filepath}")
    if not filepath.endswith(".nc"):
        raise ValueError(f"Invalid filepath: must be a .nc file {filepath}")
    if not os.path.isfile(f"/{filepath}"):
        raise ValueError(
            f"Invalid filepath: file does not exist or is not accessible. {filepath}"
        )

    # split filepath into convenient pieces
    args["timestamp"] = int(time.time())
    args["dirname"] = os.path.dirname(filepath)
    args["basename"] = os.path.basename(filepath).split(".")[0]
    args["extension"] = filepath.split(".")[-1]

    return args


def check_targets(targets):
    args = {"variable": None}

    # parse target ranges - make sure they're all numerical
    # assumptions:
    #  variables must be dimension variables lat, lon, time, and one additional variable (such as tasmax)
    #  the format is var[start:end] and both numbers must be specified
    targets = targets.split(",")
    for t in targets:
        dimreg = re.match(r"^(time|lat|lon|[a-zA-Z0-9_]+)\[(\d+):(\d+)\]$", t)
        if dimreg:
            dim = dimreg.group(1)
            start = int(dimreg.group(2))
            end = int(dimreg.group(3))

            if end < start:
                raise ValueError(
                    f"Invalid range for dimension {dim}: end {end} is less than start {start}"
                )

            args[dim] = (start, end)
        else:
            varreg = re.match(
                r"^([a-z]+)\[(\d+):(\d+)\]\[(\d+):(\d+)\]\[(\d+):(\d+)\]$", t
            )
            if varreg and args["variable"] is None:
                args["variable"] = varreg.group(1)
            elif args["variable"] is not None:
                raise ValueError(
                    f"Multiple variables specified: {args['variable']} and {varreg.group(1)}"
                )
            else:
                raise ValueError(f"Invalid target format: {t}")
    # make sure all required dimensions and a variable are present.
    for att in ["time", "lat", "lon", "variable"]:
        if att not in args:
            raise ValueError(f"Missing required target dimension or variable: {att}")
    return args


def check_ranges(args):
    """make sure requested ranges are valid for the file"""
    # todo - get dds from THREDDS and check ranges against file dimensions
    return args
