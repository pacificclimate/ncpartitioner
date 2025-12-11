import re
import os
import time


def sanitize_inputs(filepath, targets):
    """parse and sanitize user inputs, since they're passed to command line"""
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

    # check requested filepath, and fetch metadata from THREDDS.
    # file path must start with /storage and be a file this container has access to
    if not filepath.startswith("storage/"):
        raise ValueError(f"Invalid filepath: must start with /storage/ {filepath}")
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

    # todo: if file exists, check that the requested ranges and variables are valid for this file.
    # by fetching DDS from THREDDS?

    return args
