import re
import os
import time
import subprocess


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
    # file path must start with DATA_ROOT and be a file this container has access to
    # remaining filepath must end with .nc

    # Flask strips the leading slash from the filepath argument, so we strip it here (and add it later)
    data_root = os.getenv("DATA_ROOT".lstrip("/"), "storage/")
    if not filepath.startswith(data_root):
        raise ValueError(f"Invalid filepath: must start with {data_root} : {filepath}")
    # reamining filepath may end in .nc, or may be missing an extension, but must not have any
    # other extension.
    if not filepath.endswith(".nc") and "." in filepath:
        raise ValueError(f"Invalid filepath: must be a .nc file {filepath}")
    if not "." in filepath:  # add "missing" extension
        filepath = f"{filepath}.nc"
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


def check_targets_partition(targets):
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
        if att not in args or args[att] is None:
            raise ValueError(f"Missing required target dimension or variable: {att}")
    return args


def check_targets_dds(targets, args):
    """targets may specify a single variable, or may not be given at all"""
    if targets is not None:
        if targets in ["lat", "lon", "time"]:  # an expected dimension
            return {"target": targets}
        elif re.match(r"^[a-zA-Z0-9_]+$", targets):  # a possible variable name
            # see if variable exists in file
            metadata = subprocess.check_output(
                [
                    "ncks",
                    "-m",
                    f"/{args['dirname']}/{args['basename']}.{args['extension']}",
                ]
            ).decode("utf-8")
            varreg = re.search(rf"{targets}\((.+),(.+),(.+)\)", metadata)
            if varreg:
                return {"target": targets}
            else:
                raise ValueError(f"Variable {targets} not found in file")
        else:
            raise ValueError(f"Invalid target for DDS request: {targets}")
    else:
        return {}


def check_ranges(args):
    """make sure requested ranges are valid for the file"""
    # grab netcdf metadata via ncks, parse it, and compare to requested ranges
    metadata = subprocess.check_output(
        ["ncks", "-m", f"/{args['dirname']}/{args['basename']}.{args['extension']}"]
    ).decode("utf-8")

    # make sure this file contains this variable, and it has the relevant dimensions
    varreg = re.search(rf"{args['variable']}\((.+),(.+),(.+)\)", metadata)
    if not varreg:
        raise ValueError(f"Variable {args['variable']} not found in file")
    for dim in ["time", "lat", "lon"]:
        if dim not in varreg.groups():
            raise ValueError(
                f"Variable {args['variable']} does not have dimension {dim}"
            )

    # get dimension sizes and compare against request
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

        if dim_size >= 0:
            if args[dim][1] >= dim_size:
                raise ValueError(
                    f"Requested range for dimension {dim} exceeds file size: requested end {args[dim][1]}, file size {dim_size}"
                )
        else:
            raise ValueError(f"Dimension {dim} not found in file")

    return args
