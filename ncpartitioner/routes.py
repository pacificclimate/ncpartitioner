from flask import Blueprint, request, redirect
import subprocess
import logging
import os
import time

logger = logging.getLogger(__name__)

partition = Blueprint("partition", __name__, url_prefix="/partition")


@partition.route("/", methods=["GET"])
def ncpartitioner():
    """creates the requested netCDF with NCO, moves it to where THREDDS can serve it, and returns a link to the user"""
    filepath = request.args.get("filepath")
    targets = request.args.get("targets", None).split(",")
    output_dir = request.args.get("output_dir", os.getenv("OUTPUT_DIR"))
    thredds_base = os.getenv("THREDDS_HTTP_BASE")

    logger.info(
        f"Received partition request: filepath={filepath}, targets={targets}, output_dir={output_dir}"
    )

    # todo - validate targets
    # todo - regex for this - this is very fragile.
    def munge_target(t):
        dim = t.split("[")[0]
        if dim in ["time", "lat", "lon"]:
            range = t.split("[")[1].split("]")[0]
            start, end = range.split(":")
            return f"{dim},{start},{end}"
        else:
            return None  # we only care about dimensions.

    targets = [munge_target(t) for t in targets if munge_target(t) is not None]
    dim_args = []
    for t in targets:
        dim_args.append("-d")
        dim_args.append(t)

    # timestamp to avoid filename collision.
    timestamp = int(time.time())

    # remove extension from filename
    (filepath, extension) = filepath.split(".")

    logger.info(f"Partitioning file")
    subprocess.run(
        [
            "ncks",
            *dim_args,
            f"/{filepath}.{extension}",
            os.path.join(
                output_dir, f"{os.path.basename(filepath)}_{timestamp}.{extension}"
            ),
        ],
        check=True,
    )
    logger.info(
        f"Partition complete; file saved to {os.path.join(output_dir, f'{os.path.basename(filepath)}_{timestamp}.{extension}')}"
    )
    logger.info(f"Sending redirect to {thredds_base}/{os.path.basename(filepath)}_{timestamp}.{extension}")

    return redirect(
        f"{thredds_base}/{os.path.basename(filepath)}_{timestamp}.{extension}"
    )
