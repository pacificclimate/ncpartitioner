from flask import Blueprint, request, redirect
from ncpartitioner.ncpartitioner import sanitize_inputs
import subprocess
import logging
import os

logger = logging.getLogger(__name__)

partition = Blueprint("partition", __name__, url_prefix="/partition")


@partition.route("/", methods=["GET"])
def ncpartitioner():
    """creates the requested netCDF with NCO, moves it to where THREDDS can serve it, and returns a link to the user"""
    filepath = request.args.get("filepath")
    targets = request.args.get("targets", None)
    output_dir = os.getenv("OUTPUT_DIR")
    thredds_base = os.getenv("THREDDS_HTTP_BASE")

    logger.info(
        f"Received partition request: filepath={filepath}, targets={targets}, output_dir={output_dir}"
    )
    try:
        args = sanitize_inputs(filepath, targets)
    except ValueError as ve:
        logger.error(f"Input error: {ve}")
        return f"Input error: {ve}", 400

    print(args)

    logger.info(f"Partitioning file")
    subprocess.run(
        [
            "ncks",
            "-v",
            f"{args['variable']}",
            "-d",
            f"time,{args['time'][0]},{args['time'][1]}",
            "-d",
            f"lat,{args['lat'][0]},{args['lat'][1]}",
            "-d",
            f"lon,{args['lon'][0]},{args['lon'][1]}",
            f"/{args['dirname']}/{args['basename']}.{args['extension']}",
            os.path.join(
                output_dir,
                f"{args['basename']}_{args['timestamp']}.{args['extension']}",
            ),
        ],
        check=True,
    )
    logger.info(
        f"Partition complete; file saved to {os.path.join(output_dir, f'{os.path.basename(filepath)}_{timestamp}.{extension}')}"
    )
    logger.info(
        f"Sending redirect to {thredds_base}{output_dir}{os.path.basename(filepath)}_{timestamp}.{extension}"
    )

    return redirect(
        f"{thredds_base}{output_dir}{args['basename']}_{args['timestamp']}.{args['extension']}"
    )
