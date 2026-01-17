"""send responses to user requests. TResponses are always a redirect to a THREDDS-served file.
In cases of DDS and DAS, the file already exists; for data requests the filemust be created first.
"""

from posixpath import dirname
import subprocess
import os
from flask import redirect
import logging

logger = logging.getLogger(__name__)


def partition(args):
    output_dir = os.getenv("OUTPUT_DIR")
    thredds_base = os.getenv("THREDDS_HTTP_BASE")

    print("args are:")
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

    output_filename = f"{args['basename']}_{args['timestamp']}.{args['extension']}"
    logger.info(
        f"Partition complete; file saved to {os.path.join(output_dir, output_filename)}"
    )
    logger.info(f"Sending redirect to {thredds_base}{output_dir}/{output_filename}")

    return redirect(f"{thredds_base}{output_dir}/{output_filename}")


def dap_filepath(args):
    """construct the filepath for DDS/DAS requests"""
    thredds_base = os.getenv("THREDDS_DAP_BASE")
    return f"{thredds_base}/{args['dirname']}/{args['basename']}.{args['extension']}"


def dds(args):
    filepath = dap_filepath(args)
    logger.info(f"Received DDS request: filepath={filepath}")
    if "target" in args:
        return redirect(f"{filepath}.dds?{args['target']}")
    return redirect(f"{filepath}.dds")


def das(args):
    filepath = dap_filepath(args)
    logger.info(f"Received DAS request: filepath={filepath}")

    return redirect(f"{filepath}.das")
