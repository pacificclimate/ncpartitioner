"""send responses to user requests. TResponses are always a redirect to a THREDDS-served file.
In cases of DDS and DAS, the file already exists; for data requests the filemust be created first.
"""

import subprocess
import os
from flask import redirect
import logging

logger = logging.getLogger(__name__)


def partition(args):
    logger.info(
        f"Received partition request: filepath={filepath}, targets={targets}, output_dir={output_dir}"
    )
    output_dir = os.getenv("OUTPUT_DIR")
    thredds_base = os.getenv("THREDDS_HTTP_BASE")

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


def dds(args):
    logger.info(f"Received DDS request: filepath={filepath}")
    # this is wrong.
    thredds_base = os.getenv("THREDDS_DAP_BASE")

    return redirect(
        f"{thredds_base}/{args['dirname']}/{args['basename']}.{args['extension']}.dds"
    )


def das(args):
    logger.info(f"Received DAS request: filepath={filepath}")
    # this is wrong.
    thredds_base = os.getenv("THREDDS_DAP_BASE")

    return redirect(
        f"{thredds_base}/{args['dirname']}/{args['basename']}.{args['extension']}.das"
    )
