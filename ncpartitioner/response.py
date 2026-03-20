"""send responses to user requests. TResponses are always a redirect to a THREDDS-served file.
In cases of DDS and DAS, the file already exists; for data requests the filemust be created first.
"""

from math import ceil
from posixpath import dirname
import subprocess
import os
from flask import redirect
import logging

logger = logging.getLogger(__name__)


def slice(args):
    output_dir = os.getenv("OUTPUT_DIR")
    thredds_base = os.getenv("THREDDS_HTTP_BASE")
    chunk_size = int(os.getenv("CHUNK_SIZE", "1000000000"))

    # this is a home-grown and crude chunking algorithm.
    # its purpose is to reduce the amount of RAM used by calls to ncks.
    # ncks accepts arguments to configure runtime chunking, but it
    # only seems to apply these to files as it writes them, not as it
    # reads them, so the chunking arguments do not affect the RAM used
    # by the ncks process itself.
    #
    # chunk size is set by the CHUNK_SIZE environment variable, and is measured
    # in array cells (ignoring typing). Chunking is performed by slicing along
    # the time axis, since for the datasets expected to be served by this service,
    # time is the largest dimension.

    num_chunks = ceil(
        (
            ((args["time"][1] - args["time"][0]) + 1)
            * ((args["lat"][1] - args["lat"][0]) + 1)
            * ((args["lon"][1] - args["lon"][0]) + 1)
        )
        / chunk_size
    )
    t_per_chunk = ceil((args["time"][1] - args["time"][0] + 1) / num_chunks)
    output_filename = f"{args['basename']}_{args['timestamp']}.{args['extension']}"

    logger.info(
        f"Slicing request into {num_chunks} subfiles of approximately {t_per_chunk} timesteps each"
    )

    # create component files with ncks
    for i in range(num_chunks):
        chunk_start = args["time"][0] + i * t_per_chunk
        chunk_end = min(
            args["time"][1],
            args["time"][0] + (i + 1) * t_per_chunk - 1,
        )
        chunk_filename = (
            f"{args['basename']}_{args['timestamp']}_chunk{i + 1}.{args['extension']}"
        )

        logger.info(
            f"Slicing chunk {i + 1}/{num_chunks}: time[{chunk_start}:{chunk_end}] into {chunk_filename}"
        )

        try:
            subprocess.run(
                [
                    "ncks",
                    "-v",
                    f"{args['variable']}",
                    "-d",
                    f"time,{chunk_start},{chunk_end}",
                    "-d",
                    f"lat,{args['lat'][0]},{args['lat'][1]}",
                    "-d",
                    f"lon,{args['lon'][0]},{args['lon'][1]}",
                    f"/{args['dirname']}/{args['basename']}.{args['extension']}",
                    os.path.join(
                        output_dir,
                        chunk_filename,
                    ),
                ],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            logger.error(f"Error slicing chunk {i + 1}: {e}")
            raise RuntimeError(f"Error slicing chunk {i + 1}: {e}")

    # merge component files with ncrcat (if needed)
    if num_chunks > 1:
        logger.info(f"Combining {num_chunks} subfiles into final output file")
        try:
            ncrcar_command = (
                ["ncrcat"]
                + [
                    f"{output_dir}/{args['basename']}_{args['timestamp']}_chunk{i + 1}.{args['extension']}"
                    for i in range(num_chunks)
                ]
                + [f"{output_dir}/{output_filename}"]
            )
            subprocess.run(ncrcar_command, check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Error combining chunks: {e}")
            raise RuntimeError(f"Error combining chunks: {e}")
        finally:
            # clean up chunk files
            for i in range(num_chunks):
                os.remove(
                    os.path.join(
                        output_dir,
                        f"{args['basename']}_{args['timestamp']}_chunk{i + 1}.{args['extension']}",
                    )
                )
    else:  # rename single chunk file
        os.rename(
            os.path.join(
                output_dir,
                f"{args['basename']}_{args['timestamp']}_chunk1.{args['extension']}",
            ),
            os.path.join(
                output_dir,
                output_filename,
            ),
        )

    logger.info(
        f"Slice complete; file saved to {os.path.join(output_dir, output_filename)}"
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


def asc(args):
    # returns requested dimension data in ASCII format; this function does not return gridded data.
    filepath = dap_filepath(args)
    dims = (
        args["target"] if isinstance(args["target"], str) else ",".join(args["target"])
    )
    logger.info(f"Received ASCII request: filepath={filepath}")

    return redirect(f"{filepath}.ascii?{dims}")
