from flask import Blueprint, request
from ncpartitioner.sanitize import (
    check_filepath,
    check_targets_slice,
    check_targets_dds,
    check_ranges,
    check_targets_ascii,
)
from ncpartitioner.response import slice, dds, das, asc, slice_status
import logging

logger = logging.getLogger(__name__)

partition = Blueprint("partition", __name__, url_prefix="/partition")


@partition.route("/status/<job_id>", methods=["GET"])
def partition_status(job_id):
    """Return the status of an asynchronous slice job."""
    return slice_status(job_id)


@partition.route("/", methods=["GET"])
def ncpartitioner():
    """creates the requested netCDF with NCO, moves it to where THREDDS can serve it, and returns a link to the user"""
    logger.info(f"received request {request.url}")
    filepath = request.args.get("filepath")
    targets = request.args.get("targets", None)

    try:
        args = check_filepath(filepath)
    except ValueError as ve:
        logger.error(f"Input error: {ve}")
        return f"Input error: {ve}", 400

    if args["request_format"] == "dds":
        args.update(check_targets_dds(targets, args))
        return dds(args)
    elif args["request_format"] == "das":
        return das(args)
    elif args["request_format"] == "nc":
        try:
            args.update(check_targets_slice(targets))
            check_ranges(args)
        except ValueError as ve:
            logger.error(f"Input error: {ve}")
            return f"Input error: {ve}", 400

        logger.info(f"Received slice request: filepath={filepath}, targets={targets}")
        return slice(args)
    elif args["request_format"] in ["ascii", "asc"]:
        args.update(check_targets_ascii(targets))
        return asc(args)
