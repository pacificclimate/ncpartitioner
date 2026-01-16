from flask import Blueprint, request, redirect
from ncpartitioner.sanitize import check_filepath, check_targets, check_ranges
from ncpartitioner.response import partition, dds, das
import logging

logger = logging.getLogger(__name__)

partition = Blueprint("partition", __name__, url_prefix="/partition")


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
        return dds(args)
    elif args["request_format"] == "das":
        return das(args)
    elif args["request_format"] == "nc":
        try:
            args.update(check_targets(targets))
            check_ranges(args)
        except ValueError as ve:
            logger.error(f"Input error: {ve}")
            return f"Input error: {ve}", 400

        logger.info(
            f"Received partition request: filepath={filepath}, targets={targets}"
        )
        return partition(args)
