#!/usr/bin/env python
# -*- coding: utf-8 -*-
# *******************************************************
#   ____                     _               _
#  / ___|___  _ __ ___   ___| |_   _ __ ___ | |
# | |   / _ \| '_ ` _ \ / _ \ __| | '_ ` _ \| |
# | |__| (_) | | | | | |  __/ |_ _| | | | | | |
#  \____\___/|_| |_| |_|\___|\__(_)_| |_| |_|_|
#
#  Sign up for free at http://www.comet.ml
#  Copyright (C) 2015-2021 Comet ML INC
#  This file can not be copied and/or distributed without the express
#  permission of Comet ML Inc.
# *******************************************************

""" Upload an offline experiment archive to the backend
"""

import argparse
import logging
import sys

from comet_ml.exceptions import InvalidAPIKey, OfflineExperimentUploadFailed
from comet_ml.logging_messages import OFFLINE_UPLOAD_FAILED_INVALID_API_KEY
from comet_ml.offline import main_upload

LOGGER = logging.getLogger("comet_ml")
ADDITIONAL_ARGS = False


def get_parser_arguments(parser):
    parser.add_argument(
        "archives", nargs="+", help="the offline experiment archives to upload"
    )
    parser.add_argument(
        "--force-reupload",
        help="force reupload offline experiments that were already uploaded",
        action="store_const",
        const=True,
        default=False,
    )
    parser.add_argument(
        "--workspace",
        dest="override_workspace",
        help="upload all offline experiment archives to that workspace instead of the original workspace",
        action="store",
    )
    parser.add_argument(
        "--project-name",
        dest="override_project_name",
        help="upload all offline experiment archives to that project instead of the original project",
        action="store",
    )


def upload(args, rest=None):
    try:
        main_upload(
            archives=args.archives,
            force_reupload=args.force_reupload,
            override_workspace=args.override_workspace,
            override_project_name=args.override_project_name,
        )
    except InvalidAPIKey:
        LOGGER.error(OFFLINE_UPLOAD_FAILED_INVALID_API_KEY, exc_info=True)
        sys.exit(1)
    except OfflineExperimentUploadFailed as ex:
        LOGGER.error(ex.msg, exc_info=True)
        sys.exit(1)


def main(args):
    # Called via `comet upload EXP.zip`
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    get_parser_arguments(parser)
    parsed_args = parser.parse_args(args)

    upload(parsed_args)


if __name__ == "__main__":
    # Called via `python -m comet_ml.scripts.comet_upload EXP.zip`
    # Called via `comet upload EXP.zip`
    main(sys.argv[1:])
