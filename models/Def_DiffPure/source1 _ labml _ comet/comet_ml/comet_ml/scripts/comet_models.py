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
#  This file can not be copied and/or distributed without
#  the express permission of Comet ML Inc.
# *******************************************************

"""
Download a registry model locally.

To download a registry model named "My Model" from the workspace "My Workspace"
at version 1.0.0, you can run:

$ comet models download --workspace "My Workspace" --registry_name "My Model"
--model_version "1.0.0"

The registry model files will be downloaded to a directory named "model". You
can choose a different output directory by using the "--output" flag.
"""

import argparse
import logging
import sys

from comet_ml import API

LOGGER = logging.getLogger("comet_ml")
ADDITIONAL_ARGS = False


def download_model(args):
    api = API()

    api.download_registry_model(
        args.workspace,
        args.model_name,
        expand=True,
        output_path=args.output,
        stage=args.model_stage,
        version=args.model_version,
    )


def list_models(args):
    api = API()

    model_names = api.get_registry_model_names(args.workspace)

    if len(model_names) == 0:
        LOGGER.info("This workspace has no registered models")
    else:
        LOGGER.info("This workspace has the following registered models:")
        for i, name in enumerate(model_names):
            LOGGER.info("    %d. %s", i + 1, name)


def get_parser_arguments(parser):
    subparsers = parser.add_subparsers(dest="model action")
    subparsers.required = True
    download_parser = subparsers.add_parser(
        "download",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    download_parser.set_defaults(sub_func=download_model)
    download_parser.add_argument(
        "-w",
        "--workspace",
        help="the workspace name of the registry model to download",
        required=True,
    )
    download_parser.add_argument(
        "--model-name",
        help="the name of the registry model to download",
        required=True,
        default=None,
    )

    group = download_parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--model-version",
        help="the semantic version of the registry model to download (for example: 1.0.0)",
        default=None,
    )
    group.add_argument(
        "--model-stage",
        help="the stage of the registry model to download (for example: production)",
        default=None,
    )

    download_parser.add_argument(
        "--output",
        help="the output directory where to download the model, default to `model`",
        default="model",
    )

    list_parser = subparsers.add_parser(
        "list",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    list_parser.set_defaults(sub_func=list_models)
    list_parser.add_argument(
        "-w",
        "--workspace",
        help="the workspace name to list",
        required=True,
    )


def models(args, rest=None):
    # Called via `comet models list` or `comet models download`
    # args are parsed_args

    args.sub_func(args)


def main(args):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    get_parser_arguments(parser)
    parsed_args = parser.parse_args(args)

    models(parsed_args)


if __name__ == "__main__":
    # Called via `python -m comet_ml.scripts.comet_models download --workspace
    # TEST --registry_name MY_MODEL --version 1.0.0`
    main(sys.argv[1:])
