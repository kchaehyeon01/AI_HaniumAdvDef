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
Examples:

    comet upload file.zip ...
    comet upload --force-reupload file.zip ...

    comet optimize script.py optimize.config
    comet optimize -j 4 script.py optimize.config
    comet optimize -j 4 script.py optimize.config -- arg1 --flag arg2

    comet bootstrap_dir

    comet python script.py
    comet python -p /usr/bin/python3.6 script.py

    comet offline 60a1a617e4c24c8998cc78fa3bc7a31b.zip
    comet offline --csv 60a1a617e4c24c8998cc78fa3bc7a31b.zip

    comet check

    comet init

    comet models

Note that `comet optimize` requires your COMET_API_KEY
be configured in the environment, or in your .comet.config
file. For example:

    COMET_API_KEY=74345364546 comet optimize ...

For more information:
    comet COMMAND --help
"""

from __future__ import print_function

import argparse
import os.path
import sys

from comet_ml import __version__

# Import CLI commands:
from . import (
    comet_check,
    comet_init,
    comet_models,
    comet_offline,
    comet_optimize,
    comet_python,
    comet_upload,
)


def bootstrap_dir(args):
    """Print the bootstrap dir to include in PYTHONPATH for automatic early
    SDK initialization. See also `comet python` that set it automatically.
    """
    import comet_ml.bootstrap

    boostrap_dir = os.path.dirname(comet_ml.bootstrap.__file__)
    print(boostrap_dir, end="")


def add_subparser(subparsers, module, name):
    """
    Loads scripts and creates subparser.

    Assumes: NAME works for:
       * comet_NAME.NAME is the function
       * comet_NAME.ADDITIONAL_ARGS is set to True/False
       * comet_NAME.get_parser_arguments is defined
    """
    func = getattr(module, name)
    additional_args = module.ADDITIONAL_ARGS
    get_parser_arguments = module.get_parser_arguments
    docs = module.__doc__

    parser = subparsers.add_parser(
        name, description=docs, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.set_defaults(func=func)
    parser.set_defaults(additional_args=additional_args)
    get_parser_arguments(parser)


def main(raw_args=sys.argv[1:]):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--version",
        help="Display comet_ml version",
        action="store_const",
        const=True,
        default=False,
    )
    subparsers = parser.add_subparsers()

    # Register CLI commands:
    add_subparser(subparsers, comet_check, "check")
    add_subparser(subparsers, comet_models, "models")
    add_subparser(subparsers, comet_offline, "offline")
    add_subparser(subparsers, comet_optimize, "optimize")
    add_subparser(subparsers, comet_python, "python")
    add_subparser(subparsers, comet_upload, "upload")
    add_subparser(subparsers, comet_init, "init")

    bootstrap_dir_parser = subparsers.add_parser(
        "bootstrap_dir",
        description=bootstrap_dir.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    bootstrap_dir_parser.set_defaults(func=bootstrap_dir)
    bootstrap_dir_parser.set_defaults(additional_args=False)

    # First identify the subparser as some subparser pass additional args to
    # the subparser and other not

    args, rest = parser.parse_known_args(raw_args)

    # args won't have additional args if no subparser added
    if hasattr(args, "additional_args") and args.additional_args:
        parser_func = args.func

        parser_func(args, rest)
    elif args.version:
        print(__version__)
    else:
        # If the subcommand doesn't need extra args, reparse in strict mode so
        # the users get a nice message in case of unsupported CLi argument
        args = parser.parse_args(raw_args)
        if hasattr(args, "func"):
            parser_func = args.func

            parser_func(args)
        else:
            # comet with no args; call recursively:
            main(["--help"])


if __name__ == "__main__":
    main(sys.argv[1:])
