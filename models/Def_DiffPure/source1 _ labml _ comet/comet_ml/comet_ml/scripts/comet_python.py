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

""" Executes the Python script and start the SDK at startup automatically.

You still need to `import comet_ml` in your script but with the wrapper you do
not need to `import comet_ml` before your machine learning libraries anymore.
"""

import argparse
import logging
import os
import sys

import comet_ml.bootstrap

LOGGER = logging.getLogger("comet_ml")
ADDITIONAL_ARGS = True


def get_parser_arguments(parser):
    parser.add_argument("python_script", help="the python script to launch")
    parser.add_argument("-p", "--python", help="Which Python interpreter to use")
    parser.add_argument("-m", "--module", help="Run library module as a script")


def main(args):
    # Called via `comet upload EXP.zip`
    parser = argparse.ArgumentParser()
    get_parser_arguments(parser)
    parsed_args, remaining = parser.parse_known_args(args)

    python(parsed_args, remaining)


def python(parsed_args, remaining):
    # Prepare the environment
    environ = os.environ.copy()

    bootstrap_dir = os.path.dirname(comet_ml.bootstrap.__file__)

    # Prepend the bootstrap dir to a potentially existing PYTHON PATH, prepend
    # so we are sure that we are the first one to be executed and we cannot be
    # sure that other sitecustomize.py files would call us
    if "PYTHONPATH" in environ:
        if bootstrap_dir not in environ["PYTHONPATH"]:
            environ["PYTHONPATH"] = "%s:%s" % (bootstrap_dir, environ["PYTHONPATH"])
    else:
        environ["PYTHONPATH"] = bootstrap_dir

    if parsed_args.python is None:
        python_interpreter = (
            sys.executable
        )  # TODO: Check that the python of the current path is the same as sys.executable
    else:
        python_interpreter = parsed_args.python

    # Add the python script
    if parsed_args.module:
        command = [
            python_interpreter,
            "-m",
            parsed_args.module,
            parsed_args.python_script,
        ]
    else:
        command = [python_interpreter, parsed_args.python_script]
    command.extend(remaining)

    # And os.exec
    os.execve(python_interpreter, command, environ)


if __name__ == "__main__":
    main(sys.argv[1:])
