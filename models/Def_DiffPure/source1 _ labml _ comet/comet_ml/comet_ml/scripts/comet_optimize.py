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
#  This file can not be copied and/or distributed
#  without the express permission of Comet ML Inc.
# *******************************************************

"""
This is a wrapper for running the Comet.ml optimizer in
parallel.

where OPTIMIZER is a JSON file, or an optimizer
id. If OPTIMIZER is a file, then the file contains:

* the optimizer algorithm
* the optimizer algorithm parameters (SPEC)
* the parameter space to search

PYTHON_SCRIPT is a regular Python file that takes an
optimizer config file, or optimizer ID.
If PYTHON_SCRIPT is not included, then an optimizer
is created and the optimizer id is displayed.

Parameters may define the domain of a parameter in the
following formats:

1. Full format:

{
    "x": {"type": "double", "min": -10.0, "max": 10.0},
    "y": {"type": "integer", "min": -10, "max": 10},
}

2. Shortcuts:

{
    "x": [10, 11, 12],                      # discrete
    "f": ["sigmoid", "relu", "leaky-relu"], # categorical
    "y": [8.0, 16.0, 32.0],                 # discrete
}

which gets expanded into the full format:

{
    "x": {"type": "discrete", "values": [10, 11, 12]},
    "f": {"type": "categorical", "values": ["sigmoid", "relu", "leaky-relu"]},
    "y": {"type": "discrete", "values": [8.0, 16.0, 32.0]},
}

Example OPTIMIZER:

{
    "algorithm": "grid",
    "parameters": {
        "x": {"type": "double", "min": -10, "max": 10},
        "y": {"type": "double", "min": -10, "max": 10},
    },
    "trials": 1,
    #"spec": {"maxCombo": 100},
}

Examples of calling comet optimize:

$ export COMET_OPTIMIZER_ID=$(comet optimize opt.json)
$ comet optimize script.py opt.json
$ comet optimize -j 4 script.py opt.json
$ comet optimize -j 4 -t 10 script.py opt.json
$ comet optimize -j 4 -t 10 script.py opt.json -- arg1 arg2

Use `--` to pass args to script.py.

To use an executable other than python, use -e,
like so:

$ comet optimize -e CMD script.py opt.json
"""

from __future__ import division, print_function

import argparse
import os
import signal
import subprocess
import sys
import time

import comet_ml.bootstrap
from comet_ml import Optimizer
from comet_ml._typing import List
from comet_ml.config import get_config

ADDITIONAL_ARGS = True


def get_parser_arguments(parser):
    # Required arguments: input and output files.
    parser.add_argument(
        "-j", "--parallel", type=int, default=1, help="number of parallel runs"
    )
    parser.add_argument(
        "-t",
        "--trials",
        type=int,
        default=None,
        help="number of trials per parameter configuration",
    )
    parser.add_argument(
        "-e",
        "--executable",
        type=str,
        default=None,
        help="Run using an executable other than Python",
    )
    parser.add_argument(
        "-d",
        "--dump",
        type=str,
        default=None,
        help="Dump the parameters to given filename",
    )
    parser.add_argument("PYTHON_SCRIPT", help="the name of the script to run")  # noqa
    parser.add_argument(
        "OPTIMIZER", nargs="?", default=None, help="optimizer JSON file or optimizer ID"
    )


def main(args):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    get_parser_arguments(parser)
    parsed, rest = parser.parse_known_args(args)

    optimize(parsed, rest)


def optimize(parsed, subcommand_args):
    if parsed.OPTIMIZER is None:
        parsed.OPTIMIZER = parsed.PYTHON_SCRIPT
        parsed.PYTHON_SCRIPT = None

    # Pass it on, in case it needs/wants it:
    subcommand_args += [parsed.OPTIMIZER]
    if parsed.trials is not None:
        subcommand_args += ["--trials", str(parsed.trials)]

    # Is the COMET_API_KEY available?
    config = get_config()
    api_key = config["comet.api_key"]
    if api_key is None:
        raise Exception(
            """Please set your API key: see https://www.comet.ml/docs/python-sdk/advanced/#python-configuration"""
        )

    if not (os.path.isfile(parsed.OPTIMIZER) or len(parsed.OPTIMIZER) == 32):
        raise Exception(
            "Optimizer should be either file or id: '%s'" % parsed.OPTIMIZER
        )

    # Create a new Optimizer, or use existing one:
    if parsed.PYTHON_SCRIPT is None:
        # Don't echo URL if PYTHON_SCRIPT isn't listed:
        opt = Optimizer(parsed.OPTIMIZER, trials=parsed.trials, verbose=0)
    else:
        if not os.path.isfile(parsed.PYTHON_SCRIPT):
            raise Exception("Python script file '%s' not found" % parsed.PYTHON_SCRIPT)
        opt = Optimizer(parsed.OPTIMIZER, trials=parsed.trials)

    if parsed.dump is not None:
        with open(parsed.dump, "w") as fp:
            fp.write(str(opt.status()))

    if parsed.PYTHON_SCRIPT is None:
        # Just print the optimizer_id
        print(opt.id)
        # And exit
        sys.exit(0)

    environ = os.environ.copy()
    environ["COMET_OPTIMIZER_ID"] = opt.id
    COMET_EXECUTABLE = parsed.executable or sys.executable

    bootstrap_dir = os.path.dirname(comet_ml.bootstrap.__file__)

    # Prepend the bootstrap dir to a potentially existing PYTHON PATH, prepend
    # so we are sure that we are the first one to be executed and we cannot be
    # sure that other sitecustomize.py files would call us
    if "PYTHONPATH" in environ:
        if bootstrap_dir not in environ["PYTHONPATH"]:
            environ["PYTHONPATH"] = "%s:%s" % (bootstrap_dir, environ["PYTHONPATH"])
    else:
        environ["PYTHONPATH"] = bootstrap_dir

    command_line = [COMET_EXECUTABLE, parsed.PYTHON_SCRIPT] + subcommand_args
    subprocesses = []  # type: List[subprocess.Popen]

    environ["COMET_OPTIMIZER_PROCESS_JOBS"] = str(parsed.parallel)
    for j in range(parsed.parallel):
        environ["COMET_OPTIMIZER_PROCESS_ID"] = str(j)
        sub = subprocess.Popen(command_line, env=environ)
        subprocesses.append(sub)

    exit_code = 0

    try:
        for sub in subprocesses:
            sub.wait()
            if sub.returncode != 0:
                exit_code = 1
    except KeyboardInterrupt:
        # Ask nicely for subprocesses to exit
        for sub in subprocesses:
            # TODO: Check behavior on Windows
            sub.send_signal(signal.SIGINT)

        # Check that all subprocesses exit cleanly
        i = 0

        while i < 60:
            all_dead = True

            for sub in subprocesses:
                sub.poll()
                alive = sub.returncode is None
                all_dead = all_dead and not alive

            if all_dead:
                break

            i += 1
            time.sleep(1)

        # Timeout, hard-kill all the remaining subprocess
        if i >= 60:
            for sub in subprocesses:
                sub.poll()

                if sub.returncode is None:
                    sub.kill()

    print()
    results = opt.status()
    for key in ["algorithm", "status"]:
        print("   ", "%s:" % key, results[key])
    if isinstance(results["endTime"], float) and isinstance(
        results["startTime"], float
    ):
        print(
            "   ",
            "time:",
            (results["endTime"] - results["startTime"]) / 1000.0,
            "seconds",
        )

    sys.exit(exit_code)


if __name__ == "__main__":
    main(sys.argv[1:])
