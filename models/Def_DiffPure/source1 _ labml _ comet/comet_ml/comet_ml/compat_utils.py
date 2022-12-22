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

import io
import json
import subprocess
import sys

import six

from ._typing import List

if six.PY2:
    from collections import Mapping

    from StringIO import StringIO
else:
    from collections.abc import Mapping  # noqa

    StringIO = io.StringIO

PY_VERSION_MAJOR_MINOR = (sys.version_info.major, sys.version_info.minor)


def json_dump(obj, fp, **kwargs):
    """
    A special version of json.dumps for Python 2.7 and Python 3.5, fp must have been opened in
    binary mode.
    """
    # TODO: Once Python 2.7 and Python 3.5 have been dropped, replace me with json.dump and open fp
    # in text mode instead, it will be faster
    converted_data = json.dumps(obj, **kwargs)

    if isinstance(converted_data, six.text_type):
        converted_data = converted_data.encode("utf-8")

    fp.write(converted_data)


class Py2CalledProcessError(subprocess.CalledProcessError):
    def __init__(self, returncode, cmd, output=None, stderr=None):
        super(Py2CalledProcessError, self).__init__(returncode, cmd, output=output)
        # The Python 2 version of subprocess.CalledProcessError doesn't have a stderr attribute
        self.stderr = stderr


class Py2CompletedProcess(object):
    """A re-implementation of Python 3 subprocess.CompletedProcess"""

    def __init__(self, args, returncode, stdout=None, stderr=None):
        self.args = args
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    def __repr__(self):
        args = [
            "args={!r}".format(self.args),
            "returncode={!r}".format(self.returncode),
        ]
        if self.stdout is not None:
            args.append("stdout={!r}".format(self.stdout))
        if self.stderr is not None:
            args.append("stderr={!r}".format(self.stderr))
        return "{}({})".format(type(self).__name__, ", ".join(args))

    def check_returncode(self):
        """Raise CalledProcessError if the exit code is non-zero."""
        if self.returncode:
            raise Py2CalledProcessError(
                self.returncode, self.args, self.stdout, self.stderr
            )


class Py2Popen(subprocess.Popen):
    """The Python 2 version of subprocess.Popen is not usable as a context manager, re-implement it
    to keep subprocess_run re-implementation as close from the Python 3 version.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, value, traceback):
        if self.stdout:
            self.stdout.close()
        if self.stderr:
            self.stderr.close()

        # Wait for the process to terminate, to avoid zombies.
        self.wait()


def subprocess_run(args, timeout):
    # type: (List[str], int) -> subprocess.CompletedProcess[bytes]
    """Reimplementation of subprocess.run for Python 2"""
    if hasattr(subprocess, "run"):
        return subprocess.run(
            args, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

    else:
        with Py2Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
            try:
                # We don't have timeout in Python 2 :(
                stdout, stderr = process.communicate(None)
            except Exception:  # Including KeyboardInterrupt, communicate handled that.
                process.kill()
                # We don't call process.wait() as .__exit__ does that for us.
                raise
            retcode = process.poll()

            # We don't check recode yet as we force check=False

        return Py2CompletedProcess(args, retcode, stdout, stderr)  # type: ignore
