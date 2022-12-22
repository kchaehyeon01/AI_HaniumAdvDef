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

"""
Author: Boris Feld

This module contains the functions dedicated to logging the environment
information

"""

import inspect
import json
import logging
import os
import platform
import socket
import sys

import pkg_resources
import requests
import six
from six import StringIO
from six.moves.urllib.parse import urlparse

from ._typing import Any, Dict, List, Optional, Tuple
from .connection import get_backend_address
from .logging_messages import (
    LOG_CONDA_ENV_ERROR,
    LOG_CONDA_ENV_RETURN_CODE,
    LOG_CONDA_INFO_ERRORS,
    LOG_CONDA_INFO_RETURN_CODE,
    LOG_CONDA_PACKAGES_ERRORS,
    LOG_CONDA_PACKAGES_RETURN_CODE,
)
from .messages import SystemDetailsMessage
from .utils import get_comet_version, get_user, subprocess_run_and_check

LOGGER = logging.getLogger(__name__)


def get_pid():
    return os.getpid()


def get_hostname():
    return socket.gethostname()


def get_os():
    return platform.platform(aliased=True)


def get_os_type():
    return platform.system()


def get_python_version_verbose():
    return sys.version


def get_python_version():
    return platform.python_version()


def get_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_address = get_backend_address()
        parsed = urlparse(server_address)
        host = parsed.hostname
        port = parsed.port
        if port is None:
            port = {"http": 80, "https": 443}.get(parsed.scheme, 0)
        s.connect((host, port))
        addr = s.getsockname()[0]
        s.close()
        return addr

    except socket.error:
        LOGGER.warning("Failed to log ip", exc_info=True)
        return None


def get_command():
    return sys.argv


def get_env_variables(blacklist, os_environ):
    # type: (List[str], Dict[str, str]) -> Dict[str, str]

    lowercase_blacklist = [pattern.lower() for pattern in blacklist]

    def filter_env_variable(env_variable_name):
        # type: (str) -> bool
        """Filter environment variable names containing at least one of the blacklist pattern"""
        lowercase_env_variable_name = env_variable_name.lower()
        for pattern in lowercase_blacklist:
            if pattern in lowercase_env_variable_name:
                return False

        return True

    return {key: value for key, value in os_environ.items() if filter_env_variable(key)}


def get_env_details_message(env_blacklist, include_env, use_http_messages):
    # type: (List[str], bool, bool) -> SystemDetailsMessage
    if include_env:
        env = get_env_variables(env_blacklist, os.environ)
    else:
        env = None

    os_type, _, os_release, _, machine, processor = platform.uname()

    return SystemDetailsMessage.create(
        context=None,
        use_http_messages=use_http_messages,
        command=get_command(),
        env=env,
        hostname=get_hostname(),
        ip=get_ip(),
        machine=machine,
        os_release=os_release,
        os_type=os_type,
        os=get_os(),
        pid=get_pid(),
        processor=processor,
        python_exe=sys.executable,
        python_version_verbose=get_python_version_verbose(),
        python_version=get_python_version(),
        user=get_user(),
    )


def _get_non_pii_env_details(comet_url):
    # type: (str) -> Dict[str, Any]
    """Return environment details without PII, usefull to send to external services like error tracking"""

    return {
        "pid": get_pid(),
        "os": get_os(),
        "os_type": get_os_type(),
        "python_version_verbose": get_python_version_verbose(),
        "python_version": get_python_version(),
        "user": get_user(),
        "python_exe": sys.executable,
        "sdk_version": get_comet_version(),
        "comet_url": comet_url,
    }


def get_caller_file_path():
    # type: () -> Optional[Tuple[str, str]]
    """Returns the module name and file path from the first caller that isn't in the blacklisted
    list. If none is found, returns None
    """
    # Don't get source code from these libraries:
    ignore_list = (
        "comet_ml",
        "mlflow",
        "transformers",
        "ludwig",
        "pytorch_lightning",
        "metaflow",
    )

    for frame in inspect.stack(context=1):
        module = inspect.getmodule(frame[0])
        if module is not None:
            module_name = module.__name__

            if any(module_name.startswith(ignore_name) for ignore_name in ignore_list):
                continue

            filename = module.__file__.rstrip("cd")
            return module_name, filename

    # We didn't find any matching module
    LOGGER.debug("Didn't find any source code module")
    return None


def get_ipython_source_code():
    # type: () -> str
    """
    Get the IPython source code from the history. Assumes that this
    method is run in a jupyter environment.

    Returns the command-history as a string that lead to this point.
    """
    import IPython

    ipy = IPython.get_ipython()
    env = ipy.ns_table["user_local"]
    source = StringIO()
    for n, code in enumerate(env["_ih"]):
        if code:
            source.write("# %%%% In [%s]:\n" % n)
            source.write(code)
            source.write("\n\n")
    return source.getvalue()


def get_ipython_cells():
    # type: () -> List[Tuple[Optional[str], Optional[Any]]]
    """
    Get the Notebook (ipython, colab, Jupyter, PyCharm, etc.) from the
    history. Assumes that this method is run in an ipython environment.

    Returns the command-history [(input,output), ...]

    """
    import IPython

    ipy = IPython.get_ipython()
    env = ipy.ns_table["user_local"]
    cells = []
    for n, code in enumerate(env["_ih"]):
        output = env["_oh"].get(n, None)
        if code or output:
            cells.append((code, output))
    return cells


def _format_text(text):
    # type: (Any) -> List[str]
    """
    Format multi-line text in the ipython style.
    """
    if not isinstance(text, six.string_types):
        text = repr(text)

    lines = text.splitlines(True)  # type: List[str]
    # Remove potentially trailing newlines in the final element
    lines[-1] = lines[-1].rstrip("\r\n").rstrip("\n")
    return lines


def _format_output(output, execution_count):
    # type: (Any, int) -> Optional[Dict]
    """
    Get the mime bundle, etc. for each output form.
    """
    if output is None:
        return None

    if hasattr(output, "_repr_mimebundle_"):
        return {
            "data": output._repr_mimebundle_(),
            "execution_count": execution_count,
            "metadata": {},
            "output_type": "execute_result",
        }
    else:
        return {
            "data": {"text/plain": _format_text(output)},
            "execution_count": execution_count,
            "metadata": {},
            "output_type": "execute_result",
        }


def create_notebook(cells):
    # type: (List[Tuple[Optional[str], Optional[Any]]]) -> Dict
    """
    From a list of (input, output) tuples, create a JSON notebook.
    """
    final_cells = []
    for n, cell in enumerate(cells):
        new_cell = {
            "cell_type": "code",
            "execution_count": n + 1,
            "metadata": {},
            "source": _format_text(cell[0]),
        }

        if cell[1] is not None:
            new_cell["output_type"] = "execute_result"
            new_cell["outputs"] = [_format_output(cell[1], n + 1)]
        else:
            new_cell["outputs"] = []

        final_cells.append(new_cell)

    return {"nbformat": 4, "nbformat_minor": 0, "metadata": {}, "cells": final_cells}


def get_ipython_notebook():
    # type: () -> Dict
    return create_notebook(get_ipython_cells())


def postprocess_gcp_cloud_metadata(cloud_metadata):
    # type: (Dict[str, Any]) -> Dict[str, Any]

    # Attributes contains custom metadata and also contains Kubernetes config,
    # startup script and secrets, filter it out
    if "attributes" in cloud_metadata:
        del cloud_metadata["attributes"]

    return cloud_metadata


CLOUD_METADATA_MAPPING = {
    "AWS": {
        "url": "http://169.254.169.254/latest/dynamic/instance-identity/document",
        "headers": {},
    },
    "Azure": {
        "url": "http://169.254.169.254/metadata/instance?api-version=2019-08-15",
        "headers": {"Metadata": "true"},
    },
    "GCP": {
        "url": "http://169.254.169.254/computeMetadata/v1/instance/?recursive=true&alt=json",
        "headers": {"Metadata-Flavor": "Google"},
        "postprocess_function": postprocess_gcp_cloud_metadata,
    },
}


def get_env_cloud_details(timeout=1):
    # type: (int) -> Optional[Any]
    for provider in CLOUD_METADATA_MAPPING.keys():
        try:
            params = CLOUD_METADATA_MAPPING[provider]
            response = requests.get(
                params["url"], headers=params["headers"], timeout=timeout
            )
            response.raise_for_status()
            response_data = response.json()

            postprocess_function = params.get("postprocess_function")
            if postprocess_function is not None:
                response_data = postprocess_function(response_data)

            return {"provider": provider, "metadata": response_data}
        except Exception as e:
            LOGGER.debug(
                "Not running on %s, couldn't retrieving metadata: %r", provider, e
            )

    return None


def get_pip_packages():
    # type: () -> List[str]
    return sorted(["%s==%s" % (i.key, i.version) for i in pkg_resources.working_set])


def _in_conda_environment():
    # type: () -> bool
    return os.path.exists(os.path.join(sys.prefix, "conda-meta", "history"))


def _in_pydev_console():
    # type: () -> bool
    return "PYDEVD_LOAD_VALUES_ASYNC" in os.environ


def _get_conda_env():
    # type: () -> Optional[bytes]
    args = ["conda", "env", "export", "--json"]
    timeout = 30

    result = subprocess_run_and_check(
        args, timeout, LOG_CONDA_ENV_ERROR, LOG_CONDA_ENV_RETURN_CODE
    )

    try:
        decoded = json.loads(result)
        # Remove the variables key because it can contains secrets
        decoded.pop("variables", None)
        return json.dumps(decoded, indent=4, sort_keys=True)
    except Exception:
        LOGGER.warning(LOG_CONDA_ENV_ERROR, exc_info=True)
        return None


def _get_conda_explicit_packages():
    # type: () -> Optional[bytes]
    args = ["conda", "list", "--explicit", "--md5"]
    timeout = 30
    return subprocess_run_and_check(
        args, timeout, LOG_CONDA_PACKAGES_ERRORS, LOG_CONDA_PACKAGES_RETURN_CODE
    )


def _get_conda_info():
    # type: () -> Optional[bytes]
    args = ["conda", "info"]
    timeout = 30
    return subprocess_run_and_check(
        args, timeout, LOG_CONDA_INFO_ERRORS, LOG_CONDA_INFO_RETURN_CODE
    )
