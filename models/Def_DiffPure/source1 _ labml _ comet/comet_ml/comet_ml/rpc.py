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
#
"""
Author: Boris Feld

This module contains the rpc various functions
"""
import collections
import inspect
import logging
import traceback

from comet_ml.exceptions import BadCallbackArguments

import six

from ._typing import Any, Optional

LOGGER = logging.getLogger(__name__)


RemoteCall = collections.namedtuple(
    "RemoteCall", "callId userName functionName cometDefined arguments createdAt"
)


def create_remote_call(call):
    # type: (Any) -> Optional[RemoteCall]
    """Convert the backend response body to a RPCCall object"""

    # The Backend currently sends an ExperimentKey field that unnecessary for
    # us, drop it
    call.pop("experimentKey", None)
    call.pop("projectId", None)

    new_arguments = {}

    # Transform the arguments into a dict
    for arg in call.pop("arguments", []):
        new_arguments[arg["name"]] = arg["value"]

    try:
        return RemoteCall(arguments=new_arguments, **call)
    except Exception:
        LOGGER.debug("Error converting RPC payload", exc_info=True)
        return None


def get_remote_action_definition(function):

    if six.PY2:
        argspec = inspect.getargspec(function)

        arguments = argspec.args

        # Check that the function accept a keyword argument named experiment
        if "experiment" not in arguments and argspec.keywords is None:
            raise BadCallbackArguments(function)

        if "experiment" in arguments:
            arguments.remove("experiment")

    elif six.PY3:
        argspec = inspect.getfullargspec(function)

        # Check that the function accept a keyword argument named experiment
        if (
            "experiment" not in argspec.args
            and argspec.varkw is None
            and "experiment" not in argspec.kwonlyargs
        ):
            raise BadCallbackArguments(function)

        # It is forbidden to declare an argument name both as a positional and
        # keyword-only argument, so we shouldn't get duplicates names
        arguments = argspec.args + argspec.kwonlyargs

        if "experiment" in arguments:
            arguments.remove("experiment")

    return {
        "functionName": function.__name__,
        "functionDocumentation": function.__doc__ or "",
        "argumentNames": arguments,
    }


def call_remote_function(function, experiment, rpc_call):
    try:
        result = function(experiment=experiment, **rpc_call.arguments)

        return {"success": True, "result": convert_result_to_string(result)}
    except Exception as e:
        LOGGER.debug(
            "Error calling %r with %r", function, rpc_call.arguments, exc_info=True
        )
        return {
            "success": False,
            "error_traceback": traceback.format_exc(),
            "error": str(e),
        }


def convert_result_to_string(remote_call_result):
    try:
        return str(remote_call_result)
    except Exception:
        try:
            LOGGER.debug("Error casting as a string", exc_info=True)
            return repr(remote_call_result)
        except Exception:
            LOGGER.debug("Error casting with repr", exc_info=True)
            # Really nasty object, we need to be extra careful here
            result_class = remote_call_result.__class__
            result_dict = remote_call_result.__dict__

            return "Instance of class %r with dict %s" % (result_class, result_dict)
