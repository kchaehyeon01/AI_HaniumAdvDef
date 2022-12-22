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
import inspect
import logging
import sys
from copy import copy

from .._typing import Any, Callable, Dict, Tuple

LOGGER = logging.getLogger(__name__)


if sys.version_info < (3, 5):
    # Python 2.7, deprecated since 3.5:
    def get_argument_bindings(function, args, kwargs):
        # type: (Callable[..., Any], Tuple[Any, ...], Dict[str, Any]) -> Dict[str, Any]
        try:
            binding = inspect.getcallargs(function, *args, **kwargs)
        except TypeError:
            LOGGER.warning("invalid types to function; unable to get argument bindings")
            return {}
        # getcallargs returns "self", we'll remove it from argument bindings
        ignore_param_list = ["self"]
        # Side-effect, remove ignored items:
        [binding.pop(item) for item in ignore_param_list if item in binding]
        # Returns dict:
        return binding

else:

    def get_argument_bindings(function, args, kwargs):
        # type: (Callable[..., Any], Tuple[Any, ...], Dict[str, Any]) -> Dict[str, Any]
        signature = inspect.signature(function)
        try:
            binding = signature.bind(*args, **kwargs)
        except TypeError:
            LOGGER.warning("invalid types to function; unable to get argument bindings")
            return {}
        # Set default values for missing values:
        binding.apply_defaults()
        ignore_param_list = ["self"]
        # Side-effect, remove ignored items:
        [
            binding.arguments.pop(item)
            for item in ignore_param_list
            if item in binding.arguments
        ]
        # Returns OrderedDict:
        return binding.arguments


def _check_callbacks_list(user_callback_list, comet_callback, copy_list=True):
    # First copy user callback list to avoid adding our callback multiple time
    if copy_list:
        user_callback_list = copy(user_callback_list)

    if user_callback_list is None:
        user_callback_list = []

    # Then check if our callback is already present and if not append it
    callback_class = comet_callback.__class__

    for callback in user_callback_list:
        if isinstance(callback, callback_class):
            break
    else:
        user_callback_list.append(comet_callback)

    return user_callback_list
