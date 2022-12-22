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

import copy
import logging

from .._typing import Any, Dict, Optional, Tuple
from ..experiment import BaseExperiment
from ..logging_messages import GET_CALLBACK_FAILURE
from ..monkey_patching import check_module

LOGGER = logging.getLogger(__name__)


def train_logger(experiment, original, *args, **kwargs):
    # type: (BaseExperiment, Any, Any, Any) -> Optional[Tuple[Tuple[Any, ...], Dict[Any, Any]]]

    try:
        callback = experiment.get_callback("lightgbm")
    except Exception:
        LOGGER.warning(GET_CALLBACK_FAILURE, "lightgbm", exc_info=True)
        return None

    if "callbacks" in kwargs and kwargs["callbacks"] is not None:
        callbacks = kwargs["callbacks"]
        # Only append the callback if it's not there.
        if not any(isinstance(x, callback.__class__) for x in callbacks):
            LOGGER.debug("adding 'lightgbm' logger")
            # Duplicate the callbacks list to avoid mutating user-provided list
            new_callbacks = copy.copy(callbacks)
            new_callbacks.append(callback)
            kwargs["callbacks"] = new_callbacks
        else:
            LOGGER.debug("not adding 'lightgbm' logger")
    else:
        kwargs["callbacks"] = [callback]

    LOGGER.debug("New lightgbm arguments %r %r", args, kwargs)

    return args, kwargs


def patch(module_finder):
    check_module("lightgbm")

    module_finder.register_before("lightgbm.engine", "train", train_logger)


check_module("lightgbm")
