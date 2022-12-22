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

import logging

from .._typing import Any, Dict
from ..logging_messages import GET_CALLBACK_FAILURE
from ..monkey_patching import check_module
from . import tensorboard_logger

LOGGER = logging.getLogger(__name__)


def fit_keras_logger(experiment, original, *args, **kwargs):
    return fit_logger("keras", "keras", experiment, original, args, kwargs)


def fit_tf_logger(experiment, original, *args, **kwargs):
    return fit_logger(
        "tf-keras", "TensorFlow Keras", experiment, original, args, kwargs
    )


def get_callback_kwargs(args, kwargs):
    # type: (Any, Any) -> Dict[Any, Any]
    try:
        import tensorflow
    except ImportError:
        return {}

    # Find X
    if len(args) >= 2:
        x = args[1]
    elif "x" in kwargs:
        x = kwargs["x"]
    else:
        LOGGER.debug("No x argument found")
        return {}

    if isinstance(x, tensorflow.data.Dataset):
        LOGGER.warning(
            "tensorflow datasets are not currently supported for gradient and activation auto-logging"
        )
        return {}

    # Find Y
    if len(args) >= 3:
        y = args[2]
    elif "y" in kwargs:
        y = kwargs["y"]
    else:
        LOGGER.debug("No y argument found")
        return {}

    return {"inputs": x, "targets": y}


def fit_logger(keras_type, framework, experiment, original, args, kwargs):
    if not experiment.disabled_monkey_patching:
        try:
            callback_kwargs = get_callback_kwargs(args, kwargs)
            callback = experiment.get_callback(keras_type, **callback_kwargs)
        except Exception:
            LOGGER.warning(GET_CALLBACK_FAILURE, framework, exc_info=True)
            return

        if "callbacks" in kwargs and kwargs["callbacks"] is not None:
            callbacks = kwargs["callbacks"]
            # Only append the callback if it's not there.
            if not any(
                x.__class__.__name__ == callback.__class__.__name__ for x in callbacks
            ):
                LOGGER.debug("adding %r logger", framework)
                callbacks.append(callback)
            else:
                LOGGER.debug("not adding %r logger", framework)
        else:
            kwargs["callbacks"] = [callback]

        LOGGER.debug("tensorboard metric logging disabled by %r logger", framework)
        # Disable tensorboard metric logging as it conflicts with keras:
        tensorboard_logger.LOG_METRICS = False
        tensorboard_logger.LOG_HISTOGRAMS = False

        LOGGER.debug("New keras arguments %r %r", args, kwargs)

    return args, kwargs


def multi_gpu_model_wrapper(experiment, original, model, result, *args, **kwargs):
    try:
        experiment._storage["keras"]["json_model"][
            "gpu_model_%s" % id(model)
        ] = model.to_json(sort_keys=True)
    except Exception:
        experiment._log_once_at_level(
            logging.DEBUG, "Failed to saved multi-GPU model", exc_info=True
        )


def patch(module_finder):
    check_module("keras")
    check_module("tensorflow")

    module_finder.register_before("keras.models", "Model.fit", fit_keras_logger)
    module_finder.register_before(
        "keras.models", "Model.fit_generator", fit_keras_logger
    )
    module_finder.register_before(
        "tensorflow.python.keras.models", "Model.fit", fit_tf_logger
    )
    module_finder.register_before(
        "tensorflow.python.keras.models", "Model.fit_generator", fit_tf_logger
    )
    module_finder.register_after(
        "keras.utils.training_utils", "multi_gpu_model", multi_gpu_model_wrapper
    )


check_module("keras")
check_module("tensorflow")
