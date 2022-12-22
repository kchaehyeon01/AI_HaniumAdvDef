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

from ..monkey_patching import check_module

LOGGER = logging.getLogger(__name__)


def learner_constructor(experiment, original, *args, **kwargs):
    ## Constructor, so no return value
    ## Assume the last Learner has the model:
    try:
        ## args[1]: Learner
        ## args[2]: DataBunch
        ## args[3]: Model
        ## The model is actually a torch model, so we set it
        ## here to work with the torch logger:
        experiment._storage["torch"]["model"] = args[3]
    except Exception:
        LOGGER.error("Failed to run Learner.__init__ logger", exc_info=True)


def callback_on_epoch_begin(experiment, original, retval, *args, **kwargs):
    if experiment.auto_metric_logging:
        try:
            cbhandler = args[0]
            experiment.log_current_epoch(cbhandler.state_dict["epoch"])
        except Exception:
            LOGGER.error(
                "Failed to run CallbackHander.on_epoch_begin logger", exc_info=True
            )
    return retval


def callback_on_epoch_end(experiment, original, retval, *args, **kwargs):
    if experiment.auto_metric_logging:
        try:
            ## args[0]: callback handler
            ## args[1]: metrics: [loss, accuracy, ...]
            ## args[0].state_dict["metrics"]: names (not loss though)
            cbhandler = args[0]
            metric_names = ["val_loss"]
            for f in cbhandler.state_dict["metrics"]:
                metric_name = getattr(f, "__name__", "custom_metric")
                metric_names.append(metric_name)
            metric_values = cbhandler.state_dict["last_metrics"]
            for name, value in zip(metric_names, metric_values):
                experiment._log_metric(
                    name, value, step=experiment.curr_step, framework="fastai"
                )
        except Exception:
            LOGGER.error(
                "Failed to run CallbackHander.on_epoch_end logger", exc_info=True
            )
    return retval


def patch(module_finder):
    ## For testing:
    check_module("fastai")

    ## For Learner constructor:
    module_finder.register_after(
        "fastai.basic_train", "Learner.__init__", learner_constructor
    )
    module_finder.register_after(
        "fastai.callback", "CallbackHandler.on_epoch_end", callback_on_epoch_end
    )
    module_finder.register_after(
        "fastai.callback", "CallbackHandler.on_epoch_begin", callback_on_epoch_begin
    )


check_module("fastai")
