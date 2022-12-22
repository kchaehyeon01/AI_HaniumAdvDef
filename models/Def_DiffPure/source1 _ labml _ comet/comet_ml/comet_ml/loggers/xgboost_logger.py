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

import copy
import logging
from functools import partial

from pkg_resources import get_distribution, parse_version

from .._typing import Any, Dict, List, Tuple
from ..callbacks._base import (
    _log_xgboost_model_attributes,
    _log_xgboost_model_graph,
    _log_xgboost_model_metrics,
    _log_xgboost_parameters,
    _log_xgboost_step,
)
from ..experiment import BaseExperiment
from ..monkey_patching import check_module

LOGGER = logging.getLogger(__name__)


def _get_env_metrics(evaluation_result_list):
    # type: (List[Tuple[str, float]]) -> Dict[str, Dict[str, float]]
    dict_per_context = {}  # type: Dict[str, Dict[str, float]]
    for context_metric_name, metric_value in evaluation_result_list:
        context, metric_name = context_metric_name.split("-", 1)
        dict_per_context.setdefault(context, {})[metric_name] = metric_value

    return dict_per_context


def _comet_xgboost_callback(experiment, env):
    # type: (BaseExperiment, Any) -> None

    _log_xgboost_step(experiment, env.iteration)

    _log_xgboost_parameters(experiment, env)

    _log_xgboost_model_attributes(experiment, env.model)

    _log_xgboost_model_metrics(experiment, _get_env_metrics(env.evaluation_result_list))

    _log_xgboost_model_graph(experiment, env.model)


def _safe_comet_xgboost_callback(experiment, env):
    # type: (BaseExperiment, Any) -> None
    try:
        _comet_xgboost_callback(experiment, env)
    except Exception:
        LOGGER.debug("Unknown error calling XGBoost callback", exc_info=True)


def _log_xgboost_train_parameters(experiment, xgboost_params):
    # type: (BaseExperiment, Dict[str, Any]) -> None
    # Log XGBoost parameters
    if not experiment._storage["xgboost"]["train_parameter_set"]:
        params = {
            key: value for key, value in xgboost_params.items() if value is not None
        }
        experiment._log_parameters(params)
        experiment._storage["xgboost"]["train_parameter_set"] = True


def _xgboost_train(experiment, original, *args, **kwargs):
    # Positional args
    if len(args) >= 12:
        callbacks = args[11]

        if callbacks is None:
            callbacks = []
        else:
            # Copy callbacks list to avoid in-place mutation, it's gonna be the default behavior in
            # XGBoost anyway https://github.com/dmlc/xgboost/pull/6320
            callbacks = copy.copy(callbacks)

        # Inject or replace callbacks in new args
        args = args[:11] + (callbacks,) + args[12:]
    # Keyword args
    else:
        callbacks = kwargs.get("callbacks", None)
        if callbacks is None:
            callbacks = []
        else:
            # Copy callbacks list to avoid in-place mutation, it's gonna be the default behavior in
            # XGBoost anyway https://github.com/dmlc/xgboost/pull/6320
            callbacks = copy.copy(callbacks)

        # Inject or replace callbacks in new kwargs
        kwargs["callbacks"] = callbacks

    try:
        xgboost_dist = get_distribution("xgboost")
        xgboost_version = parse_version(xgboost_dist.version)

        if xgboost_version >= parse_version("1.3.0"):
            comet_callback = experiment.get_callback("xgboost")
            callbacks.append(comet_callback)
        else:
            callbacks.append(partial(_safe_comet_xgboost_callback, experiment))

    except Exception:
        LOGGER.warn("XGBoost callback couldn't be injected", exc_info=True)

    # Log params passed to training.train
    if experiment.auto_param_logging:
        try:
            if len(args) >= 1:
                booster_params = args[0]
            elif "params" in kwargs:
                booster_params = kwargs["params"]
            else:
                raise ValueError("Couldn't find booster params")
            _log_xgboost_train_parameters(experiment, booster_params)
        except Exception:
            LOGGER.debug("Error auto-logging xgboost parameters", exc_info=True)

    return (args, kwargs)


def patch(module_finder):
    check_module("xgboost")
    module_finder.register_before("xgboost.training", "train", _xgboost_train)


check_module("xgboost")
