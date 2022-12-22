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
from logging import Logger
from typing import Any, Dict

from xgboost.callback import TrainingCallback

from ._base import (
    _log_xgboost_model_graph,
    _log_xgboost_model_metrics,
    _log_xgboost_parameters,
    _log_xgboost_step,
)


def _adjust_metrics_to_comet(eval_log):
    # type: (Dict[Any, Any]) -> Dict[Any, Any]
    """
    For metrics with more than 1 value, return the average of the values.
    """
    comet_metrics = {}

    for context, metric in eval_log.items():
        per_context_metrics = {}
        for metric_name, log in metric.items():
            per_context_metrics[metric_name] = log[-1]

        comet_metrics[context] = per_context_metrics

    return comet_metrics


def get_xgboost_rabit_parameters():
    try:
        import xgboost.rabit as rabit

        return {"rank": rabit.get_rank(), "world_size": rabit.get_world_size()}
    except Exception:
        Logger.warn("Failed to log XGBoost rabit parameters", exc_info=True)
        return None


class XGBoostCometCallback(TrainingCallback):
    def __init__(self, experiment):
        self.experiment = experiment
        self.iterations = []

    def after_iteration(self, model, epoch, evals_log):
        _log_xgboost_step(self.experiment, epoch)

        _log_xgboost_parameters(self.experiment, model)

        _log_xgboost_model_metrics(self.experiment, _adjust_metrics_to_comet(evals_log))

        _log_xgboost_model_graph(self.experiment, model)

        self.iterations.append(epoch)

        if self.experiment.auto_param_logging:
            rabit_parameters = get_xgboost_rabit_parameters()
            if rabit_parameters:
                self.experiment.log_parameters(rabit_parameters)

    def after_training(self, model):
        if self.experiment.auto_param_logging:
            self.experiment.log_parameter("begin_iteration", self.iterations[0])
            self.experiment.log_parameter("end_iteration", self.iterations[-1])
        return super().after_training(model)
