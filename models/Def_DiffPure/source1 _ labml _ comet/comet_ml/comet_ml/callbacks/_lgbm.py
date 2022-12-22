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

import json
import logging

from .._typing import Any
from ..experiment import BaseExperiment

LOGGER = logging.getLogger(__name__)


class CometLGBMCallback(object):
    def __init__(self, experiment):
        # type: (BaseExperiment) -> None
        """
        Make and return a lightgbm callback for training.
        """
        self.experiment = experiment

        # We need to run before the early-stopping callback which order is 30
        self.order = 10

        self.initialized = False

    def __call__(self, env):
        # type: (Any) -> None
        """
        The call back to be inserted into lightgbm's training
        callback list.
        """
        if self.initialized is False:  # first time:
            if self.experiment.log_graph:
                try:
                    model_json = env.model.dump_model()
                    model_str = json.dumps(model_json, sort_keys=True, indent=4)
                    self.experiment._set_model_graph(model_str, framework="lightgbm")
                except Exception:
                    LOGGER.debug(
                        "unable to log lightgbm model graph; skipping", exc_info=True
                    )

            if self.experiment.auto_param_logging:
                self.experiment._log_parameters(env.params, framework="lightgbm")

            self.initialized = True

        # Process the new results:
        if self.experiment.auto_metric_logging:
            for data_name, eval_name, result, _ in env.evaluation_result_list:
                self.experiment._log_metric(
                    "%s_%s" % (data_name, eval_name),
                    result,
                    step=env.iteration,
                    framework="lightgbm",
                )
        return None
