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

import io
import json
import logging

from .._typing import Any
from ..experiment import BaseExperiment
from ..monkey_patching import check_module
from .logger_utils import get_argument_bindings

LOGGER = logging.getLogger(__name__)


class ProphetFigureLogger(object):
    def __init__(self, title):
        self.title = title

    def after(self, experiment, original, results, *args, **kwargs):
        # type: (BaseExperiment, Any, Any, Any, Any) -> None
        if experiment.config.get_bool(None, "comet.auto_log.figures"):
            experiment._storage["prophet"]["counter"] += 1
            experiment._log_figure(
                figure=results,
                figure_name="prophet-%s-%s"
                % (self.title, experiment._storage["prophet"]["counter"]),
                figure_type="prophet",
                framework="prophet",
            )


def prophet_constructor(experiment, original, results, *args, **kwargs):
    # type: (BaseExperiment, Any, Any, Any, Any) -> None
    if experiment.auto_param_logging:
        parameters = get_argument_bindings(original, args, kwargs)
        experiment._log_parameters(parameters, framework="prophet")


def prophet_fit(experiment, original, results, *args, **kwargs):
    # type: (BaseExperiment, Any, Any, Any, Any) -> None
    # Note: Experiment.log_graph controls logging the model here:
    if experiment.log_graph and not experiment._storage["prophet"]["internal"]:
        try:
            from prophet.serialize import model_to_json
        except ImportError:
            try:
                from fbprophet.serialize import model_to_json
            except ImportError:
                LOGGER.warning("unable to import prophet.serialize.model_to_json")
                return

        graph_json = model_to_json(args[0])
        fp = io.StringIO()
        json.dump(graph_json, fp)
        fp.seek(0)
        experiment._log_model(
            "prophet-model",
            fp,
            file_name="prophet_model.json",
            overwrite=True,
            copy_to_tmp=True,
        )


def prophet_cross_validation_before(experiment, original, *args, **kwargs):
    # type: (BaseExperiment, Any, Any, Any) -> None
    experiment._storage["prophet"]["internal"] = True


def prophet_cross_validation_after(experiment, original, results, *args, **kwargs):
    # type: (BaseExperiment, Any, Any, Any, Any) -> None
    experiment._storage["prophet"]["internal"] = False


def patch(module_finder):
    # fbprophet in the old module name of prophet, support needs to be removed in the future.
    check_module("prophet")
    check_module("fbprophet")

    module_finder.register_after(
        "prophet.forecaster", "Prophet.__init__", prophet_constructor
    )
    module_finder.register_after(
        "fbprophet.forecaster", "Prophet.__init__", prophet_constructor
    )

    module_finder.register_after("prophet.forecaster", "Prophet.fit", prophet_fit)
    module_finder.register_after("fbprophet.forecaster", "Prophet.fit", prophet_fit)

    module_finder.register_before(
        "prophet.diagnostics", "cross_validation", prophet_cross_validation_before
    )
    module_finder.register_before(
        "fprophet.diagnostics", "cross_validation", prophet_cross_validation_before
    )
    module_finder.register_after(
        "prophet.diagnostics", "cross_validation", prophet_cross_validation_after
    )
    module_finder.register_after(
        "fbprophet.diagnostics", "cross_validation", prophet_cross_validation_after
    )

    for module, function, title in [
        ("prophet.plot", "plot", "plot"),
        ("fbprophet.plot", "plot", "plot"),
        ("prophet.plot", "plot_components", "plot_components"),
        ("fbprophet.plot", "plot_components", "plot_components"),
        (
            "prophet.plot",
            "plot_cross_validation_metric",
            "plot_cross_validation_metric",
        ),
        (
            "fbprophet.plot",
            "plot_cross_validation_metric",
            "plot_cross_validation_metric",
        ),
    ]:
        logger = ProphetFigureLogger(title)
        module_finder.register_after(module, function, logger.after)


check_module("prophet")
check_module("fbprophet")
