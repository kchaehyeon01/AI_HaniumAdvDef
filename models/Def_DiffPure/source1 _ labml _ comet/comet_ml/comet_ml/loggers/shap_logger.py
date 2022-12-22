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

import logging

from .._typing import Any
from ..experiment import BaseExperiment
from ..monkey_patching import check_module

LOGGER = logging.getLogger(__name__)


class SHAPLogger(object):
    def __init__(self, title):
        self.show = True
        self.title = title

    def before(self, experiment, original, *args, **kwargs):
        """
        Little wrapper to make sure show is False
        """
        self.show = kwargs.get("show", True)

        kwargs["show"] = False
        return (args, kwargs)

    def after(self, experiment, original, results, *args, **kwargs):
        # type: (BaseExperiment, Any, Any, Any, Any) -> None
        """
        Little wrapper to log figure, and show, if needed.
        """

        # The post callback shouldn't execute in case of exception in original, so we should have
        # matplotlib except if some shap analysis do not need matplotlib in the future
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            LOGGER.warning("matplotlib not installed; shap logging disabled")
            return

        if experiment.config["comet.auto_log.figures"]:
            experiment._storage["shap"]["counter"] += 1
            experiment._log_figure(
                figure_name="shap-%s-%s"
                % (self.title, experiment._storage["shap"]["counter"]),
                figure_type="shap",
                framework="shap",
            )
        if self.show:
            plt.show()


def patch(module_finder):
    check_module("shap")

    modules = [
        ("shap.plots._bar", "bar", "bar"),
        ("shap.plots._bar", "bar_legacy", "bar_plot"),
        ("shap.plots._image", "image", "image_plot"),
        ("shap.plots._beeswarm", "beeswarm", "beeswarm"),
        ("shap.plots._beeswarm", "summary_legacy", "summary_plot"),
        ("shap.plots._decision", "decision", "decision_plot"),
        ("shap.plots._decision", "multioutput_decision", "multioutput_decision_plot"),
        ("shap.plots._embedding", "embedding", "embedding_plot"),
        ("shap.plots._force", "force", "force_plot"),
        ("shap.plots._group_difference", "group_difference", "group_difference_plot"),
        ("shap.plots._heatmap", "heatmap", "heatmap"),
        ("shap.plots._scatter", "scatter", "scatter"),
        ("shap.plots._scatter", "dependence_legacy", "dependence_plot"),
        ("shap.plots._monitoring", "monitoring", "monitoring_plot"),
        (
            "shap.plots._partial_dependence",
            "partial_dependence",
            "partial_dependence_plot",
        ),
        ("shap.plots._violin", "violin", "violin"),
        ("shap.plots._waterfall", "waterfall", "waterfall_plot"),
    ]
    for module, function, title in modules:
        shap_logger = SHAPLogger(title)
        module_finder.register_before(module, function, shap_logger.before)
        module_finder.register_after(module, function, shap_logger.after)


check_module("shap")
