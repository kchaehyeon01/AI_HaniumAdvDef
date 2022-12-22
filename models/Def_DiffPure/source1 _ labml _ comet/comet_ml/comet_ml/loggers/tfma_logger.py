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


import json
import logging

from ..convert_utils import data_to_fp
from ..monkey_patching import check_module

LOGGER = logging.getLogger(__name__)
SLICING_METRICS_COUNT = 0
TIME_SERIES_COUNT = 0
PLOT_COUNT = 0

HTML = """
<script src="https://cdn.comet.ml/tensorflow/vulcanized_tfma.js">
</script>
<{component_name} id="component"></{component_name}>
<script>
const element = document.getElementById('component');

element.config = JSON.parse('{config}');
element.data = JSON.parse('{data}');
</script>
"""


def get_args(data, config, event_handlers=None):
    return (data, config, event_handlers)


def render_slicing_metrics(experiment, original, value, *args, **kwargs):
    global SLICING_METRICS_COUNT

    if experiment.config.get_bool(None, "comet.auto_log.tfma"):

        data, config, event_handlers = get_args(*args, **kwargs)

        html = HTML.format(
            component_name="tfma-nb-slicing-metrics",
            config=json.dumps(config),
            data=json.dumps(data),
        )

        fp = data_to_fp(html)

        SLICING_METRICS_COUNT += 1
        experiment.log_asset(fp, "tfma_slicing_metrics_%s.html" % SLICING_METRICS_COUNT)

    return value


def render_time_series(experiment, original, value, *args, **kwargs):
    global TIME_SERIES_COUNT

    if experiment.config.get_bool(None, "comet.auto_log.tfma"):

        data, config, event_handlers = get_args(*args, **kwargs)

        html = HTML.format(
            component_name="tfma-nb-time-series",
            config=json.dumps(config),
            data=json.dumps(data),
        )
        fp = data_to_fp(html)

        TIME_SERIES_COUNT += 1
        experiment.log_asset(fp, "tfma_time_series_%s.html" % TIME_SERIES_COUNT)

    return value


def render_plot(experiment, original, value, *args, **kwargs):
    global PLOT_COUNT

    if experiment.config.get_bool(None, "comet.auto_log.tfma"):

        data, config, event_handlers = get_args(*args, **kwargs)

        html = HTML.format(
            component_name="tfma-nb-plot",
            config=json.dumps(config),
            data=json.dumps(data),
        )
        fp = data_to_fp(html)

        PLOT_COUNT += 1
        experiment.log_asset(fp, "tfma_plot_%s.html" % PLOT_COUNT)

    return value


def patch(module_finder):
    check_module("tensorflow_module_analysis")

    module_finder.register_after(
        "tensorflow_model_analysis.notebook.jupyter.renderer",
        "render_slicing_metrics",
        render_slicing_metrics,
    )
    module_finder.register_after(
        "tensorflow_model_analysis.notebook.colab.renderer",
        "render_slicing_metrics",
        render_slicing_metrics,
    )

    module_finder.register_after(
        "tensorflow_model_analysis.notebook.jupyter.renderer",
        "render_time_series",
        render_time_series,
    )
    module_finder.register_after(
        "tensorflow_model_analysis.notebook.colab.renderer",
        "render_time_series",
        render_time_series,
    )

    module_finder.register_after(
        "tensorflow_model_analysis.notebook.jupyter.renderer",
        "render_plot",
        render_plot,
    )
    module_finder.register_after(
        "tensorflow_model_analysis.notebook.colab.renderer",
        "render_plot",
        render_plot,
    )


check_module("tensorflow_module_analysis")
