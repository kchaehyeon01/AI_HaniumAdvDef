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

from .._typing import Any, Dict, List, Tuple
from ..data_structure import Histogram
from ..monkey_patching import check_module

LOGGER = logging.getLogger(__name__)

LOG_METRICS = True
LOG_HISTOGRAMS = True


def extract_from_add_summary(file_writer, summary, global_step):
    # type: (Any, Any, int) -> Tuple[Dict[str, float], List[Any], int]
    from tensorflow.core.framework import summary_pb2

    extracted_metrics = {}  # type: Dict[str, float]
    extracted_histo = []  # type: List[Any]

    if isinstance(summary, bytes):
        summ = summary_pb2.Summary()
        summ.ParseFromString(summary)
        summary = summ

    for value in summary.value:
        field = value.WhichOneof("value")

        if field == "simple_value":
            extracted_metrics[value.tag] = value.simple_value
        elif field == "histo":
            # TODO: Test ME!
            extracted_histo.append(value.histo)

    return extracted_metrics, extracted_histo, global_step


def convert_histograms(histo):
    """
    Convert tensorboard summary histogram format into a Comet histogram.
    """
    histogram = Histogram()
    values = histo.bucket_limit
    counts = histo.bucket
    histogram.add(values, counts)
    return histogram


def add_summary_logger(experiment, original, value, *args, **kwargs):
    """
    Note: auto_metric_logging controls summary metrics, and
        auto_metric_logging controls summary histograms
    Note: assumes "simple_value" is a metric
    """
    try:
        LOGGER.debug("TENSORBOARD LOGGER CALLED")
        metrics, histograms, step = extract_from_add_summary(*args, **kwargs)

        if metrics and experiment.auto_metric_logging:
            if LOG_METRICS:
                experiment._log_metrics(metrics, step=step, framework="tensorboard")
            else:
                experiment._log_once_at_level(
                    logging.INFO,
                    "ignoring tensorflow summary log of metrics because of keras; set `comet_ml.loggers.tensorboard_logger.LOG_METRICS = True` to override",
                )

        if histograms and experiment.auto_histogram_tensorboard_logging:
            if LOG_HISTOGRAMS:
                for histo in histograms:
                    experiment.log_histogram_3d(convert_histograms(histo), step=step)
            else:
                experiment._log_once_at_level(
                    logging.INFO,
                    "ignoring tensorflow summary log of histograms because of keras; set `comet_ml.loggers.tensorboard_logger.LOG_HISTOGRAMS = True` to override",
                )

    except Exception:
        LOGGER.error(
            "Failed to extract metrics/histograms from add_summary()", exc_info=True
        )


def summary_scalar_logger(experiment, original, value, *args, **kwargs):
    """
    Note: Assumes summary.scalars are metrics.
    """
    try:
        if experiment.auto_metric_logging:
            if LOG_METRICS:
                name, data, step = get_args(*args, **kwargs)
                experiment._log_metric(name, data, step=step, framework="tensorboard")
            else:
                experiment._log_once_at_level(
                    logging.INFO,
                    "ignoring tensorflow summary log of metrics because of keras; set `comet_ml.loggers.tensorboard_logger.LOG_METRICS = True` to override",
                )
    except Exception:
        LOGGER.error(
            "Failed to extract metrics from tensorflow.summary.scalar()", exc_info=True
        )


def summary_text_logger(experiment, original, value, *args, **kwargs):
    """
    Logs text from summary logger
    """
    try:
        name, data, step = get_args(*args, **kwargs)
        experiment.log_text(data, step=step, metadata={"name": name})
    except Exception:
        LOGGER.error("Failed to extract from tensorflow.summary.text()", exc_info=True)


def summary_audio_logger(experiment, original, value, *args, **kwargs):
    """
    Logs audio from summary logger
    """
    try:
        name, data, step, sample_rate = get_audio_args(*args, **kwargs)
        experiment.log_audio(data, file_name=name, sample_rate=sample_rate, step=step)
    except Exception:
        LOGGER.error("Failed to extract from tensorflow.summary.audio()", exc_info=True)


def summary_image_logger(experiment, original, value, *args, **kwargs):
    """
    Logs an image from summary logger
    """
    try:
        name, data, step = get_args(*args, **kwargs)
        if len(data) == 1:
            # single image - logging without suffix
            experiment.log_image(data[0], name=name, step=step)
        else:
            # multiple images - logging prefixed by image index if appropriate
            full_name = None
            for i, image in enumerate(data):
                if name is not None:
                    full_name = full_image_name(prefix=name, index=i)
                experiment.log_image(image, name=full_name, step=step)
    except Exception:
        LOGGER.error("Failed to extract from tensorflow.summary.image()", exc_info=True)


def summary_histogram_3d_logger(experiment, original, value, *args, **kwargs):
    try:
        if experiment.auto_histogram_tensorboard_logging:
            if LOG_HISTOGRAMS:
                name, data, step = get_args(*args, **kwargs)
                experiment.log_histogram_3d(data, name, step=step)
            else:
                experiment._log_once_at_level(
                    logging.INFO,
                    "ignoring tensorflow summary log of histograms because of keras; set `comet_ml.loggers.tensorboard_logger.LOG_HISTOGRAMS = True` to override",
                )

    except Exception:
        LOGGER.error(
            "Failed to extract histogram 3D from tensorflow.summary.histogram()",
            exc_info=True,
        )


def full_image_name(prefix, index):
    # type: (str, int) -> str
    return "%s_%d" % (prefix, index)


def get_args(name, data, step=None, **kwargs):
    """
    Wrapper to parse args
    """
    return name, data, step


def get_audio_args(name, data, sample_rate=None, step=None, **kwargs):
    """
    Wrapper to parse args
    """
    return name, data, step, sample_rate


class ContextHolder:
    def __init__(self, new_context):
        self.new_context = new_context
        self.old_context = None

    def enter(self, experiment, *args, **kwargs):
        self.old_context = experiment.context
        experiment.context = self.new_context

    def exit(self, experiment, *args, **kwargs):
        experiment.context = self.old_context
        self.old_context = None


TRAIN_HOLDER = ContextHolder("train")
EVAL_HOLDER = ContextHolder("eval")


def patch(module_finder):
    check_module("tensorflow")
    check_module("tensorboard")

    # tensorflow 1.11.0 - 1.14.0 in non-compatible mode:
    module_finder.register_after(
        "tensorflow.python.summary.writer.writer",
        "FileWriter.add_summary",
        add_summary_logger,
    )
    module_finder.register_before(
        "tensorflow.python.estimator.estimator", "Estimator.train", TRAIN_HOLDER.enter
    )
    module_finder.register_after(
        "tensorflow.python.estimator.estimator", "Estimator.train", TRAIN_HOLDER.exit
    )
    module_finder.register_before(
        "tensorflow_estimator.python.estimator.estimator",
        "Estimator.train",
        TRAIN_HOLDER.enter,
    )
    module_finder.register_after(
        "tensorflow_estimator.python.estimator.estimator",
        "Estimator.train",
        TRAIN_HOLDER.exit,
    )
    module_finder.register_before(
        "tensorflow.python.estimator.estimator", "Estimator.evaluate", EVAL_HOLDER.enter
    )
    module_finder.register_after(
        "tensorflow.python.estimator.estimator", "Estimator.evaluate", EVAL_HOLDER.exit
    )
    module_finder.register_before(
        "tensorflow_estimator.python.estimator.estimator",
        "Estimator.evaluate",
        EVAL_HOLDER.enter,
    )
    module_finder.register_after(
        "tensorflow_estimator.python.estimator.estimator",
        "Estimator.evaluate",
        EVAL_HOLDER.exit,
    )
    # tensorflow 2:
    module_finder.register_after(
        "tensorboard.plugins.scalar.summary_v2", "scalar", summary_scalar_logger
    )
    module_finder.register_after(
        "tensorboard.plugins.histogram.summary_v2",
        "histogram",
        summary_histogram_3d_logger,
    )
    module_finder.register_after(
        "tensorboard.plugins.text.summary_v2",
        "text",
        summary_text_logger,
    )
    module_finder.register_after(
        "tensorboard.plugins.audio.summary_v2",
        "audio",
        summary_audio_logger,
    )
    module_finder.register_after(
        "tensorboard.plugins.image.summary_v2",
        "image",
        summary_image_logger,
    )


check_module("tensorflow")
check_module("tensorboard")
