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
import os
import tempfile

import tensorflow as tf

LOGGER = logging.getLogger(__name__)


def get_tensorflow_graph():
    try:
        graph = tf.compat.v1.get_default_graph()
        return graph
    except Exception:
        LOGGER.debug("Failed to get Tensorflow Model Graph", exc_info=True)
        return None


def write_tensorflow_graph(graph, model_name="model.pbtxt"):
    try:
        graph_def = graph.as_graph_def()
        tempdir = tempfile.mkdtemp()
        tf.io.write_graph(graph_def, tempdir, model_name, as_text=True)
        return os.path.join(tempdir, model_name)
    except Exception:
        LOGGER.debug("Failed to save Tensorflow Model Graph", exc_info=True)
        return None


class CometTensorflowEstimatorTrainSessionHook(tf.estimator.SessionRunHook):
    def __init__(self, experiment):
        self.experiment = experiment

    def after_create_session(self, *args, **kwargs):
        try:
            graph = get_tensorflow_graph()
            if graph is not None:
                self.experiment.set_model_graph(graph)
                model_path = write_tensorflow_graph(graph)
                if model_path is not None:
                    file_name = os.path.basename(model_path)
                    self.experiment._log_asset(
                        model_path,
                        file_name=file_name,
                        copy_to_tmp=False,
                        asset_type="tensorflow-model-graph-text",
                    )
        except Exception:
            LOGGER.debug("Tensorflow Estimator unknown exception", exc_info=True)
            return
