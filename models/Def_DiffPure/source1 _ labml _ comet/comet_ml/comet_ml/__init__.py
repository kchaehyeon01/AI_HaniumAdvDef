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

"""comet-ml"""
# flake8: noqa: F401
# Ignore unused import until we can refactor the imports
from __future__ import print_function

import logging
import sys
import traceback

from ._logging import _setup_comet_http_handler, _setup_comet_logging
from ._online import ExistingExperiment, Experiment
from ._reporting import (
    EXPERIMENT_CREATED,
    EXPERIMENT_CREATION_DURATION,
    EXPERIMENT_CREATION_FAILED,
)
from ._typing import Any, Dict, Optional, Tuple
from ._ui import UI
from .api import API, APIExperiment
from .artifacts import (
    Artifact,
    LoggedArtifact,
    _get_artifact,
    _log_artifact,
    _parse_artifact_name,
)
from .comet import Streamer, format_url, is_valid_experiment_key
from .config import (
    discard_api_key,
    get_api_key,
    get_config,
    get_global_experiment,
    get_previous_experiment,
    get_ws_url,
    init,
    init_onprem,
)
from .confusion_matrix import ConfusionMatrix
from .connection import (
    INITIAL_BEAT_DURATION,
    RestApiClient,
    RestServerConnection,
    WebSocketConnection,
    get_backend_address,
    get_comet_api_client,
    get_rest_api_client,
    log_url,
)
from .data_structure import Embedding, Histogram
from .exceptionhook import _create_exception_hook
from .exceptions import (
    BadCallbackArguments,
    ExperimentCleaningException,
    ExperimentDisabledException,
    ExperimentNotAlive,
)
from .experiment import BaseExperiment
from .feature_toggles import HTTP_LOGGING, FeatureToggles
from .heartbeat import HeartbeatThread
from .json_encoder import NestedEncoder
from .loggers.fastai_logger import patch as fastai_patch
from .loggers.keras_logger import patch as keras_patch
from .loggers.lightgbm_logger import patch as lgbm_patch
from .loggers.mlflow_logger import patch as mlflow_patch
from .loggers.prophet_logger import patch as prophet_patch
from .loggers.pytorch_logger import patch as pytorch_patch
from .loggers.shap_logger import patch as shap_patch
from .loggers.sklearn_logger import patch as sklearn_patch
from .loggers.tensorboard_logger import patch as tb_patch
from .loggers.tensorflow_logger import patch as tf_patch
from .loggers.tfma_logger import patch as tfma_patch
from .loggers.xgboost_logger import patch as xg_patch
from .logging_messages import (
    ADD_SYMLINK_ERROR,
    ADD_TAGS_ERROR,
    ARTIFACT_PREVIEW_FEATURE_FLAG,
    EXPERIMENT_LIVE,
    GET_ARTIFACT_VERSION_OR_ALIAS_GIVEN_TWICE,
    GET_ARTIFACT_WORKSPACE_GIVEN_TWICE,
    INTERNET_CONNECTION_ERROR,
    INVALID_API_KEY,
    ONLINE_EXPERIMENT_THROTTLED,
    REGISTER_RPC_FAILED,
    SEND_NOTIFICATION_FAILED,
)
from .monkey_patching import CometModuleFinder
from .offline import ExistingOfflineExperiment, OfflineExperiment
from .optimizer import Optimizer
from .rpc import create_remote_call, get_remote_action_definition
from .utils import (
    generate_guid,
    get_comet_version,
    get_time_monotonic,
    make_template_filename,
    merge_url,
    valid_ui_tabs,
)

ui = UI()

__author__ = "Gideon<Gideon@comet.ml>"
__all__ = [
    "API",
    "APIExperiment",
    "Artifact",
    "ConfusionMatrix",
    "Embedding",
    "ExistingExperiment",
    "ExistingOfflineExperiment",
    "Experiment",
    "get_comet_api_client",
    "get_global_experiment",
    "Histogram",
    "init_onprem",
    "init",
    "OfflineExperiment",
    "Optimizer",
    "start",
]
__version__ = get_comet_version()

LOGGER = logging.getLogger(__name__)

if not get_config("comet.disable_auto_logging"):
    # Activate the monkey patching
    MODULE_FINDER = CometModuleFinder()
    keras_patch(MODULE_FINDER)
    sklearn_patch(MODULE_FINDER)
    tf_patch(MODULE_FINDER)
    tb_patch(MODULE_FINDER)
    pytorch_patch(MODULE_FINDER)
    fastai_patch(MODULE_FINDER)
    mlflow_patch(MODULE_FINDER)
    xg_patch(MODULE_FINDER)
    tfma_patch(MODULE_FINDER)
    prophet_patch(MODULE_FINDER)
    shap_patch(MODULE_FINDER)
    lgbm_patch(MODULE_FINDER)
    MODULE_FINDER.start()

# Configure the logging
_setup_comet_logging(get_config())

# Register exception hook to process unhandled exceptions
sys.excepthook = _create_exception_hook(sys.excepthook)


def start():
    """
    If you are not using an Experiment in your first loaded Python file, you
    must import `comet_ml` and call `comet_ml.start` before any other imports
    to ensure that comet.ml is initialized correctly.
    """
