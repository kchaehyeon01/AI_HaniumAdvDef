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

"""
Author: Boris Feld and Douglas Blank

This module contains comet base Experiment code

"""
from __future__ import print_function

import atexit
import json
import logging
import numbers
import os
import os.path
import platform
import random
import sys
import tempfile
import threading
import traceback
import types
from collections import defaultdict
from contextlib import contextmanager
from functools import reduce
from threading import Thread

import six
from six.moves._thread import get_ident

from ._jupyter import (
    DEFAULT_COLAB_NOTEBOOK_ASSET_NAME,
    DEFAULT_JUPYTER_CODE_ASSET_NAME,
    DEFAULT_JUPYTER_INTERACTIVE_FILE_NAME,
    _get_colab_notebook_json,
    _get_notebook_url,
    _in_colab_environment,
    _in_ipython_environment,
    _in_jupyter_environment,
    display_or_open_browser,
)
from ._reporting import EXPERIMENT_CREATION_FAILED, GIT_PATCH_GENERATION_FAILED
from ._typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    TemporaryFilePath,
    Tuple,
    Union,
)
from .cli_args_parse import parse_command_line_arguments
from .comet import format_url, is_valid_experiment_key
from .config import (
    DEFAULT_ASSET_UPLOAD_SIZE_LIMIT,
    DEFAULT_INITIAL_DATA_LOGGER_JOIN_TIMEOUT,
    DEFAULT_UPLOAD_SIZE_LIMIT,
    get_config,
    get_display_summary_level,
    get_global_experiment,
    set_global_experiment,
)
from .confusion_matrix import ConfusionMatrix
from .connection import compress_git_patch, get_backend_address, get_root_url
from .console import get_std_logger
from .constants import (
    CONDA_ENV_ASSET_TYPE,
    CONDA_ENV_FILE_NAME,
    CONDA_INFO_ASSET_TYPE,
    CONDA_INFO_FILE_NAME,
    CONDA_SPEC_ASSET_TYPE,
    CONDA_SPEC_FILE_NAME,
)
from .convert_utils import (
    check_is_pandas_dataframe,
    convert_log_table_input_to_io,
    convert_model_to_string,
    convert_object_to_dictionary,
    convert_pathlib_path,
    convert_to_scalar,
    convert_to_string_key,
    convert_to_string_value,
    convert_user_input_to_metric_value,
    data_to_fp,
    dataset_to_sprite_image,
    fix_special_floats,
    validate_and_convert_3d_boxes,
    validate_and_convert_3d_points,
)
from .cpu_logging import (
    DEFAULT_CPU_MONITOR_INTERVAL,
    CPULoggingThread,
    is_cpu_info_available,
)
from .data_structure import Embedding, Histogram
from .env_logging import (
    _get_conda_env,
    _get_conda_explicit_packages,
    _get_conda_info,
    _in_conda_environment,
    _in_pydev_console,
    get_caller_file_path,
    get_env_cloud_details,
    get_env_details_message,
    get_ipython_notebook,
    get_ipython_source_code,
    get_pip_packages,
)
from .exceptions import (
    AssetIsTooBig,
    CometException,
    InterruptedExperiment,
    LambdaUnsupported,
    RPCFunctionAlreadyRegistered,
)
from .experiment_storage import ExperimentStorage
from .file_uploader import (
    AssetDataUploadProcessor,
    AudioUploadProcessor,
    FigureUploadProcessor,
    FileUpload,
    GitPatchUploadProcessor,
    ImageUploadProcessor,
    ObjectToConvertFileUpload,
    PreprocessedFileAsset,
    PreprocessedMemoryFileAsset,
    PreprocessedRemoteAsset,
    UserTextFileUpload,
    dispatch_user_file_upload,
    handle_in_memory_file_upload,
    preprocess_asset_file,
    preprocess_asset_memory_file,
    preprocess_remote_asset,
    write_file_like_to_tmp_file,
)
from .gpu_logging import (
    DEFAULT_GPU_MONITOR_INTERVAL,
    GPULoggingThread,
    convert_gpu_details_to_metrics,
    get_gpu_static_info,
    get_initial_gpu_metric,
    is_gpu_details_available,
)
from .logging_messages import (
    ADD_TAGS_ERROR,
    CODECARBON_DIR_CREATION_FAILED,
    CODECARBON_NOT_INSTALLED,
    CODECARBON_START_FAILED,
    CODECARBON_STOP_FAILED,
    CONFUSION_MATRIX_ERROR,
    CONFUSION_MATRIX_GENERAL_ERROR,
    COULDNT_COMPLETE_ATEXIT_UNREGISTER,
    DATAFRAME_PROFILE_ERROR,
    EXPERIMENT_GET_PARAMETER_SHORT_NAME_DEPRECATION,
    EXPERIMENT_INITIAL_DATA_LOGGER_INCOMPLETE,
    EXPERIMENT_INVALID_EPOCH,
    EXPERIMENT_INVALID_STEP,
    FAILED_LOG_EMBEDDING_GROUPS,
    FAILED_LOG_IPYTHON_NOTEBOOK,
    GIT_LOGGING_ERROR,
    GIT_REPO_NOT_FOUND,
    GO_TO_DOCS_MSG,
    INITIAL_DATA_LOGGER_FLUSHING_EXPERIMENT_INTERRUPTED_BY_USER,
    INITIAL_DATA_LOGGER_FLUSHING_FAILED,
    JUPYTER_NEEDS_END,
    LATE_IMPORT_DEGRADED_MODE,
    LOG_ASSET_FOLDER_EMPTY,
    LOG_ASSET_FOLDER_ERROR,
    LOG_CLOUD_POINTS_3D_NO_VALID,
    LOG_CODE_CALLER_JUPYTER,
    LOG_CODE_CALLER_NOT_FOUND,
    LOG_CODE_FILE_NAME_FOLDER_MUTUALLY_EXCLUSIVE,
    LOG_CODE_MISSING_CODE_NAME,
    LOG_COLAB_NOTEBOOK_ERROR,
    LOG_DATASET_ERROR,
    LOG_GIT_METADATA_ERROR,
    LOG_GIT_PATCH_ERROR,
    LOG_PARAMS_EMPTY_CONVERTED_MAPPING,
    LOG_PARAMS_EMPTY_MAPPING,
    LOG_TABLE_FILENAME_AND_HEADERS,
    LOG_TABLE_NONE_VALUES,
    METRIC_NONE_WARNING,
    MISSING_PANDAS_LOG_DATAFRAME,
    NOT_PANDAS_DATAFRAME,
    OPTIMIZER_COULDNT_END,
    PYDEV_NEEDS_END,
    SET_CODE_CODE_DEPRECATED,
    SET_CODE_FILENAME_DEPRECATED,
    UNEXPECTED_ERROR_WHEN_LOGGING_FIGURE,
    UNEXPECTED_ERROR_WHEN_LOGGING_IMAGE_DATA,
    UNEXPECTED_ERROR_WHEN_LOGGING_IMAGE_EMBEDDING,
    UNEXPECTED_ERROR_WHEN_LOGGING_VECTOR_EMBEDDING,
    UPLOAD_ASSET_TOO_BIG,
    UPLOAD_FILE_OS_ERROR,
)
from .messages import (
    BaseMessage,
    CloudDetailsMessage,
    FileNameMessage,
    GitMetadataMessage,
    GpuStaticInfoMessage,
    HtmlMessage,
    HtmlOverrideMessage,
    InstalledPackagesMessage,
    LogDependencyMessage,
    LogOtherMessage,
    MetricMessage,
    ModelGraphMessage,
    OsPackagesMessage,
    ParameterMessage,
    SystemInfoMessage,
    WebSocketMessage,
)
from .monkey_patching import ALREADY_IMPORTED_MODULES
from .rpc import RemoteCall, call_remote_function
from .summary import Summary
from .utils import (
    atexit_unregister,
    generate_guid,
    get_dataframe_profile_html,
    get_user,
    is_list_like,
    local_timestamp,
    log_asset_folder,
    make_template_filename,
    metric_name,
    read_unix_packages,
    safe_filename,
    shape,
    verify_data_structure,
)

if six.PY2:
    from collections import Mapping
else:
    from collections.abc import Mapping


LOGGER = logging.getLogger(__name__)
LOG_ONCE_CACHE = set()  # type: Set[str]

if TYPE_CHECKING:
    import dulwich.repo  # pragma: no-cover


class CommonExperiment(object):
    """
    Class that contains common methods for all experiment types:
        * Experiment
        * OfflineExperiment
        * ExistingExperiment
        * APIExperiment

    Methods and properties required to use these methods:
        * self.project_id - str or None
        * self._get_experiment_url(tab)
    """

    def display_project(
        self, view_id=None, clear=False, wait=True, new=0, autoraise=True
    ):
        """
        Show the Comet.ml project page in an IFrame in a
        Jupyter notebook or Jupyter lab, OR open a browser
        window or tab.

        Common Args:
            view_id: (optional, string) the id of the view to show

        For Jupyter environments:

        Args:
            clear: to clear the output area, use clear=True
            wait: to wait for the next displayed item, use
                  wait=True (cuts down on flashing)

        For non-Jupyter environments:

        Args:
            new: open a new browser window if new=1, otherwise re-use
                 existing window/tab
            autoraise: make the browser tab/window active
        """
        if self.project_id is not None:
            server = get_root_url(get_backend_address())
            if view_id is not None:
                url = "%s/api/projects/redirect?projectId=%s?viewId=%s" % (
                    server,
                    self.project_id,
                    view_id,
                )
            else:
                url = "%s/api/projects/redirect?projectId=%s" % (
                    server,
                    self.project_id,
                )

            display_or_open_browser(url, clear, wait, new, autoraise)

    def display(self, clear=False, wait=True, new=0, autoraise=True, tab=None):
        """
        Show the Comet.ml experiment page in an IFrame in a
        Jupyter notebook or Jupyter lab, OR open a browser
        window or tab.

        Common Args:
            tab: name of the Tab on Experiment View

        Note: the Tab name should be one of:
            * "artifacts"
            * "assets"
            * "audio"
            * "charts"
            * "code"
            * "confusion-matrices"
            * "histograms"
            * "images"
            * "installed-packages"
            * "metrics"
            * "notes"
            * "parameters"
            * "system-metrics"
            * "text"

        For Jupyter environments:

        Args:
            clear: to clear the output area, use clear=True
            wait: to wait for the next displayed item, use
                  wait=True (cuts down on flashing)

        For non-Jupyter environments:

        Args:
            new: open a new browser window if new=1, otherwise re-use
                 existing window/tab
            autoraise: make the browser tab/window active
        """
        url = self._get_experiment_url(tab)
        display_or_open_browser(url, clear, wait, new, autoraise)


class BaseExperiment(CommonExperiment):
    """
    Experiment is a unit of measurable research that defines a single run with some data/parameters/code/results.

    Creating an Experiment object in your code will report a new experiment to your Comet.ml project. Your Experiment
    will automatically track and collect many things and will also allow you to manually report anything.

    You can create multiple objects in one script (such as when looping over multiple hyper-parameters).

    """

    def __init__(
        self,
        project_name=None,  # type: Optional[str]
        workspace=None,  # type: Optional[str]
        log_code=True,  # type: Optional[bool]
        log_graph=True,  # type: Optional[bool]
        auto_param_logging=True,  # type: Optional[bool]
        auto_metric_logging=True,  # type: Optional[bool]
        parse_args=True,  # type: Optional[bool]
        auto_output_logging="default",  # type: Optional[str]
        log_env_details=True,  # type: Optional[bool]
        log_git_metadata=True,  # type: Optional[bool]
        log_git_patch=True,  # type: Optional[bool]
        disabled=False,  # type: Optional[bool]
        log_env_gpu=True,  # type: Optional[bool]
        log_env_host=True,  # type: Optional[bool]
        display_summary=None,  # type: Optional[bool]
        log_env_cpu=True,  # type: Optional[bool]
        display_summary_level=None,  # type: Optional[int]
        optimizer_data=None,  # type: Optional[Dict[str, Any]]
        auto_weight_logging=None,  # type: Optional[bool]
        auto_log_co2=True,  # type: Optional[bool]
        auto_metric_step_rate=10,  # type: Optional[int]
        auto_histogram_tensorboard_logging=False,  # type: Optional[bool]
        auto_histogram_epoch_rate=1,  # type: Optional[int]
        auto_histogram_weight_logging=False,  # type: Optional[bool]
        auto_histogram_gradient_logging=False,  # type: Optional[bool]
        auto_histogram_activation_logging=False,  # type: Optional[bool]
        experiment_key=None,  # type: Optional[str]
    ):
        """
        Base class for all experiment classes.
        """
        self.tmpdir = tempfile.mkdtemp()
        self.config = get_config()

        if self.config.get_bool(display_summary, "comet.display_summary") is not None:
            LOGGER.warning(
                "display_summary is set in Comet config but has been deprecated; use display_summary_level instead"
            )

        self.display_summary_level = get_display_summary_level(
            display_summary_level, self.config
        )
        self._summary = Summary(self.__class__.__name__)

        self.project_name = (
            project_name if project_name else self.config["comet.project_name"]
        )
        self.workspace = workspace if workspace else self.config["comet.workspace"]
        self.name = None

        self.params = {}
        self.deprecated_params = (
            {}
        )  # holds parameters with short name to check for deprecated
        self.metrics = {}
        self.others = {}
        self.tags = set()

        # Get parameters:
        self._log_code = self.config.get_bool(
            log_code, "comet.auto_log.code", True, not_set_value=True
        )
        self.log_graph = self.config.get_bool(
            log_graph, "comet.auto_log.graph", True, not_set_value=True
        )
        self.auto_param_logging = self.config.get_bool(
            auto_param_logging, "comet.auto_log.parameters", True, not_set_value=True
        )
        self.auto_metric_logging = self.config.get_bool(
            auto_metric_logging, "comet.auto_log.metrics", True, not_set_value=True
        )
        self.parse_args = self.config.get_bool(
            parse_args, "comet.auto_log.cli_arguments", True, not_set_value=True
        )
        self.log_env_details = self.config.get_bool(
            log_env_details, "comet.auto_log.env_details", True, not_set_value=True
        )
        self.log_git_metadata = self.config.get_bool(
            log_git_metadata, "comet.auto_log.git_metadata", True, not_set_value=True
        )
        self.log_git_patch = self.config.get_bool(
            log_git_patch, "comet.auto_log.git_patch", True, not_set_value=True
        )
        self.disabled = self.config.get_bool(
            disabled, "comet.auto_log.disable", False, not_set_value=False
        )
        self.log_env_gpu = self.config.get_bool(
            log_env_gpu, "comet.auto_log.env_gpu", True, not_set_value=True
        )
        self.log_env_host = self.config.get_bool(
            log_env_host, "comet.auto_log.env_host", True, not_set_value=True
        )
        self.log_env_cpu = self.config.get_bool(
            log_env_cpu, "comet.auto_log.env_cpu", True, not_set_value=True
        )
        self.auto_log_co2 = self.config.get_bool(
            auto_log_co2, "comet.auto_log.co2", True, not_set_value=True
        )
        self.auto_histogram_tensorboard_logging = self.config.get_bool(
            auto_histogram_tensorboard_logging,
            "comet.auto_log.histogram_tensorboard",
            not_set_value=False,
        )
        self.auto_histogram_weight_logging = self.config.get_deprecated_bool(
            auto_weight_logging,
            "comet.auto_log.weights",
            auto_histogram_weight_logging,
            "comet.auto_log.histogram_weights",
            new_not_set_value=False,
        )
        self.auto_histogram_gradient_logging = self.config.get_bool(
            auto_histogram_gradient_logging,
            "comet.auto_log.histogram_gradients",
            not_set_value=False,
        )
        self.auto_histogram_activation_logging = self.config.get_bool(
            auto_histogram_activation_logging,
            "comet.auto_log.histogram_activations",
            not_set_value=False,
        )
        self.auto_metric_step_rate = self.config.get_bool(
            auto_metric_step_rate,
            "comet.auto_log.metric_step_rate",
            not_set_value=10,
        )
        self.auto_histogram_epoch_rate = self.config.get_bool(
            auto_histogram_epoch_rate,
            "comet.auto_log.histogram_epoch_rate",
            not_set_value=1,
        )
        # Default is "native" for regular environments, "simple" for IPython:
        auto_output_logging = self.config.get_raw(
            auto_output_logging,
            "comet.auto_log.output_logger",
            "default",
            not_set_value="default",
        )
        if auto_output_logging == "default":
            if _in_ipython_environment():
                self.auto_output_logging = "simple"
            # Mac OS default start mode is now spawn and fork is unsafe https://bugs.python.org/issue33725
            elif platform.system() == "Darwin":
                self.auto_output_logging = "simple"
            else:
                self.auto_output_logging = "native"
        else:
            self.auto_output_logging = auto_output_logging or None  # in case of ""

        # Deactivate git logging in case the user disabled logging code
        if not self._log_code:
            self.log_git_metadata = False
            self.log_git_patch = False

        # Disable some logging if log_env_details is False
        if not self.log_env_details:
            self.log_env_gpu = False
            self.log_env_cpu = False
            self.log_env_host = False

        self.autolog_others_ignore = set(self.config["comet.logging.others_ignore"])
        self.autolog_metrics_ignore = set(self.config["comet.logging.metrics_ignore"])
        self.autolog_parameters_ignore = set(
            self.config["comet.logging.parameters_ignore"]
        )

        # Keep track of which auto-loggers data came from
        self._frameworks = set()  # type: Set[str]

        if not self.disabled:
            if len(ALREADY_IMPORTED_MODULES) > 0:
                LOGGER.warning(
                    LATE_IMPORT_DEGRADED_MODE, ", ".join(ALREADY_IMPORTED_MODULES)
                )

        # Generate a unique identifier for this experiment.
        self.id = self._get_experiment_key(experiment_key)

        self.alive = False
        self.ended = False
        self.is_github = False
        self.focus_link = None
        self.upload_limit = DEFAULT_UPLOAD_SIZE_LIMIT
        self.asset_upload_limit = DEFAULT_ASSET_UPLOAD_SIZE_LIMIT
        self.upload_web_asset_url_prefix = None  # type: Optional[str]
        self.upload_web_image_url_prefix = None  # type: Optional[str]
        self.upload_api_asset_url_prefix = None  # type: Optional[str]
        self.upload_api_image_url_prefix = None  # type: Optional[str]

        self.streamer = None
        self.logger = None
        self.gpu_thread = None  # type: Optional[GPULoggingThread]
        self.cpu_thread = None  # type: Optional[CPULoggingThread]
        self.initial_data_logger_thread = None  # type: Optional[Thread]
        self.run_id = None
        self.project_id = None
        self.optimizer = None

        self.main_thread_id = get_ident()

        # If set to True, wrappers should only run the original code
        self.disabled_monkey_patching = False

        # Experiment state
        self.context = None
        self.curr_step = None
        self.curr_epoch = None
        self.filename = None

        self.figure_counter = 0

        self.feature_toggles = {}

        # Storage area for use by loggers
        self._storage = ExperimentStorage(
            {
                "keras": {"json_model": {}},
                "torch": {"model": None},
                "xgboost": {
                    "env_model_parameter_set": False,
                    "env_parameter_set": False,
                    "model_graph_set": False,
                    "train_parameter_set": False,
                },
                "shap": {"counter": 0},
                "prophet": {"counter": 0, "internal": False},
            }
        )
        self._localstorage = threading.local()
        self._localstorage.__dict__.update({"pytorch": {"amp_loss_mapping": {}}})

        self._graph_set = False
        self._code_set = False
        self._notebook_logged = False
        self._pending_calls = []  # type: List[RemoteCall]
        self._embedding_groups = defaultdict(list)

        self._co2_tracker_directory = os.path.join(self.tmpdir, "codecarbon")
        self._co2_tracker = None  # type: Any

        # Cleanup old experiment before replace it
        previous_experiment = get_global_experiment()
        if previous_experiment is not None and previous_experiment is not self:
            try:
                previous_experiment._on_end(wait=False)
            except Exception:
                LOGGER.debug(
                    "Failing to clean up Experiment %s",
                    previous_experiment.id,
                    exc_info=True,
                )

        set_global_experiment(self)

        self.rpc_callbacks = {}

        if optimizer_data is not None:
            self._set_optimizer_from_data(optimizer_data)

        if _in_jupyter_environment():
            LOGGER.warning(JUPYTER_NEEDS_END)

        if _in_pydev_console():
            LOGGER.warning(PYDEV_NEEDS_END)

    @property
    def url(self):
        """
        Get the url of the experiment.

        Example:

        ```python
        >>> experiment.url
        "https://www.comet.ml/username/34637643746374637463476"
        ```
        """
        return None

    def clean(self):
        """Clean the experiment loggers, useful in case you want to debug
        your scripts with IPDB.
        """
        self._on_end(wait=False)

    def end(self):
        """
        Use to indicate that the experiment is complete.
        """
        self._on_end(wait=True)

    def _check_metric_step_report_rate(self, value):
        # type: (int) -> bool
        """
        Check to see we should report at the current batch and value.
        """
        if value is not None and isinstance(value, six.integer_types):
            if self.auto_metric_step_rate == 0:
                return False
            else:
                return (value % self.auto_metric_step_rate) == 0
        else:
            LOGGER.debug(
                "report rate value is not an integer: %r; logging anyway", value
            )
            return True

    def _check_histogram_epoch_report_rate(self, value):
        # type: (int) -> bool
        """
        Check to see we should report at the current epoch and value.

        Note: If unknown level or invalid value, returns True
        """
        if value is not None and isinstance(value, six.integer_types):
            if self.auto_histogram_epoch_rate == 0:
                return False
            else:
                return (value % self.auto_histogram_epoch_rate) == 0
        else:
            LOGGER.debug(
                "report rate value is not an integer: %r; logging anyway", value
            )
            return True

    def _get_experiment_url(self, tab=None):
        raise NotImplementedError()

    def _get_experiment_key(self, user_experiment_key):
        # type: (Optional[str]) -> str
        experiment_key = self.config.get_string(
            user_experiment_key, "comet.experiment_key"
        )
        if experiment_key is not None:
            if is_valid_experiment_key(experiment_key):
                return experiment_key
            else:
                raise ValueError(
                    "COMET_EXPERIMENT_KEY is invalid: '%s' must be alphanumeric and between 32 and 50 characters"
                    % experiment_key
                )
        else:
            return generate_guid()

    def _on_start(self):
        """Called when the Experiment is started"""
        self._mark_as_started()

    def _mark_as_started(self):
        pass

    def _mark_as_ended(self):
        pass

    def _report_experiment_error(self, message=None):
        pass

    def _report_summary(self):
        """
        Display to logger a summary of experiment if there
        is anything to report. If not, no summary will be
        shown.
        """
        # We wait to set this until now:
        self._summary.set("data", "url", self._get_experiment_url())
        self._summary.set("data", "display_summary_level", self.display_summary_level)

        summary = self._summary.generate_summary(self.display_summary_level)
        if self.get_name() is None:
            notification_title = "%s Experiment summary" % get_user()
        else:
            notification_title = "%s - %s Experiment summary" % (
                get_user(),
                self.get_name(),
            )

        self.send_notification(notification_title, "finished", summary)

    def _log_once_at_level(self, logging_level, message, *args, **kwargs):
        """Log the given message once at the given level then at the DEBUG level on further calls"""
        global LOG_ONCE_CACHE

        if message not in LOG_ONCE_CACHE:
            LOG_ONCE_CACHE.add(message)
            LOGGER.log(logging_level, message, *args, **kwargs)
        else:
            LOGGER.debug(message, *args, **kwargs)

    def _clear_storage(self):
        """Called to clear storage associated with this experiment to avoid memory leaks."""
        self._storage.clear()
        self._localstorage.__dict__.clear()

    def _on_end(self, wait=True):
        """Called when the Experiment is replaced by another one or at the
        end of the script
        """
        LOGGER.debug(
            "Experiment on_end called, wait: %s, experiment ID: %s", wait, self.id
        )

        successful_clean = True

        # flush initial data logger first to have summary populated with GIT patch data
        if wait:
            LOGGER.debug("Waiting for initial data logger flush")
            initial_thread_clean = self._flush_initial_data_logger(
                DEFAULT_INITIAL_DATA_LOGGER_JOIN_TIMEOUT
            )
            if not initial_thread_clean:
                LOGGER.debug(
                    "The initial data logger thread did not finished correctly"
                )
                successful_clean = False

        if self.alive:
            if self.optimizer is not None:
                LOGGER.debug("optimizer.end() called")
                try:
                    force_wait = self.optimizer["optimizer"].end(self)
                    if force_wait:
                        # Force wait to be true causes all uploads to finish:
                        LOGGER.debug("forcing wait to be True for optimizer")
                        wait = True
                except Exception:
                    LOGGER.error(OPTIMIZER_COULDNT_END, exc_info=True)

            if len(self._embedding_groups) > 0:
                try:
                    self._log_embedding_groups()
                except Exception:
                    LOGGER.error(FAILED_LOG_EMBEDDING_GROUPS, exc_info=True)

            # Co2 Tracking
            if self._co2_tracker is not None:
                try:
                    self._co2_tracker.stop()
                except Exception:
                    LOGGER.debug(CODECARBON_STOP_FAILED, exc_info=True)

                if os.path.isdir(self._co2_tracker_directory) and os.listdir(
                    self._co2_tracker_directory
                ):
                    self._log_asset_folder(
                        self._co2_tracker_directory, folder_name="co2-tracking"
                    )

            # Try to log IPython notebook code
            if _in_ipython_environment() and self._log_code:
                try:
                    self._log_ipython_notebook_code()
                except Exception:
                    LOGGER.error(FAILED_LOG_IPYTHON_NOTEBOOK, exc_info=True)
            # Try to log Google Colab notebook code
            if _in_colab_environment() and self._log_code and not self._notebook_logged:
                logged = self._log_colab_notebook_code()
                if not logged:
                    LOGGER.warning(LOG_COLAB_NOTEBOOK_ERROR)

            # Logs the source of the data
            for framework in sorted(self._frameworks):
                # Temporary way of logging multiple frameworks. Pass `framework="comet"` so it won't
                # show in the Experiment summary by default
                self._log_other("Created from", framework, framework="comet")

            try:
                self._report_summary()
            except Exception:
                LOGGER.debug("Summary not reported", exc_info=True)

            if len(ALREADY_IMPORTED_MODULES) > 0:
                LOGGER.warning(
                    LATE_IMPORT_DEGRADED_MODE, ", ".join(ALREADY_IMPORTED_MODULES)
                )

                self.log_other(
                    "sys.already_imported_modules",
                    json.dumps(list(ALREADY_IMPORTED_MODULES)),
                )

        if self.logger is not None:
            LOGGER.debug("Cleaning STDLogger")
            self.logger.clean()

        if self.gpu_thread is not None:
            self.gpu_thread.close()
            if wait is True:
                LOGGER.debug(
                    "GPU THREAD before join; gpu_thread.isAlive = %s",
                    self.gpu_thread.is_alive(),
                )
                self.gpu_thread.join(2)

                if self.gpu_thread.is_alive():
                    LOGGER.debug("GPU Thread didn't clean successfully after 2s")
                    successful_clean = False
                else:
                    LOGGER.debug("GPU Thread clean cleanfully")

        if self.cpu_thread is not None:
            self.cpu_thread.close()
            if wait is True:
                LOGGER.debug(
                    "CPU THREAD before join; cpu_thread.isAlive = %s",
                    self.cpu_thread.is_alive(),
                )
                self.cpu_thread.join(2)

                if self.cpu_thread.is_alive():
                    LOGGER.debug("CPU Thread didn't clean successfully after 2s")
                    successful_clean = False
                else:
                    LOGGER.debug("CPU Thread clean cleanfully")

        if self.streamer is not None:
            LOGGER.debug("Closing streamer")
            self.streamer.close()
            if wait is True:
                if self.streamer.wait_for_finish():
                    LOGGER.debug("Streamer clean successfully")
                else:
                    LOGGER.debug("Streamer DIDN'T clean successfully")
                    successful_clean = False
                # No need for atexit run as we waited until finished:
                try:
                    atexit_unregister(self._on_end)
                except Exception:
                    LOGGER.error(COULDNT_COMPLETE_ATEXIT_UNREGISTER, exc_info=True)

            elif self.initial_data_logger_thread.is_alive():
                LOGGER.warning(EXPERIMENT_INITIAL_DATA_LOGGER_INCOMPLETE, self.id)

        self._mark_as_ended()

        # Mark the experiment as not alive anymore to avoid future new
        # messages
        self.alive = False

        # Mark also the experiment as ended as some experiments might never be
        # alive
        self.ended = True

        # Clear associated storage only after unsetting the experiment.alive to make sure that storage consumers
        # can check alive flag before trying to access storage
        self._clear_storage()

        return successful_clean

    def _start(self, log_initial_data_asynchronously=True):
        """Starts the current experiment by preparing all relevant routines and data loggers.
        Args:
            log_initial_data_asynchronously: Optional, bool - allows to switch ON/OFF initial data logging in the
            separate thread. It is useful for testing purposes to quickly switch between logging modes.
        """
        try:
            self.alive = self._setup_streamer()

            if not self.alive:
                LOGGER.debug("Experiment is not alive, exiting")
                return

            # Register the cleaning method to be called when the script ends
            atexit.register(self._on_end)

        except CometException as exception:
            tb = traceback.format_exc()

            exc_log_message = getattr(exception, "log_message", None)
            exc_args = getattr(exception, "args", None)

            if exc_log_message is not None:
                if exc_args is not None:
                    LOGGER.error(exc_log_message, *exc_args, exc_info=True)
                else:
                    # We don't have any log args
                    LOGGER.error(exc_log_message, exc_info=True)
            else:
                LOGGER.error("Run will not be logged" + GO_TO_DOCS_MSG, exc_info=True)

            self._report(event_name=EXPERIMENT_CREATION_FAILED, err_msg=tb)
            return None
        except Exception:
            tb = traceback.format_exc()
            err_msg = "Run will not be logged" + GO_TO_DOCS_MSG
            LOGGER.error(err_msg, exc_info=True, extra={"show_traceback": True})
            self._report(event_name=EXPERIMENT_CREATION_FAILED, err_msg=tb)
            return None

        # After the handshake is done, mark the experiment as alive
        self._on_start()

        try:
            self._setup_std_logger()
        except Exception:
            LOGGER.error("Failed to setup the std logger", exc_info=True)

        ##############################################################
        # log_co2:
        ##############################################################
        if self.auto_log_co2:
            try:
                import codecarbon

                # Ensure the codecarbon directory exists
                if not os.path.isdir(self._co2_tracker_directory):
                    try:
                        os.makedirs(self._co2_tracker_directory)
                    except OSError as exc:
                        LOGGER.warning(
                            CODECARBON_DIR_CREATION_FAILED,
                            self._co2_tracker_directory,
                            exc,
                            exc_info=True,
                        )

                self._co2_tracker = codecarbon.EmissionsTracker(
                    project_name=self.project_name,
                    output_dir=self._co2_tracker_directory,
                )
                self._co2_tracker.start()
            except ImportError:
                LOGGER.debug(CODECARBON_NOT_INSTALLED)

            except Exception:
                LOGGER.debug(CODECARBON_START_FAILED, exc_info=True)

        ##############################################################
        # log_code:
        ##############################################################
        if self._log_code:
            try:
                filename = self._get_filename()
                self.set_filename(filename)
            except Exception:
                LOGGER.error("Failed to set run file name", exc_info=True)

            try:
                # Do not log ipython related files
                if not _in_ipython_environment():
                    caller = get_caller_file_path()
                    if caller is not None:
                        (
                            caller_module_name,
                            experiment_creation_file,
                        ) = caller
                        self._log_code_asset(
                            "experiment_creation", file_name=experiment_creation_file
                        )

                        script_name = sys.argv[0]
                        if caller_module_name != "__main__" and os.path.isfile(
                            script_name
                        ):
                            self._log_code_asset(
                                "python_script_name", file_name=script_name
                            )
            except Exception:
                LOGGER.error("Failed to set run source code", exc_info=True)

        ##############################################################
        # log_env_details: CPU and GPU logging threads
        ##############################################################
        if self.log_env_details:
            if _in_colab_environment():
                try:
                    notebook_url = _get_notebook_url()
                    if notebook_url:
                        self.log_others({"notebook_url": notebook_url})
                except Exception:
                    LOGGER.error("Failed to log colab notebook_url", exc_info=True)

            try:
                self.set_pip_packages()
            except Exception:
                LOGGER.error("Failed to set run pip packages", exc_info=True)

            try:
                self.set_os_packages()
            except Exception:
                LOGGER.error("Failed to set run os packages", exc_info=True)

            try:
                self._log_cloud_details()
            except Exception:
                LOGGER.error("Failed to log cloud details", exc_info=True)

            try:
                if self.log_env_host:
                    self._log_env_details()
            except Exception:
                LOGGER.error("Failed to log environment details", exc_info=True)

            try:
                if self.log_env_gpu:
                    if is_gpu_details_available():
                        self._start_gpu_thread()
                    else:
                        LOGGER.debug(
                            "GPU details unavailable, don't start the GPU thread"
                        )
            except Exception:
                LOGGER.error("Failed to start the GPU tracking thread", exc_info=True)

            try:
                if self.log_env_cpu:
                    if is_cpu_info_available():
                        self._start_cpu_thread()
                    else:
                        LOGGER.debug(
                            "CPU details unavailable, don't start the CPU thread"
                        )
            except Exception:
                LOGGER.error("Failed to start the CPU tracking thread", exc_info=True)

        ##############################################################
        # parse_args:
        ##############################################################
        if self.parse_args:
            try:
                self.set_cmd_args()
            except Exception:
                LOGGER.error("Failed to set run cmd args", exc_info=True)

        ##############################################################
        # _log_initial_data: Start thread to log the rest of the data
        ##############################################################
        if log_initial_data_asynchronously:
            self.initial_data_logger_thread = Thread(
                group=None, target=self._log_initial_data
            )
            self.initial_data_logger_thread.daemon = True
            self.initial_data_logger_thread.start()
        else:
            self._log_initial_data()  # pragma: no-cover

    def _flush_initial_data_logger(self, timeout=None):
        #  type: (Optional[float]) -> bool
        """Invoked to flush the initial data logger thread. Thus, all collected data will be put into message queue."""
        if self.initial_data_logger_thread is not None:
            try:
                self.initial_data_logger_thread.join(timeout)
                return not self.initial_data_logger_thread.is_alive()
            except KeyboardInterrupt:
                LOGGER.warning(
                    INITIAL_DATA_LOGGER_FLUSHING_EXPERIMENT_INTERRUPTED_BY_USER
                )
                return False
            except Exception:
                LOGGER.warning(INITIAL_DATA_LOGGER_FLUSHING_FAILED)
                return False

        return True

    def _log_initial_data(self):
        """Defines all data to be logged during experiment initialization in the separate thread"""
        ##############################################################
        # log_code:
        ##############################################################
        if self._log_code:
            try:
                self._log_git_information()
            except Exception:
                LOGGER.error(GIT_LOGGING_ERROR, exc_info=True)

        if self.log_env_details:
            # Must be last as in Python 2 subprocess don't have any timeout
            try:
                self._log_conda_packages()
            except Exception:
                LOGGER.debug("Failing to collect conda information", exc_info=True)

    def _report(self, *args, **kwargs):
        """Do nothing, could be overridden by subclasses"""
        pass

    def _setup_streamer(self):
        """
        Do the necessary work to create mandatory objects, like the streamer
        and feature flags
        """
        raise NotImplementedError()

    def _setup_std_logger(self):
        # Override default sys.stdout and feed to streamer.
        self.logger = get_std_logger(self.auto_output_logging, self.streamer)
        if self.logger is not None:
            self.logger.set_experiment(self)

    def _create_message(self, include_context=True):
        # type: (bool) -> WebSocketMessage
        """
        Utility wrapper around the Message() constructor
        Returns: Message() object.

        """

        if include_context is True:
            context = self.context
        else:
            context = None

        return WebSocketMessage(context=context)

    def _enqueue_message(self, message):
        # type: (BaseMessage) -> None
        """Queue a single message in the streamer"""
        # First check for pending callbacks call.
        # We do the check in _enqueue_message as it is the most central code
        if get_ident() == self.main_thread_id:
            self._check_rpc_callbacks()
        self.streamer.put_message_in_q(message)

    def _get_asset_upload_step(self):
        """
        Get the current step of the experiment.
        """
        if self.curr_step is not None:
            # Temporary fix to force step to be integer:
            return int(self.curr_step)
        else:
            return self.curr_step

    def _get_asset_upload_epoch(self):
        """
        Get the current epoch of the experiment.
        """
        if self.curr_epoch is not None:
            # Temporary fix to force epoch to be integer:
            return int(self.curr_epoch)
        else:
            return self.curr_epoch

    def get_name(self):
        """
        Get the name of the experiment, if one.

        Example:

        ```python
        >>> experiment.set_name("My Name")
        >>> experiment.get_name()
        'My Name'
        ```
        """
        return self.name

    def get_metric(self, name):
        """
        Get a metric from those logged.

        Args:
            name: str, the name of the metric to get
        """
        return self.metrics[name]

    def get_parameter(self, name):
        """
        Get a parameter that was logged previously in this Experiment instance. Doesn't retrieve
        parameter that were logged before when using the ExistingExperiment class.

        If this method is called inside a context, like [`test`](#experimenttest),
        [`train`](#experimenttrain), [`validate`](#experimentvalidate) or
        [`context_manager`](#experimentcontext_manager), the current context name will be
        automatically added at the front of parameter name.

        For example:

        ```python
        experiment = Experiment(api_key="MY_API_KEY")

        experiment.log_parameter("training_rate", 0.0001)
        with experiment.train():
            experiment.log_parameter("batch_size", 64)

        assert experiment.get_parameter("training_rate") == 0.0001
        assert experiment.get_parameter("train_batch_size") == 64

        with experiment.train():
            assert experiment.get_parameter("batch_size") == 64
        ```


        Args:
            name: str, the name of the parameter to get
        Raises:
            KeyError: if parameter with given name not found
        """
        # first: assume the call is within context
        full_name = self._fullname(name)
        if full_name in self.params:
            return self.params[full_name]

        # second: assume the full name provided
        if name in self.params:
            return self.params[name]

        # TODO: current implementation allows cross-context parameter retrieval which should be fixed
        #  in the next major release, see test_get_parameter_cross_context_retrieval_allowed_bug() for details

        # third: check in the deprecated parameters
        if name in self.deprecated_params:
            potential = [k for k in self.params.keys() if k.endswith(name)]
            LOGGER.warning(
                EXPERIMENT_GET_PARAMETER_SHORT_NAME_DEPRECATION % (name, potential)
            )
            return self.deprecated_params[name]

        # not found anywhere - raise KeyError
        raise KeyError(name)

    def get_other(self, name):
        """
        Get an other from those logged.

        Args:
            name: str, the name of the other to get
        """
        return self.others[name]

    def get_key(self):
        """
        Returns the experiment key, useful for using with the ExistingExperiment class
        Returns: Experiment Key (String)
        """
        return self.id

    def log_other(self, key, value):
        # type: (Any, Any) -> None
        """
        Reports a key and value to the `Other` tab on
        Comet.ml. Useful for reporting datasets attributes, datasets
        path, unique identifiers etc.

        See related methods: [`log_parameter`](#experimentlog_parameter) and
            [`log_metric`](#experimentlog_parameter)

        Args:
            key: Any type of key (str,int,float..)
            value: Any type of value (str,int,float..)

        Returns: None
        """
        try:
            return self._log_other(key, value)
        except Exception:
            LOGGER.error(
                "Unknown exception happened in Experiment.log_other; ignoring",
                exc_info=True,
            )

    def _log_other(self, key, value, framework=None):
        # type: (Any, Any, Optional[str]) -> None
        # Internal logging handler with option to ignore auto-logged keys
        if not self.alive:
            return

        key = convert_to_string_key(key)
        if framework:
            if ("%s:%s" % (framework, key)) in self.autolog_others_ignore:
                # Use % in this message to cache specific string:
                self._log_once_at_level(
                    logging.INFO,
                    "Ignoring automatic log_other(%r) because '%s:%s' is in COMET_LOGGING_OTHERS_IGNORE"
                    % (key, framework, key),
                )
                return
            else:
                self._track_framework_usage(framework)

        if not (isinstance(value, numbers.Number) or value is None):
            value = convert_to_string_value(value)

        message = LogOtherMessage.create(
            context=self.context,
            use_http_messages=self.streamer.use_http_messages,
            key=key,
            value=value,
        )

        self._enqueue_message(message)
        self._summary.set("others", self._fullname(key), value, framework=framework)
        self.others[key] = value

    def log_others(self, dictionary):
        """
        Reports dictionary of key/values to the `Other` tab on
        Comet.ml. Useful for reporting datasets attributes, datasets
        path, unique identifiers etc.

        See [`log_other`](#experimentlog_other)

        Args:
            key: dict of key/values where value is Any type of
                value (str,int,float..)

        Returns: None
        """
        if self.alive:
            if not isinstance(dictionary, Mapping):
                LOGGER.error(
                    "log_other requires a dict or key/value but not both", exc_info=True
                )
                return
            else:
                for other in dictionary:
                    self.log_other(other, dictionary[other])

    def log_dependency(self, name, version):
        """
        Reports name,version to the `Installed Packages` tab on Comet.ml. Useful to track dependencies.
        Args:
            name: Any type of key (str,int,float..)
            version: Any type of value (str,int,float..)

        Returns: None

        """
        if not self.alive:
            return

        message = LogDependencyMessage.create(
            context=self.context,
            use_http_messages=self.streamer.use_http_messages,
            name=name,
            version=version,
        )

        self._enqueue_message(message)
        self._summary.increment_section("uploads", "dependency")

    def log_system_info(self, key, value):
        """
        Reports the key and value to the `System Metric` tab on
        Comet.ml. Useful to track general system information.
        This information can be added to the table on the
        Project view. You can retrieve this information via
        the Python API.

        Args:
            key: Any type of key (str,int,float..)
            value: Any type of value (str,int,float..)

        Returns: None

        Example:

        ```python
        # Can also use ExistingExperiment here instead of Experiment:
        >>> from comet_ml import Experiment, APIExperiment
        >>> e = Experiment()
        >>> e.log_system_info("info-about-system", "debian-based")
        >>> e.end()

        >>> apie = APIExperiment(previous_experiment=e.id)
        >>> apie.get_system_details()['logAdditionalSystemInfoList']
        [{"key": "info-about-system", "value": "debian-based"}]
        ```
        """
        if not self.alive:
            return

        message = SystemInfoMessage.create(
            context=self.context,
            use_http_messages=self.streamer.use_http_messages,
            key=key,
            value=value,
        )
        self._enqueue_message(message)
        self._summary.set("system-info", key, value)

    def log_html(self, html, clear=False):
        """
        Reports any HTML blob to the `HTML` tab on Comet.ml. Useful for creating your own rich reports.
        The HTML will be rendered as an Iframe. Inline CSS/JS supported.
        Args:
            html: Any html string. for example:
            clear: Default to False. when setting clear=True it will remove all previous html.
            ```python
            experiment.log_html('<a href="www.comet.ml"> I love Comet.ml </a>')
            ```

        Returns: None

        """
        if not self.alive:
            return

        if clear:
            message = HtmlOverrideMessage.create(
                context=self.context,
                use_http_messages=self.streamer.use_http_messages,
                htmlOverride=html,
            )
        else:
            message = HtmlMessage.create(
                context=self.context,
                use_http_messages=self.streamer.use_http_messages,
                html=html,
            )

        self._enqueue_message(message)
        self._summary.increment_section("uploads", html)

    def log_html_url(self, url, text=None, label=None):
        """
        Easy to use method to add a link to a URL in the `HTML` tab
        on Comet.ml.

        Args:
            url: a link to a file or notebook, for example
            text: text to use a clickable word or phrase (optional; uses url if not given)
            label: text that precedes the link

        Examples:

        ```python
        >>> experiment.log_html_url("https://my-company.com/file.txt")
        ```

        Adds html similar to:

        ```html
        <a href="https://my-company.com/file.txt">
          https://my-company.com/file.txt
        </a>
        ```
        ```python
        >>> experiment.log_html_url("https://my-company.com/file.txt",
                                    "File")
        ```

        Adds html similar to:

        ```html
        <a href="https://my-company.com/file.txt">File</a>
        ```

        ```python
        >>> experiment.log_html_url("https://my-company.com/file.txt",
                                    "File", "Label")
        ```

        Adds html similar to:

        ```
        Label: <a href="https://my-company.com/file.txt">File</a>
        ```

        """
        text = text if text is not None else url
        if label:
            self.log_html(
                """<div><b>%s</b>: <a href="%s" target="_blank">%s</a></div>"""
                % (label, url, text)
            )
        else:
            self.log_html(
                """<div><a href="%s" target="_blank">%s</a></div>""" % (url, text)
            )

    def set_step(self, step):
        """
        Sets the current step in the training process. In Deep Learning each
        step is after feeding a single batch into the network. This is used to
        generate correct plots on Comet.ml. You can also pass the step directly
        when reporting [log_metric](#experimentlog_metric), and
        [log_parameter](#experimentlog_parameter).

        Args: step: Integer value

        Returns: None

        """

        if step is not None:
            step = convert_to_scalar(step)

            if isinstance(step, numbers.Number):
                self.curr_step = step
                self._log_parameter(name="curr_step", value=step, framework="comet")
            else:
                LOGGER.warning(EXPERIMENT_INVALID_STEP, step)

    def set_epoch(self, epoch):
        """
        Sets the current epoch in the training process. In Deep Learning each
        epoch is an iteration over the entire dataset provided. This is used to
        generate plots on comet.ml. You can also pass the epoch
        directly when reporting [log_metric](#experimentlog_metric).

        Args:
            epoch: Integer value

        Returns: None
        """

        if epoch is not None:
            epoch = convert_to_scalar(epoch)

            if isinstance(epoch, numbers.Number):
                self.curr_epoch = epoch
                self._log_parameter("curr_epoch", epoch, framework="comet")
            else:
                LOGGER.warning(EXPERIMENT_INVALID_EPOCH, epoch)

    def log_epoch_end(self, epoch_cnt, step=None):
        """
        Logs that the epoch finished. Required for progress bars.

        Args:
            epoch_cnt: integer

        Returns: None

        """
        self.set_step(step)

        if self.alive:
            self.set_epoch(epoch_cnt)

    def log_metric(self, name, value, step=None, epoch=None, include_context=True):
        # type: (Any, Any, Optional[Any], Optional[Any], bool) -> None
        """
        Logs a general metric (i.e accuracy, f1).

        e.g.
        ```python
        y_pred_train = model.predict(X_train)
        acc = compute_accuracy(y_pred_train, y_train)
        experiment.log_metric("accuracy", acc)
        ```

        See also [`log_metrics`](#experimentlog_metrics)


        Args:
            name: String - name of your metric
            value: Float/Integer
            step: Optional. Used as the X axis when plotting on comet.ml
            epoch: Optional. Used as the X axis when plotting on comet.ml
            include_context: Optional. If set to True (the default), the
                current context will be logged along the metric.

        Returns: None

        Down sampling metrics:
        Comet guarantees to store 15,000 data points for each metric. If more than 15,000 data points are reported we
        perform a form of reservoir sub sampling - https://en.wikipedia.org/wiki/Reservoir_sampling.

        """
        try:
            return self._log_metric(name, value, step, epoch, include_context)
        except Exception:
            LOGGER.error(
                "Unknown exception happened in Experiment.log_metric; ignoring",
                exc_info=True,
            )
            return None

    def _log_metric(
        self, name, value, step=None, epoch=None, include_context=True, framework=None
    ):
        # type: (Any, Any, Optional[Any], Optional[Any], bool, Optional[str]) -> None
        name = convert_to_string_key(name)
        # Internal logging handler with option to ignore auto-logged names
        if framework:
            if ("%s:%s" % (framework, name)) in self.autolog_metrics_ignore:
                # Use % in this message to cache specific string:
                self._log_once_at_level(
                    logging.INFO,
                    "Ignoring automatic log_metric(%r) because '%s:%s' is in COMET_LOGGING_METRICS_IGNORE"
                    % (name, framework, name),
                )
                return

        LOGGER.debug("Log metric: %s %r %r", name, value, step)

        self.set_step(step)
        self.set_epoch(epoch)

        if self.alive:
            if value is None:
                LOGGER.warning(METRIC_NONE_WARNING, name)
                return None

            self._track_framework_usage(framework)

            if include_context is True:
                message = MetricMessage(context=self.context)
            else:
                message = MetricMessage()

            value = convert_user_input_to_metric_value(value)

            message.set_metric(name, value, self.curr_step, self.curr_epoch)
            self._enqueue_message(message)
            self._summary.set(
                "metrics", self._fullname(name), value, framework=framework
            )

        # save state.
        self.metrics[name] = value

    def _fullname(self, name):
        """
        If in a context manager, add the context name.
        """
        if self.context is not None:
            return "%s_%s" % (self.context, name)
        else:
            return name

    def log_parameter(self, name, value, step=None):
        # type: (Any, Any, Optional[Any]) -> None
        """
        Logs a single hyperparameter. For additional values that are not hyper parameters it's encouraged to use [log_other](#experimentlog_other).

        See also [`log_parameters`](#experimentlog_parameters).

        If the same key is reported multiple times only the last reported value will be saved.

        If this method is called inside a context, like [`test`](#experimenttest),
        [`train`](#experimenttrain), [`validate`](#experimentvalidate) or
        [`context_manager`](#experimentcontext_manager), the parameter will be stored with the
        current context name as a prefix.

        For example, the following code:

        ```python
        experiment = Experiment(api_key="MY_API_KEY")

        with experiment.train():
            experiment.log_parameter("batch_size", 64)
        ```

        Will logs the hyper-parameter `train_batch_size`.

        Args:
            name: String - name of your parameter
            value: Float/Integer/Boolean/String/List
            step: Optional. Used as the X axis when plotting on Comet.ml

        Returns: None

        """
        try:
            return self._log_parameter(name, value, step)
        except Exception:
            LOGGER.error(
                "Unknown exception happened in Experiment.log_parameter; ignoring",
                exc_info=True,
            )

    def _log_parameter(self, name, value, step=None, framework=None):
        # type: (Any, Any, Optional[Any], Optional[str]) -> None
        # Internal logging handler with option to ignore auto-logged names
        name = convert_to_string_key(name)
        if framework:
            if ("%s:%s" % (framework, name)) in self.autolog_parameters_ignore:
                # Use % in this message to cache specific string:
                self._log_once_at_level(
                    logging.INFO,
                    "Ignoring automatic log_parameter(%r) because '%s:%s' is in COMET_LOGGING_PARAMETERS_IGNORE"
                    % (name, framework, name),
                )
                return None

        self.set_step(step)

        # de-duplicate using parameter's full name, i.e., including the context name
        param_full_name = self._fullname(name)
        if param_full_name in self.params and self.params[param_full_name] == value:
            return None

        if self.alive:
            # create message
            message = ParameterMessage(context=self.context)
            value = convert_to_scalar(value)

            # Check if we have a list-like, dict, number, bool, None, or a string
            if isinstance(value, Mapping):
                value = convert_to_string_value(value)
                message.set_param(name, value, self.curr_step)
            elif is_list_like(value):
                message.set_params(name, value, self.curr_step)
            elif (
                isinstance(value, numbers.Number) or value is None
            ):  # booleans are Numbers
                message.set_param(name, value, self.curr_step)
            else:
                value = convert_to_string_value(value)
                message.set_param(name, value, self.curr_step)

            self._enqueue_message(message)
            self._summary.set("parameters", param_full_name, value, framework=framework)

        self.params[param_full_name] = value
        # save with the the short name to allow check for deprecated usage of the get_parameter()
        self.deprecated_params[name] = value

    def log_figure(self, figure_name=None, figure=None, overwrite=False, step=None):
        # type: (Optional[str], Optional[Any], bool, Optional[int]) -> Optional[Dict[str, Optional[str]]]
        """
        Logs the global Pyplot figure or the passed one and upload its svg
        version to the backend.

        Args:
            figure_name: Optional. String - name of the figure
            figure: Optional. The figure you want to log. If not set, the global
                pyplot figure will be logged and uploaded
            overwrite: Optional. Boolean - if another figure with the same name
                exists, it will be overwritten if overwrite is set to True.
            step: Optional. Used to associate the audio asset to a specific step.
        """
        return self._log_figure(figure_name, figure, overwrite, step)

    def _log_figure(
        self,
        figure_name=None,  # type: Optional[str]
        figure=None,  # type: Optional[Any]
        overwrite=False,  # type: bool
        step=None,  # type: Optional[int]
        figure_type=None,  # type: Optional[str]
        framework=None,  # type: Optional[str]
    ):
        # type: (...) -> Optional[Dict[str, Optional[str]]]
        if not self.alive:
            return None

        self.set_step(step)

        # Pass additional url params
        figure_number = self.figure_counter
        figure_id = generate_guid()
        url_params = {
            "step": self._get_asset_upload_step(),
            "figCounter": figure_number,
            "context": self.context,
            "runId": self.run_id,
            "overwrite": overwrite,
            "imageId": figure_id,
        }

        if figure_name is not None:
            url_params["figName"] = figure_name

        processor = FigureUploadProcessor(
            figure,
            self.upload_limit,
            url_params,
            metadata=None,
            copy_to_tmp=False,
            error_message_identifier=figure_number,
            tmp_dir=self.tmpdir,
            upload_type=figure_type,
            critical=False,
        )
        upload_message = processor.process()

        if upload_message is None:
            self._report_experiment_error(UNEXPECTED_ERROR_WHEN_LOGGING_FIGURE)
            return None

        self._enqueue_message(upload_message)

        self._summary.increment_section("uploads", "figures")
        self.figure_counter += 1

        return self._get_uploaded_figure_url(figure_id)

    def log_tensorflow_folder(self, folder):
        # type: (str) -> None
        """
        Logs all the tensorflow log files located in the given folder as assets.

        Args:
            folder: String - the path to the folder you want to log.

        Use `APIExperiment.download_tensorflow_folder()` to get
        the files.

        Example:

        ```python
        >>> experiment = comet_ml.Experiment()
        >>> experiment.log_tensorboard_folder("logs")
        >>> api = comet_ml.API()
        >>> api_experiment = api.get_experiment_by_id(experiment.id)
        >>> api_experiment.download_tensorflow_folder()
        ```
        """
        if not self.alive:
            return

        if folder.endswith(os.sep):
            folder = folder[:-1]

        self._log_asset_folder(
            folder, log_file_name=True, recursive=True, asset_type="tensorflow-file"
        )

    def log_text(self, text, step=None, metadata=None):
        """
        Logs the text. These strings appear on the Text Tab in the
        Comet UI.

        Args:
            text: string to be stored
            step: Optional. Used to associate the asset to a specific step.
            metadata: Some additional data to attach to the text.
                Must be a JSON-encodable dict.
        """
        # Send fake file_name, which is replaced on the backend:
        return self._log_asset_data(
            text,
            file_name="auto-generated-in-the-backend",
            asset_type="text-sample",
            step=step,
            metadata=metadata,
        )

    def log_model(
        self,
        name,
        file_or_folder,
        file_name=None,  # does not apply to folders
        overwrite=False,  # does not apply to folders
        metadata=None,
        copy_to_tmp=True,  # if data is a file pointer
        prepend_folder_name=True,
    ):
        # type: (str, Union[str, dict], Optional[str], Optional[bool], Optional[dict], Optional[bool], bool) -> Any
        """
        Logs the model data under the name. Data can be a file path, a folder
        path or a file-like object.

        Args:
            name: string (required), the name of the model
            file_or_folder: the model data (required); can be a file path, a
                folder path or a file-like object.
            file_name: (optional) the name of the model data. Used with file-like
                objects or files only.
            overwrite: boolean, if True, then overwrite previous versions
                Does not apply to folders.
            metadata: Some additional data to attach to the the data.
                Must be a JSON-encodable dict.
            copy_to_tmp: for file name or file-like; if True copy to
                temporary location before uploading; if False, then
                upload from current location
            prepend_folder_name: boolean, default True. If True and logging a folder, prepend file
                path by the folder name.

        Returns: dictionary of model URLs
        """
        return self._log_model(
            name,
            file_or_folder,
            file_name,
            overwrite,
            metadata,
            copy_to_tmp,
            prepend_folder_name=prepend_folder_name,
        )

    def _log_model(
        self,
        model_name,
        file_or_folder,
        file_name=None,  # does not apply to folders
        overwrite=False,  # does not apply to folders
        metadata=None,
        copy_to_tmp=True,  # if data is a file pointer
        folder_name=None,  # if data is a folder
        prepend_folder_name=True,  # if data is a folder
    ):
        if isinstance(file_or_folder, str) and os.path.isfile(
            file_or_folder
        ):  # filname
            return self._log_asset(
                file_or_folder,  # filename
                file_name=file_name,
                overwrite=overwrite,
                copy_to_tmp=copy_to_tmp,
                asset_type="model-element",
                metadata=metadata,
                grouping_name=model_name,  # model name
            )
        elif hasattr(file_or_folder, "read"):  # file-like object
            return self._log_asset(
                file_or_folder,  # file-like object
                file_name=file_name,  # filename
                overwrite=overwrite,
                copy_to_tmp=copy_to_tmp,
                asset_type="model-element",
                metadata=metadata,
                grouping_name=model_name,  # model name
            )
        elif isinstance(file_or_folder, str) and os.path.isdir(
            file_or_folder
        ):  # foldername
            return self._log_asset_folder(
                file_or_folder,  # folder
                recursive=True,
                log_file_name=True,
                asset_type="model-element",
                metadata=metadata,
                grouping_name=model_name,  # model name
                folder_name=folder_name,
                prepend_folder_name=prepend_folder_name,
            )
        else:
            LOGGER.error(
                "Experiment.log_model() requires a file or folder", exc_info=True
            )
            return None

    def _log_ipython_notebook_code(self):
        """Invoked to log the IPython notebook code if appropriate"""
        if not (_in_ipython_environment() and self._log_code):
            # safeguard check
            return

        source_code = get_ipython_source_code()
        if source_code == "":
            # We might be running script directly:
            caller = get_caller_file_path()
            if caller is not None:
                self._log_code_asset("experiment_creation", file_name=caller[1])
        else:
            self._log_code_asset(
                "experiment_creation",
                code=source_code,
                code_name=DEFAULT_JUPYTER_INTERACTIVE_FILE_NAME,
            )

        # Now attempt to log the code as a notebook:
        if self.alive:
            notebook_json = get_ipython_notebook()
            name = DEFAULT_JUPYTER_CODE_ASSET_NAME
            return self._log_asset_data(
                notebook_json,
                file_name=name,
                overwrite=True,
                asset_type="notebook",
            )

    def _log_colab_notebook_code(self):
        # type: () -> None
        """Invoked to log the Google Colab notebook code if appropriate"""
        if self.alive:
            notebook_json = _get_colab_notebook_json()

            if notebook_json is None:
                return None

            name = DEFAULT_COLAB_NOTEBOOK_ASSET_NAME
            return self._log_asset_data(
                notebook_json,
                file_name=name,
                overwrite=True,
                asset_type="notebook",
            )

    def log_notebook(self, filename, overwrite=False):
        # type: (str, Optional[bool]) -> Optional[Dict[str, str]]
        """
        Log a Jupyter Notebook file as an asset.

        Args:
            filename: (str) the path and name of notebook
            overwrite: (optional, bool) if True, overwrite previous notebook

        Example:

        ```python
        >>> experiment = Experiment()
        # Save the notebook, if currently editing the notebook
        >>> experiment.log_notebook("~/Untitled2394.ipynb")
        ```
        """
        if not isinstance(filename, str):
            raise ValueError("filename should be a string")

        if not filename.endswith(".ipynb"):
            raise ValueError("filename should be a notebook file ending in '.ipynb'")

        filename = os.path.expanduser(filename)
        self._notebook_logged = True
        return self._log_asset(filename, asset_type="notebook", overwrite=overwrite)

    def log_curve(self, name, x, y, overwrite=False, step=None):
        """
        Log timeseries data.

        Args:
            name: (str) name of data
            x: list of x-axis values
            y: list of y-axis values
            overwrite: (optional, bool) if True, overwrite previous log
            step: (optional, int) the step value

        Examples:

        ```python
        >>> experiment.log_curve("my curve", x=[1, 2, 3, 4, 5],
                                             y=[10, 20, 30, 40, 50])
        >>> experiment.log_curve("my curve", [1, 2, 3, 4, 5],
                                             [10, 20, 30, 40, 50])
        ```
        """
        if self.alive:
            data = {"x": list(x), "y": list(y), "name": name}

            try:
                verify_data_structure("curve", data)
            except Exception:
                LOGGER.error("invalid 'curve' data; ignored", exc_info=True)
                return

            return self._log_asset_data(
                data, file_name=name, overwrite=overwrite, asset_type="curve", step=step
            )

    def log_asset_data(
        self,
        data,
        name=None,
        overwrite=False,
        step=None,
        metadata=None,
        file_name=None,
        epoch=None,
    ):
        """
        Logs the data given (str, binary, or JSON).

        Args:
            data: data to be saved as asset
            name: String, optional. A custom file name to be displayed
               If not provided the filename from the temporary saved file will be used.
            overwrite: Boolean, optional. Default False. If True will overwrite all existing
               assets with the same name.
            step: Optional. Used to associate the asset to a specific step.
            epoch: Optional. Used to associate the asset to a specific epoch.
            metadata: Optional. Some additional data to attach to the the asset data.
                Must be a JSON-encodable dict.

        See also: `APIExperiment.get_experiment_asset(return_type="json")`
        """
        if file_name is not None:
            LOGGER.warning(
                "log_asset_data(..., file_name=...) is deprecated; use log_asset_data(..., name=...)"
            )
            name = file_name

        if name is None:
            name = "data"

        return self._log_asset_data(
            data,
            file_name=name,
            overwrite=overwrite,
            asset_type="asset",
            step=step,
            epoch=epoch,
            metadata=metadata,
        )

    def _log_asset_data(
        self,
        data,
        file_name=None,
        overwrite=False,
        asset_type="asset",
        step=None,
        require_step=False,
        metadata=None,
        grouping_name=None,
        epoch=None,
    ):
        # type: (Any, str, bool, str, Optional[int], bool, Any, Optional[str], Optional[int]) -> Optional[Dict[str, str]]
        if not self.alive:
            return None

        self.set_step(step)
        self.set_epoch(epoch)

        if require_step:
            if self.curr_step is None:
                err_msg = (
                    "Step is mandatory.\n It can either be passed on "
                    "most log methods, set manually with set_step method or "
                    "set automatically through auto-logging integrations"
                )
                raise TypeError(err_msg)

        asset_id = generate_guid()
        url_params = {
            "assetId": asset_id,
            "context": self.context,
            "fileName": file_name,
            "overwrite": overwrite,
            "runId": self.run_id,
            "step": self._get_asset_upload_step(),
            "epoch": self._get_asset_upload_epoch(),
        }

        # If the asset type is more specific, include the
        # asset type as "type" in query parameters:
        if asset_type != "asset":
            url_params["type"] = asset_type

        processor = AssetDataUploadProcessor(
            data,
            asset_type,
            url_params,
            metadata,
            self.asset_upload_limit,
            copy_to_tmp=False,
            error_message_identifier=None,
            tmp_dir=self.tmpdir,
            critical=False,
        )
        upload_message = processor.process()

        if not upload_message:
            return None

        self._enqueue_message(upload_message)

        self._summary.increment_section("uploads", asset_type)
        return self._get_uploaded_asset_url(asset_id)

    def log_asset_folder(self, folder, step=None, log_file_name=None, recursive=False):
        # type: (str, Optional[int], Optional[bool], bool) -> Union[None, List[Tuple[str, Dict[str, str]]]]
        """
        Logs all the files located in the given folder as assets.

        Args:
            folder: String - the path to the folder you want to log.
            step: Optional. Used to associate the asset to a specific step.
            log_file_name: Optional. if True, log the file path with each file.
            recursive: Optional. if True, recurse folder and save file names.

        If log_file_name is set to True, each file in the given folder will be
        logged with the following name schema:
        `FOLDER_NAME/RELPATH_INSIDE_FOLDER`. Where `FOLDER_NAME` is the basename
        of the given folder and `RELPATH_INSIDE_FOLDER` is the file path
        relative to the folder itself.

        """

        # Current default is False, we want to move it to True in a future release
        if log_file_name is None:
            LOGGER.warning(
                "The default value for the log_file_name parameter will change from False to True in a future version. "
                "Explicitly pass log_file_name=True or log_file_name=False to disable this warning"
            )
            log_file_name = False

        return self._log_asset_folder(
            folder, step=step, log_file_name=log_file_name, recursive=recursive
        )

    def _log_asset_folder(
        self,
        folder,
        step=None,
        log_file_name=False,
        recursive=False,
        asset_type="asset",
        metadata=None,
        grouping_name=None,
        folder_name=None,
        extension_filter=None,
        prepend_folder_name=True,
    ):
        # type: (str, Optional[int], bool, bool, str, Any, Optional[str], Optional[str], Optional[List[str]], bool) -> Optional[List[Tuple[str, Dict[str, str]]]]
        self.set_step(step)

        urls = []

        if not os.path.isdir(folder):
            LOGGER.error(LOG_ASSET_FOLDER_ERROR, folder, exc_info=True)
            return None

        folder_abs_path = os.path.abspath(folder)
        if folder_name is None:
            folder_name = os.path.basename(folder)

        try:
            for file_name, file_path in log_asset_folder(
                folder_abs_path, recursive, extension_filter
            ):
                # The file path should be absolute as we are passing the folder
                # path as an absolute path
                if log_file_name:
                    if prepend_folder_name is True:
                        asset_file_name = os.path.join(
                            folder_name, os.path.relpath(file_path, folder_abs_path)
                        )
                    else:
                        asset_file_name = os.path.relpath(file_path, folder_abs_path)

                    asset_url = self._log_asset(
                        file_data=file_path,
                        file_name=asset_file_name,
                        asset_type=asset_type,
                        metadata=metadata,
                        grouping_name=grouping_name,
                    )
                else:
                    asset_url = self._log_asset(
                        file_data=file_path,
                        asset_type=asset_type,
                        metadata=metadata,
                        grouping_name=grouping_name,
                    )

                # Ignore files that has failed to be logged
                if asset_url:
                    urls.append((file_name, asset_url))
        except Exception:
            # raise
            LOGGER.error(LOG_ASSET_FOLDER_ERROR, folder, exc_info=True)
            return None

        if not urls:
            LOGGER.warning(LOG_ASSET_FOLDER_EMPTY, folder)
            return None

        return urls

    def log_asset(
        self,
        file_data,
        file_name=None,
        overwrite=False,
        copy_to_tmp=True,  # if file_data is a file pointer
        step=None,
        metadata=None,
    ):
        # type: (Any, Optional[str], bool, bool, Optional[int], Any) -> Optional[Dict[str, str]]
        """
        Logs the Asset determined by file_data.

        Args:
            file_data: String or File-like - either the file path of the file you want
                to log, or a file-like asset.
            file_name: String - Optional. A custom file name to be displayed. If not
                provided the filename from the `file_data` argument will be used.
            overwrite: if True will overwrite all existing assets with the same name.
            copy_to_tmp: If `file_data` is a file-like object, then this flag determines
                if the file is first copied to a temporary file before upload. If
                `copy_to_tmp` is False, then it is sent directly to the cloud.
            step: Optional. Used to associate the asset to a specific step.
            metadata: Optional. Some additional data to attach to the the audio asset. Must be a
                JSON-encodable dict.

        Examples:
        ```python
        >>> experiment.log_asset("model1.h5")

        >>> fp = open("model2.h5", "rb")
        >>> experiment.log_asset(fp,
        ...                      file_name="model2.h5")
        >>> fp.close()

        >>> fp = open("model3.h5", "rb")
        >>> experiment.log_asset(fp,
        ...                      file_name="model3.h5",
        ...                      copy_to_tmp=False)
        >>> fp.close()
        ```
        """
        return self._log_asset(
            file_data,
            file_name=file_name,
            overwrite=overwrite,
            copy_to_tmp=copy_to_tmp,
            asset_type="asset",
            step=step,
            metadata=metadata,
        )

    def _log_asset(
        self,
        file_data,  # type: Any
        file_name=None,  # type: Optional[str]
        overwrite=False,  # type: bool
        copy_to_tmp=True,  # type: bool
        asset_type="asset",  # type: str
        step=None,  # type: Optional[int]
        require_step=False,  # type: bool
        grouping_name=None,  # type: Optional[str]
        metadata=None,  # type: Any
        artifact_version_id=None,  # type: Optional[str]
        critical=False,  # type: bool
        asset_id=None,  # type: Optional[str]
        return_url=True,  # type: bool
        on_asset_upload=None,  # type: Callable
        on_failed_asset_upload=None,  # type: Callable
    ):
        # type: (...) -> Optional[Dict[str, str]]

        # Keep to speed up the case where Experiment is not alive
        if not self.alive:
            return None

        if file_data is None:
            raise TypeError("file_data cannot be None")

        self.set_step(step)

        dispatched = dispatch_user_file_upload(file_data)

        if isinstance(dispatched, UserTextFileUpload):
            LOGGER.error(UPLOAD_FILE_OS_ERROR, dispatched.user_input)
            return None
        elif isinstance(dispatched, ObjectToConvertFileUpload):
            raise TypeError("Unsupported upload input %r" % type(dispatched.user_input))
        elif isinstance(dispatched, FileUpload):
            preprocessed = preprocess_asset_file(
                dispatched=dispatched,
                upload_type=asset_type,
                file_name=file_name,
                metadata=metadata,
                overwrite=overwrite,
                asset_id=asset_id,
                copy_to_tmp=copy_to_tmp,
                grouping_name=grouping_name,
                step=self._get_asset_upload_step(),
            )
        else:
            preprocessed = preprocess_asset_memory_file(
                dispatched=dispatched,
                upload_type=asset_type,
                file_name=file_name,
                metadata=metadata,
                overwrite=overwrite,
                asset_id=asset_id,
                copy_to_tmp=copy_to_tmp,
                grouping_name=grouping_name,
                step=self._get_asset_upload_step(),
            )

        try:
            return self._log_preprocessed_asset(
                preprocessed,
                artifact_version_id=artifact_version_id,
                critical=critical,
                return_url=return_url,
                require_step=require_step,
                on_asset_upload=on_asset_upload,
                on_failed_asset_upload=on_failed_asset_upload,
            )
        except AssetIsTooBig as e:
            LOGGER.error(UPLOAD_ASSET_TOO_BIG, e.file_path, e.file_size, e.max_size)
            return None

    def _log_preprocessed_asset(
        self,
        preprocessed_asset,  # type: Union[PreprocessedFileAsset, PreprocessedMemoryFileAsset]
        artifact_version_id=None,  # type: Optional[str]
        critical=False,  # type: bool
        return_url=True,  # type: bool
        require_step=False,  # type: bool
        on_asset_upload=None,
        on_failed_asset_upload=None,
    ):
        # type: (...) -> Optional[Dict[str, str]]
        if not self.alive:
            return None

        self.set_step(preprocessed_asset.step)

        if require_step:
            if self.curr_step is None:
                if on_failed_asset_upload is not None:
                    try:
                        on_failed_asset_upload((None, None, None))
                    except Exception:
                        LOGGER.warning(
                            "Failed to call on_failed_asset_upload", exc_info=True
                        )
                err_msg = (
                    "Step is mandatory.\n It can either be passed on "
                    "most log methods, set manually with set_step method or "
                    "set automatically through auto-logging integrations"
                )
                raise TypeError(err_msg)

        if preprocessed_asset.size > self.asset_upload_limit:
            error_message_identifier = preprocessed_asset.local_path_or_data

            raise AssetIsTooBig(
                error_message_identifier,
                preprocessed_asset.size,
                self.asset_upload_limit,
            )

        if preprocessed_asset.copy_to_tmp is True:
            if isinstance(preprocessed_asset, PreprocessedFileAsset):
                new_upload_filepath = handle_in_memory_file_upload(
                    self.tmpdir, preprocessed_asset.local_path_or_data
                )

                # If we failed to copy the file, abort
                if not new_upload_filepath:
                    return None

                preprocessed_asset = preprocessed_asset.copy(
                    new_upload_filepath, new_copy_to_tmp=False
                )
            else:
                new_upload_tmp_file = write_file_like_to_tmp_file(
                    preprocessed_asset.local_path_or_data, self.tmpdir
                )

                preprocessed_asset = preprocessed_asset.to_preprocessed_file_asset(
                    new_upload_tmp_file, new_copy_to_tmp=False
                )

                # # TODO it would be easier to use the same field name for a file or a figure upload
                # if "fileName" in self.url_params and self.url_params["fileName"] is None:
                #     self.url_params["fileName"] = os.path.basename(file_path)

                # if "figName" in self.url_params and self.url_params["figName"] is None:
                #     self.url_params["figName"] = os.path.basename(file_path)

        # Clean only temporary files
        if isinstance(preprocessed_asset.local_path_or_data, TemporaryFilePath):
            clean = True
        else:
            clean = False

        upload_message = preprocessed_asset.to_message(
            critical,
            on_asset_upload,
            on_failed_asset_upload,
            clean=clean,
            experiment_url_params=self._get_asset_url_params(artifact_version_id),
        )

        if not upload_message:
            if on_failed_asset_upload is not None:
                try:
                    on_failed_asset_upload((None, None, None))
                except Exception:
                    LOGGER.warning(
                        "Failed to call on_failed_asset_upload", exc_info=True
                    )
            return None

        self._enqueue_message(upload_message)

        if artifact_version_id is None:
            self._summary.increment_section(
                "uploads", preprocessed_asset.upload_type, size=preprocessed_asset.size
            )
        else:
            self._summary.increment_section(
                "uploads", "artifact assets", size=preprocessed_asset.size
            )

        if return_url:
            return self._get_uploaded_asset_url(preprocessed_asset.asset_id)
        else:
            return None

    def log_remote_asset(
        self,
        uri,  # type: Any
        remote_file_name=None,  # type: Any
        overwrite=False,
        asset_type="asset",
        step=None,
        metadata=None,
    ):
        # type: (...) -> Optional[Dict[str, str]]
        """
        Logs a Remote Asset identified by an URI. A Remote Asset is an asset but its content is not
        uploaded and stored on Comet. Rather a link for its location is stored, so you can identify
        and distinguish between two experiment using different version of a dataset stored somewhere
        else.

        Args:
            uri: String - the remote asset location, there is no imposed format, and it could be a
                private link.
            remote_file_name: String, Optional. The "name" of the remote asset, could be a dataset
                name, a model file name.
            overwrite: if True will overwrite all existing assets with the same name.
            step: Optional. Used to associate the asset to a specific step.
            metadata: Some additional data to attach to the remote asset.
                Must be a JSON-encodable dict.

        Examples:
        ```python
        >>> experiment.log_remote_asset("s3://bucket/folder/file")

        >>> experiment.log_remote_asset("dataset:701bd06b43b7423296fb626027d02198")
        ```
        """
        return self._log_remote_asset(
            uri, remote_file_name, overwrite, asset_type, step, metadata
        )

    def _log_remote_asset(
        self,
        uri,  # type: Any
        remote_file_name=None,  # type: Any
        overwrite=False,
        asset_type="asset",
        step=None,
        metadata=None,
        artifact_version_id=None,  # type: Optional[str]
        critical=False,  # type: bool
        return_url=True,  # type: bool
        asset_id=None,  # type: Optional[str]
        on_asset_upload=None,
        on_failed_asset_upload=None,
    ):
        # type: (...) -> Optional[Dict[str, str]]
        self.set_step(step)

        preprocessed = preprocess_remote_asset(
            uri,
            remote_file_name,
            overwrite,
            asset_type,
            asset_id=asset_id,
            metadata=metadata,
            step=self._get_asset_upload_step(),
        )

        return self._log_preprocessed_remote_asset(
            preprocessed,
            artifact_version_id=artifact_version_id,
            critical=critical,
            on_asset_upload=on_asset_upload,
            on_failed_asset_upload=on_failed_asset_upload,
            return_url=return_url,
        )

    def _get_asset_url_params(self, artifact_version_id):
        exp_url_params = {
            "context": self.context,
            "runId": self.run_id,
            "step": self._get_asset_upload_step(),
        }

        if artifact_version_id:
            exp_url_params.update({"artifactVersionId": artifact_version_id})

        return exp_url_params

    def _log_preprocessed_remote_asset(
        self,
        preprocessed_remote_asset,  # type: PreprocessedRemoteAsset
        artifact_version_id=None,  # type: Optional[str]
        critical=False,  # type: bool
        return_url=True,  # type: bool
        on_asset_upload=None,
        on_failed_asset_upload=None,
    ):
        # type: (...) -> Optional[Dict[str, str]]
        if not self.alive:
            return None

        self.set_step(preprocessed_remote_asset.step)

        upload_message = preprocessed_remote_asset.to_message(
            critical,
            on_asset_upload,
            on_failed_asset_upload,
            self._get_asset_url_params(artifact_version_id),
        )

        if not upload_message:
            return None

        self._enqueue_message(upload_message)

        if artifact_version_id is None:
            self._summary.increment_section(
                "uploads",
                upload_message.upload_type,
                size=preprocessed_remote_asset.size,
            )
        else:
            self._summary.increment_section(
                "uploads", "artifact assets", size=preprocessed_remote_asset.size
            )

        if return_url:
            return self._get_uploaded_asset_url(preprocessed_remote_asset.asset_id)
        else:
            return None

    def log_audio(
        self,
        audio_data,
        sample_rate=None,
        file_name=None,
        metadata=None,
        overwrite=False,
        copy_to_tmp=True,
        step=None,
    ):
        # type: (Any, Optional[int], str, Any, bool, bool, Optional[int]) -> Optional[Dict[str, Optional[str]]]
        """
        Logs the audio Asset determined by audio data.

        Args:
            audio_data: String or a numpy array - either the file path of the file you want
                to log, or a numpy array given to `scipy.io.wavfile.write` for wav conversion.
            sample_rate: Integer - Optional. The sampling rate given to
                `scipy.io.wavfile.write` for creating the wav file.
            file_name: String - Optional. A custom file name to be displayed.
                If not provided, the filename from the `audio_data` argument
                will be used.
            metadata: Some additional data to attach to the audio asset.
                Must be a JSON-encodable dict.
            overwrite: if True will overwrite all existing assets with the same name.
            copy_to_tmp: If `audio_data` is a numpy array, then this flag
                determines if the WAV file is first copied to a temporary file
                before upload. If `copy_to_tmp` is False, then it is sent
                directly to the cloud.
            step: Optional. Used to associate the audio asset to a specific step.
        """
        if not self.alive:
            return None

        if audio_data is None:
            raise TypeError("audio_data cannot be None")

        self.set_step(step)

        asset_id = generate_guid()
        url_params = {
            "step": self._get_asset_upload_step(),
            "context": self.context,
            "fileName": file_name,
            "runId": self.run_id,
            "overwrite": overwrite,
            "assetId": asset_id,
            "type": "audio",
        }

        audio_data = fix_special_floats(audio_data)

        processor = AudioUploadProcessor(
            audio_data,
            sample_rate,
            overwrite,
            self.asset_upload_limit,
            url_params,
            metadata=metadata,
            copy_to_tmp=copy_to_tmp,
            error_message_identifier=None,
            tmp_dir=self.tmpdir,
            critical=False,
        )
        upload_message = processor.process()

        if not upload_message:
            return None

        self._enqueue_message(upload_message)
        self._summary.increment_section("uploads", "audio")
        return self._get_uploaded_audio_url(asset_id)

    def create_confusion_matrix(
        self,
        y_true=None,
        y_predicted=None,
        labels=None,
        matrix=None,
        title="Confusion Matrix",
        row_label="Actual Category",
        column_label="Predicted Category",
        max_examples_per_cell=25,
        max_categories=25,
        winner_function=None,
        index_to_example_function=None,
        cache=True,
        selected=None,
        images=None,
        **kwargs  # keyword args for index_to_example_function
    ):
        """
        Create a confusion matrix for use over multiple epochs.

        Args:
            y_true: (optional) list of vectors representing the targets, or a list
                of integers representing the correct label. If
                not provided, then matrix may be provided.
            y_predicted: (optional) list of vectors representing predicted
                values, or a list of integers representing the output. If
                not provided, then matrix may be provided.
            images: (optional) a list of data that can be passed to
                Experiment.log_image()
            labels: (optional) a list of strings that name of the
                columns and rows, in order. By default, it will be
                "0" through the number of categories (e.g., rows/columns).
            matrix: (optional) the confusion matrix (list of lists).
                Must be square, if given. If not given, then it is
                possible to provide y_true and y_predicted.
            title: (optional) a custom name to be displayed. By
                default, it is "Confusion Matrix".
            row_label: (optional) label for rows. By default, it is
                "Actual Category".
            column_label: (optional) label for columns. By default,
                it is "Predicted Category".
            max_example_per_cell: (optional) maximum number of
                examples per cell. By default, it is 25.
            max_categories: (optional) max number of columns and rows to
                use. By default, it is 25.
            winner_function: (optional) a function that takes in an
                entire list of rows of patterns, and returns
                the winning category for each row. By default, it is argmax.
            index_to_example_function: (optional) a function
                that takes an index and returns either
                a number, a string, a URL, or a {"sample": str,
                "assetId": str} dictionary. See below for more info.
                By default, the function returns a number representing
                the index of the example.
            cache: (optional) should the results of index_to_example_function
                be cached and reused? By default, cache is True.
            selected: (optional) None, or list of selected category
                indices. These are the rows/columns that will be shown. By
                default, select is None. If the number of categories is
                greater than max_categories, and selected is not provided,
                then selected will be computed automatically by selecting
                the most confused categories.
            kwargs: (optional) any extra keywords and their values will
                be passed onto the index_to_example_function.
            file_name: (optional) logging option, by default is
                "confusion-matrix.json",
            overwrite: (optional) logging option, by default is False
            step: (optional) logging option, by default is None
            epoch: (optional) logging option, by default is None

        See the executable Jupyter Notebook tutorial at
        [Comet Confusion Matrix](https://www.comet.ml/docs/python-sdk/Comet-Confusion-Matrix/).

        Note:
            Uses winner_function to compute winning categories for
            y_true and y_predicted, if they are vectors.

        Examples:

        ```python
        >>> experiment = Experiment()

        # If you have a y_true and y_predicted:
        >>> y_predicted = model.predict(x_test)
        >>> experiment.log_confusion_matrix(y_true, y_predicted)

        # Or, if you have already computed the matrix:
        >>> experiment.log_confusion_matrix(labels=["one", "two", "three"],
                                            matrix=[[10, 0, 0],
                                                    [ 0, 9, 1],
                                                    [ 1, 1, 8]])

        # Or, if you have the categories for y_true or y_predicted
        # you can just pass those in:
        >>> experiment.log_confusion_matrix([0, 1, 2, 3],
                                            [2, 2, 2, 2]) # guesses 2 for all

        # However, if you want to reuse examples from previous runs,
        # you can reuse a Confusion Matrix instance.

        >>> cm = experiment.create_confusion_matrix()
        >>> y_predicted = model.predict(x_test)
        >>> cm.compute_matrix(y_true, y_predicted)
        >>> experiment.log_confusion_matrix(matrix=cm)

        # Log again, using previously cached values:
        >>> y_predicted = model.predict(x_test)
        >>> cm.compute_matrix(y_true, y_predicted)
        >>> experiment.log_confusion_matrix(matrix=cm)
        ```

        For more details and example uses, please see:
        https://www.comet.ml/docs/python-sdk/Comet-Confusion-Matrix/

        Also, for more low-level information, see comet_ml.utils.ConfusionMatrix
        """
        confusion_matrix = ConfusionMatrix(
            y_true=y_true,
            y_predicted=y_predicted,
            labels=labels,
            matrix=matrix,
            title=title,
            row_label=row_label,
            column_label=column_label,
            max_examples_per_cell=max_examples_per_cell,
            max_categories=max_categories,
            winner_function=winner_function,
            index_to_example_function=index_to_example_function,
            cache=cache,
            selected=selected,
            images=images,
            experiment=self,
            **kwargs
        )
        return confusion_matrix

    def log_confusion_matrix(
        self,
        y_true=None,
        y_predicted=None,
        matrix=None,
        labels=None,
        title="Confusion Matrix",
        row_label="Actual Category",
        column_label="Predicted Category",
        max_examples_per_cell=25,
        max_categories=25,
        winner_function=None,
        index_to_example_function=None,
        cache=True,
        # Logging options:
        file_name="confusion-matrix.json",
        overwrite=False,
        step=None,
        epoch=None,
        images=None,
        selected=None,
        **kwargs
    ):
        """
        Logs a confusion matrix.

        Args:
            y_true: (optional) list of vectors representing the targets, or a list
                of integers representing the correct label. If
                not provided, then matrix may be provided.
            y_predicted: (optional) list of vectors representing predicted
                values, or a list of integers representing the output. If
                not provided, then matrix may be provided.
            images: (optional) a list of data that can be passed to
                Experiment.log_image().
            labels: (optional) a list of strings that name of the
                columns and rows, in order. By default, it will be
                "0" through the number of categories (e.g., rows/columns).
            matrix: (optional) the confusion matrix (list of lists).
                Must be square, if given. If not given, then it is
                possible to provide y_true and y_predicted.
            title: (optional) a custom name to be displayed. By
                default, it is "Confusion Matrix".
            row_label: (optional) label for rows. By default, it is
                "Actual Category".
            column_label: (optional) label for columns. By default,
                it is "Predicted Category".
            max_examples_per_cell: (optional) maximum number of
                examples per cell. By default, it is 25.
            max_categories: (optional) max number of columns and rows to
                use. By default, it is 25.
            winner_function: (optional) a function that takes in an
                entire list of rows of patterns, and returns
                the winning category for each row. By default, it is argmax.
            index_to_example_function: (optional) a function
                that takes an index and returns either
                a number, a string, a URL, or a {"sample": str,
                "assetId": str} dictionary. See below for more info.
                By default, the function returns a number representing
                the index of the example.
            cache: (optional) should the results of index_to_example_function
                be cached and reused? By default, cache is True.
            selected: (optional) None, or list of selected category
                indices. These are the rows/columns that will be shown. By
                default, select is None. If the number of categories is
                greater than max_categories, and selected is not provided,
                then selected will be computed automatically by selecting
                the most confused categories.
            kwargs: (optional) any extra keywords and their values will
                be passed onto the index_to_example_function.
            file_name: (optional) logging option, by default is
                "confusion-matrix.json",
            overwrite: (optional) logging option, by default is False
            step: (optional) logging option, by default is None
            epoch: (optional) logging option, by default is None

        See the executable Jupyter Notebook tutorial at
        [Comet Confusion Matrix](https://www.comet.ml/docs/python-sdk/Comet-Confusion-Matrix/).

        Note:
            Uses winner_function to compute winning categories for
            y_true and y_predicted, if they are vectors.

        Examples:

        ```python
        >>> experiment = Experiment()

        # If you have a y_true and y_predicted:
        >>> y_predicted = model.predict(x_test)
        >>> experiment.log_confusion_matrix(y_true, y_predicted)

        # Or, if you have already computed the matrix:
        >>> experiment.log_confusion_matrix(labels=["one", "two", "three"],
                                            matrix=[[10, 0, 0],
                                                    [ 0, 9, 1],
                                                    [ 1, 1, 8]])

        # Or, if you have the categories for y_true or y_predicted
        # you can just pass those in:
        >>> experiment.log_confusion_matrix([0, 1, 2, 3],
                                            [2, 2, 2, 2]) # guesses 2 for all

        # However, if you want to reuse examples from previous runs,
        # you can reuse a ConfusionMatrix instance.

        >>> from comet_ml import ConfusionMatrix

        >>> cm = ConfusionMatrix()
        >>> y_predicted = model.predict(x_test)
        >>> cm.compute_matrix(y_true, y_predicted)
        >>> experiment.log_confusion_matrix(matrix=cm)

        # Log again, using previously cached values:
        >>> y_predicted = model.predict(x_test)
        >>> cm.compute_matrix(y_true, y_predicted)
        >>> experiment.log_confusion_matrix(matrix=cm)
        ```

        For more details and example uses, please see:
        https://www.comet.ml/docs/python-sdk/Comet-Confusion-Matrix/

        Also, for more low-level information, see comet_ml.utils.ConfusionMatrix
        """
        if isinstance(matrix, ConfusionMatrix):
            confusion_matrix = matrix
            confusion_matrix.need_init()
        else:
            try:
                confusion_matrix = self.create_confusion_matrix(
                    y_true=y_true,
                    y_predicted=y_predicted,
                    matrix=matrix,
                    labels=labels,
                    title=title,
                    row_label=row_label,
                    column_label=column_label,
                    max_examples_per_cell=max_examples_per_cell,
                    max_categories=max_categories,
                    winner_function=winner_function,
                    index_to_example_function=index_to_example_function,
                    cache=cache,
                    images=images,
                    selected=selected,
                    **kwargs
                )
            except Exception as exc:
                LOGGER.error(CONFUSION_MATRIX_ERROR, exc, exc_info=True)
                self._report_experiment_error(CONFUSION_MATRIX_ERROR)
                return

        try:
            confusion_matrix_json = confusion_matrix.to_json()
            if confusion_matrix_json["matrix"] is None:
                LOGGER.error("Attempt to log empty confusion matrix; ignoring")
                return
            return self._log_asset_data(
                confusion_matrix_json,
                file_name=file_name,
                overwrite=overwrite,
                asset_type="confusion-matrix",
                step=step,
                epoch=epoch,
            )
        except Exception:
            LOGGER.error(CONFUSION_MATRIX_GENERAL_ERROR, exc_info=True)
            self._report_experiment_error(CONFUSION_MATRIX_GENERAL_ERROR)
            return

    def log_histogram_3d(
        self, values, name=None, step=None, epoch=None, metadata=None, **kwargs
    ):
        """
        Logs a histogram of values for a 3D chart as an asset for this
        experiment. Calling this method multiple times with the same
        name and incremented steps will add additional histograms to
        the 3D chart on Comet.ml.

        Args:
            values: a list, tuple, array (any shape) to summarize, or a
                Histogram object
            name: str (optional), name of summary
            step: Optional. Used as the Z axis when plotting on Comet.ml.
            epoch: Optional. Used as the Z axis when plotting on Comet.ml.
            metadata: Optional: Used for items like prefix for histogram
                name.
            kwargs: Optional. Additional keyword arguments for histogram.

        Note:
            This method requires that step is either given here, or has
            been set elsewhere. For example, if you are using an auto-
            logger that sets step then you don't need to set it here.
        """
        if isinstance(values, Histogram):
            histogram = values
        else:
            histogram = Histogram(**kwargs)
            histogram.add(values)
        if name is None:
            name = "histogram_3d.json"

        if histogram.is_empty():
            LOGGER.warning("ignoring empty histogram")
            return

        try:
            histogram_json = histogram.to_json()
        except Exception:
            LOGGER.error("invalid histogram data; ignoring", exc_info=True)
            return

        return self._log_asset_data(
            histogram_json,
            file_name=name,
            overwrite=False,
            asset_type="histogram3d",
            step=step,
            epoch=epoch,
            require_step=True,
            metadata=metadata,
        )

    def log_image(
        self,
        image_data,  # type: Any
        name=None,  # type: Optional[str]
        overwrite=False,  # type: bool
        image_format="png",  # type: str
        image_scale=1.0,  # type: float
        image_shape=None,  # type: Tuple[int, int]
        image_colormap=None,  # type: str
        image_minmax=None,  # type: Tuple[int, int]
        image_channels="last",  # type: str
        copy_to_tmp=True,  # type: bool
        step=None,  # type: Optional[int]
    ):
        # type: (...) -> Optional[Dict[str, str]]
        """
        Logs the image. Images are displayed on the Graphics tab on
        Comet.ml.

        Args:
            image_data: Required. image_data is one of the following:
                - a path (string) to an image
                - a file-like object containing an image
                - a numpy matrix
                - a TensorFlow tensor
                - a PyTorch tensor
                - a list or tuple of values
                - a PIL Image
            name: String - Optional. A custom name to be displayed on the dashboard.
                If not provided the filename from the `image_data` argument will be
                used if it is a path.
            overwrite: Optional. Boolean - If another image with the same name
                exists, it will be overwritten if overwrite is set to True.
            image_format: Optional. String. Default: 'png'. If the image_data is
                actually something that can be turned into an image, this is the
                format used. Typical values include 'png' and 'jpg'.
            image_scale: Optional. Float. Default: 1.0. If the image_data is actually
                something that can be turned into an image, this will be the new
                scale of the image.
            image_shape: Optional. Tuple. Default: None. If the image_data is actually
                something that can be turned into an image, this is the new shape
                of the array. Dimensions are (width, height) or (width, height, colors)
                where `colors` is 3 (RGB) or 1 (grayscale).
            image_colormap: Optional. String. If the image_data is actually something
                that can be turned into an image, this is the colormap used to
                colorize the matrix.
            image_minmax: Optional. (Number, Number). If the image_data is actually
                something that can be turned into an image, this is the (min, max)
                used to scale the values. Otherwise, the image is autoscaled between
                (array.min, array.max).
            image_channels: Optional. Default 'last'. If the image_data is
                actually something that can be turned into an image, this is the
                setting that indicates where the color information is in the format
                of the 2D data. 'last' indicates that the data is in (rows, columns,
                channels) where 'first' indicates (channels, rows, columns).
            copy_to_tmp: If `image_data` is not a file path, then this flag determines
                if the image is first copied to a temporary file before upload. If
                `copy_to_tmp` is False, then it is sent directly to the cloud.
            step: Optional. Used to associate the image asset to a specific step.

        """
        if not self.alive:
            return None

        self.set_step(step)

        if image_data is None:
            raise TypeError("image_data cannot be None")

        # Prepare parameters
        figure_number = self.figure_counter

        image_id = generate_guid()
        url_params = {
            "step": self._get_asset_upload_step(),
            "context": self.context,
            "runId": self.run_id,
            "figName": convert_pathlib_path(name),
            "figCounter": figure_number,
            "overwrite": overwrite,
            "imageId": image_id,
        }

        processor = ImageUploadProcessor(
            image_data,
            convert_pathlib_path(name),
            overwrite,
            image_format,
            image_scale,
            image_shape,
            image_colormap,
            image_minmax,
            image_channels,
            self.upload_limit,
            url_params,
            metadata=None,
            copy_to_tmp=copy_to_tmp,
            error_message_identifier=None,
            tmp_dir=self.tmpdir,
            critical=False,
        )
        upload_message = processor.process()

        if upload_message is None:
            self._report_experiment_error(UNEXPECTED_ERROR_WHEN_LOGGING_IMAGE_DATA)
            return None

        self._enqueue_message(upload_message)
        self._summary.increment_section("uploads", "images")
        self.figure_counter += 1

        return self._get_uploaded_image_url(image_id)

    def log_current_epoch(self, value):
        """
        Deprecated.
        """
        if self.alive:
            message = MetricMessage(context=self.context)
            message.set_metric("curr_epoch", value)
            self._enqueue_message(message)
            self._summary.set("metrics", self._fullname("curr_epoch"), value)

    def log_parameters(self, parameters, prefix=None, step=None):
        """
        Logs a dictionary (or dictionary-like object) of multiple parameters.
        See also [log_parameter](#experimentlog_parameter).

        e.g:
        ```python
        experiment = Experiment(api_key="MY_API_KEY")
        params = {
            "batch_size":64,
            "layer1":"LSTM(128)",
            "layer2":"LSTM(128)",
            "MAX_LEN":200
        }

        experiment.log_parameters(params)
        ```

        If you call this method multiple times with the same
        keys your values would be overwritten.  For example:

        ```python
        experiment.log_parameters({"key1":"value1","key2":"value2"})
        ```
        On Comet.ml you will see the pairs of key1 and key2.

        If you then call:
        ```python
        experiment.log_parameters({"key1":"other value"})l
        ```
        On the UI you will see the pairs key1: other value, key2: value2

        If this method is called inside a context, like [`test`](#experimenttest),
        [`train`](#experimenttrain), [`validate`](#experimentvalidate) or
        [`context_manager`](#experimentcontext_manager), the parameters will be stored with the
        current context name as a prefix.
        """
        return self._log_parameters(parameters, prefix, step)

    def _log_parameters(self, parameters, prefix=None, step=None, framework=None):
        # Internal logging handler with option to ignore auto-logged keys
        if isinstance(parameters, Mapping):

            if len(parameters) == 0:
                self._log_once_at_level(
                    logging.WARNING, LOG_PARAMS_EMPTY_MAPPING, parameters
                )

                return None

            dic = parameters
        else:
            dic = convert_object_to_dictionary(parameters)

            if len(dic) == 0:
                self._log_once_at_level(
                    logging.WARNING, LOG_PARAMS_EMPTY_CONVERTED_MAPPING, parameters
                )

                return None

        self.set_step(step)

        for k in sorted(dic):
            delim = "_"  # default prefix_name delimiter
            delims = ["_", ":", "-", "+", ".", "/", "|"]
            if prefix is not None:
                if any(prefix.endswith(d) for d in delims):
                    # prefix already has a delim
                    delim = ""
                self._log_parameter(
                    "%s%s%s" % (prefix, delim, k),
                    dic[k],
                    framework=framework,
                )
            else:
                self._log_parameter(k, dic[k], framework=framework)

    def log_metrics(self, dic, prefix=None, step=None, epoch=None):
        """
        Logs a key,value dictionary of metrics.
        See also [`log_metric`](#experimentlog_metric)
        """
        return self._log_metrics(dic, prefix, step, epoch)

    def _log_metrics(self, dic, prefix=None, step=None, epoch=None, framework=None):
        # Internal logging handler with option to ignore auto-logged names
        self.set_step(step)
        self.set_epoch(epoch)

        if self.alive:
            for k in sorted(dic):
                if prefix is not None:
                    self._log_metric(
                        prefix + "_" + str(k),
                        dic[k],
                        framework=framework,
                    )
                else:
                    self._log_metric(k, dic[k], framework=framework)

    def log_dataset_info(self, name=None, version=None, path=None):
        """
        Used to log information about your dataset.

        Args:
            name: Optional string representing the name of the dataset.
            version: Optional string representing a version identifier.
            path: Optional string that represents the path to the dataset.
                Potential values could be a file system path, S3 path
                or Database query.

        At least one argument should be included. The logged values will
        show on the `Other` tab.
        """
        if name is None and version is None and path is None:
            LOGGER.warning(
                "log_dataset_info: name, version, and path can't all be None"
            )
            return
        info = ""
        if name is not None:
            info += str(name)
        if version is not None:
            if info:
                info += "-"
            info += str(version)
        if path is not None:
            if info:
                info += ", "
            info += str(path)
        self.log_other("dataset_info", info)

    def log_dataset_hash(self, data):
        """
        Used to log the hash of the provided object. This is a best-effort hash computation which is based on the md5
        hash of the underlying string representation of the object data. Developers are encouraged to implement their
        own hash computation that's tailored to their underlying data source. That could be reported as
        `experiment.log_parameter("dataset_hash", your_hash)`.

        data: Any object that when casted to string (e.g str(data)) returns a value that represents the underlying data.

        """
        try:
            import hashlib

            data_hash = hashlib.md5(str(data).encode("utf-8")).hexdigest()
            self._log_parameter("dataset_hash", data_hash[:12], framework="comet")
        except Exception:
            LOGGER.warning(LOG_DATASET_ERROR, exc_info=True)

    def log_table(self, filename, tabular_data=None, headers=False, **format_kwargs):
        # type: (str, Optional[Any], Optional[Any], Union[Sequence[str], bool]) -> Optional[Dict[str, str]]
        """
        Log tabular data, including data, csv files, tsv files, and Pandas dataframes.

        Args:
            filename: str (required), a filename ending in ".csv", or ".tsv" (for tablular
                data) or ".json", ".csv", ".md", or ".html" (for Pandas dataframe data).
            tabular_data: (optional) data that can be interpreted as 2D tabular data
                or a Pandas dataframe).
            headers: bool or list, if True, will add column headers automatically
                if tabular_data is given; if False, no headers will be added; if list
                then it will be used as headers. Only useful with tabular data (csv, or tsv).
            format_kwargs: (optional keyword arguments), when passed a Pandas dataframe
                these keyword arguments are used in the conversion to "json", "csv",
                "md", or "html". See Pandas Dataframe conversion methods (like `to_json()`)
                for more information.

        See also:

        * [pandas.DataFrame.to_json documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_json.html)
        * [pandas.DataFrame.to_csv documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html)
        * [pandas.DataFrame.to_html documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_html.html)
        * [pandas.DataFrame.to_markdown documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_markdown.html)

        Examples:

        ```python
        >>> experiment.log_table("vectors.tsv",
        ...                      [["one", "two", "three"],
        ...                       [1, 2, 3],
        ...                       [4, 5, 6]],
        ...
        >>> experiment.log_table("dataframe.json", pandas_dataframe)
        ```

        See also: `Experiment.log_panadas_profile()`
        """
        if not self.alive:
            return None

        if os.path.isfile(filename):
            if headers is not False:
                LOGGER.info(LOG_TABLE_FILENAME_AND_HEADERS)

            return self._log_asset(filename)

        # Filename is not a file
        if tabular_data is None:
            LOGGER.warning(LOG_TABLE_NONE_VALUES)
            return None

        # Tabular-data is not None
        if not isinstance(filename, str):
            raise ValueError("filename argument of log_table must be a string")

        converted = convert_log_table_input_to_io(
            filename=filename,
            tabular_data=tabular_data,
            headers=headers,
            format_kwargs=format_kwargs,
        )

        if not converted:
            return None

        fp, asset_type = converted
        return self._log_asset(fp, filename, asset_type=asset_type)

    def _log_embedding_groups(self):
        # type: () -> None
        """
        Log all of the embedding groups together in one
        template config file.

        Example:

        ```python
        >>> experiment.log_embedding(..., group="hidden-layer")
        >>> experiment.log_embedding(..., group="hidden-layer")

        >>> experiment._log_embedding_groups()
        ```
        """
        if not self.alive:
            return None

        groups = list(self._embedding_groups.keys())
        for group in groups:
            embedding_list = self._embedding_groups[group]
            self._log_embedding_list(
                embedding_list,
                "template-%s-configs.json" % safe_filename(group),
            )
            del self._embedding_groups[group]

    def _log_embedding_list(self, embeddings, template_filename=None):
        # type: (List[Embedding], Optional[str]) -> Any
        """
        Log a list of Embeddings.

        Args:
            embeddings: list of Embeddings
            template_filename: (optional) name of template JSON file

        Example:
        ```python
        >>> embeddings = [Embedding(...), Emedding(...), ...]
        >>> experiment._log_embedding_list(embeddings)
        ```

        See also: `Experiment.log_embedding()` and `comet_ml.Embedding`
        """
        if not self.alive:
            return None

        # Log the template:
        template = {"embeddings": [embed.to_json() for embed in embeddings]}
        if template_filename is None:
            template_filename = make_template_filename()

        return self._log_asset_data(
            template, template_filename, asset_type="embeddings"
        )

    def create_embedding_image(
        self,
        image_data,  # type: Any
        image_size,  # type: Optional[List[int]]
        image_preprocess_function=None,  # type: Optional[Callable]
        image_transparent_color=None,  # type: Optional[List[int]]
        image_background_color_function=None,  # type: Optional[Callable]
    ):
        # type: (...) -> Optional[Tuple[Any, str]]
        """
        Create an embedding image (a sprite sheet). Returns the image
        and the url to the image.

        Args:
            image_data: list of arrays or Images
            image_size: the size of each image
            image_preprocess_function: (optional) if image_data is an
                array, apply this function to each element first
            image_transparent_color: a (red, green, blue) tuple
            image_background_color_function: a function that takes an
                index, and returns a (red, green, blue) color tuple

        Returns: image and url

        ```python
        >>> def label_to_color(index):
        ...     label = labels[index]
        ...     if label == 0:
        ...         return (255, 0, 0)
        ...     elif label == 1:
        ...         return (0, 255, 0)
        ...     elif label == 2:
        ...         return (0, 0, 255)
        ...     elif label == 3:
        ...         return (255, 255, 0)
        ...     elif label == 4:
        ...         return (0, 255, 255)
        ...     elif label == 5:
        ...         return (128, 128, 0)
        ...     elif label == 6:
        ...         return (0, 128, 128)
        ...     elif label == 7:
        ...         return (128, 0, 128)
        ...     elif label == 8:
        ...         return (255, 0, 255)
        ...     elif label == 9:
        ...         return (255, 255, 255)
        ...
        >>> image, image_url = experiment.create_embedding_image(inputs,
        ...     image_preprocess_function=lambda matrix: np.round(matrix/255,0) * 2,
        ...     image_transparent_color=(0, 0, 0),
        ...     image_size=(28, 28),
        ...     image_background_color_function=label_to_color)
        ...
        ```
        """
        if not self.alive:
            return None

        try:
            image = dataset_to_sprite_image(
                image_data,
                size=image_size,
                preprocess_function=image_preprocess_function,
                transparent_color=image_transparent_color,
                background_color_function=image_background_color_function,
            )
        except Exception:
            LOGGER.warning(
                "create_embedding_image: error creating images; continuing without images",
                exc_info=True,
            )
            self._report_experiment_error(UNEXPECTED_ERROR_WHEN_LOGGING_IMAGE_EMBEDDING)
            return None

        # We assume that the error resulting in an empty image is already logged
        if not image:
            return None

        sprite_url = None
        random_id = random.randint(0, 10000000)
        results = self.log_image(image, "embedding-image-%s.png" % random_id)
        if results is not None:
            sprite_url = results["web"]
        return image, sprite_url

    def _create_embedding(
        self,
        vectors,  # type: Any
        labels,  # type: Any
        image_data=None,  # type: Optional[Any]
        image_size=None,  # type: Optional[List[int]]
        image_preprocess_function=None,  # type: Optional[Callable]
        image_transparent_color=None,  # type: Optional[List[int]]
        image_background_color_function=None,  # type: Optional[Callable]
        title="Comet Embedding",  # type: Optional[str]
    ):
        # type: (...) -> Optional[Embedding]
        """
        Create a multidimensional dataset and metadata for viewing with
        Comet's Embedding Projector.

        Args:
            vectors: the tensors to visualize in 3D
            labels: labels for each tensor
            image_data: (optional) list of arrays or Images, or a URL
            image_size: (optional, required if image_data is given) the size of each image
            image_preprocess_function: (optional) if image_data is an
                array, apply this function to each element first
            image_transparent_color: a (red, green, blue) tuple
            image_background_color_function: a function that takes an
                index, and returns a (red, green, blue) color tuple
            title: (optional) name of tensor

        See also: `Experiment.log_embedding()`, `Experiment._log_embedding_list()`, and `comet_ml.Embedding`
        """

        # Log vector
        random_id = random.randint(0, 10000000)
        vector = self.log_table("vectors-%s.tsv" % random_id, vectors)

        if vector is None:
            LOGGER.error("create_embedding: empty vector; ignoring")
            return None

        try:
            vector_url = vector["web"]
            shape_vectors = shape(vectors)
            vector_shape = [
                shape_vectors[0],
                reduce((lambda x, y: x * y), shape_vectors[1:]),
            ]
        except Exception:
            LOGGER.error(
                "create_embedding: error getting vector shape; ignoring",
                exc_info=True,
            )
            self._report_experiment_error(
                UNEXPECTED_ERROR_WHEN_LOGGING_VECTOR_EMBEDDING
            )
            return None

        # Log metadata
        metadata = self.log_table("metadata-%s.tsv" % random_id, labels)
        if metadata is None:
            LOGGER.error("create_embedding: empty metadata; ignoring")
            return None

        # Log image data if any
        sprite_url = None
        if image_data is not None:
            if isinstance(image_data, str):
                sprite_url = image_data
            else:
                if image_size is None:
                    LOGGER.error(
                        "create_embedding: no image_size given; ignoring images"
                    )
                else:
                    results = self.create_embedding_image(
                        image_data,
                        image_size,
                        image_preprocess_function,
                        image_transparent_color,
                        image_background_color_function,
                    )
                    if results is not None and len(results) == 2:
                        sprite_image, sprite_url = results

        # Construct a data structure:
        embedding = Embedding(
            vector_url=vector_url,
            vector_shape=vector_shape,
            metadata_url=metadata["web"],
            sprite_url=sprite_url,
            image_size=image_size,
            title=title,
        )

        return embedding

    def log_embedding(
        self,
        vectors,
        labels,
        image_data=None,
        image_size=None,
        image_preprocess_function=None,
        image_transparent_color=None,
        image_background_color_function=None,
        title="Comet Embedding",
        template_filename=None,
        group=None,
    ):
        """
        Logging embedding is only supported for Online Experiment at the moment
        """
        raise NotImplementedError(
            "Logging embedding is only supported for Online Experiment at the moment"
        )

    def log_dataframe_profile(
        self,
        dataframe,
        name="dataframe",
        minimal=False,
        log_raw_dataframe=True,
        dataframe_format="json",
        **format_kwargs
    ):
        # type: (Any, Optional[str], bool, bool, str, Any) -> Optional[Dict[str, Optional[Dict[str, str]]]]
        """
        Log a pandas DataFrame profile as an asset. Optionally, can
        also log the dataframe.

        Args:
            * dataframe: the dataframe to profile and/or log
            * name (optional, default "dataframe"): the basename
                (without extension) of the dataframe assets
            * minimal (optional, default False): if True, create a
                minimal profile. Useful for large datasets.
            * log_raw_dataframe: (optional, default True), log the
                dataframe as an asset (same as calling `log_table()`)
            * dataframe_format: (optional, default "json"), the format
                for optionally logging the dataframe.
            * format_kwargs: (optional), keyword args for dataframe
                logging as an asset.

        Example:

        ```python
        >>> from comet_ml import Experiment
        >>> import pandas as pd
        >>> experiment = Experiment()
        >>> df = pd.read_csv("https://data.nasa.gov/api/views/gh4g-9sfh/rows.csv?accessType=DOWNLOAD",
        ...     parse_dates=['year'], encoding='UTF-8')
        >>> experiment.log_dataframe_profile(df)
        ```

        See also: `Experiment.log_table(pandas_dataframe)`
        """
        if not self.alive:
            return None

        if not check_is_pandas_dataframe(dataframe):
            # Check if pandas is the issue
            try:
                import pandas  # noqa
            except ImportError:
                LOGGER.warning(MISSING_PANDAS_LOG_DATAFRAME)
            else:
                LOGGER.warning(NOT_PANDAS_DATAFRAME)
            return None

        retval = {
            "profile": None,
            "dataframe": None,
        }  # type: Dict[str, Optional[Dict[str, str]]]

        try:
            profile_html = get_dataframe_profile_html(
                dataframe=dataframe, minimal=minimal
            )
            if profile_html is not None:
                fp = data_to_fp(profile_html)
                results = self._log_asset(
                    fp, "%s-profile.html" % name, asset_type="dataframe-profile"
                )
                retval["profile"] = results
        except Exception:
            LOGGER.warning(DATAFRAME_PROFILE_ERROR, exc_info=True)
            self._report_experiment_error(DATAFRAME_PROFILE_ERROR)

        if log_raw_dataframe:
            results = self.log_table(
                "%s.%s" % (name, dataframe_format),
                tabular_data=dataframe,
                **format_kwargs
            )
            retval["dataframe"] = results

        return retval

    def log_points_3d(self, scene_name, points=None, boxes=None, step=None, epoch=None):
        """Log 3d points and bounding boxes as an asset. You can visualize the asset with the
        following panel:
        [https://www.comet.ml/docs/user-interface/panel-associations/#3d-points](https://www.comet.ml/docs/user-interface/panel-associations/#3d-points).

        Args:
            * scene_name: a string identifying the 3d scene to render. A same scene name could be
            logged across different steps.
            * points (optional, default None): a list of points, each point being a list (or
            equivalent like Numpy array). Each point length should be either 3, if only the position
            is given: [X, Y, Z]. The length could also be 6, if color is passed as well: [X, Y, Z,
            R, G, B]. Red, Green and Blue should be a number between 0 and 1. Either points or boxes
            are required.
            * boxes (optional, default None): a list of box definition Dict. Each box should match
            the following format:

            ```python
            {
                "position": [0.5, 0.5, 0.5], # Required, [X, Y, Z]
                "size": {"height": 1, "width": 1, "depth": 1}, # Required
                "rotation": {"alpha": 1, "beta": 1, "gamma": 1}, # Optional, radians
                "label": "prediction", # Required
                "color": [1, 0, 0], # Optional, [R, G, B], values between 0 and 1.
                "probability": 0.96, # Optional, value between 0 and 1.
                "class": "1", # Optional
            }
            ```

            Either points or boxes are required.

            * step: Optional. Used to associate the asset to a specific step.
            * epoch: Optional. Used to associate the asset to a specific epoch.
        """
        if points is None and boxes is None:
            raise TypeError("Either points or boxes are required.")

        points = validate_and_convert_3d_points(points)
        boxes = validate_and_convert_3d_boxes(boxes)

        # All inputs (points only, boxes only or both) turned out to be invalid, don't log an empty
        # asset
        if not points and not boxes:
            LOGGER.warning(LOG_CLOUD_POINTS_3D_NO_VALID)
            return None

        data = {"version": 1, "points": points, "boxes": boxes}

        return self._log_asset_data(
            data,
            file_name="%s.3d_points.json" % scene_name,
            asset_type="3d-points",
            step=step,
            epoch=epoch,
        )

    def log_code(self, file_name=None, folder=None, code=None, code_name=None):
        # type: (Any, Any, Any, Any) -> Optional[List[Tuple[str, Dict[str, str]]]]
        """
        Log additional source code files.

        Args:
            file_name: optional, string: the file path of the file to log
            folder: optional, string: the folder path where the code files are stored
            code: optional, string: source code, either as a string or a file-like object (like
                StringIO). If passed, code_name is mandatory
            code_name: optional, string: name of the source code file when code parameter is passed
        """
        return self._log_code_asset(
            "manual", file_name=file_name, folder=folder, code=code, code_name=code_name
        )

    def _log_code_asset(
        self,
        source_code_type,
        file_name=None,
        folder=None,
        code=None,
        code_name=None,
    ):
        # type: (str, Any, Any, Any, Any) -> Optional[List[Tuple[str, Dict[str, str]]]]
        """
        Log additional source code files.
        Args:
            file_name: optional, string: the file path of the file to log
            folder: optional, string: the folder path where the code files are stored
        """
        if not self.alive:
            return None

        metadata = {"source_code_type": source_code_type}

        # Check for mutually exclusive params
        non_null_params = [x for x in [code, file_name, folder] if x is not None]
        if len(non_null_params) > 1:
            LOGGER.warning(LOG_CODE_FILE_NAME_FOLDER_MUTUALLY_EXCLUSIVE)
            return None

        if code is None and file_name is None and folder is None:
            if _in_ipython_environment():
                LOGGER.warning(LOG_CODE_CALLER_JUPYTER)
                return None

            caller = get_caller_file_path()
            if caller is None:
                LOGGER.warning(LOG_CODE_CALLER_NOT_FOUND)
                return None

            caller_file_path = caller[1]

            log_result = self._log_asset(
                file_data=caller_file_path,
                file_name=caller_file_path,
                asset_type="source_code",
                metadata=metadata,
            )

            if log_result is None:
                return None

            return [(caller_file_path, log_result)]

        elif code is not None:
            if code_name is None:
                LOGGER.warning(LOG_CODE_MISSING_CODE_NAME)
                return None

            log_result = self._log_asset_data(
                data=code,
                file_name=code_name,
                asset_type="source_code",
                metadata=metadata,
            )

            if log_result is None:
                return None

            return [(code_name, log_result)]

        elif file_name is not None:
            log_result = self._log_asset(
                file_data=file_name,
                file_name=file_name,
                asset_type="source_code",
                metadata=metadata,
            )

            if log_result is None:
                return None

            return [(file_name, log_result)]

        else:
            return self._log_asset_folder(
                folder,
                log_file_name=True,
                recursive=True,
                asset_type="source_code",
                extension_filter=[".py"],
                metadata=metadata,
            )

    def log_artifact(self, artifact):
        raise NotImplementedError(
            "Logging an artifact is only supported for Online Experiment at the moment"
        )

    def get_artifact(self, *args, **kwargs):
        raise NotImplementedError(
            "Getting an artifact is only supported for Online Experiment at the moment"
        )

    def set_code(self, code=None, overwrite=False, filename=None):
        """
        Sets the current experiment script's code. Should be called once per experiment.

        Deprecated: Use Experiment.log_code()

        Args:
            code: optional, string: experiment source code.
            overwrite: optional, bool: if True, send the code
            filename: optional, str: name of file to get source code from
        """
        if filename:
            if code is not None:
                LOGGER.warning(
                    "can't set code from string and filename; ignoring filename"
                )
            elif os.path.isfile(filename):
                LOGGER.warning(SET_CODE_FILENAME_DEPRECATED)
                self.log_code(file_name=filename)
            else:
                LOGGER.warning("filename %r is not a file; ignoring", filename)
                return

        self._set_code(code, overwrite)

    def _set_code(self, code, overwrite=False, framework=None):
        if self.alive and code is not None:

            if self._code_set and not overwrite:
                if framework:
                    # Called by an auto-logger
                    self._log_once_at_level(
                        logging.DEBUG,
                        "Set code by %r ignored; already called. Future attempts are silently ignored."
                        % framework,
                    )
                else:
                    LOGGER.warning(
                        "Set code ignored; already called. Call with overwrite=True to replace code"
                    )
                return

            self._track_framework_usage(framework)

            self._code_set = True

            LOGGER.warning(SET_CODE_CODE_DEPRECATED)
            self.log_code(code=code, code_name="Default")

    def set_model_graph(self, graph, overwrite=False):
        # type: (Any, bool) -> None
        """
        Sets the current experiment computation graph.
        Args:
            graph: String or Google Tensorflow Graph Format.
            overwrite: Bool, if True, send the graph again
        """
        return self._set_model_graph(graph, overwrite)

    def _set_model_graph(self, graph, overwrite=False, framework=None):
        # type: (Any, bool, Optional[str]) -> None
        if not self.alive:
            return

        if not graph:
            LOGGER.debug("Empty model graph logged")
            return None

        if self._graph_set and not overwrite:
            if framework:
                # Called by an auto-logger
                self._log_once_at_level(
                    logging.DEBUG,
                    "Set model graph by %r ignored; already called. Future attempts are silently ignored."
                    % framework,
                )
            else:
                LOGGER.warning(
                    "Set model graph ignored; already called. Call with overwrite=True to replace graph definition"
                )
            return

        graph = convert_model_to_string(graph)

        self._graph_set = True

        self._track_framework_usage(framework)

        LOGGER.debug("Set model graph called")

        message = ModelGraphMessage.create(
            context=self.context,
            use_http_messages=self.streamer.use_http_messages,
            graph=graph,
        )
        self._enqueue_message(message)

        self._summary.increment_section("uploads", "model graph", framework=framework)

    def set_filename(self, fname):
        """
        Sets the current experiment filename.
        Args:
            fname: String. script's filename.
        """
        self.filename = fname
        if not self.alive:
            return
        message = FileNameMessage.create(
            context=self.context,
            use_http_messages=self.streamer.use_http_messages,
            file_name=fname,
        )
        self._enqueue_message(message)
        self._summary.increment_section("uploads", "filename")

    def set_name(self, name):
        """
        Set a name for the experiment. Useful for filtering and searching on Comet.ml.
        Will shown by default under the `Other` tab.
        Args:
            name: String. A name for the experiment.
        """
        self.name = name
        self.log_other("Name", name)

    def set_os_packages(self):
        """
        Reads the installed os packages and reports them to server
        as a message.
        Returns: None

        """
        if not self.alive:
            return

        try:
            os_packages_list = read_unix_packages()
            if os_packages_list is None:
                return
            message = OsPackagesMessage.create(
                context=self.context,
                use_http_messages=self.streamer.use_http_messages,
                os_packages=os_packages_list,
            )
            self._enqueue_message(message)
            self._summary.increment_section("uploads", "os packages")

        except Exception:
            LOGGER.warning(
                "Failing to collect the installed os packages", exc_info=True
            )

    def set_pip_packages(self):
        """
        Get the installed pip packages using pkg resources and reports them to server as a message.
        Returns: None

        """
        if not self.alive:
            return

        try:
            packages = get_pip_packages()
            message = InstalledPackagesMessage.create(
                context=self.context,
                use_http_messages=self.streamer.use_http_messages,
                installed_packages=packages,
            )
            self._enqueue_message(message)
            self._summary.increment_section("uploads", "installed packages")
        except Exception:
            LOGGER.warning(
                "Failing to collect the installed pip packages", exc_info=True
            )

    def _log_conda_packages(self):
        """
        Reads the installed pip packages using pip's CLI and reports them to server as a message.
        Returns: None

        """
        if self.alive:
            try:
                if not _in_conda_environment():
                    LOGGER.debug("Not in conda environment")
                    return

                conda_env = _get_conda_env()

                if conda_env:
                    self._log_asset_data(
                        conda_env,
                        file_name=CONDA_ENV_FILE_NAME,
                        asset_type=CONDA_ENV_ASSET_TYPE,
                    )

                conda_explicit_packages = _get_conda_explicit_packages()

                if conda_explicit_packages:
                    self._log_asset_data(
                        conda_explicit_packages,
                        file_name=CONDA_SPEC_FILE_NAME,
                        asset_type=CONDA_SPEC_ASSET_TYPE,
                    )

                conda_info = _get_conda_info()

                if conda_info:
                    self._log_asset_data(
                        conda_info,
                        file_name=CONDA_INFO_FILE_NAME,
                        asset_type=CONDA_INFO_ASSET_TYPE,
                    )
            except Exception:
                LOGGER.debug("Failing to collect conda information", exc_info=True)

    def set_cmd_args(self):
        if self.alive:
            args = parse_command_line_arguments()
            LOGGER.debug("Command line arguments %r", args)
            if args is not None:
                for k, v in args.items():
                    self._log_parameter(k, v, framework="comet")

    # Context context-managers

    @contextmanager
    def context_manager(self, context):
        """
        A context manager to mark the beginning and the end of the training
        phase. This allows you to provide a namespace for metrics/params.
        For example:

        ```python
        experiment = Experiment(api_key="MY_API_KEY")
        with experiment.context_manager("validation"):
            model.fit(x_train, y_train)
            accuracy = compute_accuracy(model.predict(x_validate), y_validate)
            # returns the validation accuracy
            experiment.log_metric("accuracy", accuracy)
            # this will be logged as validation_accuracy based on the context.
        ```
        """
        # Save the old context and set the new one
        old_context = self.context
        self.context = context

        yield self

        # Restore the old one
        self.context = old_context

    @contextmanager
    def train(self):
        """
        A context manager to mark the beginning and the end of the training
        phase. This allows you to provide a namespace for metrics/params.
        For example:

        ```python
        experiment = Experiment(api_key="MY_API_KEY")
        with experiment.train():
            model.fit(x_train, y_train)
            accuracy = compute_accuracy(model.predict(x_train),y_train)
            # returns the train accuracy
            experiment.log_metric("accuracy",accuracy)
            # this will be logged as train accuracy based on the context.
        ```

        """
        # Save the old context and set the new one
        old_context = self.context
        self.context = "train"

        yield self

        # Restore the old one
        self.context = old_context

    @contextmanager
    def validate(self):
        """
        A context manager to mark the beginning and the end of the validating
        phase. This allows you to provide a namespace for metrics/params.
        For example:

        ```python
        with experiment.validate():
            pred = model.predict(x_validation)
            val_acc = compute_accuracy(pred, y_validation)
            experiment.log_metric("accuracy", val_acc)
            # this will be logged as validation accuracy
            # based on the context.
        ```


        """
        # Save the old context and set the new one
        old_context = self.context
        self.context = "validate"

        yield self

        # Restore the old one
        self.context = old_context

    @contextmanager
    def test(self):
        """
        A context manager to mark the beginning and the end of the testing phase. This allows you to provide
        a namespace for metrics/params.
        For example:

        ```python
        with experiment.test():
            pred = model.predict(x_test)
            test_acc = compute_accuracy(pred, y_test)
            experiment.log_metric("accuracy", test_acc)
            # this will be logged as test accuracy
            # based on the context.
        ```

        """
        # Save the old context and set the new one
        old_context = self.context
        self.context = "test"

        yield self

        # Restore the old one
        self.context = old_context

    def get_keras_callback(self):
        """
        This method is deprecated. See Experiment.get_callback("keras")
        """
        LOGGER.warning(
            "Experiment.get_keras_callback() is deprecated; use Experiment.get_callback('keras')"
        )
        return self.get_callback("keras")

    def disable_mp(self):
        """Disabling the auto-collection of metrics and monkey-patching of
        the Machine Learning frameworks.
        """
        self.disabled_monkey_patching = True

    def register_callback(self, function):
        """
        Register the function passed as argument to be a RPC.
        Args:
            function: Callable.
        """
        function_name = function.__name__

        if isinstance(function, types.LambdaType) and function_name == "<lambda>":
            raise LambdaUnsupported()

        if function_name in self.rpc_callbacks:
            raise RPCFunctionAlreadyRegistered(function_name)

        self.rpc_callbacks[function_name] = function

    def unregister_callback(self, function):
        """
        Unregister the function passed as argument.
        Args:
            function: Callable.
        """
        function_name = function.__name__

        self.rpc_callbacks.pop(function_name, None)

    def _get_filename(self):
        """
        Get the filename of the executing code, if possible.
        """
        if _in_ipython_environment():
            return DEFAULT_JUPYTER_INTERACTIVE_FILE_NAME
        elif sys.argv:
            pathname = os.path.dirname(sys.argv[0])
            abs_path = os.path.abspath(pathname)
            filename = os.path.basename(sys.argv[0])
            full_path = os.path.join(abs_path, filename)
            return full_path

        return None

    def _log_git_information(self):
        """
        Try to find a git repository and logs git-patch and git-metadata is the corresponding
        experiment parameter are enabled.

        The directory preference order is:
            2. the COMET_GIT_DIRECTORY
            3. the current working directory
        """
        if not self.alive:
            return

        from .git_logging import (  # Dulwich imports fails when running in sitecustomize.py
            find_git_repo,
        )

        repo_search_path = (
            self.config.get_string(None, "comet.git_directory") or os.getcwd()
        )

        LOGGER.debug("Looking for git-metadata starting at %r", repo_search_path)

        repo = find_git_repo(repo_search_path)

        if not repo:
            LOGGER.info(GIT_REPO_NOT_FOUND, repo_search_path)
            return None

        try:
            if self.log_git_metadata:
                self._set_git_metadata(repo)
        except Exception:
            LOGGER.error(LOG_GIT_METADATA_ERROR, exc_info=True)

        try:
            if self.log_git_patch:
                self._set_git_patch(repo)
        except Exception:
            LOGGER.error(LOG_GIT_PATCH_ERROR, exc_info=True)
            tb = traceback.format_exc()
            self._report(event_name=GIT_PATCH_GENERATION_FAILED, err_msg=tb)

        # Close the repo to close all opened fds
        repo.close()

    def _set_git_metadata(self, repo):
        # type: (dulwich.repo.Repo) -> None
        """
        Set the git-metadata for this experiment.
        """
        # This shouldn't be important, the only public caller is _log_git_information which should
        # already check for it
        if not self.alive:
            return

        from .git_logging import (  # Dulwich imports fails when running in sitecustomize.py
            get_git_metadata,
        )

        git_metadata = get_git_metadata(repo)

        if git_metadata:
            message = GitMetadataMessage.create(
                context=self.context,
                use_http_messages=self.streamer.use_http_messages,
                git_metadata=git_metadata,
            )
            self._enqueue_message(message)
            self._summary.increment_section("uploads", "git metadata")

    def _set_git_patch(self, repo):
        # type: (dulwich.repo.Repo) -> None
        """
        Set the git-patch for this experiment.

        """
        if not self.alive:
            return

        from .git_logging import (  # Dulwich imports fails when running in sitecustomize.py
            get_git_patch,
        )

        git_patch = get_git_patch(repo)
        if not git_patch:
            LOGGER.debug("Git patch is empty, nothing to upload")
            return None

        _, zip_path = compress_git_patch(git_patch)

        # TODO: Previously there was not upload limit check for git-patch
        processor = GitPatchUploadProcessor(
            TemporaryFilePath(zip_path),
            self.asset_upload_limit,
            url_params=None,
            metadata=None,
            copy_to_tmp=False,
            error_message_identifier=None,
            tmp_dir=self.tmpdir,
            critical=False,
        )
        upload_message = processor.process()

        if not upload_message:
            return None

        self._enqueue_message(upload_message)
        self._summary.increment_section(
            "uploads", "git-patch (uncompressed)", size=len(git_patch)
        )

    def _log_env_details(self):
        if not self.alive:
            return

        message = get_env_details_message(
            env_blacklist=self.config["comet.logging.env_blacklist"],
            include_env=self.config["comet.override_feature.sdk_log_env_variables"],
            use_http_messages=self.streamer.use_http_messages,
        )
        self._enqueue_message(message)

        self._summary.increment_section("uploads", "environment details")

    def _log_cloud_details(self):
        if not self.alive:
            return

        # this message only supported through the REST API
        if not self.streamer.use_http_messages:
            return

        cloud_details = get_env_cloud_details()
        if cloud_details is None:
            return

        message = CloudDetailsMessage.create(
            context=self.context,
            use_http_messages=True,  # it's only REST API
            provider=cloud_details["provider"],
            cloud_metadata=cloud_details["metadata"],
        )
        self._enqueue_message(message)

    def _start_gpu_thread(self):
        if not self.alive:
            return

        # First sends the static info as a message
        gpu_static_info = get_gpu_static_info()
        message = GpuStaticInfoMessage.create(
            context=self.context,
            use_http_messages=self.streamer.use_http_messages,
            gpu_static_info=gpu_static_info,
        )
        self._enqueue_message(message)

        # Second sends the one-time metrics
        one_time_gpu_metrics = get_initial_gpu_metric()
        metrics = convert_gpu_details_to_metrics(
            one_time_gpu_metrics,
            prefix=self.config["comet.distributed_node_identifier"],
        )
        for metric in metrics:
            self._log_metric(metric["name"], metric["value"], framework="comet")

        # Now starts the thread that will be called recurrently
        self.gpu_thread = GPULoggingThread(
            DEFAULT_GPU_MONITOR_INTERVAL, self._log_gpu_details
        )
        self.gpu_thread.start()

    def _start_cpu_thread(self):
        if not self.alive:
            return

        # Start the thread that will be called recurrently
        self.cpu_thread = CPULoggingThread(
            DEFAULT_CPU_MONITOR_INTERVAL,
            self._log_cpu_details,
        )
        self.cpu_thread.start()

    def _log_cpu_details(self, metrics):
        prefix = self.config["comet.distributed_node_identifier"]
        for metric in metrics:
            full_name = metric_name(metric, prefix=prefix)
            self._log_metric(
                full_name, metrics[metric], include_context=False, framework="comet"
            )

    def _log_gpu_details(self, gpu_details):
        metrics = convert_gpu_details_to_metrics(
            gpu_details, prefix=self.config["comet.distributed_node_identifier"]
        )
        for metric in metrics:
            self._log_metric(
                metric["name"],
                metric["value"],
                include_context=False,
                framework="comet",
            )

    def _get_uploaded_asset_url(self, asset_id):
        # type: (str) -> Dict[str, str]
        web_url = format_url(
            self.upload_web_asset_url_prefix, assetId=asset_id, experimentKey=self.id
        )
        api_url = format_url(
            self.upload_api_asset_url_prefix, assetId=asset_id, experimentKey=self.id
        )
        return {"web": web_url, "api": api_url, "assetId": asset_id}

    def _get_uploaded_image_url(self, image_id):
        # type: (str) -> Dict[str, str]
        web_url = format_url(
            self.upload_web_image_url_prefix, imageId=image_id, experimentKey=self.id
        )
        api_url = format_url(
            self.upload_api_image_url_prefix, imageId=image_id, experimentKey=self.id
        )
        return {"web": web_url, "api": api_url, "imageId": image_id}

    def _get_uploaded_figure_url(self, figure_id):
        # type: (str) -> Dict[str, Optional[str]]
        web_url = format_url(
            self.upload_web_image_url_prefix, imageId=figure_id, experimentKey=self.id
        )
        api_url = format_url(
            self.upload_api_image_url_prefix, imageId=figure_id, experimentKey=self.id
        )
        return {"web": web_url, "api": api_url, "imageId": figure_id}

    def _get_uploaded_audio_url(self, audio_id):
        # type: (str) -> Dict[str, Optional[str]]
        web_url = format_url(
            self.upload_web_asset_url_prefix, assetId=audio_id, experimentKey=self.id
        )
        api_url = format_url(
            self.upload_api_asset_url_prefix, assetId=audio_id, experimentKey=self.id
        )
        return {"web": web_url, "api": api_url, "assetId": audio_id}

    def _add_pending_call(self, rpc_call):
        self._pending_calls.append(rpc_call)

    def _check_rpc_callbacks(self):
        while len(self._pending_calls) > 0:
            call = self._pending_calls.pop()
            if call is not None:
                try:
                    result = self._call_rpc_callback(call)

                    self._send_rpc_callback_result(call.callId, *result)
                except Exception:
                    LOGGER.debug("Failed to call rpc %r", call, exc_info=True)

    def _call_rpc_callback(self, rpc_call):
        # type: (RemoteCall) -> Tuple[Any, int, int]
        if rpc_call.cometDefined is False:
            function_name = rpc_call.functionName

            start_time = local_timestamp()

            try:
                function = self.rpc_callbacks[function_name]
                remote_call_result = call_remote_function(function, self, rpc_call)
            except KeyError:
                error = "Unregistered remote action %r" % function_name
                remote_call_result = {"success": False, "error": error}

            end_time = local_timestamp()

            return remote_call_result, start_time, end_time

        # Hardcoded internal callbacks
        if rpc_call.functionName == "stop":
            self.log_other("experiment_stopped_by_user", True)
            raise InterruptedExperiment(rpc_call.userName)
        else:
            raise NotImplementedError()

    def _send_rpc_callback_result(
        self, call_id, remote_call_result, start_time, end_time
    ):
        raise NotImplementedError()

    def add_tag(self, tag):
        """
        Add a tag to the experiment. Tags will be shown in the dashboard.
        Args:
            tag: String. A tag to add to the experiment.
        """
        try:
            self.tags.add(tag)
            return True
        except Exception:
            LOGGER.warning(ADD_TAGS_ERROR, tag, exc_info=True)
            return False

    def add_tags(self, tags):
        """
        Add several tags to the experiment. Tags will be shown in the
        dashboard.
        Args:
            tags: List<String>. Tags list to add to the experiment.
        """
        try:
            self.tags = self.tags.union(tags)
            return True
        except Exception:
            LOGGER.warning(ADD_TAGS_ERROR, tags, exc_info=True)
            return False

    def get_tags(self):
        """
        Return the tags of this experiment.
        Returns: set<String>. The set of tags.
        """
        return list(self.tags)

    def _set_optimizer(self, optimizer, pid, trial, count):
        """
        Set the optimizer dictionary and logs
        optimizer data.

        Arguments:
            optimizer: the Optimizer object
            pid: the parameter set id
            trial: the trial number
            count: the running count
        """
        self.optimizer = {
            "optimizer": optimizer,
            "pid": pid,
            "trial": trial,
            "count": count,
        }

    def _get_optimizer_data(self):
        # type: () -> Optional[Dict[str, Any]]
        """
        If this experiment is being run with the Comet Optimizer,
        return the optimizer data.
        """
        if self.optimizer is not None:
            optimizer_data = {
                "id": self.optimizer["optimizer"].id,
                "pid": self.optimizer["pid"],
                "trial": self.optimizer["trial"],
                "count": self.optimizer["count"],
            }
        else:
            optimizer_data = None

        return optimizer_data

    def _set_optimizer_from_data(self, optimizer_data):
        # type: (Dict[str, Any]) -> None
        """
        Set the Optimizer from optimizer_data

        Args:
            optimizer_data: a dictionary with fields: id, pid, trial, and count

        Used when this experiment has been recreated and needs to
        restore the optimizer.
        """
        from comet_ml import Optimizer

        optimizer = Optimizer(optimizer_data["id"])
        self._set_optimizer(
            optimizer,
            optimizer_data["pid"],
            optimizer_data["trial"],
            optimizer_data["count"],
        )

        self.log_other("optimizer_id", optimizer_data["id"])
        self.log_other("optimizer_pid", optimizer_data["pid"])
        self.log_other("optimizer_trial", optimizer_data["trial"])

        if "parameters" in optimizer_data:
            self.log_parameters(optimizer_data["parameters"])

    def stop_early(self, epoch):
        """
        Should the experiment stop early?
        """
        return False

    def get_callback(self, framework, *args, **kwargs):
        """
        Get a callback for a particular framework.

        When framework == 'keras' then return an instance of
        Comet.ml's Keras callback.

        When framework == 'tf-keras' then return an instance of
        Comet.ml's TensorflowKeras callback.

        When framework == "tf-estimator-train" then return an instance
        of Comet.ml's Tensorflow Estimator Train callback.

        Note:
            The keras callbacks are added to your Keras `model.fit()`
            callbacks list automatically to report model training metrics
            to Comet.ml so you do not need to add them manually.

        Note:
            The lightgbm callback is added to the `lightgbm.train()`
            callbacks list automatically to report model training metrics
            to Comet.ml so you do not need to add it manually.
        """
        if framework in ["keras", "tf-keras", "tensorflow-keras"]:
            if framework == "keras":
                from .callbacks._keras import (
                    CometEmptyKerasCallback,
                    CometKerasCallback,
                )
            elif framework == "tf-keras":
                from .callbacks._tensorflow_keras import (
                    CometEmptyKerasCallback,
                    CometKerasCallback,
                )

            if self.alive:
                return CometKerasCallback(self, **kwargs)
            else:
                return CometEmptyKerasCallback()

        elif framework == "lightgbm":
            from .callbacks._lgbm import CometLGBMCallback

            return CometLGBMCallback(self)

        elif framework in ["tf-estimator-train", "tensorflow-estimator-train"]:
            from .callbacks._tensorflow_estimator import (
                CometTensorflowEstimatorTrainSessionHook,
            )

            return CometTensorflowEstimatorTrainSessionHook(self)

        elif framework == "xgboost":
            from .callbacks._xgboost import XGBoostCometCallback

            return XGBoostCometCallback(self)

        else:
            raise NotImplementedError(
                "No such framework for callback: `%s`" % framework
            )

    def send_notification(self, title, status=None, additional_data=None):
        """
        With an `Experiment`, this method will send you a notification
        through email when an experiment ends. However with other
        experiment types, this method does nothing.
        """
        pass

    # Internal methods

    def _track_framework_usage(self, framework):
        # type: (Optional[str]) -> None
        """Track which framework logged data, framework could be internal auto-loggers or external
        integrations.
        """

        if framework is None:
            return

        if framework == "comet":
            return

        self._frameworks.add(framework)
