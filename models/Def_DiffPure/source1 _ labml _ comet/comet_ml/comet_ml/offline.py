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
Author: Boris Feld

This module contains the code related to offline feature

"""
import io
import json
import logging
import os
import os.path
import random
import shutil
import string
import tempfile
import traceback
import zipfile
from os.path import join
from zipfile import ZipFile

from jsonschema import ValidationError

from ._reporting import (
    EXPERIMENT_CREATED,
    OFFLINE_INVALID_CLOUD_DETAILS_MSG,
    OFFLINE_INVALID_FILE_NAME_MSG,
    OFFLINE_INVALID_GIT_METADATA_MSG,
    OFFLINE_INVALID_GPU_STATIC_INFO_MSG,
    OFFLINE_INVALID_GRAPH_MSG,
    OFFLINE_INVALID_HTML_MSG,
    OFFLINE_INVALID_HTML_OVERRIDE_MSG,
    OFFLINE_INVALID_INSTALLED_PACKAGES_MSG,
    OFFLINE_INVALID_LOG_DEPENDENCY_MESSAGE,
    OFFLINE_INVALID_LOG_OTHER_MSG,
    OFFLINE_INVALID_METRIC_MSG,
    OFFLINE_INVALID_OS_PACKAGES_MSG,
    OFFLINE_INVALID_PARAMETER_MSG,
    OFFLINE_INVALID_STANDARD_OUTPUT_MESSAGE,
    OFFLINE_INVALID_SYSTEM_DETAILS_MSG,
    OFFLINE_INVALID_SYSTEM_INFO_MSG,
    OFFLINE_INVALID_UPLOAD_MSG,
    OFFLINE_INVALID_WS_MSG,
)
from ._typing import Any, Dict, List, Optional, Tuple
from .batch_utils import MessageBatch, MessageBatchItem, ParametersBatch
from .comet import OfflineStreamer, format_url
from .compat_utils import json_dump
from .config import (
    ADDITIONAL_STREAMER_UPLOAD_TIMEOUT,
    DEFAULT_OFFLINE_DATA_DIRECTORY,
    DEFAULT_WAIT_FOR_FINISH_SLEEP_INTERVAL,
    DEFAULT_WS_JOIN_TIMEOUT,
    MESSAGE_BATCH_METRIC_INTERVAL_SECONDS,
    MESSAGE_BATCH_METRIC_MAX_BATCH_SIZE,
    MESSAGE_BATCH_USE_COMPRESSION_DEFAULT,
    OFFLINE_EXPERIMENT_JSON_FILE_NAME,
    OFFLINE_EXPERIMENT_MESSAGES_JSON_FILE_NAME,
    get_api_key,
    get_config,
)
from .connection import (
    FileUploadManager,
    FileUploadManagerMonitor,
    RestApiClient,
    RestServerConnection,
    WebSocketConnection,
    format_messages_for_ws,
    get_backend_address,
    get_rest_api_client,
)
from .constants import (
    DEPRECATED_OFFLINE_MODE_CREATE,
    DEPRECATED_OFFLINE_MODE_TO_RESUME_STRATEGY_MAP,
    RESUME_STRATEGY_CREATE,
    RESUME_STRATEGY_GET,
    RESUME_STRATEGY_GET_OR_CREATE,
)
from .exceptions import (
    CometRestApiException,
    ExperimentAlreadyUploaded,
    InvalidAPIKey,
    InvalidOfflineDirectory,
    OfflineExperimentUploadFailed,
)
from .experiment import BaseExperiment
from .feature_toggles import USE_HTTP_MESSAGES, FeatureToggles
from .logging_messages import (
    CLOUD_DETAILS_MSG_SENDING_ERROR,
    FILE_UPLOADS_PROMPT,
    FILENAME_DETAILS_MSG_SENDING_ERROR,
    GIT_METADATA_MSG_SENDING_ERROR,
    GPU_STATIC_INFO_MSG_SENDING_ERROR,
    HTML_MSG_SENDING_ERROR,
    HTML_OVERRIDE_MSG_SENDING_ERROR,
    INSTALLED_PACKAGES_MSG_SENDING_ERROR,
    LOG_DEPENDENCY_MESSAGE_SENDING_ERROR,
    LOG_OTHER_MSG_SENDING_ERROR,
    METRICS_BATCH_MSG_SENDING_ERROR,
    MODEL_GRAPH_MSG_SENDING_ERROR,
    OFFLINE_AT_LEAST_ONE_EXPERIMENT_UPLOAD_FAILED,
    OFFLINE_DATA_DIR_DEFAULT_WARNING,
    OFFLINE_DATA_DIR_FAILED_WARNING,
    OFFLINE_EXPERIMENT_ALREADY_UPLOADED,
    OFFLINE_EXPERIMENT_CREATION_PROJECT_NAME_OVERRIDDEN_CONFIG,
    OFFLINE_EXPERIMENT_CREATION_PROJECT_NAME_OVERRIDDEN_PARAMETER,
    OFFLINE_EXPERIMENT_CREATION_WORKSPACE_OVERRIDDEN_CONFIG,
    OFFLINE_EXPERIMENT_CREATION_WORKSPACE_OVERRIDDEN_PARAMETER,
    OFFLINE_EXPERIMENT_END,
    OFFLINE_EXPERIMENT_INVALID_CLOUD_DETAILS_MSG,
    OFFLINE_EXPERIMENT_INVALID_FILE_NAME_MSG,
    OFFLINE_EXPERIMENT_INVALID_GIT_METADATA_MSG,
    OFFLINE_EXPERIMENT_INVALID_GPU_STATIC_INFO_MSG,
    OFFLINE_EXPERIMENT_INVALID_GRAPH_MSG,
    OFFLINE_EXPERIMENT_INVALID_HTML_MSG,
    OFFLINE_EXPERIMENT_INVALID_HTML_OVERRIDE_MSG,
    OFFLINE_EXPERIMENT_INVALID_INSTALLED_PACKAGES_MSG,
    OFFLINE_EXPERIMENT_INVALID_LOG_DEPENDENCY_MESSAGE,
    OFFLINE_EXPERIMENT_INVALID_LOG_OTHER_MSG,
    OFFLINE_EXPERIMENT_INVALID_METRIC_MSG,
    OFFLINE_EXPERIMENT_INVALID_OS_PACKAGES_MSG,
    OFFLINE_EXPERIMENT_INVALID_PARAMETER_MSG,
    OFFLINE_EXPERIMENT_INVALID_STANDARD_OUTPUT_MESSAGE,
    OFFLINE_EXPERIMENT_INVALID_SYSTEM_DETAILS_MSG,
    OFFLINE_EXPERIMENT_INVALID_SYSTEM_INFO_MSG,
    OFFLINE_EXPERIMENT_INVALID_UPLOAD_MSG,
    OFFLINE_EXPERIMENT_INVALID_WS_MSG,
    OFFLINE_EXPERIMENT_NAME_ACCESS,
    OFFLINE_FAILED_UPLOADED_EXPERIMENTS,
    OFFLINE_SENDER_ENDS,
    OFFLINE_SENDER_ENDS_PROCESSING,
    OFFLINE_SENDER_STARTS,
    OFFLINE_SUCCESS_UPLOADED_EXPERIMENTS,
    OFFLINE_UPLOAD_FAILED_UNEXPECTED_ERROR,
    OFFLINE_UPLOADING_EXPERIMENT_FILE_PROMPT,
    OFFLINE_UPLOADS_FAILED_DUE_TIMEOUT,
    OS_PACKAGE_MSG_SENDING_ERROR,
    PARAMETERS_BATCH_MSG_SENDING_ERROR,
    STANDARD_OUTPUT_SENDING_ERROR,
    SYSTEM_DETAILS_MSG_SENDING_ERROR,
    SYSTEM_INFO_MESSAGE_SENDING_ERROR,
    WAITING_DATA_UPLOADED,
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
    RemoteAssetMessage,
    StandardOutputMessage,
    SystemDetailsMessage,
    SystemInfoMessage,
    UploadFileMessage,
    WebSocketMessage,
)
from .metrics import MetricsSampler
from .schemas import (
    get_cloud_details_msg_validator,
    get_experiment_file_validator,
    get_file_name_msg_validator,
    get_git_metadata_msg_validator,
    get_gpu_static_info_msg_validator,
    get_graph_msg_validator,
    get_html_msg_validator,
    get_html_override_msg_validator,
    get_installed_packages_msg_validator,
    get_log_dependency_msg_validator,
    get_log_other_msg_validator,
    get_metric_msg_validator,
    get_os_packages_msg_validator,
    get_parameter_msg_validator,
    get_remote_file_msg_validator,
    get_standard_output_msg_validator,
    get_system_details_msg_validator,
    get_system_info_msg_validator,
    get_upload_msg_validator,
    get_ws_msg_validator,
)
from .utils import generate_guid, local_timestamp, wait_for_done

LOGGER = logging.getLogger(__name__)


class OfflineExperiment(BaseExperiment):
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
        offline_directory=None,  # type: Optional[str]
        log_env_gpu=True,  # type: Optional[bool]
        log_env_host=True,  # type: Optional[bool]
        api_key=None,  # type: Optional[str]
        display_summary=None,  # type: Optional[bool]
        log_env_cpu=True,  # type: Optional[bool]
        display_summary_level=None,  # type: Optional[int]
        auto_weight_logging=None,  # type: Optional[bool]
        auto_log_co2=False,  # type: Optional[bool]
        auto_metric_step_rate=10,  # type: Optional[int]
        auto_histogram_tensorboard_logging=False,  # type: Optional[bool]
        auto_histogram_epoch_rate=1,  # type: Optional[int]
        auto_histogram_weight_logging=False,  # type: Optional[bool]
        auto_histogram_gradient_logging=False,  # type: Optional[bool]
        auto_histogram_activation_logging=False,  # type: Optional[bool]
        experiment_key=None,  # type: Optional[str]
    ):
        # type: (...) -> None
        """
        Creates a new experiment and serialize it on disk. The experiment file will need to be
        uploaded manually later to appear on the frontend.
        Args:
            project_name: Optional. Send your experiment to a specific project.
                            Otherwise, will be sent to `Uncategorized Experiments`.
                            If project name does not already exist Comet.ml will create a new project.
            workspace: Optional. Attach an experiment to a project that belongs to this workspace
            log_code: Default(True) - allows you to enable/disable code logging
            log_graph: Default(True) - allows you to enable/disable automatic computation graph logging.
            auto_param_logging: Default(True) - allows you to enable/disable hyper-parameters logging
            auto_metric_logging: Default(True) - allows you to enable/disable metrics logging
            auto_metric_step_rate: Default(10) - controls how often batch metrics are logged
            auto_histogram_tensorboard_logging: Default(False) - allows you to enable/disable automatic histogram logging
            auto_histogram_epoch_rate: Default(1) - controls how often histograms are logged
            auto_histogram_weight_logging: Default(False) - allows you to enable/disable automatic histogram logging of biases and weights
            auto_histogram_gradient_logging: Default(False) - allows you to enable/disable automatic histogram logging of gradients
            auto_histogram_activation_logging: Default(False) - allows you to enable/disable automatic histogram logging of activations
            auto_output_logging: Default("default") - allows you to select
                which output logging mode to use. You can pass `"native"`
                which will log all output even when it originated from a C
                native library. You can also pass `"simple"` which will work
                only for output made by Python code. If you want to disable
                automatic output logging, you can pass `False`. The default is
                `"default"` which will detect your environment and deactivate
                the output logging for IPython and Jupyter environment and
                sets `"native"` in the other cases.
            auto_log_co2: Default(True) - automatically tracks the CO2 emission of
                this experiment if `codecarbon` package is installed in the environment
            parse_args: Default(True) - allows you to enable/disable automatic parsing of CLI arguments
            log_env_details: Default(True) - log various environment
                information in order to identify where the script is running
            log_env_gpu: Default(True) - allow you to enable/disable the
                automatic collection of gpu details and metrics (utilization, memory usage etc..).
                `log_env_details` must also be true.
            log_env_cpu: Default(True) - allow you to enable/disable the
                automatic collection of cpu details and metrics (utilization, memory usage etc..).
                `log_env_details` must also be true.
            log_env_host: Default(True) - allow you to enable/disable the
                automatic collection of host information (ip, hostname, python version, user etc...).
                `log_env_details` must also be true.
            log_git_metadata: Default(True) - allow you to enable/disable the
                automatic collection of git details
            display_summary_level: Default(1) - control the summary detail that is
                displayed on the console at end of experiment. If 0, the summary
                notification is still sent. Valid values are 0 to 2.
            disabled: Default(False) - allows you to disable all network
                communication with the Comet.ml backend. It is useful when you
                want to test to make sure everything is working, without actually
                logging anything.
            offline_directory: the directory used to save the offline archive
                for the experiment.
            experiment_key: Optional. If provided, will be used as the experiment key. If an experiment
                with the same key already exists, it will raise an Exception during upload. Could be set
                through configuration as well.
        """
        self.config = get_config()

        self.api_key = get_api_key(
            api_key, self.config
        )  # optional, except for on-line operations

        # Start and ends time
        self.start_time = None
        self.stop_time = None
        self.resume_strategy = RESUME_STRATEGY_CREATE
        self.customer_error_reported = False
        self.customer_error_message = None  # type: Optional[str]

        super(OfflineExperiment, self).__init__(
            project_name=project_name,
            workspace=workspace,
            log_code=log_code,
            log_graph=log_graph,
            auto_param_logging=auto_param_logging,
            auto_metric_logging=auto_metric_logging,
            parse_args=parse_args,
            auto_output_logging=auto_output_logging,
            log_env_details=log_env_details,
            log_git_metadata=log_git_metadata,
            log_git_patch=log_git_patch,
            disabled=disabled,
            log_env_gpu=log_env_gpu,
            log_env_host=log_env_host,
            display_summary=display_summary,
            display_summary_level=display_summary_level,
            log_env_cpu=log_env_cpu,
            auto_weight_logging=auto_weight_logging,
            auto_log_co2=auto_log_co2,
            auto_metric_step_rate=auto_metric_step_rate,
            auto_histogram_epoch_rate=auto_histogram_epoch_rate,
            auto_histogram_tensorboard_logging=auto_histogram_tensorboard_logging,
            auto_histogram_weight_logging=auto_histogram_weight_logging,
            auto_histogram_gradient_logging=auto_histogram_gradient_logging,
            auto_histogram_activation_logging=auto_histogram_activation_logging,
            experiment_key=experiment_key,
        )

        self.offline_directory, default_dir = self._get_offline_data_dir_path(
            offline_directory
        )

        if not self.disabled:
            # Check that the offline directory is usable
            # Try to create ZIP file for the experiment
            zip_file, self.offline_directory = self._create_offline_archive(
                fallback_to_temp=default_dir
            )
            # Close the file handle, it will be reopened later
            zip_file.close()

        if self.disabled is not True:
            if api_key is not None:
                self._log_once_at_level(
                    logging.WARNING,
                    "api_key was given, but is ignored in OfflineExperiment(); remember to set when you upload",
                )
            elif self.api_key is not None:
                self._log_once_at_level(
                    logging.INFO,
                    "COMET_API_KEY was set, but is ignored in OfflineExperiment(); remember to set when you upload",
                )

            self._start()

            if self.alive is True:
                self._report(event_name=EXPERIMENT_CREATED)

    def _create_offline_archive(self, fallback_to_temp=True):
        # type: (bool) -> Tuple[ZipFile, str]
        """Attempts to create ZIP file in the offline_directory. If attempt failed it will try to repeat in the
        temporary directory if fallback_to_temp is set. Otherwise, InvalidOfflineDirectory raised.
        Args:
            fallback_to_temp:
                The flag to indicate whether automatic recovery should be performed in case if failed to create archive
                in the current offline_directory.
        Returns:
            The ZipFile for offline archive and the path to the offline directory where archive was created
        """
        # Add a random string to offline experiment archives to avoid conflict when using ExistingExperiment
        offline_archive_file_name = self._get_offline_archive_file_name()
        try:
            # create parent directory
            if not os.path.exists(self.offline_directory):
                os.mkdir(self.offline_directory, 0o700)

            # Try to create the archive now
            return (
                _create_zip_file(self.offline_directory, offline_archive_file_name),
                self.offline_directory,
            )
        except (OSError, IOError) as exc:
            if fallback_to_temp:
                # failed - use temporary directory instead
                offline_dir = tempfile.mkdtemp()
                LOGGER.warning(
                    OFFLINE_DATA_DIR_FAILED_WARNING,
                    os.path.abspath(self.offline_directory),
                    os.path.abspath(offline_dir),
                    str(exc),
                    exc_info=True,
                )
                return (
                    _create_zip_file(offline_dir, offline_archive_file_name),
                    offline_dir,
                )
            else:
                raise InvalidOfflineDirectory(self.offline_directory, str(exc))

    def _get_offline_data_dir_path(self, offline_directory):
        # type: (Optional[str]) -> Tuple[str, bool]
        """looks for the offline data directory path. If offline_directory parameter is not None it will be returned.
        Otherwise, the experiment configuration settings will be used. Finally, if later failed the default data
        directory will be used.
        Args:
            offline_directory:
                The provided by user path to the offline directory or None if not set.
        Returns:
            The calculated path to the offline directory and flag to indicate if default data directory suffix was used.
        """
        if offline_directory is None:
            offline_directory = self.config["comet.offline_directory"]

        if (
            offline_directory is not None
            and offline_directory != DEFAULT_OFFLINE_DATA_DIRECTORY
        ):
            return offline_directory, False

        # use DEFAULT_OFFLINE_DATA_DIRECTORY in the CWD
        offline_directory = join(os.getcwd(), DEFAULT_OFFLINE_DATA_DIRECTORY)

        # display warning to the user that offline directory was chosen with default path
        LOGGER.info(OFFLINE_DATA_DIR_DEFAULT_WARNING, offline_directory)

        return offline_directory, True

    def _get_offline_archive_file_name(self):
        # type: () -> str
        """Return the offline archive file name, used for creating it on the file-system."""
        return "%s.zip" % self.id

    def display(self, *args, **kwargs):
        """Do nothing"""
        pass

    def display_project(self, *args, **kwargs):
        """Do nothing"""
        pass

    def _start(self, **kwargs):
        self.start_time = local_timestamp()
        super(OfflineExperiment, self)._start(**kwargs)
        self.log_other("offline_experiment", True)

    def _write_experiment_meta_file(self):
        meta_file_path = join(self.tmpdir, OFFLINE_EXPERIMENT_JSON_FILE_NAME)
        meta = {
            "offline_id": self.id,
            "project_name": self.project_name,
            "start_time": self.start_time,
            "stop_time": self.stop_time,
            "tags": self.get_tags(),
            "workspace": self.workspace,
            "resume_strategy": self.resume_strategy,
            "customer_error_reported": self.customer_error_reported,
            "customer_error_message": self.customer_error_message,
        }
        with open(meta_file_path, "wb") as f:
            json_dump(meta, f)

    def _mark_as_ended(self):
        if not self.alive:
            LOGGER.debug("Skipping creating the offline archive as we are not alive")
            return

        LOGGER.info("Starting saving the offline archive")
        self.stop_time = local_timestamp()

        self._write_experiment_meta_file()

        zip_file, self.offline_directory = self._create_offline_archive(
            fallback_to_temp=True
        )

        for file in os.listdir(self.tmpdir):
            zip_file.write(os.path.join(self.tmpdir, file), file)

        zip_file.close()

        # Clean the tmpdir to avoid filling up the disk
        try:
            shutil.rmtree(self.tmpdir)
        except OSError:
            # We made our best effort to clean ourselves
            LOGGER.debug(
                "Error cleaning offline experiment tmpdir: %r",
                self.tmpdir,
                exc_info=True,
            )

        # Display the full command to upload the offline experiment
        LOGGER.info(OFFLINE_EXPERIMENT_END, zip_file.filename)

    def _report_experiment_error(self, message=None):
        self.customer_error_reported = True
        self.customer_error_message = message

    def _setup_streamer(self):
        """
        Initialize the streamer and feature flags.
        """
        # init feature toggles first, thus configuration will be applied before everything else
        self.feature_toggles = FeatureToggles({USE_HTTP_MESSAGES: False}, self.config)

        # Initiate the streamer
        self.streamer = OfflineStreamer(
            tmp_dir=self.tmpdir,
            initial_offset=0,
            wait_timeout=60,
            use_http_messages=self.feature_toggles[USE_HTTP_MESSAGES],
            on_error_callback=self._report_experiment_error,
        )

        # Start streamer thread.
        self.streamer.start()

        # Mark the experiment as alive
        return True

    def _report(self, *args, **kwrags):
        # TODO WHAT TO DO WITH REPORTING?
        pass

    def _get_experiment_url(self, tab=None):
        return "[OfflineExperiment will get URL after upload]"

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
        if self.name is None:
            LOGGER.warning(OFFLINE_EXPERIMENT_NAME_ACCESS)
        return self.name


class ExistingOfflineExperiment(OfflineExperiment):
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
        offline_directory=None,  # type: Optional[str]
        log_env_gpu=True,  # type: Optional[bool]
        log_env_host=True,  # type: Optional[bool]
        api_key=None,  # type: Optional[str]
        display_summary=None,  # type: Optional[bool]
        log_env_cpu=True,  # type: Optional[bool]
        display_summary_level=None,  # type: Optional[int]
        auto_weight_logging=False,  # type: Optional[bool]
        previous_experiment=None,  # type: Optional[str]
        experiment_key=None,  # type: Optional[str]
    ):
        # type: (...) -> None
        """
        Continue a previous experiment (identified by previous_experiment) and serialize it on disk.
        The experiment file will need to be uploaded manually later to append new information to the
        previous experiment. The previous experiment need to exist before upload of the
        ExistingOfflineExperiment.
        Args:
            previous_experiment: Deprecated. Use `experiment_key` instead.
            project_name: Optional. Send your experiment to a specific project.
                            Otherwise, will be sent to `Uncategorized Experiments`.
                            If project name does not already exist Comet.ml will create a new project.
            workspace: Optional. Attach an experiment to a project that belongs to this workspace
            log_code: Default(True) - allows you to enable/disable code logging
            log_graph: Default(True) - allows you to enable/disable automatic computation graph logging.
            auto_param_logging: Default(True) - allows you to enable/disable hyper-parameters logging
            auto_metric_logging: Default(True) - allows you to enable/disable metrics logging
            parse_args: Default(True) - allows you to enable/disable automatic parsing of CLI arguments
            auto_output_logging: Default("default") - allows you to select
                which output logging mode to use. You can pass `"native"`
                which will log all output even when it originated from a C
                native library. You can also pass `"simple"` which will work
                only for output made by Python code. If you want to disable
                automatic output logging, you can pass `False`. The default is
                `"default"` which will detect your environment and deactivate
                the output logging for IPython and Jupyter environment and
                sets `"native"` in the other cases.
            log_env_details: Default(True) - log various environment
                information in order to identify where the script is running
            log_env_gpu: Default(True) - allow you to enable/disable the
                automatic collection of gpu details and metrics (utilization, memory usage etc..).
                `log_env_details` must also be true.
            log_env_cpu: Default(True) - allow you to enable/disable the
                automatic collection of cpu details and metrics (utilization, memory usage etc..).
                `log_env_details` must also be true.
            log_env_host: Default(True) - allow you to enable/disable the
                automatic collection of host information (ip, hostname, python version, user etc...).
                `log_env_details` must also be true.
            log_git_metadata: Default(True) - allow you to enable/disable the
                automatic collection of git details
            display_summary_level: Default(1) - control the summary detail that is
                displayed on the console at end of experiment. If 0, the summary
                notification is still sent. Valid values are 0 to 2.
            disabled: Default(False) - allows you to disable all network
                communication with the Comet.ml backend. It is useful when you
                want to test to make sure everything is working, without actually
                logging anything.
            offline_directory: the directory used to save the offline archive
                for the experiment.
            experiment_key: Optional. Your experiment key from comet.ml, could be set through
                configuration as well.
        """
        self.config = get_config()

        if previous_experiment is not None and experiment_key is not None:
            # TODO: SHOW LOG MESSAGE?
            pass
        elif previous_experiment is not None:
            experiment_key = previous_experiment

        # Generate once the random string used when creating the offline archive on the file-system
        self._random_string = "".join(
            random.choice(string.ascii_letters) for _ in range(6)
        )

        super(ExistingOfflineExperiment, self).__init__(
            project_name=project_name,
            workspace=workspace,
            log_code=log_code,
            log_graph=log_graph,
            auto_param_logging=auto_param_logging,
            auto_metric_logging=auto_metric_logging,
            parse_args=parse_args,
            auto_output_logging=auto_output_logging,
            log_env_details=log_env_details,
            log_git_metadata=log_git_metadata,
            log_git_patch=log_git_patch,
            disabled=disabled,
            offline_directory=offline_directory,
            log_env_gpu=log_env_gpu,
            log_env_host=log_env_host,
            api_key=api_key,
            log_env_cpu=log_env_cpu,
            display_summary=display_summary,
            display_summary_level=display_summary_level,
            auto_weight_logging=auto_weight_logging,
            experiment_key=experiment_key,
        )

        self.resume_strategy = RESUME_STRATEGY_GET

    def _get_offline_archive_file_name(self):
        # type: () -> str
        """Return the offline archive file name, used for creating it on the file-system. For
        ExistingOfflineExperiment, add a random string suffix to avoid ovewriting existing archive file.
        """
        return "%s-%s.zip" % (self.id, self._random_string)


class OfflineSender(object):
    def __init__(
        self,
        api_key,  # type: str
        offline_dir,  # type: str
        force_reupload=False,  # type: bool
        display_level="info",  # type: str
        raise_validation_error_for_tests=False,  # type: bool
        file_upload_waiting_timeout=ADDITIONAL_STREAMER_UPLOAD_TIMEOUT,  # type: int
        override_workspace=None,  # type: Optional[str]
        override_project_name=None,  # type: Optional[str]
        use_http_messages=False,
        message_batch_metric_interval=MESSAGE_BATCH_METRIC_INTERVAL_SECONDS,
        message_batch_metric_max_size=MESSAGE_BATCH_METRIC_MAX_BATCH_SIZE,
        message_batch_compress=MESSAGE_BATCH_USE_COMPRESSION_DEFAULT,
        wait_for_finish_sleep_interval=DEFAULT_WAIT_FOR_FINISH_SLEEP_INTERVAL,
        connection=None,  # type: RestServerConnection
        rest_api_client=None,  # type: RestApiClient
    ):
        # type: (...) -> None
        self.config = get_config()
        self.api_key = api_key
        self.offline_dir = offline_dir
        self.force_reupload = force_reupload
        self.counter = 0
        self.display_level = logging.getLevelName(display_level.upper())

        # Validators
        self.experiment_file_validator = get_experiment_file_validator()
        self.ws_msg_validator = get_ws_msg_validator()
        self.parameter_msg_validator = get_parameter_msg_validator()
        self.metric_msg_validator = get_metric_msg_validator()
        self.os_packages_msg_validator = get_os_packages_msg_validator()
        self.graph_msg_validator = get_graph_msg_validator()
        self.system_details_msg_validator = get_system_details_msg_validator()
        self.cloud_details_msg_validator = get_cloud_details_msg_validator()
        self.upload_msg_validator = get_upload_msg_validator()
        self.remote_file_msg_validator = get_remote_file_msg_validator()
        self.log_other_message_validator = get_log_other_msg_validator()
        self.file_name_msg_validator = get_file_name_msg_validator()
        self.html_msg_validator = get_html_msg_validator()
        self.html_override_msg_validator = get_html_override_msg_validator()
        self.installed_packages_validator = get_installed_packages_msg_validator()
        self.gpu_static_info_msg_validator = get_gpu_static_info_msg_validator()
        self.git_metadata_msg_validator = get_git_metadata_msg_validator()
        self.system_info_msg_validator = get_system_info_msg_validator()
        self.standard_output_msg_validator = get_standard_output_msg_validator()
        self.log_dependency_msg_validator = get_log_dependency_msg_validator()

        self.server_address = get_backend_address()

        self.override_workspace = override_workspace
        self.override_project_name = override_project_name

        self._read_experiment_file()

        self.check_tls_certificate = self.config.get_bool(
            None, "comet.internal.check_tls_certificate"
        )

        if connection is None:
            self.connection = RestServerConnection(
                self.api_key,
                self.experiment_id,
                self.server_address,
                self.config["comet.timeout.http"],
                verify_tls=self.check_tls_certificate,
            )
        else:
            self.connection = connection

        self.rest_api_client = rest_api_client
        self.focus_link = None

        self.file_upload_manager = FileUploadManager(
            self.config.get_int(None, "comet.internal.file_upload_worker_ratio"),
            self.config.get_raw(None, "comet.internal.worker_count"),
        )
        self.file_upload_waiting_timeout = file_upload_waiting_timeout
        self.wait_for_finish_sleep_interval = wait_for_finish_sleep_interval

        self.raise_validation_error_for_tests = raise_validation_error_for_tests

        self.use_http_messages = use_http_messages
        self.message_batch_metrics = MessageBatch(
            base_interval=message_batch_metric_interval,
            max_size=message_batch_metric_max_size,
        )
        self.message_batch_compress = message_batch_compress

        # Self._resuming will be on only if we append to an existing experiment
        self._resuming = False

        self.ws_connection = None

    def send(self):
        self._handshake()

        self._status_report_start()

        if self.customer_error_reported:
            self._report_experiment_error()

        LOGGER.log(self.display_level, OFFLINE_SENDER_STARTS)

        self._send_messages()

        self._status_report_end()

        self._send_start_ends_time()

        LOGGER.debug(
            "Offline http sending enabled: %s, metric batch size: %d, metrics batch interval: %s seconds",
            self.use_http_messages,
            self.message_batch_metrics.max_size,
            self.message_batch_metrics.base_interval,
        )

    def _read_experiment_file(self):
        with io.open(
            join(self.offline_dir, OFFLINE_EXPERIMENT_JSON_FILE_NAME), encoding="utf-8"
        ) as experiment_file:
            metadata = json.load(experiment_file)

        self.experiment_file_validator.validate(metadata)

        self.experiment_id = metadata.get("offline_id")

        # Offline experiments created with old versions of the SDK will be
        # missing this field, so generate a new one if that's the case
        if not self.experiment_id:
            self.experiment_id = generate_guid()

        self.start_time = metadata["start_time"]
        self.stop_time = metadata["stop_time"]
        self.tags = metadata["tags"]
        self.metadata_project_name = metadata["project_name"]  # type: Optional[str]
        self.metadata_workspace = metadata["workspace"]  # type: Optional[str]
        self.customer_error_reported = metadata.get("customer_error_reported", False)

        # Up to Python SDK 3.19.0, we used to have the "mode" metadata
        old_mode = metadata.get("mode", DEPRECATED_OFFLINE_MODE_CREATE)
        old_mode_fallback = DEPRECATED_OFFLINE_MODE_TO_RESUME_STRATEGY_MAP[old_mode]

        self.resume_strategy = metadata.get("resume_strategy", old_mode_fallback)

    def get_creation_workspace(self):
        # type: () -> Tuple[Optional[str], Optional[str]]
        """Return the correct workspace to use for experiment creation. The order of priority is:
        * Explicit workspace parameter (either passed in Python when calling main_upload or
          upload_single_offline_experiment, or using the --workspace CLI flag of comet upload)
        * Implicit workspace taken from config
        * The workspace from the offline archive metadata

        Returns a tuple of three items:
        * The workspace to use
        * Optionally, a log message to display when an experiment has been successfully created
        * Optionally, a log message to display when an experiment fails to be created
        """
        workspace_config = self.config.get_string(None, "comet.workspace")
        if self.override_workspace is not None:
            return (
                self.override_workspace,
                OFFLINE_EXPERIMENT_CREATION_WORKSPACE_OVERRIDDEN_PARAMETER,
            )
        elif workspace_config is not None:
            return (
                workspace_config,
                OFFLINE_EXPERIMENT_CREATION_WORKSPACE_OVERRIDDEN_CONFIG,
            )
        else:
            return self.metadata_workspace, None

    def get_creation_project_name(self):
        # type: () -> Tuple[Optional[str], Optional[str]]
        """Return the correct project_name to use for experiment creation. The order of priority is:
        * Explicit project_name parameter (either passed in Python when calling main_upload or
          upload_single_offline_experiment, or using the --project_name CLI flag of comet upload)
        * Implicit project_name taken from config
        * The project_name from the offline archive metadata
        """
        project_name_config = self.config.get_string(None, "comet.project_name")
        if self.override_project_name is not None:
            return (
                self.override_project_name,
                OFFLINE_EXPERIMENT_CREATION_PROJECT_NAME_OVERRIDDEN_PARAMETER,
            )
        elif project_name_config is not None:
            return (
                project_name_config,
                OFFLINE_EXPERIMENT_CREATION_PROJECT_NAME_OVERRIDDEN_CONFIG,
            )
        else:
            return self.metadata_project_name, None

    def _handshake(self):
        # type: () -> None

        (
            creation_workspace,
            workspace_overridden,
        ) = self.get_creation_workspace()

        (
            creation_project_name,
            project_name_overridden,
        ) = self.get_creation_project_name()

        if self.resume_strategy == RESUME_STRATEGY_CREATE:
            try:
                # We know the workspace and project_name are taken into account in all cases,
                # display the log message before the actual creation
                if workspace_overridden is not None:
                    LOGGER.info(
                        workspace_overridden,
                        {
                            "experiment_id": self.experiment_id,
                            "creation_workspace": creation_workspace,
                            "metadata_workspace": self.metadata_workspace,
                        },
                    )

                if project_name_overridden is not None:
                    LOGGER.info(
                        project_name_overridden,
                        {
                            "experiment_id": self.experiment_id,
                            "creation_project_name": creation_project_name,
                            "metadata_project_name": self.metadata_project_name,
                        },
                    )

                run_id_results = self.connection.get_run_id(
                    creation_project_name,
                    creation_workspace,
                    offline=True,
                )
            except ExperimentAlreadyUploaded:
                # If the experiment already exists and the force flag is set, generate a new experiment ID and retry
                if self.force_reupload:
                    self.experiment_id = generate_guid()

                    # Re-create a new RestServerConnection with the new experiment id
                    self.connection.close()
                    self.connection = RestServerConnection(
                        self.api_key,
                        self.experiment_id,
                        self.server_address,
                        self.config.get_int(None, "comet.timeout.http"),
                        verify_tls=self.check_tls_certificate,
                    )

                    run_id_results = self.connection.get_run_id(
                        creation_project_name,
                        creation_workspace,
                        offline=True,
                    )
                else:
                    raise

        elif self.resume_strategy == RESUME_STRATEGY_GET:
            run_id_results = self.connection.get_old_run_id(self.experiment_id)
            self._resuming = True
        elif self.resume_strategy == RESUME_STRATEGY_GET_OR_CREATE:
            # Try to create an experiment and if we get an exception that the experiment_id already
            # exists, try to resume it instead
            try:
                run_id_results = self.connection.get_run_id(
                    creation_project_name,
                    creation_workspace,
                    offline=True,
                )

                # If we successfully create a new experiment, display the log message about
                # overridden workspace and project_name. If we do before, we might display them even
                # if they ends-up not being used because we fallback on an ExistingExperiment
                if workspace_overridden is not None:
                    LOGGER.info(
                        workspace_overridden,
                        {
                            "experiment_id": self.experiment_id,
                            "creation_workspace": creation_workspace,
                            "metadata_workspace": self.metadata_workspace,
                        },
                    )

                if project_name_overridden is not None:
                    LOGGER.info(
                        project_name_overridden,
                        {
                            "experiment_id": self.experiment_id,
                            "creation_project_name": creation_project_name,
                            "metadata_project_name": self.metadata_project_name,
                        },
                    )
            except ExperimentAlreadyUploaded:
                run_id_results = self.connection.get_old_run_id(self.experiment_id)

            self._resuming = True
        else:
            raise ValueError("Unknown resume strategy value %r" % self.resume_strategy)

        self.run_id = run_id_results.run_id
        self.ws_url = run_id_results.ws_server
        self.project_id = run_id_results.project_id
        self.is_github = run_id_results.is_github
        self.focus_link = run_id_results.focus_link

        self.feature_toggles = FeatureToggles(
            run_id_results.feature_toggles, self.config
        )
        self.use_http_messages = self.feature_toggles[USE_HTTP_MESSAGES]

        # Send tags if present
        if self.tags:
            self.connection.add_tags(self.tags)

        full_ws_url = format_url(self.ws_url, apiKey=self.api_key, runId=self.run_id)

        if not self.use_http_messages:
            self.ws_connection = WebSocketConnection(
                full_ws_url, self.connection, verify_tls=self.check_tls_certificate
            )
            self.ws_connection.start()
            self.ws_connection.wait_for_connection()

        if self.rest_api_client is None:
            self.rest_api_client = get_rest_api_client("v2", api_key=self.api_key)

    def _send_messages(self):
        i = 0

        # Samples down the metrics
        sampling_size = self.config["comet.offline_sampling_size"]

        LOGGER.debug("Sampling metrics to %d values per metric name", sampling_size)

        sampler = MetricsSampler(sampling_size)
        parameter_batch = ParametersBatch(0)  # We don't care about the timing here
        stdout_batch = MessageBatch(0, 0)  # All messages will be sent in one batch

        with io.open(
            join(self.offline_dir, OFFLINE_EXPERIMENT_MESSAGES_JSON_FILE_NAME),
            encoding="utf-8",
        ) as messages_files:
            for i, line in enumerate(messages_files):
                try:
                    message = json.loads(line)

                    LOGGER.debug("Message %r", message)

                    message_type = message["type"]

                    if (
                        message_type == WebSocketMessage.type
                        or message_type == MetricMessage.type
                    ):
                        message_payload = message["payload"]
                        # Inject the offset now
                        message_payload["offset"] = i + 1

                        message_metric = message_payload.get(MetricMessage.metric_key)
                        old_message_param = (
                            "param" in message_payload or "params" in message_payload
                        )

                        if message_metric:
                            sampler.sample_metric(message_payload)
                        elif old_message_param:
                            # The new parameter message payload is a subset of the old WS payload
                            # so it should be compatible
                            self._process_parameter_message(
                                message, offset=i + 1, parameter_batch=parameter_batch
                            )
                        else:
                            self._process_ws_msg(message_payload)
                    elif message_type == ParameterMessage.type:
                        self._process_parameter_message(
                            message, offset=i + 1, parameter_batch=parameter_batch
                        )
                    elif message_type == UploadFileMessage.type:
                        self._process_upload_message(message)
                    elif message_type == RemoteAssetMessage.type:
                        self._process_remote_file_message(message)
                    elif message_type == OsPackagesMessage.type:
                        self._process_os_packages_message(message)
                    elif message_type == ModelGraphMessage.type:
                        self._process_graph_message(message)
                    elif message_type == SystemDetailsMessage.type:
                        self._process_system_details_message(message)
                    elif message_type == CloudDetailsMessage.type:
                        self._process_cloud_details_message(message)
                    elif message_type == LogOtherMessage.type:
                        self._process_log_other_message(message)
                    elif message_type == FileNameMessage.type:
                        self._process_file_name_message(message)
                    elif message_type == HtmlMessage.type:
                        self._process_html_message(message)
                    elif message_type == HtmlOverrideMessage.type:
                        self._process_html_override_message(message)
                    elif message_type == InstalledPackagesMessage.type:
                        self._process_installed_packages_message(message)
                    elif message_type == GpuStaticInfoMessage.type:
                        self._process_gpu_static_info_message(message)
                    elif message_type == GitMetadataMessage.type:
                        self._process_git_metadata_message(message)
                    elif message_type == SystemInfoMessage.type:
                        self._process_system_info_message(message)
                    elif message_type == StandardOutputMessage.type:
                        self._process_standard_output_message(
                            message, offset=i + 1, message_batch=stdout_batch
                        )
                    elif message_type == LogDependencyMessage.type:
                        self._process_log_dependency_message(message)
                    else:
                        raise ValueError("Unknown message type %r", message_type)
                except Exception:
                    LOGGER.warning("Error processing line %d", i + 1, exc_info=True)
                    # report experiment error
                    self._report_experiment_error()

        # Then send the sampled metrics
        samples = sampler.get_samples()
        if self.use_http_messages:
            # send all collected metric samples as batch(-es)
            self._send_sampled_metrics_batch(samples)
        else:
            # send samples one-by-one through WS
            for metric in samples:
                try:
                    self._send_metric_message(message=metric)
                except Exception:
                    LOGGER.warning("Error processing metric", exc_info=True)

        # And the batched hyper-parameters
        if parameter_batch.accept(
            self._send_parameter_messages_batch, unconditional=True
        ):
            LOGGER.debug("Parameters batch was sent")

        if stdout_batch.accept(self._send_stdout_message_batch, unconditional=True):
            LOGGER.debug("Stdout messages batch was sent")

        LOGGER.debug("Done sending %d messages", i)

    def _report_experiment_error(self):
        self.rest_api_client.update_experiment_error_status(
            experiment_key=self.experiment_id, is_alive=True, has_error=True
        )

    def _send_sampled_metrics_batch(self, samples):
        # type: (List[Dict[str, Any]]) -> None
        for metric_sample in samples:
            metric = self._parse_metric_message(metric_sample)
            if metric is None:
                return

            self.message_batch_metrics.append(metric)
            # attempt to send batch of collected metrics immediately if batch size limit was hit
            # after appending new metric
            self.message_batch_metrics.accept(self._send_metric_messages_batch)

        # send the last part of messages if appropriate
        self.message_batch_metrics.accept(
            self._send_metric_messages_batch, unconditional=True
        )

    def _send_parameter_messages_batch(self, message_items):
        # type: (List[MessageBatchItem]) -> None
        """Attempts to send batch of parameters"""
        if self.use_http_messages:
            self._process_rest_api_send(
                self.connection.log_parameters_batch,
                rest_fail_prompt=PARAMETERS_BATCH_MSG_SENDING_ERROR,
                general_fail_prompt="Error sending parameters batch (offline experiment)",
                items=message_items,
                compress=self.message_batch_compress,
            )
        else:
            # send parameter messages using web socket
            for item in message_items:
                self._send_ws_message(message=item.message, offset=item.offset)

    def _send_metric_messages_batch(self, message_items):
        # type: (List[MessageBatchItem]) -> None
        """Attempts to send batch of metrics."""
        self._process_rest_api_send(
            self.connection.log_metrics_batch,
            rest_fail_prompt=METRICS_BATCH_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending metrics batch (offline experiment)",
            items=message_items,
            compress=self.message_batch_compress,
        )

    def _process_rest_api_send(
        self, sender, rest_fail_prompt, general_fail_prompt, **kwargs
    ):
        try:
            sender(**kwargs)
        except CometRestApiException as exc:
            LOGGER.error(
                rest_fail_prompt,
                exc.response.status_code,
                exc.response.content,
            )
            # report experiment error
            self._report_experiment_error()
        except Exception:
            LOGGER.error(general_fail_prompt, exc_info=True)
            # report experiment error
            self._report_experiment_error()

    def _validate_msg(
        self,
        message,
        msg_validator,
        fail_message,
        fail_event_name,
        raise_validation_error,
    ):
        """Validates message using given JSON schema validator. If validation failed reports experiment error
        and raises exception if appropriate or log corresponding warning and reports failure event to backend"""
        try:
            msg_validator.validate(message)
            return True
        except ValidationError:
            # report experiment error
            self._report_experiment_error()

            if raise_validation_error:
                raise

            tb = traceback.format_exc()
            LOGGER.warning(fail_message, exc_info=True)
            self.connection.report(event_name=fail_event_name, err_msg=tb)

        return False

    def _process_ws_msg(self, message):
        self._validate_msg(
            message=message,
            msg_validator=self.ws_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_WS_MSG,
            fail_event_name=OFFLINE_INVALID_WS_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        # Inject api key and run_id
        to_send = self._serialise_message_for_ws(message)

        # The ws connection is created during handshake
        try:
            self.ws_connection.send(to_send)  # type: ignore
        except Exception:
            LOGGER.debug("WS sending error", exc_info=True)
            # report experiment error
            self._report_experiment_error()

    def _send_ws_message(self, message, offset=None):
        # type: (BaseMessage, Optional[int]) -> None
        """The method to be used as callback for parameters batch processing or to send WS messages immediately"""
        try:
            message_dict = message.non_null_dict()
            data = self._serialise_message_for_ws(message=message_dict, offset=offset)
            self.ws_connection.send(data)
        except Exception:
            LOGGER.debug("WS sending error", exc_info=True)
            # report experiment error
            self._report_experiment_error()

    def _send_metric_message(self, message):
        # type: (Dict[str, Any]) -> None
        metric_message = self._parse_metric_message(message)
        if metric_message is not None:
            self._send_ws_message(message=metric_message, offset=message["offset"])

    def _parse_metric_message(self, message):
        # type: (Dict[str, Any]) -> Optional[MetricMessage]
        """Validates and deserialize raw metric message (dictionary)"""
        if not self._validate_msg(
            message=message,
            msg_validator=self.metric_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_METRIC_MSG,
            fail_event_name=OFFLINE_INVALID_METRIC_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        ):
            return None

        return MetricMessage.deserialize(message_dict=message)

    def _process_parameter_message(self, message, offset, parameter_batch):
        # type: (Dict[Any, Any], int, ParametersBatch) -> None
        message = message["payload"]

        if not self._validate_msg(
            message=message,
            msg_validator=self.parameter_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_PARAMETER_MSG,
            fail_event_name=OFFLINE_INVALID_PARAMETER_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        ):
            return

        # We know the message is valid, recreate a ParameterMessage from it
        param_message = ParameterMessage.deserialize(message)

        parameter_batch.append(param_message, offset)

    def _inject_fields(self, message, offset=None):
        # type: (Dict[str, Any], Optional[int]) -> Dict[str, Any]
        """Enhance provided message with relevant meta-data"""

        # Inject CometML specific values
        message["apiKey"] = self.api_key
        message["runId"] = self.run_id
        message["projectId"] = self.project_id
        message["experimentKey"] = self.experiment_id

        if offset:
            message["offset"] = offset

        return message

    def _serialise_message_for_ws(self, message, offset=None):
        # type: (Dict[str, Any], Optional[int]) -> str
        """Enhance provided message with relevant meta-data and serialize it to JSON compatible with WS format"""

        return format_messages_for_ws([self._inject_fields(message, offset=offset)])

    def _process_upload_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.upload_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_UPLOAD_MSG,
            fail_event_name=OFFLINE_INVALID_UPLOAD_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        # Compute the url from the upload type
        url = self.connection.get_upload_url(message["upload_type"])

        additional_params = message["additional_params"] or {}
        additional_params["runId"] = self.run_id
        # Temporary fix to ensure integers:
        if "step" in additional_params and additional_params["step"] is not None:
            additional_params["step"] = int(additional_params["step"])
        if "epoch" in additional_params and additional_params["epoch"] is not None:
            additional_params["epoch"] = int(additional_params["epoch"])

        file_path = join(self.offline_dir, message["file_path"])
        file_size = os.path.getsize(file_path)

        self.file_upload_manager.upload_file_thread(
            project_id=self.project_id,
            experiment_id=self.experiment_id,
            file_path=file_path,
            upload_endpoint=url,
            api_key=self.api_key,
            additional_params=additional_params,
            clean=True,
            timeout=self.config.get_int(None, "comet.timeout.file_upload"),
            verify_tls=self.check_tls_certificate,
            estimated_size=file_size,
        )
        LOGGER.debug("Processing uploading message done")

    def _process_remote_file_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.remote_file_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_UPLOAD_MSG,
            fail_event_name=OFFLINE_INVALID_UPLOAD_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        # Compute the url from the upload type
        url = self.connection.get_upload_url(message["upload_type"])

        additional_params = message["additional_params"] or {}
        additional_params["runId"] = self.run_id
        # Temporary fix to ensure integers:
        if "step" in additional_params and additional_params["step"] is not None:
            additional_params["step"] = int(additional_params["step"])
        if "epoch" in additional_params and additional_params["epoch"] is not None:
            additional_params["epoch"] = int(additional_params["epoch"])

        self.file_upload_manager.upload_remote_asset_thread(
            project_id=self.project_id,
            experiment_id=self.experiment_id,
            remote_uri=message["remote_uri"],
            upload_endpoint=url,
            api_key=self.api_key,
            additional_params=additional_params,
            metadata=message["metadata"],
            timeout=self.config.get_int(None, "comet.timeout.file_upload"),
            verify_tls=self.check_tls_certificate,
        )
        LOGGER.debug("Processing remote uploading message done")

    def _process_os_packages_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.os_packages_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_OS_PACKAGES_MSG,
            fail_event_name=OFFLINE_INVALID_OS_PACKAGES_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        self._process_rest_api_send(
            self.rest_api_client.set_experiment_os_packages,
            rest_fail_prompt=OS_PACKAGE_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending os_packages message",
            experiment_key=self.experiment_id,
            os_packages=message["os_packages"],
        )

    def _process_graph_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.graph_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_GRAPH_MSG,
            fail_event_name=OFFLINE_INVALID_GRAPH_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        self._process_rest_api_send(
            self.rest_api_client.set_experiment_model_graph,
            rest_fail_prompt=MODEL_GRAPH_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending model graph message",
            experiment_key=self.experiment_id,
            graph_str=message["graph"],
        )

    def _process_system_details_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.system_details_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_SYSTEM_DETAILS_MSG,
            fail_event_name=OFFLINE_INVALID_SYSTEM_DETAILS_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )
        self._process_rest_api_send(
            self.rest_api_client.set_experiment_system_details,
            rest_fail_prompt=SYSTEM_DETAILS_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending system details message",
            _os=message["os"],
            command=message["command"],
            env=message["env"],
            experiment_key=self.experiment_id,
            hostname=message["hostname"],
            ip=message["ip"],
            machine=message["machine"],
            os_release=message["os_release"],
            os_type=message["os_type"],
            pid=message["pid"],
            processor=message["processor"],
            python_exe=message["python_exe"],
            python_version_verbose=message["python_version_verbose"],
            python_version=message["python_version"],
            user=message["user"],
        )

    def _process_cloud_details_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.cloud_details_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_CLOUD_DETAILS_MSG,
            fail_event_name=OFFLINE_INVALID_CLOUD_DETAILS_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        self._process_rest_api_send(
            self.rest_api_client.set_experiment_cloud_details,
            rest_fail_prompt=CLOUD_DETAILS_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending cloud details message",
            experiment_key=self.experiment_id,
            provider=message["provider"],
            cloud_metadata=message["cloud_metadata"],
        )

    def _process_log_other_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.log_other_message_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_LOG_OTHER_MSG,
            fail_event_name=OFFLINE_INVALID_LOG_OTHER_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        self._process_rest_api_send(
            self.rest_api_client.log_experiment_other,
            rest_fail_prompt=LOG_OTHER_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending log other message",
            experiment_key=self.experiment_id,
            key=message["key"],
            value=message["value"],
        )

    def _process_file_name_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.file_name_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_FILE_NAME_MSG,
            fail_event_name=OFFLINE_INVALID_FILE_NAME_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        self._process_rest_api_send(
            self.rest_api_client.set_experiment_filename,
            rest_fail_prompt=FILENAME_DETAILS_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending file name message",
            experiment_key=self.experiment_id,
            filename=message["file_name"],
        )

    def _process_html_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.html_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_HTML_MSG,
            fail_event_name=OFFLINE_INVALID_HTML_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        self._process_rest_api_send(
            self.rest_api_client.log_experiment_html,
            rest_fail_prompt=HTML_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending html message",
            experiment_key=self.experiment_id,
            html=message["html"],
        )

    def _process_installed_packages_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.installed_packages_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_INSTALLED_PACKAGES_MSG,
            fail_event_name=OFFLINE_INVALID_INSTALLED_PACKAGES_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        self._process_rest_api_send(
            self.rest_api_client.set_experiment_installed_packages,
            rest_fail_prompt=INSTALLED_PACKAGES_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending installed packages message",
            experiment_key=self.experiment_id,
            installed_packages=message["installed_packages"],
        )

    def _process_html_override_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.html_override_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_HTML_OVERRIDE_MSG,
            fail_event_name=OFFLINE_INVALID_HTML_OVERRIDE_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        self._process_rest_api_send(
            self.rest_api_client.log_experiment_html,
            rest_fail_prompt=HTML_OVERRIDE_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending html override message",
            experiment_key=self.experiment_id,
            html=message["htmlOverride"],
            overwrite=True,
        )

    def _process_gpu_static_info_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.gpu_static_info_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_GPU_STATIC_INFO_MSG,
            fail_event_name=OFFLINE_INVALID_GPU_STATIC_INFO_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        self._process_rest_api_send(
            self.rest_api_client.set_experiment_gpu_static_info,
            rest_fail_prompt=GPU_STATIC_INFO_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending gpu static info message",
            experiment_key=self.experiment_id,
            gpu_static_info=message["gpu_static_info"],
        )

    def _process_git_metadata_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.git_metadata_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_GIT_METADATA_MSG,
            fail_event_name=OFFLINE_INVALID_GIT_METADATA_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        git_metadata = message["git_metadata"]

        self._process_rest_api_send(
            self.rest_api_client.set_experiment_git_metadata,
            rest_fail_prompt=GIT_METADATA_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending git metadata message",
            experiment_key=self.experiment_id,
            user=git_metadata["user"],
            root=git_metadata["root"],
            branch=git_metadata["branch"],
            parent=git_metadata["parent"],
            origin=git_metadata["origin"],
        )

    def _process_system_info_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.system_info_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_SYSTEM_INFO_MSG,
            fail_event_name=OFFLINE_INVALID_SYSTEM_INFO_MSG,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        self._process_rest_api_send(
            self.rest_api_client.log_experiment_system_info,
            rest_fail_prompt=SYSTEM_INFO_MESSAGE_SENDING_ERROR,
            general_fail_prompt="Error sending system info message",
            experiment_key=self.experiment_id,
            system_info=[message["system_info"]],
        )

    def _process_standard_output_message(self, message, offset, message_batch):
        # type: (Dict[str, Any], int, MessageBatch) -> None
        message = message["payload"]

        if not self._validate_msg(
            message=message,
            msg_validator=self.standard_output_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_STANDARD_OUTPUT_MESSAGE,
            fail_event_name=OFFLINE_INVALID_STANDARD_OUTPUT_MESSAGE,
            raise_validation_error=self.raise_validation_error_for_tests,
        ):
            return

        # We know the message is valid, recreate a StandardOutputMessage from it
        stdout_message = StandardOutputMessage.deserialize(message)
        message_batch.append(stdout_message, offset=offset)

    def _send_stdout_message_batch(self, batch_items):
        # type: (List[MessageBatchItem]) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.send_stdout_batch,
            rest_fail_prompt=STANDARD_OUTPUT_SENDING_ERROR,
            general_fail_prompt="Error sending stdout/stderr batch (offline experiment)",
            batch_items=batch_items,
            compress=self.message_batch_compress,
            experiment_key=self.experiment_id,
        )

    def _process_log_dependency_message(self, message):
        # type: (Dict[str, Any]) -> None
        message = message["payload"]

        self._validate_msg(
            message=message,
            msg_validator=self.log_dependency_msg_validator,
            fail_message=OFFLINE_EXPERIMENT_INVALID_LOG_DEPENDENCY_MESSAGE,
            fail_event_name=OFFLINE_INVALID_LOG_DEPENDENCY_MESSAGE,
            raise_validation_error=self.raise_validation_error_for_tests,
        )

        self._process_rest_api_send(
            sender=self.rest_api_client.log_experiment_dependency,
            rest_fail_prompt=LOG_DEPENDENCY_MESSAGE_SENDING_ERROR,
            general_fail_prompt="Error sending log dependency message",
            experiment_key=self.experiment_id,
            name=message["name"],
            version=message["version"],
            timestamp=message["local_timestamp"],
        )

    def _status_report_start(self):
        self.connection.update_experiment_status(
            self.run_id, self.project_id, True, offline=True
        )

    def _status_report_end(self):
        self.connection.update_experiment_status(
            self.run_id, self.project_id, False, offline=True
        )

    def _send_start_ends_time(self):
        # We created a new experiment, update the start time and stop time
        if self._resuming is False:
            self.connection.offline_experiment_start_end_time(
                self.run_id, self.start_time, self.stop_time
            )
        else:
            self.connection.offline_experiment_start_end_time(
                self.run_id, None, self.stop_time
            )

    def _get_experiment_url(self):
        if self.focus_link:
            return self.focus_link + self.experiment_id

        return ""

    def close(self):
        LOGGER.info(WAITING_DATA_UPLOADED)
        if self.ws_connection is not None:
            self.ws_connection.close()
            # Use 3 times the default timeout as they might be a lot of backend messages to process
            ws_cleaned = self.ws_connection.wait_for_finish(
                timeout=DEFAULT_WS_JOIN_TIMEOUT * 3
            )

            if not ws_cleaned:
                LOGGER.error(
                    "Failed to send all messages, metrics and output will likely be incomplete"
                )
                # report experiment error
                self._report_experiment_error()

        self.file_upload_manager.close()
        # Finish remained uploads and display upload progress
        if not self.file_upload_manager.all_done():
            monitor = FileUploadManagerMonitor(self.file_upload_manager)
            LOGGER.info(FILE_UPLOADS_PROMPT)

            wait_for_done(
                monitor.all_done,
                self.file_upload_waiting_timeout,
                progress_callback=monitor.log_remaining_uploads,
                sleep_time=self.wait_for_finish_sleep_interval,
            )

        if not self.file_upload_manager.all_done():
            remaining_upload = self.file_upload_manager.remaining_uploads()
            LOGGER.error(OFFLINE_UPLOADS_FAILED_DUE_TIMEOUT, remaining_upload)

            # report experiment error
            self._report_experiment_error()

        self.file_upload_manager.join()
        LOGGER.debug("Upload threads %r", self.file_upload_manager)

        # close the REST API client last to make sure that experiment error reported if any
        if self.rest_api_client is not None:
            self.rest_api_client.close()

        LOGGER.log(self.display_level, OFFLINE_SENDER_ENDS, self._get_experiment_url())
        LOGGER.log(self.display_level, OFFLINE_SENDER_ENDS_PROCESSING)


def unzip_offline_archive(offline_archive_path):
    temp_dir = tempfile.mkdtemp()

    zip_file = zipfile.ZipFile(offline_archive_path, mode="r", allowZip64=True)

    # Extract the archive
    zip_file.extractall(temp_dir)

    return temp_dir


def upload_single_offline_experiment(
    offline_archive_path,  # type: str
    api_key,  # type: str
    force_reupload,  # type: bool
    display_level="info",  # type: str
    override_workspace=None,  # type: Optional[str]
    override_project_name=None,  # type: Optional[str]
):
    # type: (...) -> bool
    unzipped_directory = unzip_offline_archive(offline_archive_path)
    settings = get_config()
    sender = OfflineSender(
        api_key=api_key,
        offline_dir=unzipped_directory,
        force_reupload=force_reupload,
        display_level=display_level,
        override_workspace=override_workspace,
        override_project_name=override_project_name,
        message_batch_compress=settings.get_bool(
            None, "comet.message_batch.use_compression"
        ),
        message_batch_metric_interval=settings.get_int(
            None, "comet.message_batch.metric_interval"
        ),
        message_batch_metric_max_size=settings.get_int(
            None, "comet.message_batch.metric_max_size"
        ),
    )
    try:
        sender.send()
        sender.close()
        return True
    except ExperimentAlreadyUploaded:
        LOGGER.error(OFFLINE_EXPERIMENT_ALREADY_UPLOADED, offline_archive_path)
        return False
    finally:
        try:
            shutil.rmtree(unzipped_directory)
        except OSError:
            # We made our best effort to clean after ourselves
            msg = "Failed to clean the Offline sender tmpdir %r"
            LOGGER.debug(msg, unzipped_directory, exc_info=True)


def main_upload(
    archives, force_reupload, override_workspace=None, override_project_name=None
):
    # type: (List[str], bool, Optional[str], Optional[str]) -> None
    upload_count = 0
    fail_count = 0

    # Common code
    config = get_config()
    api_key = get_api_key(None, config)

    for filename in archives:
        LOGGER.info(OFFLINE_UPLOADING_EXPERIMENT_FILE_PROMPT, filename)
        try:
            success = upload_single_offline_experiment(
                filename,
                api_key,
                force_reupload,
                override_workspace=override_workspace,
                override_project_name=override_project_name,
            )
            if success:
                upload_count += 1
                LOGGER.info("    Done!")
            else:
                fail_count += 1

        except InvalidAPIKey:
            # raise an exception - no need to continue with other archives as it will fail for them as well
            raise
        except Exception:
            # log exception and continue with other experiments
            LOGGER.error(
                OFFLINE_UPLOAD_FAILED_UNEXPECTED_ERROR,
                filename,
                exc_info=True,
                extra={"show_traceback": True},
            )
            fail_count += 1

    LOGGER.info(OFFLINE_SUCCESS_UPLOADED_EXPERIMENTS, upload_count)
    if fail_count > 0:
        LOGGER.info(OFFLINE_FAILED_UPLOADED_EXPERIMENTS, fail_count)
        raise OfflineExperimentUploadFailed(
            OFFLINE_AT_LEAST_ONE_EXPERIMENT_UPLOAD_FAILED
        )


def _create_zip_file(directory, name):
    file_path = _offline_zip_path(directory, name)
    return ZipFile(file_path, "w", allowZip64=True)


def _offline_zip_path(directory, name):
    return os.path.join(directory, name)
