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
from __future__ import print_function

import logging
import traceback

import sentry_sdk

from ._error_tracking import _setup_sentry_error_tracker
from ._logging import _setup_comet_http_handler
from ._reporting import (
    EXPERIMENT_CREATED,
    EXPERIMENT_CREATION_DURATION,
    EXPERIMENT_CREATION_FAILED,
)
from ._typing import Any, Dict, Optional, Tuple
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
    get_previous_experiment,
    get_ws_url,
)
from .connection import (
    INITIAL_BEAT_DURATION,
    RestApiClient,
    RestServerConnection,
    WebSocketConnection,
    get_backend_address,
    get_rest_api_client,
    log_url,
)
from .exceptions import (
    BadCallbackArguments,
    ExperimentCleaningException,
    ExperimentDisabledException,
    ExperimentNotAlive,
    InvalidAPIKey,
)
from .experiment import BaseExperiment
from .feature_toggles import HTTP_LOGGING, USE_HTTP_MESSAGES, FeatureToggles
from .heartbeat import HeartbeatThread
from .json_encoder import NestedEncoder
from .logging_messages import (
    ADD_SYMLINK_ERROR,
    ADD_TAGS_ERROR,
    ARTIFACT_PREVIEW_FEATURE_FLAG,
    EXPERIMENT_LIVE,
    EXPERIMENT_MARK_AS_ENDED_FAILED,
    EXPERIMENT_MARK_AS_STARTED_FAILED,
    GET_ARTIFACT_VERSION_OR_ALIAS_GIVEN_TWICE,
    GET_ARTIFACT_WORKSPACE_GIVEN_TWICE,
    INTERNET_CONNECTION_ERROR,
    INVALID_API_KEY,
    ONLINE_EXPERIMENT_THROTTLED,
    REGISTER_RPC_FAILED,
    SEND_NOTIFICATION_FAILED,
)
from .rpc import create_remote_call, get_remote_action_definition
from .utils import (
    generate_guid,
    get_time_monotonic,
    make_template_filename,
    merge_url,
    valid_ui_tabs,
)

LOGGER = logging.getLogger(__name__)


class Experiment(BaseExperiment):
    """
    Experiment is a unit of measurable research that defines a single run with some data/parameters/code/results.

    Creating an Experiment object in your code will report a new experiment to your Comet.ml project. Your Experiment
    will automatically track and collect many things and will also allow you to manually report anything.

    You can create multiple objects in one script (such as when looping over multiple hyper parameters).

    """

    def __init__(
        self,
        api_key=None,  # type: Optional[str]
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
        Creates a new experiment on the Comet.ml frontend.
        Args:
            api_key: Your API key obtained from comet.ml
            project_name: Optional. Send your experiment to a specific project. Otherwise will be sent to `Uncategorized Experiments`.
                             If project name does not already exists Comet.ml will create a new project.
            workspace: Optional. Attach an experiment to a project that belongs to this workspace
            log_code: Default(True) - allows you to enable/disable code logging
            log_graph: Default(True) - allows you to enable/disable automatic computation graph logging.
            auto_param_logging: Default(True) - allows you to enable/disable hyper parameters logging
            auto_metric_logging: Default(True) - allows you to enable/disable metrics logging
            auto_metric_step_rate: Default(10) - controls how often batch metrics are logged
            auto_histogram_tensorboard_logging: Default(False) - allows you to enable/disable automatic tensorboard histogram logging
            auto_histogram_epoch_rate: Default(1) - controls how often histograms are logged
            auto_histogram_weight_logging: Default(False) - allows you to enable/disable histogram logging for biases and weights
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
            log_git_patch: Default(True) - allow you to enable/disable the
                automatic collection of git patch
            display_summary_level: Default(1) - control the summary detail that is
                displayed on the console at end of experiment. If 0, the summary
                notification is still sent. Valid values are 0 to 2.
            disabled: Default(False) - allows you to disable all network
                communication with the Comet.ml backend. It is useful when you
                want to test to make sure everything is working, without actually
                logging anything.
            experiment_key: Optional. If provided, will be used as the experiment key. If an experiment
                with the same key already exists, it will raises an Exception. Could be set through
                configuration as well.
        """
        self._startup_duration_start = get_time_monotonic()
        self.config = get_config()

        self.api_key = get_api_key(api_key, self.config)

        if self.api_key is None:
            raise ValueError(
                "Comet.ml requires an API key. Please provide as the "
                "first argument to Experiment(api_key) or as an environment"
                " variable named COMET_API_KEY "
            )

        super(Experiment, self).__init__(
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
            display_summary=display_summary,  # deprecated
            display_summary_level=display_summary_level,
            log_env_cpu=log_env_cpu,
            optimizer_data=optimizer_data,
            auto_weight_logging=auto_weight_logging,  # deprecated
            auto_log_co2=auto_log_co2,
            auto_metric_step_rate=auto_metric_step_rate,
            auto_histogram_tensorboard_logging=auto_histogram_tensorboard_logging,
            auto_histogram_epoch_rate=auto_histogram_epoch_rate,
            auto_histogram_weight_logging=auto_histogram_weight_logging,
            auto_histogram_gradient_logging=auto_histogram_gradient_logging,
            auto_histogram_activation_logging=auto_histogram_activation_logging,
            experiment_key=experiment_key,
        )

        self.ws_connection = None  # type: Optional[WebSocketConnection]
        self.connection = None  # type: Optional[RestServerConnection]
        self.rest_api_client = None  # type: Optional[RestApiClient]
        self._heartbeat_thread = None  # type: Optional[HeartbeatThread]

        self._check_tls_certificate = self.config.get_bool(
            None, "comet.internal.check_tls_certificate"
        )

        self._sentry_client = None

        if self.disabled is not True:
            self._start()

            if self.alive is True:
                self._report(event_name=EXPERIMENT_CREATED)

                startup_duration = get_time_monotonic() - self._startup_duration_start
                self._report(
                    event_name=EXPERIMENT_CREATION_DURATION,
                    err_msg=str(startup_duration),
                )

                LOGGER.info(EXPERIMENT_LIVE, self._get_experiment_url())
                LOGGER.debug(
                    "communication exclusively via http(s): %s"
                    % self.config.get_bool(
                        None, "comet.override_feature.sdk_use_http_messages"
                    )
                )

    def _setup_http_handler(self):
        if not self.feature_toggles[HTTP_LOGGING]:
            LOGGER.debug("Do not setup http logger, disabled by feature toggle")
            return

        self.http_handler = _setup_comet_http_handler(
            log_url(get_backend_address()), self.api_key, self.id
        )

    def _setup_streamer(self):
        """
        Do the necessary work to create mandatory objects, like the streamer
        and feature flags
        """

        server_address = get_backend_address(self.config)

        self.connection = RestServerConnection(
            self.api_key,
            self.id,
            server_address,
            self.config["comet.timeout.http"],
            verify_tls=self._check_tls_certificate,
        )
        self.rest_api_client = get_rest_api_client(
            "v2",
            api_key=self.api_key,
            use_cache=False,
            headers={"X-COMET-SDK-SOURCE": "Experiment"},
        )

        try:
            results = self._authenticate()
            if results is not None:
                authenticated, ws_server_from_backend, initial_offset = results
            else:
                authenticated, ws_server_from_backend, initial_offset = (
                    False,
                    None,
                    None,
                )
        except ValueError:
            tb = traceback.format_exc()
            LOGGER.error(INTERNET_CONNECTION_ERROR, exc_info=True)
            self._report(event_name=EXPERIMENT_CREATION_FAILED, err_msg=tb)
            return False

        # Authentication failed somehow
        if not authenticated:
            return False

        # Setup the sentry client as soon as possible to make sure we catch as many potential errors as possible
        # TODO: Read the sentry DSN from the backend and pass it there - CM-3142
        self._setup_error_tracking(server_address)

        # Setup the HTTP handler
        self._setup_http_handler()

        # Initiate the streamer
        ws_server = get_ws_url(ws_server_from_backend, self.config)
        full_ws_url = format_url(ws_server, apiKey=self.api_key, runId=self.run_id)
        use_http_messages = self.feature_toggles[USE_HTTP_MESSAGES]

        self._initialize_streamer(
            full_ws_url=full_ws_url,
            initial_offset=initial_offset,
            use_http_messages=use_http_messages,
        )

        # Initiate the heartbeat thread
        self._heartbeat_thread = HeartbeatThread(
            INITIAL_BEAT_DURATION / 1000.0,
            self.connection,
            self._on_pending_rpcs_callback,
        )

        # setup parameters update callback
        self._heartbeat_thread.on_parameters_update_interval_callback = (
            self.streamer.parameters_batch.update_interval
        )

        self._heartbeat_thread.start()
        return True

    def _setup_error_tracking(self, server_address):
        # type: (str) -> None
        """Setup error tracking"""

        sentry_dsn = self.config.get_string(None, "comet.internal.sentry_dsn")
        sentry_enabled = self.config.get_bool(None, "comet.error_tracking.enable")

        if not sentry_dsn:
            return

        if not sentry_enabled:
            return

        if not self.feature_toggles:
            feature_toggles = {}
        else:
            feature_toggles = self.feature_toggles.raw_toggles

        try:
            self._sentry_client = _setup_sentry_error_tracker(
                sentry_dsn,
                self.id,
                server_address,
                debug=self.config.get_bool(None, "comet.internal.sentry_debug"),
                feature_toggles=feature_toggles,
            )
        except Exception:
            LOGGER.warning("Error setting up error tracker", exc_info=True)

    def _authenticate(self):
        # type: () -> Optional[Tuple[bool, str, int]]
        """
        Do the handshake with the Backend to authenticate the api key and get
        various parameters and settings
        """
        # Get an id for this run
        try:
            run_id_response = self._get_run_id()

            self.run_id = run_id_response.run_id
            self.project_id = run_id_response.project_id
            self.is_github = run_id_response.is_github
            self.focus_link = run_id_response.focus_link
            self.upload_limit = run_id_response.upload_limit
            self.asset_upload_limit = run_id_response.asset_upload_limit
            self.upload_web_asset_url_prefix = run_id_response.web_asset_url
            self.upload_web_image_url_prefix = run_id_response.web_image_url
            self.upload_api_asset_url_prefix = run_id_response.api_asset_url
            self.upload_api_image_url_prefix = run_id_response.api_image_url
            self.name = run_id_response.experiment_name
        except InvalidAPIKey as e:
            backend_host = self.connection.server_hostname()
            LOGGER.error(INVALID_API_KEY, e.api_key, backend_host, exc_info=True)
            # discard invalid API key
            discard_api_key(e.api_key)
            raise e

        self.feature_toggles = FeatureToggles(
            run_id_response.feature_toggles, self.config
        )

        authenticated = run_id_response.run_id is not None
        return (
            authenticated,
            run_id_response.ws_server,
            run_id_response.last_offset,
        )

    def _get_run_id(self):
        return self.connection.get_run_id(self.project_name, self.workspace)

    def _initialize_streamer(self, full_ws_url, initial_offset, use_http_messages):
        """
        Initialize the streamer with the websocket url received during the
        handshake.
        """
        # Start WS connection if appropriate
        if not use_http_messages:
            LOGGER.debug(
                "Create and start WebSocket connection, enabled by feature toggle"
            )
            self.ws_connection = WebSocketConnection(
                full_ws_url, self.connection, verify_tls=self._check_tls_certificate
            )
            self.ws_connection.start()
            self.ws_connection.wait_for_connection()
        else:
            LOGGER.debug(
                "Do not create and start WebSocket connection, disabled by feature toggle"
            )

        self.streamer = Streamer(
            ws_connection=self.ws_connection,
            beat_duration=INITIAL_BEAT_DURATION,
            connection=self.connection,
            initial_offset=initial_offset,
            experiment_key=self.id,
            api_key=self.api_key,
            run_id=self.run_id,
            project_id=self.project_id,
            rest_api_client=self.rest_api_client,
            worker_cpu_ratio=self.config.get_int(
                None, "comet.internal.file_upload_worker_ratio"
            ),
            worker_count=self.config.get_raw(None, "comet.internal.worker_count"),
            verify_tls=self._check_tls_certificate,
            pending_rpcs_callback=self._on_pending_rpcs_callback,
            msg_waiting_timeout=self.config["comet.timeout.cleaning"],
            file_upload_waiting_timeout=self.config["comet.timeout.upload"],
            file_upload_read_timeout=self.config.get_int(
                None, "comet.timeout.file_upload"
            ),
            use_http_messages=use_http_messages,
            message_batch_compress=self.config.get_bool(
                None, "comet.message_batch.use_compression"
            ),
            message_batch_metric_interval=self.config.get_int(
                None, "comet.message_batch.metric_interval"
            ),
            message_batch_metric_max_size=self.config.get_int(
                None, "comet.message_batch.metric_max_size"
            ),
            parameters_batch_base_interval=self.config.get_int(
                None, "comet.message_batch.parameters_interval"
            ),
            message_batch_stdout_interval=self.config.get_int(
                None, "comet.message_batch.stdout_interval"
            ),
            message_batch_stdout_max_size=self.config.get_int(
                None, "comet.message_batch.stdout_max_size"
            ),
        )

        # Start streamer thread.
        self.streamer.start()

    def _mark_as_started(self):
        try:
            self.connection.update_experiment_status(
                self.run_id, self.project_id, self.alive
            )
        except Exception:
            LOGGER.error(EXPERIMENT_MARK_AS_STARTED_FAILED, exc_info=True)
            self._report_experiment_error(EXPERIMENT_MARK_AS_STARTED_FAILED)

    def _start_gpu_thread(self):
        super(Experiment, self)._start_gpu_thread()

        if not self.alive:
            return

        # Connect heartbeat thread and the gpu thread
        if self._heartbeat_thread is not None and self.gpu_thread is not None:
            self._heartbeat_thread.on_gpu_monitor_interval_callback = (
                self.gpu_thread.update_interval
            )

    def _start_cpu_thread(self):
        super(Experiment, self)._start_cpu_thread()

        if not self.alive:
            return

        # Connect heartbeat thread and the cpu thread
        if self._heartbeat_thread is not None and self.cpu_thread is not None:
            self._heartbeat_thread.on_cpu_monitor_interval_callback = (
                self.cpu_thread.update_interval
            )

    def _mark_as_ended(self):
        if self.alive:
            try:
                self.connection.update_experiment_status(
                    self.run_id, self.project_id, False
                )
            except Exception:
                LOGGER.error(EXPERIMENT_MARK_AS_ENDED_FAILED, exc_info=True)
                # report error
                self._report_experiment_error(EXPERIMENT_MARK_AS_ENDED_FAILED)

    def _report_experiment_error(self, message=None):
        if not self.alive:
            return
        if self.streamer is None:
            return
        self.streamer._report_experiment_error(message)

    def _report(self, *args, **kwargs):
        self.connection.report(*args, **kwargs)

    def _on_end(self, wait=True):
        """Called when the Experiment is replaced by another one or at the
        end of the script
        """
        successful_clean = super(Experiment, self)._on_end(wait=wait)

        if not successful_clean:
            LOGGER.warning("Failed to log run in comet.ml")
        else:
            if self.alive:
                LOGGER.info(EXPERIMENT_LIVE, self._get_experiment_url())

            # If we didn't drain the streamer, don't close the websocket connection
            if self.ws_connection is not None and wait is True:
                self.ws_connection.close()
                LOGGER.debug("Waiting for WS connection to close")
                if wait is True:
                    ws_cleaned = self.ws_connection.wait_for_finish()
                    if ws_cleaned is True:
                        LOGGER.debug("Websocket connection clean successfully")
                    else:
                        LOGGER.debug("Websocket connection DIDN'T clean successfully")
                        successful_clean = False
                        self.ws_connection.force_close()

            if self._heartbeat_thread is not None:
                self._heartbeat_thread.close()
                if wait is True:
                    self._heartbeat_thread.join(10)

            if self.connection is not None:
                self.connection.close()

        # Display throttling message
        try:
            if self._check_experiment_throttled():
                LOGGER.warning(ONLINE_EXPERIMENT_THROTTLED)
        except Exception:
            LOGGER.debug("Failed to check experiment metadata", exc_info=True)

        try:
            client = sentry_sdk.Hub.current.client
            if client is not None:
                client.close(timeout=10)
        except Exception:
            LOGGER.debug("Error closing Sentry error tracking", exc_info=True)

        if self.streamer is not None and self.streamer.has_failed():
            raise ExperimentCleaningException(
                "Failed to successfully send all of the artifact's assets"
            )

        return successful_clean

    def _check_experiment_throttled(self):
        experiment_metadata = self.rest_api_client.get_experiment_metadata(self.id)
        return experiment_metadata.get("throttle", False)

    @property
    def url(self):
        """
        Get the url of the experiment.

        Example:

        ```python
        >>> api_experiment.url
        "https://www.comet.ml/username/34637643746374637463476"
        ```
        """
        return self._get_experiment_url()

    def _get_experiment_url(self, tab=None):
        if self.focus_link:
            if tab:
                if tab in valid_ui_tabs():
                    return merge_url(
                        self.focus_link + self.id,
                        {"experiment-tab": valid_ui_tabs(tab)},
                    )
                else:
                    LOGGER.info("tab must be one of: %r", valid_ui_tabs(preferred=True))
            return self.focus_link + self.id

        return ""

    def _on_pending_rpcs_callback(self):
        """Called by streamer when we have pending rpcs"""
        LOGGER.debug("Checking pending rpcs")
        calls = self.connection.get_pending_rpcs()["remoteProcedureCalls"]

        LOGGER.debug("Got pending rpcs: %r", calls)
        for raw_call in calls:
            call = create_remote_call(raw_call)
            if call is None:
                continue
            self._add_pending_call(call)

    def _send_rpc_callback_result(
        self, call_id, remote_call_result, start_time, end_time
    ):
        # Send the result to the backend
        self.connection.send_rpc_result(
            call_id, remote_call_result, start_time, end_time
        )

    def create_symlink(self, project_name):
        """
        creates a symlink for this experiment in another project.
        The experiment will now be displayed in the project provided and the original project.

        Args:
            project_name: String. represents the project name. Project must exists.
        """
        try:
            if self.alive:
                self.connection.send_new_symlink(project_name)
        except Exception:
            LOGGER.warning(ADD_SYMLINK_ERROR, project_name, exc_info=True)
            # report error
            self._report_experiment_error(ADD_SYMLINK_ERROR)

    def add_tag(self, tag):
        """
        Add a tag to the experiment. Tags will be shown in the dashboard.
        Args:
            tag: String. A tag to add to the experiment.
        """
        try:
            if self.alive:
                self.connection.add_tags([tag])

            super(Experiment, self).add_tag(tag)
        except Exception:
            LOGGER.warning(ADD_TAGS_ERROR, tag, exc_info=True)
            # report error
            self._report_experiment_error(ADD_TAGS_ERROR)

    def add_tags(self, tags):
        """
        Add several tags to the experiment. Tags will be shown in the
        dashboard.
        Args:
            tag: List<String>. Tags list to add to the experiment.
        """
        try:
            if self.alive:
                self.connection.add_tags(tags)

            # If we successfully send them to the backend, save them locally
            super(Experiment, self).add_tags(tags)
        except Exception:
            LOGGER.warning(ADD_TAGS_ERROR, tags, exc_info=True)
            # report error
            self._report_experiment_error(ADD_TAGS_ERROR)

    def register_callback(self, remote_action):
        """
        Register the remote_action passed as argument to be a RPC.
        Args:
            remote_action: Callable.
        """
        super(Experiment, self).register_callback(remote_action)

        try:
            remote_action_definition = get_remote_action_definition(remote_action)
        except BadCallbackArguments as exc:
            # Don't keep bad callbacks registered
            self.unregister_callback(remote_action)
            LOGGER.warning(str(exc), exc_info=True)
            return

        try:
            self._register_callback_remotely(remote_action_definition)
        except Exception:
            # Don't keep bad callbacks registered
            self.unregister_callback(remote_action)
            LOGGER.warning(
                REGISTER_RPC_FAILED, remote_action_definition["functionName"]
            )
            # report error
            self._report_experiment_error(
                REGISTER_RPC_FAILED % remote_action_definition["functionName"]
            )

    def _register_callback_remotely(self, remote_action_definition):
        self.connection.register_rpc(remote_action_definition)

    def send_notification(self, title, status=None, additional_data=None):
        # type: (str, Optional[str], Optional[Dict[str, Any]]) -> None
        """
        Send yourself a notification through email when an experiment
        ends.

        Args:
            title: str - the email subject.
            status: str - the final status of the experiment. Typically,
                something like "finished", "completed" or "aborted".
            additional_data: dict - a dictionary of key/values to notify.

        Note:
            In order to receive the notification, you need to have turned
            on Notifications in your Settings in the Comet user interface.

        You can programmatically send notifications at any time during the
        lifecycle of an experiment.

        Example:

        ```python
        experiment = Experiment()

        experiment.send_notification(
            "Experiment %s" % experiment.get_key(),
            "started"
        )
        try:
            train(...)
            experiment.send_notification(
                "Experiment %s" % experiment.get_key(),
                "completed successfully"
            )
        except Exception:
            experiment.send_notification(
                "Experiment %s" % experiment.get_key(),
                "failed"
            )
        ```

        If you wish to have the `additional_data` saved with the
        experiment, you should also call `Experiment.log_other()` with
        this data as well.

        This method uses the email address associated with your account.
        """
        try:
            name = self.others.get("Name")

            if additional_data is None:
                additional_data = {}

            self.connection.send_notification(
                title,
                status,
                name,
                self._get_experiment_url(),
                additional_data,
                custom_encoder=NestedEncoder,
            )

        except Exception:
            LOGGER.error(SEND_NOTIFICATION_FAILED, exc_info=True)
            # report error
            self._report_experiment_error(SEND_NOTIFICATION_FAILED)

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
        Log a multi-dimensional dataset and metadata for viewing with
        Comet's Embedding Projector (experimental).

        Args:
            vectors: the tensors to visualize in 3D
            labels: labels for each tensor
            image_data: (optional) list of arrays or Images
            image_size: (optional, required if image_data is given) the size of each image
            image_preprocess_function: (optional) if image_data is an
                array, apply this function to each element first
            image_transparent_color: a (red, green, blue) tuple
            image_background_color_function: a function that takes an
                index, and returns a (red, green, blue) color tuple
            title: (optional) name of tensor
            template_filename: (optional) name of template JSON file
            group: (optional) name of group of embeddings

        See also: `Experiment._log_embedding_list()` and `comet_ml.Embedding`

        Example:

        ```python
        from comet_ml import Experiment

        import numpy as np
        from keras.datasets import mnist

        (x_train, y_train), (x_test, y_test) = mnist.load_data()

        def label_to_color(index):
            label = y_test[index]
            if label == 0:
                return (255, 0, 0)
            elif label == 1:
                return (0, 255, 0)
            elif label == 2:
                return (0, 0, 255)
            elif label == 3:
                return (255, 255, 0)
            elif label == 4:
                return (0, 255, 255)
            elif label == 5:
                return (128, 128, 0)
            elif label == 6:
                return (0, 128, 128)
            elif label == 7:
                return (128, 0, 128)
            elif label == 8:
                return (255, 0, 255)
            elif label == 9:
                return (255, 255, 255)

        experiment = Experiment(project_name="projector-embedding")

        experiment.log_embedding(
            vectors=x_test,
            labels=y_test,
            image_data=x_test,
            image_preprocess_function=lambda matrix: np.round(matrix/255,0) * 2,
            image_transparent_color=(0, 0, 0),
            image_size=(28, 28),
            image_background_color_function=label_to_color,
        )
        ```
        """
        if not self.alive:
            return None

        LOGGER.warning(
            "Logging embedding is experimental - the API and logged data are subject to change"
        )

        embedding = self._create_embedding(
            vectors,
            labels,
            image_data,
            image_size,
            image_preprocess_function,
            image_transparent_color,
            image_background_color_function,
            title,
        )

        if embedding is None:
            return None

        if group is not None:
            self._embedding_groups[group].append(embedding)
            return embedding
        else:
            # Log the template:
            template = {"embeddings": [embedding.to_json()]}
            if template_filename is None:
                template_filename = make_template_filename()

            return self._log_asset_data(
                template, template_filename, asset_type="embeddings"
            )

    def log_artifact(self, artifact):
        # type: (Artifact) -> LoggedArtifact
        """
        Log an Artifact object, synchronously create a new Artifact Version and upload
        asynchronously all local and remote assets attached to the Artifact object.

        Args:
            artifact: an Artifact object

        Returns: a [LoggedArtifact](/docs/python-sdk/LoggedArtifact/)
        """

        if self.disabled:
            raise ExperimentDisabledException(
                "Experiment %r is disabled, cannot log artifact" % self
            )
        elif not self.alive:
            raise ExperimentNotAlive(
                "Experiment %r is not alive, cannot log artifact" % self
            )

        if not isinstance(artifact, Artifact):
            raise ValueError("%r is not an Artifact and cannot be logged" % artifact)

        if not self.config["comet.override_feature.artifacts"]:
            LOGGER.warning(ARTIFACT_PREVIEW_FEATURE_FLAG)

        return _log_artifact(artifact, self)

    def get_artifact(
        self,
        artifact_name,
        workspace=None,
        version_or_alias=None,
    ):
        # type: (str, Optional[str], Optional[str]) -> LoggedArtifact
        """Returns a logged artifact object that can be used to access the artifact version assets and
        download them locally.

        If no version or alias is provided, the latest version for that artifact is returned.

        Args:
            artifact_name: Retrieve an artifact with that name. This could either be a fully
                qualified artifact name like `workspace/artifact-name:versionOrAlias` or just the name
                of the artifact like `artifact-name`.
            workspace: Retrieve an artifact belonging to that workspace
            version_or_alias: Optional. Retrieve the artifact by the given alias or version.

        Returns: the LoggedArtifact
        For example:

        ```python
        logged_artifact = experiment.get_artifact("workspace/artifact-name:version_or_alias")
        ```

        Which is equivalent to:

        ```python
        logged_artifact = experiment.get_artifact(
            artifact_name="artifact-name",
            workspace="workspace",
            version_or_alias="version_or_alias")
        ```
        """
        if self.disabled:
            raise ExperimentDisabledException(
                "Experiment %r is disabled, cannot get artifact" % self
            )
        elif not self.alive:
            raise ExperimentNotAlive(
                "Experiment %r is not alive, cannot get artifact" % self
            )

        if not self.config["comet.override_feature.artifacts"]:
            LOGGER.warning(ARTIFACT_PREVIEW_FEATURE_FLAG)

        # Parse the artifact_name
        parsed_workspace, artifact_name, parsed_version_or_alias = _parse_artifact_name(
            artifact_name
        )

        params = {}  # type: Dict[str, Optional[str]]

        if parsed_workspace is None and workspace is None:
            # In that case, the backend will use the experiment id to get the workspace
            param_workspace = None
        elif parsed_workspace is not None and workspace is not None:
            if parsed_workspace != workspace:
                LOGGER.warning(
                    GET_ARTIFACT_WORKSPACE_GIVEN_TWICE
                    % (parsed_workspace, artifact_name)
                )
            param_workspace = workspace
        elif workspace is None:
            param_workspace = parsed_workspace
        else:
            param_workspace = workspace

        if parsed_version_or_alias is not None and version_or_alias is not None:
            if parsed_version_or_alias != version_or_alias:
                LOGGER.warning(
                    GET_ARTIFACT_VERSION_OR_ALIAS_GIVEN_TWICE
                    % (parsed_version_or_alias, artifact_name)
                )
            param_version_or_alias = version_or_alias
        elif parsed_version_or_alias is not None:
            param_version_or_alias = parsed_version_or_alias
        else:
            param_version_or_alias = version_or_alias

        params = {
            "consumer_experiment_key": self.id,
            "experiment_key": self.id,
            "name": artifact_name,
            "version_or_alias": param_version_or_alias,
            "workspace": param_workspace,
        }

        logged_artifact = _get_artifact(
            self.rest_api_client, params, self.id, self._summary, self.config
        )

        self._summary.increment_section("downloads", "artifacts")

        return logged_artifact


class ExistingExperiment(Experiment):
    """Existing Experiment allows you to report information to an
    experiment that already exists on comet.ml and is not currently
    running. This is useful when your training and testing happen on
    different scripts.

    For example:

    train.py:
    ```
    exp = Experiment(api_key="my-key")
    score = train_model()
    exp.log_metric("train accuracy", score)
    ```

    Now obtain the experiment key from comet.ml. If it's not visible
    on your experiment table you can click `Customize` and add it as a
    column.


    test.py:
    ```
    exp = ExistingExperiment(api_key="my-key",
             previous_experiment="your experiment key from comet.ml")
    score = test_model()
    exp.log_metric("test accuracy", score)
    ```

    Alternatively, you can pass the api_key via an environment
    variable named `COMET_API_KEY` and the previous experiment id via
    an environment variable named `COMET_EXPERIMENT_KEY` and omit them
    from the ExistingExperiment constructor:

    ```
    exp = ExistingExperiment()
    score = test_model()
    exp.log_metric("test accuracy", score)
    ```

    """

    def __init__(self, api_key=None, previous_experiment=None, **kwargs):
        """
        Append to an existing experiment on the Comet.ml frontend.
        Args:
            api_key: Your API key obtained from comet.ml
            previous_experiment: Deprecated. Use `experiment_key` instead.
            project_name: Optional. Send your experiment to a specific project. Otherwise will be sent to `Uncategorized Experiments`.
                             If project name does not already exists Comet.ml will create a new project.
            workspace: Optional. Attach an experiment to a project that belongs to this workspace
            log_code: Default(False) - allows you to enable/disable code logging
            log_graph: Default(False) - allows you to enable/disable automatic computation graph logging.
            auto_param_logging: Default(True) - allows you to enable/disable hyper parameters logging
            auto_metric_logging: Default(True) - allows you to enable/disable metrics logging
            auto_metric_step_rate: Default(10) - controls how often batch metrics are logged
            auto_histogram_tensorboard_logging: Default(False) - allows you to enable/disable automatic tensorboard histogram logging
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
            parse_args: Default(False) - allows you to enable/disable automatic parsing of CLI arguments
            log_env_details: Default(False) - log various environment
                information in order to identify where the script is running
            log_env_gpu: Default(False) - allow you to enable/disable the
                automatic collection of gpu details and metrics (utilization, memory usage etc..).
                `log_env_details` must also be true.
            log_env_cpu: Default(False) - allow you to enable/disable the
                automatic collection of cpu details and metrics (utilization, memory usage etc..).
                `log_env_details` must also be true.
            log_env_host: Default(False) - allow you to enable/disable the
                automatic collection of host information (ip, hostname, python version, user etc...).
                `log_env_details` must also be true.
            log_git_metadata: Default(False) - allow you to enable/disable the
                automatic collection of git details
            log_git_patch: Default(False) - allow you to enable/disable the
                automatic collection of git patch
            display_summary_level: Default(1) - control the summary detail that is
                displayed on the console at end of experiment. If 0, the summary
                notification is still sent. Valid values are 0 to 2.
            disabled: Default(False) - allows you to disable all network
                communication with the Comet.ml backend. It is useful when you
                just needs to works on your machine-learning scripts and need
                to relaunch them several times at a time.
            experiment_key: Optional. Your experiment key from comet.ml, could be set through
                configuration as well.

        Note: ExistingExperiment does not alter nor destroy previously
        logged information. To override or add to previous information
        you will have to set the appropriate following parameters to True:

        * log_code
        * log_graph
        * parse_args
        * log_env_details
        * log_git_metadata
        * log_git_patch
        * log_env_gpu
        * log_env_cpu
        * log_env_host

        For example, to continue to collect GPU information in an
        `ExistingExperiment` you will need to override these parameters:

        ```python
        >>> experiment = ExistingExperiment(
        ...                 log_env_details=True,
        ...                 log_env_gpu=True)
        ```
        """
        # Validate the previous experiment id
        self.config = get_config()

        if previous_experiment is not None and "experiment_key" in kwargs:
            # TODO: SHOW LOG MESSAGE?
            pass

        # TODO: Document the parameter
        self.step_copy = kwargs.pop("step_copy", None)

        self.previous_experiment = None
        if "experiment_key" in kwargs:
            self.previous_experiment = kwargs["experiment_key"]
        elif previous_experiment is not None:
            self.previous_experiment = previous_experiment
            kwargs["experiment_key"] = previous_experiment

        self.previous_experiment = get_previous_experiment(
            self.previous_experiment, self.config
        )

        if not is_valid_experiment_key(self.previous_experiment):
            raise ValueError("Invalid experiment key: %s" % self.previous_experiment)

        kwargs["experiment_key"] = self.previous_experiment

        ## Defaults for ExistingExperiment:
        ## For now, don't destroy previous Experiment information by default:

        for (key, config_name, default) in [
            ("log_code", "comet.auto_log.code", False),
            ("log_graph", "comet.auto_log.graph", False),
            ("parse_args", "comet.auto_log.cli_arguments", False),
            ("log_env_details", "comet.auto_log.env_details", False),
            ("log_git_metadata", "comet.auto_log.git_metadata", False),
            ("log_git_patch", "comet.auto_log.git_patch", False),
            ("log_env_gpu", "comet.auto_log.env_gpu", False),
            ("log_env_cpu", "comet.auto_log.env_cpu", False),
            ("log_env_host", "comet.auto_log.env_host", False),
        ]:
            if key not in kwargs or kwargs[key] is None:
                kwargs[key] = self.config.get_bool(
                    None, config_name, default, not_set_value=None
                )

        super(ExistingExperiment, self).__init__(api_key, **kwargs)

    def _get_experiment_key(self, user_experiment_key):
        # type: (Optional[str]) -> str
        # In this case we know user_experiment_key is not None as we validated it in __init__
        assert user_experiment_key is not None

        # If we don't copy the experiment, uses the same experiment ID
        if self.step_copy is None:
            return user_experiment_key
        else:
            # When copying, generate a new one
            return generate_guid()

    def _get_run_id(self):
        if self.step_copy is None:
            return self.connection.get_old_run_id(self.previous_experiment)
        else:
            return self.connection.copy_run(self.previous_experiment, self.step_copy)

    def send_notification(self, *args, **kwargs):
        """
        With an `Experiment`, this method will send you a notification
        through email when an experiment ends. However, with an
        `ExistingExperiment` this method does nothing.
        """
        pass
