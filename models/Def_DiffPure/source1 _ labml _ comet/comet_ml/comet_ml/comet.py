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
Author: Gideon Mendels

This module contains the main components of comet.ml client side

"""
import abc
import logging
import os
import shutil
import tempfile
import threading
import time
from os.path import basename, splitext

from six.moves.queue import Empty, Queue
from six.moves.urllib.parse import urlencode, urlsplit, urlunsplit

from ._reporting import ON_EXIT_DIDNT_FINISH_UPLOAD_SDK
from ._typing import Any, AnyStr, Callable, Dict, List, Optional, Tuple, Union
from .batch_utils import MessageBatch, MessageBatchItem, ParametersBatch
from .compat_utils import json_dump
from .config import (
    ADDITIONAL_STREAMER_UPLOAD_TIMEOUT,
    DEFAULT_FILE_UPLOAD_READ_TIMEOUT,
    DEFAULT_PARAMETERS_BATCH_INTERVAL_SECONDS,
    DEFAULT_STREAMER_MSG_TIMEOUT,
    DEFAULT_WAIT_FOR_FINISH_SLEEP_INTERVAL,
    MESSAGE_BATCH_METRIC_INTERVAL_SECONDS,
    MESSAGE_BATCH_METRIC_MAX_BATCH_SIZE,
    MESSAGE_BATCH_STDOUT_INTERVAL_SECONDS,
    MESSAGE_BATCH_STDOUT_MAX_BATCH_SIZE,
    MESSAGE_BATCH_USE_COMPRESSION_DEFAULT,
    OFFLINE_EXPERIMENT_MESSAGES_JSON_FILE_NAME,
)
from .connection import (
    FileUploadManager,
    FileUploadManagerMonitor,
    RestServerConnection,
    format_messages_for_ws,
)
from .convert_utils import data_to_fp
from .exceptions import CometRestApiException
from .file_uploader import is_user_text
from .json_encoder import NestedEncoder
from .logging_messages import (
    CLOUD_DETAILS_MSG_SENDING_ERROR,
    FAILED_SEND_METRICS_BATCH_AT_EXPERIMENT_END,
    FAILED_SEND_PARAMETERS_BATCH_AT_EXPERIMENT_END,
    FAILED_SEND_STDOUT_BATCH_AT_EXPERIMENT_END,
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
    OFFLINE_SENDER_FAILED_TO_WRITE_ALL_DATA,
    OFFLINE_SENDER_REMAINING_DATA_ITEMS_TO_WRITE,
    OFFLINE_SENDER_WAIT_FOR_FINISH_PROMPT,
    OS_PACKAGE_MSG_SENDING_ERROR,
    PARAMETERS_BATCH_MSG_SENDING_ERROR,
    STANDARD_OUTPUT_SENDING_ERROR,
    STREAMER_CLOSED_PUT_MESSAGE_FAILED,
    STREAMER_FAILED_TO_PROCESS_ALL_MESSAGES,
    STREAMER_WAIT_FOR_FINISH_FAILED,
    SYSTEM_DETAILS_MSG_SENDING_ERROR,
    SYSTEM_INFO_MESSAGE_SENDING_ERROR,
    UNEXPECTED_STREAMING_ERROR,
    WAITING_DATA_UPLOADED,
)
from .messages import (
    BaseMessage,
    CloseMessage,
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
    UploadInMemoryMessage,
    WebSocketMessage,
)
from .utils import log_once_at_level, wait_for_done, write_file_like_to_tmp_file

DEBUG = False
LOGGER = logging.getLogger(__name__)


class BaseStreamer(threading.Thread):
    __metaclass__ = abc.ABCMeta

    def __init__(self, initial_offset, queue_timeout, use_http_messages=False):
        threading.Thread.__init__(self)

        self.counter = initial_offset
        self.messages = Queue()  # type: Queue
        self.queue_timeout = queue_timeout
        self.closed = False
        self.use_http_messages = use_http_messages
        self.__lock__ = threading.RLock()

        LOGGER.debug("%r instantiated with duration %s", self, self.queue_timeout)

    def put_message_in_q(self, message):
        """
        Puts a message in the queue
        :param message: Some kind of payload, type agnostic
        """
        with self.__lock__:
            if message is not None:
                if not self.closed:
                    self.messages.put(message)
                else:
                    LOGGER.debug(STREAMER_CLOSED_PUT_MESSAGE_FAILED)
                    LOGGER.debug("Ignored message (streamer closed): %s", message)

    def close(self):
        """
        Puts a None in the queue which leads to closing it.
        """
        with self.__lock__:
            if self.closed is True:
                LOGGER.debug("Streamer tried to be closed more than once")
                return

            # Send a message to close
            self.put_message_in_q(CloseMessage())

            self.closed = True

    def _before_run(self):
        pass

    def run(self):
        """
        Continuously pulls messages from the queue and process them.
        """
        self._before_run()

        while True:
            out = self._loop()

            # Exit the infinite loop
            if isinstance(out, CloseMessage):
                break

        self._after_run()

        LOGGER.debug("%s has finished", self.__class__)

        return

    @abc.abstractmethod
    def _loop(self):
        pass

    def _after_run(self):
        pass

    def getn(self, n):
        # type: (int) -> Optional[List[Tuple[BaseMessage, int]]]
        """
        Pops n messages from the queue.
        Args:
            n: Number of messages to pull from queue

        Returns: n messages

        """
        try:
            msg = self.messages.get(
                timeout=self.queue_timeout
            )  # block until at least 1
        except Empty:
            LOGGER.debug("No message in queue, timeout")
            return None

        if isinstance(msg, CloseMessage):
            return [(msg, self.counter + 1)]

        self.counter += 1
        result = [(msg, self.counter)]
        try:
            while len(result) < n:
                another_msg = self.messages.get(
                    block=False
                )  # don't block if no more messages
                self.counter += 1
                result.append((another_msg, self.counter))
        except Exception:
            LOGGER.debug("Exception while getting more than 1 message", exc_info=True)
        return result


class Streamer(BaseStreamer):
    """
    This class extends threading.Thread and provides a simple concurrent queue
    and an async service that pulls data from the queue and sends it to the server.
    """

    def __init__(
        self,
        ws_connection,
        beat_duration,
        connection,  # type: RestServerConnection
        initial_offset,
        experiment_key,
        api_key,
        run_id,
        project_id,
        rest_api_client,
        worker_cpu_ratio,  # type: int
        worker_count,  # type: Optional[int]
        verify_tls,  # type: bool
        pending_rpcs_callback=None,
        msg_waiting_timeout=DEFAULT_STREAMER_MSG_TIMEOUT,
        file_upload_waiting_timeout=ADDITIONAL_STREAMER_UPLOAD_TIMEOUT,
        file_upload_read_timeout=DEFAULT_FILE_UPLOAD_READ_TIMEOUT,
        wait_for_finish_sleep_interval=DEFAULT_WAIT_FOR_FINISH_SLEEP_INTERVAL,
        parameters_batch_base_interval=DEFAULT_PARAMETERS_BATCH_INTERVAL_SECONDS,
        use_http_messages=False,
        message_batch_compress=MESSAGE_BATCH_USE_COMPRESSION_DEFAULT,
        message_batch_metric_interval=MESSAGE_BATCH_METRIC_INTERVAL_SECONDS,
        message_batch_metric_max_size=MESSAGE_BATCH_METRIC_MAX_BATCH_SIZE,
        message_batch_stdout_interval=MESSAGE_BATCH_STDOUT_INTERVAL_SECONDS,
        message_batch_stdout_max_size=MESSAGE_BATCH_STDOUT_MAX_BATCH_SIZE,
    ):
        # type: (...) -> None
        super(Streamer, self).__init__(
            initial_offset, beat_duration / 1000.0, use_http_messages=use_http_messages
        )
        self.daemon = True
        self.name = "Streamer(%r)" % ws_connection
        self.ws_connection = ws_connection
        self.connection = connection
        self.rest_api_client = rest_api_client

        self.stop_processing = False
        self.on_gpu_monitor_interval = None
        self.on_cpu_monitor_interval = None

        self.on_pending_rpcs_callback = pending_rpcs_callback

        self.last_beat = time.time()
        self.msg_waiting_timeout = msg_waiting_timeout
        self.wait_for_finish_sleep_interval = wait_for_finish_sleep_interval
        self.file_upload_waiting_timeout = file_upload_waiting_timeout
        self.file_upload_read_timeout = file_upload_read_timeout

        self.file_upload_manager = FileUploadManager(worker_cpu_ratio, worker_count)

        self.experiment_key = experiment_key
        self.api_key = api_key
        self.run_id = run_id
        self.project_id = project_id

        self.verify_tls = verify_tls

        self.parameters_batch = ParametersBatch(parameters_batch_base_interval)

        self.message_batch_compress = message_batch_compress
        self.message_batch_metrics = MessageBatch(
            base_interval=message_batch_metric_interval,
            max_size=message_batch_metric_max_size,
        )

        self.message_batch_stdout = MessageBatch(
            base_interval=message_batch_stdout_interval,
            max_size=message_batch_stdout_max_size,
        )

        LOGGER.debug("Streamer instantiated with ws url %s", self.ws_connection)
        LOGGER.debug(
            "Http messaging enabled: %s, metric batch size: %d, metrics batch interval: %s seconds",
            use_http_messages,
            message_batch_metric_max_size,
            message_batch_metric_interval,
        )

    def _before_run(self):
        if not self.use_http_messages:
            self.ws_connection.wait_for_connection()

    def _loop(self):
        """
        A single loop of running
        """
        try:
            # If we should stop processing the queue, abort early
            if self.stop_processing is True:
                return CloseMessage()

            ws_connected = (
                self.ws_connection is not None and self.ws_connection.is_connected()
            )

            if self.use_http_messages or ws_connected:
                messages = self.getn(1)

                if messages is not None:
                    for (message, offset) in messages:
                        if isinstance(message, CloseMessage):
                            return message

                        message_handlers = {
                            UploadFileMessage: self._process_upload_message,
                            UploadInMemoryMessage: self._process_upload_in_memory_message,
                            RemoteAssetMessage: self._process_upload_remote_asset_message,
                            WebSocketMessage: self._send_ws_message,
                            MetricMessage: self._process_metric_message,
                            ParameterMessage: self._process_parameter_message,
                            OsPackagesMessage: self._process_os_package_message,
                            ModelGraphMessage: self._process_model_graph_message,
                            SystemDetailsMessage: self._process_system_details_message,
                            CloudDetailsMessage: self._process_cloud_details_message,
                            LogOtherMessage: self._process_log_other_message,
                            FileNameMessage: self._process_file_name_message,
                            HtmlMessage: self._process_html_message,
                            HtmlOverrideMessage: self._process_html_override_message,
                            InstalledPackagesMessage: self._process_installed_packages_message,
                            GpuStaticInfoMessage: self._process_gpu_static_info_message,
                            GitMetadataMessage: self._process_git_metadata_message,
                            SystemInfoMessage: self._process_system_info_message,
                            LogDependencyMessage: self._process_log_dependency_message,
                            StandardOutputMessage: self._process_standard_output_message,
                        }

                        handler = message_handlers.get(type(message))
                        if handler is None:
                            raise ValueError("Unknown message type %r", message)
                        if isinstance(
                            message,
                            (
                                WebSocketMessage,
                                ParameterMessage,
                                MetricMessage,
                                StandardOutputMessage,
                            ),
                        ):
                            handler(message, offset)
                        else:
                            handler(message)

                # attempt to send collected parameters
                if self.parameters_batch.accept(self._send_parameter_messages_batch):
                    LOGGER.debug("Parameters batch was sent")

                # attempt to send batched messages via HTTP REST
                if self.use_http_messages:
                    if self.message_batch_metrics.accept(
                        self._send_metric_messages_batch
                    ):
                        LOGGER.debug("Metrics batch was sent")

                    if self.message_batch_stdout.accept(
                        self._send_stdout_messages_batch
                    ):
                        LOGGER.debug("stdout/stderr batch was sent")

            else:
                LOGGER.debug("WS connection is not ready")
                # Basic backoff
                time.sleep(0.5)
        except Exception:
            LOGGER.debug(UNEXPECTED_STREAMING_ERROR, exc_info=True)
            # report experiment error
            self._report_experiment_error(UNEXPECTED_STREAMING_ERROR)

    def _process_upload_message(self, message):
        # type: (UploadFileMessage) -> None
        # Compute the url from the upload type
        url = self.connection.get_upload_url(message.upload_type)

        self.file_upload_manager.upload_file_thread(
            additional_params=message.additional_params,
            api_key=self.api_key,
            clean=message.clean,
            critical=message._critical,
            experiment_id=self.experiment_key,
            file_path=message.file_path,
            metadata=message.metadata,
            project_id=self.project_id,
            timeout=self.file_upload_read_timeout,
            verify_tls=self.verify_tls,
            upload_endpoint=url,
            on_asset_upload=message._on_asset_upload,
            on_failed_asset_upload=message._on_failed_asset_upload,
            estimated_size=message._size,
        )
        LOGGER.debug("Processing uploading message done")

    def _process_upload_in_memory_message(self, message):
        # type: (UploadInMemoryMessage) -> None
        # Compute the url from the upload type
        url = self.connection.get_upload_url(message.upload_type)

        self.file_upload_manager.upload_file_like_thread(
            additional_params=message.additional_params,
            api_key=self.api_key,
            critical=message._critical,
            experiment_id=self.experiment_key,
            file_like=message.file_like,
            metadata=message.metadata,
            project_id=self.project_id,
            timeout=self.file_upload_read_timeout,
            verify_tls=self.verify_tls,
            upload_endpoint=url,
            on_asset_upload=message._on_asset_upload,
            on_failed_asset_upload=message._on_failed_asset_upload,
            estimated_size=message._size,
        )
        LOGGER.debug("Processing in-memory uploading message done")

    def _process_upload_remote_asset_message(self, message):
        # type: (RemoteAssetMessage) -> None
        # Compute the url from the upload type
        url = self.connection.get_upload_url(message.upload_type)

        self.file_upload_manager.upload_remote_asset_thread(
            additional_params=message.additional_params,
            api_key=self.api_key,
            critical=message._critical,
            experiment_id=self.experiment_key,
            metadata=message.metadata,
            project_id=self.project_id,
            remote_uri=message.remote_uri,
            timeout=self.file_upload_read_timeout,
            verify_tls=self.verify_tls,
            upload_endpoint=url,
            on_asset_upload=message._on_asset_upload,
            on_failed_asset_upload=message._on_failed_asset_upload,
            estimated_size=message._size,
        )
        LOGGER.debug("Processing remote uploading message done")

    def _send_stdout_messages_batch(self, message_items):
        # type: (List[MessageBatchItem]) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.send_stdout_batch,
            rest_fail_prompt=STANDARD_OUTPUT_SENDING_ERROR,
            general_fail_prompt="Error sending stdout/stderr batch (online experiment)",
            batch_items=message_items,
            compress=self.message_batch_compress,
            experiment_key=self.experiment_key,
        )

    def _send_metric_messages_batch(self, message_items):
        # type: (List[MessageBatchItem]) -> None
        self._process_rest_api_send(
            sender=self.connection.log_metrics_batch,
            rest_fail_prompt=METRICS_BATCH_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending metrics batch (online experiment)",
            items=message_items,
            compress=self.message_batch_compress,
        )

    def _send_parameter_messages_batch(self, message_items):
        # type: (List[MessageBatchItem]) -> None
        if self.use_http_messages:
            self._process_rest_api_send(
                sender=self.connection.log_parameters_batch,
                rest_fail_prompt=PARAMETERS_BATCH_MSG_SENDING_ERROR,
                general_fail_prompt="Error sending parameters batch (online experiment)",
                items=message_items,
                compress=self.message_batch_compress,
            )
        else:
            # send parameter messages using web socket
            for item in message_items:
                self._send_ws_message(message=item.message, offset=item.offset)

    def _send_ws_message(self, message, offset):
        # type: (Union[WebSocketMessage, BaseMessage], int) -> None
        """To send WS messages immediately"""
        try:
            data = self._serialise_message_for_ws(message=message, offset=offset)
            self.ws_connection.send(data)
        except Exception:
            LOGGER.debug("WS sending error", exc_info=True)
            # report experiment error
            self._report_experiment_error()

    def _process_parameter_message(self, message, offset):
        # type: (ParameterMessage, int) -> None
        # add message to the parameters batch
        if not self.parameters_batch.append(message=message, offset=offset):
            LOGGER.debug("Failed to add message to the parameters batch: %r" % message)
            # report experiment error
            self._report_experiment_error()

    def _process_metric_message(self, message, offset):
        # type: (MetricMessage, int) -> None
        if self.use_http_messages:
            self.message_batch_metrics.append(message=message, offset=offset)
        else:
            self._send_ws_message(message, offset)

    def _serialise_message_for_ws(self, message, offset):
        # type: (BaseMessage, int) -> str
        """Enhance provided message with relevant meta-data and serialize it to JSON compatible with WS format"""
        message_dict = message.non_null_dict()

        # Inject online specific values
        message_dict["apiKey"] = self.api_key
        message_dict["runId"] = self.run_id
        message_dict["projectId"] = self.project_id
        message_dict["experimentKey"] = self.experiment_key
        message_dict["offset"] = offset

        return format_messages_for_ws([message_dict])

    def _process_rest_api_send(
        self, sender, rest_fail_prompt, general_fail_prompt, **kwargs
    ):
        try:
            sender(**kwargs)
        except CometRestApiException as exc:
            msg = rest_fail_prompt % (exc.response.status_code, exc.response.content)
            LOGGER.error(msg)
            # report experiment error
            self._report_experiment_error(msg)
        except Exception:
            LOGGER.error(general_fail_prompt, exc_info=True)
            # report experiment error
            self._report_experiment_error(general_fail_prompt)

    def _process_os_package_message(self, message):
        # type: (OsPackagesMessage) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.set_experiment_os_packages,
            rest_fail_prompt=OS_PACKAGE_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending os_packages message",
            experiment_key=self.experiment_key,
            os_packages=message.os_packages,
        )

    def _process_model_graph_message(self, message):
        # type: (ModelGraphMessage) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.set_experiment_model_graph,
            rest_fail_prompt=MODEL_GRAPH_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending model_graph message",
            experiment_key=self.experiment_key,
            graph_str=message.graph,
        )

    def _process_system_details_message(self, message):
        # type: (SystemDetailsMessage) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.set_experiment_system_details,
            rest_fail_prompt=SYSTEM_DETAILS_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending system details message",
            _os=message.os,
            command=message.command,
            env=message.env,
            experiment_key=self.experiment_key,
            hostname=message.hostname,
            ip=message.ip,
            machine=message.machine,
            os_release=message.os_release,
            os_type=message.os_type,
            pid=message.pid,
            processor=message.processor,
            python_exe=message.python_exe,
            python_version_verbose=message.python_version_verbose,
            python_version=message.python_version,
            user=message.user,
        )

    def _process_log_other_message(self, message):
        # type: (LogOtherMessage) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.log_experiment_other,
            rest_fail_prompt=LOG_OTHER_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending log other message",
            experiment_key=self.experiment_key,
            key=message.key,
            value=message.value,
        )

    def _process_cloud_details_message(self, message):
        # type: (CloudDetailsMessage) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.set_experiment_cloud_details,
            rest_fail_prompt=CLOUD_DETAILS_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending cloud details message",
            experiment_key=self.experiment_key,
            provider=message.provider,
            cloud_metadata=message.cloud_metadata,
        )

    def _process_file_name_message(self, message):
        # type: (FileNameMessage) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.set_experiment_filename,
            rest_fail_prompt=FILENAME_DETAILS_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending file name message",
            experiment_key=self.experiment_key,
            filename=message.file_name,
        )

    def _process_html_message(self, message):
        # type: (HtmlMessage) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.log_experiment_html,
            rest_fail_prompt=HTML_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending html message",
            experiment_key=self.experiment_key,
            html=message.html,
        )

    def _process_installed_packages_message(self, message):
        # type: (InstalledPackagesMessage) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.set_experiment_installed_packages,
            rest_fail_prompt=INSTALLED_PACKAGES_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending installed packages message",
            experiment_key=self.experiment_key,
            installed_packages=message.installed_packages,
        )

    def _process_html_override_message(self, message):
        # type: (HtmlOverrideMessage) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.log_experiment_html,
            rest_fail_prompt=HTML_OVERRIDE_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending html override message",
            experiment_key=self.experiment_key,
            html=message.htmlOverride,
            overwrite=True,
        )

    def _process_gpu_static_info_message(self, message):
        # type: (GpuStaticInfoMessage) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.set_experiment_gpu_static_info,
            rest_fail_prompt=GPU_STATIC_INFO_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending gpu static info message",
            experiment_key=self.experiment_key,
            gpu_static_info=message.gpu_static_info,
        )

    def _process_git_metadata_message(self, message):
        # type: (GitMetadataMessage) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.set_experiment_git_metadata,
            rest_fail_prompt=GIT_METADATA_MSG_SENDING_ERROR,
            general_fail_prompt="Error sending git metadata message",
            experiment_key=self.experiment_key,
            user=message.git_metadata.get("user"),
            root=message.git_metadata.get("root"),
            branch=message.git_metadata.get("branch"),
            parent=message.git_metadata.get("parent"),
            origin=message.git_metadata.get("origin"),
        )

    def _process_system_info_message(self, message):
        # type: (SystemInfoMessage) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.log_experiment_system_info,
            rest_fail_prompt=SYSTEM_INFO_MESSAGE_SENDING_ERROR,
            general_fail_prompt="Error sending system_info message",
            experiment_key=self.experiment_key,
            system_info=[message.system_info],
        )

    def _process_log_dependency_message(self, message):
        # type: (LogDependencyMessage) -> None
        self._process_rest_api_send(
            sender=self.rest_api_client.log_experiment_dependency,
            rest_fail_prompt=LOG_DEPENDENCY_MESSAGE_SENDING_ERROR,
            general_fail_prompt="Error sending log dependency message",
            experiment_key=self.experiment_key,
            name=message.name,
            version=message.version,
            timestamp=message.local_timestamp,
        )

    def _process_standard_output_message(self, message, offset):
        # type: (StandardOutputMessage, int) -> None
        self.message_batch_stdout.append(message=message, offset=offset)

    def wait_for_finish(self):
        """Blocks the experiment from exiting until all data was sent to server
        OR the configured timeouts has expired."""

        if not self._is_msg_queue_empty():
            log_once_at_level(logging.INFO, WAITING_DATA_UPLOADED)
            log_once_at_level(
                logging.INFO,
                "The Python SDK has %d seconds to finish before aborting...",
                self.msg_waiting_timeout,
            )

            wait_for_done(
                self._is_msg_queue_empty,
                self.msg_waiting_timeout,
                progress_callback=self._show_remaining_messages,
                sleep_time=self.wait_for_finish_sleep_interval,
            )

        if not self._is_msg_queue_empty():
            LOGGER.warning(STREAMER_FAILED_TO_PROCESS_ALL_MESSAGES)

        # From now on, stop processing the message queue as it might contains file upload messages
        # TODO: Find a correct way of testing it
        self.stop_processing = True
        self.file_upload_manager.close()

        # Send all remained parameters from the batch if any
        if not self.parameters_batch.empty():
            if not self.parameters_batch.accept(
                self._send_parameter_messages_batch, unconditional=True
            ):
                LOGGER.warning(FAILED_SEND_PARAMETERS_BATCH_AT_EXPERIMENT_END)

        # send all remained metrics from the batch if any
        if self.use_http_messages:
            if not self.message_batch_metrics.empty():
                if not self.message_batch_metrics.accept(
                    self._send_metric_messages_batch, unconditional=True
                ):
                    LOGGER.warning(FAILED_SEND_METRICS_BATCH_AT_EXPERIMENT_END)

            if not self.message_batch_stdout.empty():
                if not self.message_batch_stdout.accept(
                    self._send_stdout_messages_batch, unconditional=True
                ):
                    LOGGER.warning(FAILED_SEND_STDOUT_BATCH_AT_EXPERIMENT_END)

        if not self.file_upload_manager.all_done():
            monitor = FileUploadManagerMonitor(self.file_upload_manager)

            LOGGER.info(FILE_UPLOADS_PROMPT)
            LOGGER.info(
                "The Python SDK has %d seconds to finish before aborting...",
                self.file_upload_waiting_timeout,
            )
            wait_for_done(
                monitor.all_done,
                self.file_upload_waiting_timeout,
                progress_callback=monitor.log_remaining_uploads,
                sleep_time=self.wait_for_finish_sleep_interval,
            )

        if not self._is_msg_queue_empty() or not self.file_upload_manager.all_done():
            remaining = self.messages.qsize()
            remaining_upload = self.file_upload_manager.remaining_uploads()
            LOGGER.error(STREAMER_WAIT_FOR_FINISH_FAILED, remaining, remaining_upload)

            self.connection.report(
                event_name=ON_EXIT_DIDNT_FINISH_UPLOAD_SDK,
                err_msg=(
                    STREAMER_WAIT_FOR_FINISH_FAILED % (remaining, remaining_upload)
                ),
            )
            # report experiment error
            self._report_experiment_error()

            return False

        self.file_upload_manager.join()

        return True

    def _report_experiment_error(self, message=None):
        self.rest_api_client.update_experiment_error_status(
            experiment_key=self.experiment_key, is_alive=True, has_error=True
        )

    def _is_msg_queue_empty(self):
        finished = self.messages.empty()

        if finished is False:
            LOGGER.debug(
                "Messages queue not empty, %d messages, closed %s",
                self.messages.qsize(),
                self.closed,
            )
            if not self.use_http_messages:
                LOGGER.debug(
                    "WS Connection connected? %s %s",
                    self.ws_connection.is_connected(),
                    self.ws_connection.address,
                )

        return finished

    def _show_remaining_messages(self):
        remaining = self.messages.qsize()
        LOGGER.info("Uploading %d metrics, params and output messages", remaining)

    def has_failed(self):
        # type: (...) -> bool
        return self.file_upload_manager.has_failed()


def compact_json_dump(data, fp):
    return json_dump(data, fp, sort_keys=True, separators=(",", ":"), cls=NestedEncoder)


class OfflineStreamer(BaseStreamer):
    """
    This class extends threading.Thread and provides a simple concurrent queue
    and an async service that pulls data from the queue and writes it to the file.
    """

    def __init__(
        self,
        tmp_dir,  # type: AnyStr
        initial_offset,  # type: int
        wait_timeout,  # type: int
        use_http_messages=False,  # type: bool
        on_error_callback=None,  # type: Callable[[str], None]
    ):
        super(OfflineStreamer, self).__init__(
            initial_offset=initial_offset,
            queue_timeout=1,
            use_http_messages=use_http_messages,
        )
        self.daemon = True
        self.tmp_dir = tmp_dir
        self.wait_timeout = wait_timeout
        self.on_error = on_error_callback

        self.file = open(
            os.path.join(self.tmp_dir, OFFLINE_EXPERIMENT_MESSAGES_JSON_FILE_NAME), "wb"
        )

    def _write(self, json_line_message):
        # type: (Dict[str, Any]) -> None
        compact_json_dump(json_line_message, self.file)
        self.file.write(b"\n")
        self.file.flush()

    def _after_run(self):
        # Close the messages files once we are sure we won't write in it
        # anymore
        self.file.close()

    def _loop(self):
        """
        A single loop of running
        """
        try:
            messages = self.getn(1)

            if messages is not None:
                LOGGER.debug(
                    "Got %d messages, %d still in queue",
                    len(messages),
                    self.messages.qsize(),
                )

                for (message, offset) in messages:
                    if isinstance(message, CloseMessage):
                        return message

                    message_handlers = {
                        UploadFileMessage: self._process_upload_message,
                        UploadInMemoryMessage: self._process_upload_in_memory_message,
                        RemoteAssetMessage: self._process_message,
                        WebSocketMessage: self._process_message,
                        MetricMessage: self._process_message,
                        ParameterMessage: self._process_message,
                        OsPackagesMessage: self._process_message,
                        ModelGraphMessage: self._process_message,
                        SystemDetailsMessage: self._process_message,
                        CloudDetailsMessage: self._process_message,
                        FileNameMessage: self._process_message,
                        HtmlMessage: self._process_message,
                        LogOtherMessage: self._process_message,
                        HtmlOverrideMessage: self._process_message,
                        InstalledPackagesMessage: self._process_message,
                        GpuStaticInfoMessage: self._process_message,
                        GitMetadataMessage: self._process_message,
                        SystemInfoMessage: self._process_message,
                        StandardOutputMessage: self._process_message,
                        LogDependencyMessage: self._process_message,
                    }

                    handler = message_handlers.get(type(message))

                    if handler is None:
                        raise ValueError("Unknown message type %r", message)
                    else:
                        handler(message)

        except Exception as ex:
            LOGGER.debug("Unknown streaming error", exc_info=True)
            self._report_error("Unknown offline streaming error: %r" % ex)

    def _report_error(self, message):
        if self.on_error is not None:
            self.on_error(message)

    def _process_upload_message(self, message):
        # type: (UploadFileMessage) -> None
        # Create the file on disk with the same extension if set
        ext = splitext(message.file_path)[1]

        if ext:
            suffix = ".%s" % ext
        else:
            suffix = ""

        tmpfile = tempfile.NamedTemporaryFile(
            dir=self.tmp_dir, suffix=suffix, delete=False
        )
        tmpfile.close()

        if message.clean:
            # TODO: Avoid un-necessary file copy by checking if the file is
            # already at the top-level of self.tmp_di

            # Then move the original file to the newly create file
            shutil.move(message.file_path, tmpfile.name)
        else:
            shutil.copy(message.file_path, tmpfile.name)
            # Mark the file to be cleaned as we copied it to our tmp dir
            message.clean = True

        message.file_path = basename(tmpfile.name)

        msg_json = message.repr_json()
        data = {"type": UploadFileMessage.type, "payload": msg_json}
        self._write(data)

    def _process_upload_in_memory_message(self, message):
        # type: (UploadInMemoryMessage) -> None

        # We need to convert the in-memory file to a file one
        if is_user_text(message.file_like):
            file_like = data_to_fp(message.file_like)
        else:
            file_like = message.file_like

        tmpfile = write_file_like_to_tmp_file(file_like, self.tmp_dir)

        new_message = UploadFileMessage(
            tmpfile,
            message.upload_type,
            message.additional_params,
            message.metadata,
            clean=True,
            size=message._size,
        )

        return self._process_upload_message(new_message)

    def _process_message(self, message):
        msg_json = message.non_null_dict()

        data = {"type": message.type, "payload": msg_json}
        self._write(data)

    def wait_for_finish(self):
        """Blocks the experiment from exiting until all data is saved or timeout exceeded."""

        log_once_at_level(
            logging.INFO,
            OFFLINE_SENDER_WAIT_FOR_FINISH_PROMPT,
            int(self.wait_timeout * 2),
        )

        # Wait maximum for 2 times of self.wait_timeout
        wait_for_done(check_function=self.messages.empty, timeout=self.wait_timeout)

        if not self.messages.empty():
            LOGGER.info(OFFLINE_SENDER_WAIT_FOR_FINISH_PROMPT, int(self.wait_timeout))

            def progress_callback():
                LOGGER.info(
                    OFFLINE_SENDER_REMAINING_DATA_ITEMS_TO_WRITE, self.messages.qsize()
                )

            if not self.messages.empty():
                wait_for_done(
                    check_function=self.messages.empty,
                    timeout=self.wait_timeout,
                    progress_callback=progress_callback,
                    sleep_time=5,
                )

        if not self.messages.empty():
            remaining = self.messages.qsize()
            LOGGER.info(OFFLINE_SENDER_FAILED_TO_WRITE_ALL_DATA, remaining)
            self._report_error(OFFLINE_SENDER_FAILED_TO_WRITE_ALL_DATA % remaining)

        # Also wait for the thread to finish to be sure that all messages are
        # written to the messages file
        self.join(10)

        if self.is_alive():
            LOGGER.info(
                "OfflineStreamer didn't finished in time, message data files might be incomplete"
            )
            return False
        else:
            LOGGER.debug("OfflineStreamer finished in time")
            return True


def is_valid_experiment_key(experiment_key):
    """Validate an experiment_key; returns True or False"""
    return (
        isinstance(experiment_key, str)
        and experiment_key.isalnum()
        and (32 <= len(experiment_key) <= 50)
    )


def format_url(prefix, **query_arguments):
    if prefix is None:
        return None

    splitted = list(urlsplit(prefix))

    splitted[3] = urlencode(query_arguments)

    return urlunsplit(splitted)
