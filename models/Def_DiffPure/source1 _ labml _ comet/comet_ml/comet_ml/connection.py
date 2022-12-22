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

from __future__ import print_function

import gzip
import itertools
import json
import logging
import math
import multiprocessing.pool
import os
import platform
import ssl
import struct
import sys
import tempfile
import threading
import time
import warnings
import zipfile

import comet_ml
from comet_ml import config
from comet_ml._reporting import WS_ON_CLOSE, WS_ON_ERROR, WS_ON_OPEN
from comet_ml.config import (
    DEFAULT_ASSET_UPLOAD_SIZE_LIMIT,
    DEFAULT_UPLOAD_SIZE_LIMIT,
    Config,
    get_config,
)
from comet_ml.file_downloader import FileDownloadSizeMonitor
from comet_ml.json_encoder import NestedEncoder

import requests
import requests.utils
import six
import urllib3.exceptions
import websocket
import websocket._url
from requests import HTTPError, Response, Session
from requests.adapters import HTTPAdapter
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter
from urllib3.util.retry import Retry
from websocket._exceptions import WebSocketConnectionClosedException

from ._reporting import FILE_UPLOADED_FAILED
from ._typing import IO, Any, Dict, List, Mapping, Optional, Tuple, Type
from .batch_utils import MessageBatchItem
from .config import DEFAULT_WS_JOIN_TIMEOUT
from .constants import (
    PAYLOAD_ADDITIONAL_SYSTEM_INFO_LIST,
    PAYLOAD_COMMAND,
    PAYLOAD_DEPENDENCIES,
    PAYLOAD_DEPENDENCY_NAME,
    PAYLOAD_DEPENDENCY_VERSION,
    PAYLOAD_ENV,
    PAYLOAD_EXECUTABLE,
    PAYLOAD_EXPERIMENT_KEY,
    PAYLOAD_FILE_PATH,
    PAYLOAD_GPU_STATIC_INFO_LIST,
    PAYLOAD_HOSTNAME,
    PAYLOAD_HTML,
    PAYLOAD_INSTALLED_PACKAGES,
    PAYLOAD_IP,
    PAYLOAD_LOCAL_TIMESTAMP,
    PAYLOAD_MACHINE,
    PAYLOAD_METADATA,
    PAYLOAD_MODEL_GRAPH,
    PAYLOAD_OFFSET,
    PAYLOAD_OS,
    PAYLOAD_OS_PACKAGES,
    PAYLOAD_OS_RELEASE,
    PAYLOAD_OS_TYPE,
    PAYLOAD_OUTPUT,
    PAYLOAD_OUTPUT_LINES,
    PAYLOAD_OVERRIDE,
    PAYLOAD_PID,
    PAYLOAD_PROCESSOR,
    PAYLOAD_PROVIDER,
    PAYLOAD_PYTHON_VERSION,
    PAYLOAD_PYTHON_VERSION_VERBOSE,
    PAYLOAD_RUN_CONTEXT,
    PAYLOAD_STDERR,
    PAYLOAD_TIMESTAMP,
    PAYLOAD_TOTAL_RAM,
    PAYLOAD_USED_RAM,
    PAYLOAD_USER,
    STATUS_RESPONSE_CPU_MONITOR_INTERVAL_MILLIS,
    STATUS_RESPONSE_GPU_MONITOR_INTERVAL_MILLIS,
    STATUS_RESPONSE_IS_ALIVE_BEAT_DURATION_MILLIS,
    STATUS_RESPONSE_PARAMETER_UPDATE_INTERVAL_MILLIS,
    STATUS_RESPONSE_PENDING_RPCS,
)
from .exceptions import (
    API_KEY_NOT_REGISTERED,
    EXPERIMENT_ALREADY_EXISTS,
    NO_PROJECT_NAME_SPECIFIED,
    NON_EXISTING_TEAM,
    PROJECT_NAME_TOO_LONG,
    BackendVersionTooOld,
    CometRestApiException,
    CometRestApiValueError,
    ExperimentAlreadyUploaded,
    InvalidAPIKey,
    InvalidRestAPIKey,
    InvalidWorkspace,
    NotFound,
    PaymentRequired,
    ProjectNameEmpty,
    ProjectNameIsTooLong,
)
from .file_uploader import AssetDataUploadProcessor, AssetUploadProcessor
from .heartbeat import (
    HEARTBEAT_CPU_MONITOR_INTERVAL,
    HEARTBEAT_GPU_MONITOR_INTERVAL,
    HEARTBEAT_PARAMETERS_BATCH_UPDATE_INTERVAL,
)
from .logging_messages import (
    BACKEND_VERSION_CHECK_ERROR,
    FILE_UPLOAD_MANAGER_MONITOR_FIRST_MESSAGE,
    FILE_UPLOAD_MANAGER_MONITOR_PROGRESSION,
    FILE_UPLOAD_MANAGER_MONITOR_PROGRESSION_UNKOWN_ETA,
    FILE_UPLOAD_MANAGER_MONITOR_WAITING_BACKEND_ANSWER,
    INVALID_CONFIG_MINIMAL_BACKEND_VERSION,
    REPORTING_ERROR,
    WS_ON_CLOSE_MSG,
    WS_ON_OPEN_MSG,
    WS_SSL_ERROR_MSG,
)
from .messages import StandardOutputMessage, UploadFileMessage
from .parallel_utils import get_thread_pool
from .utils import (
    encode_metadata,
    format_bytes,
    get_comet_version,
    get_root_url,
    get_time_monotonic,
    local_timestamp,
    log_once_at_level,
    merge_url,
    parse_version_number,
    proper_registry_model_name,
    sanitize_url,
)

if six.PY2:
    from urllib import getproxies

    from urlparse import urljoin, urlparse
else:
    from urllib.parse import urljoin, urlparse
    from urllib.request import getproxies

INITIAL_BEAT_DURATION = 10000  # 10 second

LOGGER = logging.getLogger(__name__)


def _comet_version():
    try:
        return comet_ml.__version__
    except NameError:
        return None


def get_retry_strategy():
    # type: () -> Retry

    # The total backoff sleeping time is computed like that:
    # backoff = 2
    # total = 3
    # s = lambda b, i: b * (2 ** (i - 1))
    # sleep = sum(s(backoff, i) for i in range(1, total + 1))

    return Retry(
        total=3,
        backoff_factor=2,
        method_whitelist=False,
        status_forcelist=[500, 502, 503, 504],
    )  # Will wait up to 24s


def get_backend_address(config=None):
    # type: (Optional[Config]) -> str
    if config is None:
        config = get_config()

    return sanitize_url(config["comet.url_override"])


def get_comet_root_url(config=None):
    # type: (Config) -> str

    return get_root_url(sanitize_url(config.get_string(None, "comet.url_override")))


def should_report(config, server_address):
    # type: (Config, str) -> bool
    backend_host = urlparse(server_address).hostname

    # Disable reporting for backend address != comet.ml
    if backend_host.endswith("comet-ml.com"):
        default = True
    else:
        default = False

    return config.get_bool(None, "comet.internal.reporting", default=default)


def get_optimizer_address(config):
    # type: (Config) -> str
    optimizer_url = config.get_string(None, "comet.optimizer_url")

    if optimizer_url is None:
        return url_join(get_comet_root_url(config), "optimizer/")
    else:
        return sanitize_url(optimizer_url, ending_slash=True)


def get_http_session(retry_strategy=None, verify_tls=True, tcp_keep_alive=False):
    # type: (Optional[Retry], bool, bool) -> Session
    session = Session()

    # Add default debug headers
    session.headers.update(
        {
            "X-COMET-DEBUG-SDK-VERSION": get_comet_version(),
            "X-COMET-DEBUG-PY-VERSION": platform.python_version(),
        }
    )

    # Setup retry strategy if asked
    http_adapter = None
    https_adapter = None
    if tcp_keep_alive is True:
        http_adapter = TCPKeepAliveAdapter(
            idle=60, count=5, interval=60, max_retries=retry_strategy
        )
        https_adapter = TCPKeepAliveAdapter(
            idle=60, count=5, interval=60, max_retries=retry_strategy
        )
    elif tcp_keep_alive is False and retry_strategy is not None:
        http_adapter = HTTPAdapter(max_retries=retry_strategy)
        https_adapter = HTTPAdapter(max_retries=retry_strategy)

    if http_adapter is not None:
        session.mount("http://", http_adapter)

    if https_adapter is not None:
        session.mount("https://", https_adapter)

    # Setup HTTP allow header if configured
    config = get_config()  # This can be slow if called for every new session
    allow_header_name = config["comet.allow_header.name"]
    allow_header_value = config["comet.allow_header.value"]

    if allow_header_name and allow_header_value:
        session.headers[allow_header_name] = allow_header_value

    if verify_tls is False:
        # Only the set the verify if it's disabled. The current default for the verify attribute is
        # True but this way we will survive any change of the default value
        session.verify = False
        # Also filter the warning that urllib3 emits to not overflow the output with them
        warnings.filterwarnings(
            "ignore", category=urllib3.exceptions.InsecureRequestWarning
        )

    return session


THREAD_SESSIONS = threading.local()


def get_thread_session(retry, verify_tls, tcp_keep_alive):
    # type: (bool, bool, bool) -> Session

    # As long as the session is not part of a reference loop, the thread local dict will be cleaned
    # up when each thread ends, garbage-collecting the Session object and closing the
    # resources
    session_key = (retry, tcp_keep_alive, verify_tls)

    cached_session = THREAD_SESSIONS.__dict__.get(
        session_key, None
    )  # type: Optional[Session]

    if cached_session:
        return cached_session

    retry_strategy = False
    if retry is True:
        retry_strategy = get_retry_strategy()

    new_session = get_http_session(
        retry_strategy=retry_strategy,
        tcp_keep_alive=tcp_keep_alive,
        verify_tls=verify_tls,
    )
    THREAD_SESSIONS.__dict__[session_key] = new_session

    return new_session


def url_join(base, *parts, **kwargs):
    """Given a base and url parts (for example [workspace, project, id]) returns a full URL"""
    # TODO: Enforce base to have a scheme and netloc?
    result = base

    for part in parts[:-1]:
        if not part.endswith("/"):
            raise ValueError("Intermediary part not ending with /")

        result = urljoin(result, part)

    result = urljoin(result, parts[-1])

    if kwargs:
        # merge the url with additional query args:
        result = merge_url(result, kwargs)

    return result


def json_post(url, session, headers, body, timeout):
    response = session.post(
        url=url, data=json.dumps(body), headers=headers, timeout=timeout
    )

    response.raise_for_status()
    return response


def format_messages_for_ws(messages):
    # type: (Any) -> str
    """Encode a list of messages into JSON"""
    messages_arr = []
    for message in messages:
        payload = {}
        # make sure connection is actually alive
        if message.get("stdout") is not None:
            payload["stdout"] = message
        else:
            payload["log_data"] = message

        messages_arr.append(payload)

    data = json.dumps(messages_arr, cls=NestedEncoder, allow_nan=False)
    LOGGER.debug("ENCODED DATA %r", data)
    return data


def format_message_batch_items(batch_items, experiment_key):
    # type: (List[MessageBatchItem], str) -> Dict[str, Any]
    """Encodes a list of messages into batch body dictionary to be used for batch endpoints"""
    messages_arr = []
    for item in batch_items:
        messages_arr.append(item.message.repr_json_batch())

    batch_body = {
        PAYLOAD_EXPERIMENT_KEY: experiment_key,
        "values": messages_arr,
    }
    LOGGER.debug("ENCODED BATCH BODY DATA %r", batch_body)
    return batch_body


def format_stdout_message_batch_items(batch_items, timestamp, experiment_key, stderr):
    # type: (List[MessageBatchItem], int, str, bool) -> Optional[Dict[str, Any]]
    stdout_lines = []
    timestamp = int(timestamp * 1000)  # the Java format - milliseconds since epoch

    for item in batch_items:
        if not isinstance(item.message, StandardOutputMessage):
            continue

        if stderr != item.message.stderr:
            # different message type than requested
            continue

        stdout_lines.append(
            {
                PAYLOAD_STDERR: stderr,
                PAYLOAD_OUTPUT: item.message.output,
                PAYLOAD_LOCAL_TIMESTAMP: timestamp,
                PAYLOAD_OFFSET: item.offset,
            }
        )

    if len(stdout_lines) == 0:
        return None

    payload = {
        PAYLOAD_EXPERIMENT_KEY: experiment_key,
        PAYLOAD_OUTPUT_LINES: stdout_lines,
        PAYLOAD_RUN_CONTEXT: None,
    }
    return payload


class LowLevelHTTPClient(object):
    """A low-level HTTP client that centralize common code and behavior between
    the two backends clients, the optimizer"""

    def __init__(
        self,
        server_address,
        default_timeout,
        verify_tls,
        headers=None,
        default_retry=True,
    ):
        # type: (str, int, bool, Optional[Dict[str, Optional[str]]], bool) -> None
        self.server_address = server_address

        self.session = get_http_session(verify_tls=verify_tls)
        self.retry_session = get_http_session(
            retry_strategy=get_retry_strategy(), verify_tls=verify_tls
        )

        if headers is None:
            headers = {}

        self.headers = headers

        self.default_retry = default_retry
        self.default_timeout = default_timeout

    def close(self):
        self.session.close()
        self.retry_session.close()

    def get(
        self,
        url,  # type: str
        params=None,  # type: Optional[Dict[str, str]]
        headers=None,  # type: Optional[Dict[str, str]]
        retry=None,  # type: Optional[bool]
        timeout=None,  # type: Optional[int]
        check_status_code=False,  # type: bool
        stream=False,  # type: bool
    ):
        # type: (...) -> requests.Response
        if retry is None:
            retry = self.default_retry

        if timeout is None:
            timeout = self.default_timeout

        final_headers = self.headers.copy()
        if headers:
            final_headers.update(headers)

        # Do not logs the headers as they might contains the authentication keys
        LOGGER.debug(
            "GET HTTP Call, url %r, params %r, retry %r, timeout %r",
            url,
            params,
            retry,
            timeout,
        )

        if retry:
            session = self.retry_session
        else:
            session = self.session

        response = session.get(
            url,
            params=params,
            headers=final_headers,
            timeout=timeout,
            stream=stream,
        )  # type: requests.Response

        if check_status_code:
            response.raise_for_status()

        return response

    def post(
        self,
        url,  # type: str
        payload,  # type: Any
        headers=None,  # type: Optional[Mapping[str, Optional[str]]]
        retry=None,  # type: Optional[bool]
        timeout=None,  # type: Optional[float]
        params=None,  # type: Optional[Any]
        files=None,  # type: Optional[Any]
        check_status_code=False,  # type: bool
        custom_encoder=None,  # type: Optional[Type[json.JSONEncoder]]
        compress=False,  # type: bool
    ):
        # type: (...) -> requests.Response
        return self.do(
            method="POST",
            url=url,
            payload=payload,
            headers=headers,
            retry=retry,
            timeout=timeout,
            params=params,
            files=files,
            check_status_code=check_status_code,
            custom_encoder=custom_encoder,
            compress=compress,
        )

    def put(
        self,
        url,  # type: str
        payload,  # type: Any
        headers=None,  # type: Optional[Mapping[str, Optional[str]]]
        retry=None,  # type: Optional[bool]
        timeout=None,  # type: Optional[float]
        params=None,  # type: Optional[Any]
        files=None,  # type: Optional[Any]
        check_status_code=False,  # type: bool
        custom_encoder=None,  # type: Optional[Type[json.JSONEncoder]]
        compress=False,  # type: bool
    ):
        # type: (...) -> requests.Response
        return self.do(
            method="PUT",
            url=url,
            payload=payload,
            headers=headers,
            retry=retry,
            timeout=timeout,
            params=params,
            files=files,
            check_status_code=check_status_code,
            custom_encoder=custom_encoder,
            compress=compress,
        )

    def do(
        self,
        method,  # type: str
        url,  # type: str
        payload,  # type: Any
        headers=None,  # type: Optional[Mapping[str, Optional[str]]]
        retry=None,  # type: Optional[bool]
        timeout=None,  # type: Optional[float]
        params=None,  # type: Optional[Any]
        files=None,  # type: Optional[Any]
        check_status_code=False,  # type: bool
        custom_encoder=None,  # type: Optional[Type[json.JSONEncoder]]
        compress=False,  # type: bool
    ):
        # type: (...) -> requests.Response
        if retry is None:
            retry = self.default_retry

        if timeout is None:
            timeout = self.default_timeout

        final_headers = self.headers.copy()
        if headers:
            final_headers.update(headers)

        # Do not log the headers as they might contain the authentication keys
        LOGGER.debug(
            "%s HTTP Call, url %r, payload %r, retry %r, timeout %r",
            method,
            url,
            payload,
            retry,
            timeout,
        )

        if retry:
            session = self.retry_session
        else:
            session = self.session

        if files:
            # File upload, multipart request
            response = session.request(
                method=method,
                url=url,
                data=payload,
                headers=final_headers,
                timeout=timeout,
                files=files,
                params=params,
            )
        else:
            # JSON request

            # Format the payload with potentially some custom encoder
            data = json.dumps(payload, cls=custom_encoder).encode("utf-8")
            final_headers["Content-Type"] = "application/json;charset=utf-8"
            if compress is True and six.PY2 is False:
                data = gzip.compress(data)
                final_headers["Content-Encoding"] = "gzip"

            response = session.request(
                method=method,
                url=url,
                data=data,
                headers=final_headers,
                timeout=timeout,
                params=params,
            )

        LOGGER.debug(
            "%s HTTP Response, url %r, status_code %d, response %r",
            method,
            url,
            response.status_code,
            response.content,
        )
        if response.status_code != 200:
            LOGGER.debug(
                "Not OK %s HTTP Response headers: %s", method, response.headers
            )

        if check_status_code:
            response.raise_for_status()

        return response


class RestServerConnection(object):
    """
    A static class that handles the connection with the server endpoints.
    """

    def __init__(
        self, api_key, experiment_id, server_address, default_timeout, verify_tls
    ):
        # type: (str, str, str, float, bool) -> None
        self.api_key = api_key
        self.experiment_id = experiment_id

        # Set once get_run_id is called
        self.run_id = None
        self.project_id = None

        self.server_address = server_address

        # TODO: Get the config as an input parameter
        self.config = get_config()

        self.default_timeout = default_timeout
        self._low_level_http_client = LowLevelHTTPClient(
            server_address,
            default_retry=False,
            default_timeout=default_timeout,
            headers={"X-COMET-DEBUG-EXPERIMENT-KEY": experiment_id},
            verify_tls=verify_tls,
        )

    def server_hostname(self):
        parsed = six.moves.urllib.parse.urlparse(self.server_address)
        return parsed.netloc

    def close(self):
        self._low_level_http_client.close()

    def heartbeat(self):
        """Inform the backend that we are still alive"""
        LOGGER.debug("Doing an heartbeat")
        return self.update_experiment_status(self.run_id, self.project_id, True)

    def update_experiment_status(self, run_id, project_id, is_alive, offline=False):
        endpoint_url = self.server_address + "status-report/update"

        payload = {
            "apiKey": self.api_key,
            "runId": run_id,
            PAYLOAD_EXPERIMENT_KEY: self.experiment_id,
            "projectId": project_id,
            "is_alive": is_alive,
            "local_timestamp": local_timestamp(),
            "offline": offline,
        }

        LOGGER.debug("Experiment status update with payload: %s", payload)

        r = self._low_level_http_client.post(
            url=endpoint_url, payload=payload, retry=True
        )

        if r.status_code != 200:
            raise ValueError(r.content)

        data = r.json()
        LOGGER.debug("Update experiment status response payload: %r", data)
        beat_duration = data.get(STATUS_RESPONSE_IS_ALIVE_BEAT_DURATION_MILLIS)

        if beat_duration is None:
            raise ValueError("Missing heart-beat duration")

        gpu_monitor_interval = data.get(STATUS_RESPONSE_GPU_MONITOR_INTERVAL_MILLIS)

        if gpu_monitor_interval is None:
            raise ValueError("Missing gpu-monitor interval")

        # Default the backend response until it responds:
        cpu_monitor_interval = data.get(
            STATUS_RESPONSE_CPU_MONITOR_INTERVAL_MILLIS, 68 * 1000
        )

        if cpu_monitor_interval is None:
            raise ValueError("Missing cpu-monitor interval")

        # Default the backend response until actual data received
        parameters_update_interval = data.get(
            STATUS_RESPONSE_PARAMETER_UPDATE_INTERVAL_MILLIS,
            self.config.get_int(None, "comet.message_batch.parameters_interval") * 1000,
        )
        if parameters_update_interval is None:
            raise ValueError("Missing parameters update interval")

        pending_rpcs = data.get(STATUS_RESPONSE_PENDING_RPCS, False)

        return_data = {
            HEARTBEAT_GPU_MONITOR_INTERVAL: gpu_monitor_interval,
            HEARTBEAT_CPU_MONITOR_INTERVAL: cpu_monitor_interval,
            HEARTBEAT_PARAMETERS_BATCH_UPDATE_INTERVAL: parameters_update_interval,
        }

        return beat_duration, return_data, pending_rpcs

    def get_run_id(self, project_name, workspace, offline=False):
        # type: (Optional[str], Optional[str], bool) -> ExperimentHandshakeResponse
        """
        Gets a new run id from the server.
        :param project_name: project name for the new experiment, can be None
        :param workspace: workspace name for the new experiment, can be None
        :param offline: should the new experiment be marked as offline
        :return: ExperimentHandshakeResponse
        """
        endpoint_url = get_run_id_url(self.server_address)

        # We used to pass the team name as second parameter then we migrated
        # to workspaces. We keep using the same payload field as compatibility
        # is ensured by the backend and old SDK version will still uses it
        # anyway
        payload = {
            "apiKey": self.api_key,
            "local_timestamp": local_timestamp(),
            PAYLOAD_EXPERIMENT_KEY: self.experiment_id,
            "offline": offline,
            "projectName": project_name,
            "teamName": workspace,
            "libVersion": _comet_version(),
        }

        LOGGER.debug("Get run id URL: %s", endpoint_url)
        r = self._low_level_http_client.post(
            url=endpoint_url, payload=payload, retry=True
        )

        if r.status_code != 200:
            if r.status_code == 400:
                # Check if the api key was invalid
                data = r.json()  # Raise a ValueError if failing
                code = data.get("sdk_error_code")
                if code == API_KEY_NOT_REGISTERED:
                    raise InvalidAPIKey(self.api_key, self.server_hostname())

                elif code == NON_EXISTING_TEAM:
                    raise InvalidWorkspace(workspace)

                elif code == NO_PROJECT_NAME_SPECIFIED:
                    raise ProjectNameEmpty()

                elif code == EXPERIMENT_ALREADY_EXISTS:
                    raise ExperimentAlreadyUploaded(self.experiment_id)

                elif code == PROJECT_NAME_TOO_LONG:
                    # Add fallback if the backend stop sending the msg
                    err_msg = data.get(
                        "msg",
                        "Project name it too long, it should should be < 100 characters",
                    )
                    raise ProjectNameIsTooLong(err_msg)

            raise ValueError(r.content)

        res_body = json.loads(r.content.decode("utf-8"))

        LOGGER.debug("New run response body: %s", res_body)

        return self._parse_run_id_res_body(res_body)

    def get_old_run_id(self, previous_experiment):
        # type: (str) -> ExperimentHandshakeResponse
        """
        Gets a run id from an existing experiment.
        :param previous_experiment: the experiment id to continue logging to
        :return: ExperimentHandshakeResponse
        """
        endpoint_url = get_old_run_id_url(self.server_address)

        payload = {
            "apiKey": self.api_key,
            "local_timestamp": local_timestamp(),
            "previousExperiment": previous_experiment,
            "libVersion": _comet_version(),
        }
        LOGGER.debug("Get old run id URL: %s", endpoint_url)
        r = self._low_level_http_client.post(
            url=endpoint_url, payload=payload, retry=True
        )

        if r.status_code != 200:
            if r.status_code == 400:
                # Check if the api key was invalid
                data = r.json()  # Raise a ValueError if failing
                if data.get("sdk_error_code") == API_KEY_NOT_REGISTERED:
                    raise InvalidAPIKey(self.api_key, self.server_hostname())

            raise ValueError(r.content)

        res_body = json.loads(r.content.decode("utf-8"))

        LOGGER.debug("Old run response body: %s", res_body)

        return self._parse_run_id_res_body(res_body)

    def copy_run(self, previous_experiment, copy_step):
        # type: (str, bool) -> ExperimentHandshakeResponse
        """
        Gets a run id from an existing experiment.
        :param previous_experiment: the experiment id to copy
        :param copy_step: copy up the step passed
        :return: ExperimentHandshakeResponse
        """
        endpoint_url = copy_experiment_url(self.server_address)

        payload = {
            "apiKey": self.api_key,
            "copiedExperimentKey": previous_experiment,
            "newExperimentKey": self.experiment_id,
            "stepToCopyTo": copy_step,
            "localTimestamp": local_timestamp(),
            "libVersion": _comet_version(),
        }
        LOGGER.debug("Copy run URL: %s", endpoint_url)

        r = self._low_level_http_client.post(
            url=endpoint_url, payload=payload, retry=True
        )

        if r.status_code != 200:
            if r.status_code == 400:
                # Check if the api key was invalid
                data = r.json()  # Raise a ValueError if failing
                if data.get("sdk_error_code") == API_KEY_NOT_REGISTERED:
                    raise InvalidAPIKey(self.api_key, self.server_hostname())

            raise ValueError(r.content)

        res_body = json.loads(r.content.decode("utf-8"))

        LOGGER.debug("Copy run response body: %s", res_body)

        return self._parse_run_id_res_body(res_body)

    def _parse_run_id_res_body(self, res_body):
        # type: (Dict[str, Any]) -> ExperimentHandshakeResponse

        response = parse_experiment_handshake_response(res_body)

        # Save run_id and project_id around
        self.run_id = response.run_id
        self.project_id = response.project_id

        return response

    def report(self, event_name=None, err_msg=None):
        try:
            if event_name is not None:

                if not should_report(self.config, self.server_address):
                    return None

                endpoint_url = notify_url(self.server_address)
                # Automatically add the sdk_ prefix to the event name
                real_event_name = "sdk_{}".format(event_name)

                payload = {
                    "event_name": real_event_name,
                    "api_key": self.api_key,
                    "run_id": self.run_id,
                    "experiment_key": self.experiment_id,
                    "project_id": self.project_id,
                    "err_msg": err_msg,
                    "timestamp": local_timestamp(),
                }

                LOGGER.debug("Report notify URL: %s", endpoint_url)

                # We use half of the timeout as the call might happens in the
                # main thread that we don't want to block and report data is
                # usually not critical
                timeout = int(self.default_timeout / 2)

                self._low_level_http_client.post(
                    endpoint_url,
                    payload,
                    timeout=timeout,
                    check_status_code=True,
                )

        except Exception:
            LOGGER.debug("Error reporting %s", event_name, exc_info=True)
            pass

    def offline_experiment_start_end_time(self, run_id, start_time, stop_time):
        endpoint_url = offline_experiment_times_url(self.server_address)

        payload = {
            "apiKey": self.api_key,
            "runId": run_id,
            PAYLOAD_EXPERIMENT_KEY: self.experiment_id,
            "startTimestamp": start_time,
            "endTimestamp": stop_time,
        }

        LOGGER.debug(
            "Offline experiment start time and end time update with payload: %s",
            payload,
        )

        r = self._low_level_http_client.post(endpoint_url, payload)

        if r.status_code != 200:
            raise ValueError(r.content)

        return None

    def add_tags(self, added_tags):
        endpoint_url = add_tags_url(self.server_address)

        payload = {
            "apiKey": self.api_key,
            PAYLOAD_EXPERIMENT_KEY: self.experiment_id,
            "addedTags": added_tags,
        }

        LOGGER.debug("Add tags with payload: %s", payload)

        r = self._low_level_http_client.post(
            url=endpoint_url, payload=payload, retry=True
        )

        if r.status_code != 200:
            raise ValueError(r.content)

        return None

    def get_upload_url(self, upload_type):
        return get_upload_url(self.server_address, upload_type)

    def get_pending_rpcs(self):
        endpoint_url = pending_rpcs_url(self.server_address)

        payload = {
            "apiKey": self.api_key,
            PAYLOAD_EXPERIMENT_KEY: self.experiment_id,
        }

        LOGGER.debug("Get pending RPCS with payload: %s", payload)

        r = self._low_level_http_client.get(url=endpoint_url, params=payload)

        if r.status_code != 200:
            raise ValueError(r.content)

        res_body = json.loads(r.content.decode("utf-8"))

        return res_body

    def register_rpc(self, function_definition):
        endpoint_url = register_rpc_url(self.server_address)

        payload = {
            "apiKey": self.api_key,
            PAYLOAD_EXPERIMENT_KEY: self.experiment_id,
        }
        # We might replace this payload.update by defining the exact
        # parameters once the payload body has been stabilized
        payload.update(function_definition)

        LOGGER.debug("Register RPC with payload: %s", payload)

        r = self._low_level_http_client.post(endpoint_url, payload=payload)

        if r.status_code != 200:
            raise ValueError(r.content)

        return None

    def send_rpc_result(self, callId, result, start_time, end_time):
        endpoint_url = rpc_result_url(self.server_address)

        error = result.get("error", "")
        error_stacktrace = result.get("traceback", "")
        result = result.get("result", "")

        payload = {
            "apiKey": self.api_key,
            PAYLOAD_EXPERIMENT_KEY: self.experiment_id,
            "callId": callId,
            "result": result,
            "errorMessage": error,
            "errorStackTrace": error_stacktrace,
            "startTimeMs": start_time,
            "endTimeMs": end_time,
        }

        LOGGER.debug("Sending RPC result with payload: %s", payload)

        r = self._low_level_http_client.post(endpoint_url, payload=payload)

        if r.status_code != 200:
            raise ValueError(r.content)

        return None

    def send_new_symlink(self, project_name):
        endpoint_url = new_symlink_url(self.server_address)

        payload = {
            "apiKey": self.api_key,
            PAYLOAD_EXPERIMENT_KEY: self.experiment_id,
            "projectName": project_name,
        }

        LOGGER.debug("new symlink: %s", payload)

        r = self._low_level_http_client.get(url=endpoint_url, params=payload)

        if r.status_code != 200:
            raise ValueError(r.content)

    def send_notification(
        self,
        title,
        status,
        experiment_name,
        experiment_link,
        notification_map,
        custom_encoder,
    ):
        # type: (str, Optional[str], Any, str, Optional[Dict[str, Any]], Optional[Type[json.JSONEncoder]]) -> None
        endpoint_url = notification_url(self.server_address)

        payload = {
            "api_key": self.api_key,
            "title": title,
            "status": status,
            "experiment_key": self.experiment_id,
            "experiment_name": experiment_name,
            "experiment_link": experiment_link,
            "additional_data": notification_map,
        }

        LOGGER.debug("Notification: %s", payload)

        try:
            r = self._low_level_http_client.post(
                endpoint_url,
                payload=payload,
                check_status_code=True,
                custom_encoder=custom_encoder,
            )

            if r.status_code != 200 and r.status_code != 204:
                raise ValueError(r.content)

        except HTTPError as e:
            # for backwards and forwards compatibility, this endpoint was introduced backend v1.1.103
            if e.response.status_code != 404:
                raise

    def log_parameters_batch(self, items, compress=True):
        # type: (List[MessageBatchItem], bool) -> None
        endpoint_url = parameters_batch_url(self.server_address)

        LOGGER.debug(
            "Sending parameters batch, length: %d, compression enabled: %s, endpoint: %s",
            len(items),
            compress,
            endpoint_url,
        )

        self._send_batch(
            batch_items=items, endpoint_url=endpoint_url, compress=compress
        )

    def log_metrics_batch(self, items, compress=True):
        # type: (List[MessageBatchItem], bool) -> None
        endpoint_url = metrics_batch_url(self.server_address)

        LOGGER.debug(
            "Sending metrics batch, length: %d, compression enabled: %s, endpoint: %s",
            len(items),
            compress,
            endpoint_url,
        )

        self._send_batch(
            batch_items=items, endpoint_url=endpoint_url, compress=compress
        )

    def _send_batch(self, batch_items, endpoint_url, compress=True):
        payload = format_message_batch_items(
            batch_items=batch_items, experiment_key=self.experiment_id
        )

        headers = {BaseApiClient.API_KEY_HEADER: self.api_key}
        r = self._low_level_http_client.post(
            url=endpoint_url,
            payload=payload,
            check_status_code=True,
            custom_encoder=NestedEncoder,
            headers=headers,
            compress=compress,
            retry=True,
        )

        if r.status_code != 200:
            raise ValueError(r.content)


class Reporting(object):
    @staticmethod
    def report(
        config,  # type: Config
        event_name=None,  # type: Optional[str]
        api_key=None,  # type: Optional[str]
        run_id=None,  # type: Optional[str]
        experiment_key=None,  # type: Optional[str]
        project_id=None,  # type: Optional[str]
        err_msg=None,  # type: Optional[str]
        is_alive=None,  # type: Any
    ):
        try:
            if event_name is not None:
                server_address = get_backend_address(config)

                if not should_report(config, server_address):
                    return None

                verify_tls = config.get_bool(
                    None, "comet.internal.check_tls_certificate"
                )

                endpoint_url = notify_url(server_address)
                headers = {"Content-Type": "application/json;charset=utf-8"}
                # Automatically add the sdk_ prefix to the event name
                real_event_name = "sdk_{}".format(event_name)

                payload = {
                    "event_name": real_event_name,
                    "api_key": api_key,
                    "run_id": run_id,
                    "experiment_key": experiment_key,
                    "project_id": project_id,
                    "err_msg": err_msg,
                    "timestamp": local_timestamp(),
                }

                session = get_http_session(
                    retry_strategy=get_retry_strategy(), verify_tls=verify_tls
                )

                if experiment_key:
                    headers["X-COMET-DEBUG-EXPERIMENT-KEY"] = experiment_key

                # We use half of the timeout as the call might happens in the
                # main thread that we don't want to block and report data is
                # usually not critical
                timeout = int(config.get_int(None, "comet.timeout.http") / 2)

                with session:
                    json_post(endpoint_url, session, headers, payload, timeout)

        except Exception:
            LOGGER.debug(REPORTING_ERROR, event_name, exc_info=True)


class WebSocketConnection(threading.Thread):
    """
    Handles the ongoing connection to the server via Web Sockets.
    """

    def __init__(self, ws_server_address, connection, verify_tls):
        # type: (str, RestServerConnection, bool) -> None
        threading.Thread.__init__(self)
        self.daemon = True
        self.name = "WebSocketConnection(%s)" % (ws_server_address)
        self.closed = False

        if config.DEBUG:
            websocket.enableTrace(True)

        self.address = ws_server_address

        self.config = get_config()

        self.allow_header_name = self.config["comet.allow_header.name"]
        self.allow_header_value = self.config["comet.allow_header.value"]

        LOGGER.debug("Creating a new WSConnection with url: %s", ws_server_address)

        self.ws = self.connect_ws()

        self.connection = connection

        self.last_error = None

        self.sslopts = get_websocket_sslopt(verify_tls)

    def is_connected(self):
        return getattr(self.ws.sock, "connected", False)

    def connect_ws(self):
        header = []

        if self.allow_header_name and self.allow_header_value:
            header.append("%s: %s" % (self.allow_header_name, self.allow_header_value))

        ws = websocket.WebSocketApp(
            self.address,
            header=header,
            on_message=lambda *args, **kwargs: self.on_message(*args, **kwargs),
            on_error=lambda *args, **kwargs: self.on_error(*args, **kwargs),
            on_close=lambda *args, **kwargs: self.on_close(*args, **kwargs),
        )
        ws.on_open = lambda *args, **kwargs: self.on_open(*args, **kwargs)
        return ws

    def run(self):
        while self.closed is False:
            LOGGER.debug("%r run loop", self)
            try:
                self._loop()
            except Exception:
                LOGGER.debug("Run forever error", exc_info=True)
                # Avoid hammering the backend
                time.sleep(0.5)
        LOGGER.debug("WebSocketConnection has ended")

    def _loop(self):
        # Pass the default ping_timeout to avoid issues with websocket-client
        # >= 0.50.0
        (
            http_proxy_host,
            http_proxy_port,
            http_proxy_auth,
            proxy_type,
        ) = get_and_format_proxy_for_ws(self.address)
        self.ws.run_forever(
            ping_timeout=10,
            http_proxy_host=http_proxy_host,
            http_proxy_port=http_proxy_port,
            http_proxy_auth=http_proxy_auth,
            proxy_type=proxy_type,
            sslopt=self.sslopts,
        )
        LOGGER.debug("Run forever has ended")

    def send(self, data, retry=5):
        """Encode the messages into JSON and send them on the websocket
        connection
        """
        for i in range(retry):
            success = self._send(data)
            if success:
                return
            else:
                LOGGER.debug("Retry WS sending!")

        LOGGER.debug("Message %r failed to be sent", data)

    def close(self):
        LOGGER.debug("Closing %r", self)
        self.closed = True
        # Send the close opcode frame and let the thread clean itself properly
        try:
            # Copied from https://github.com/websocket-client/websocket-client/blob/29c15714ac9f5272e1adefc9c99b83420b409f63/websocket/_core.py#L410
            self.ws.send(
                struct.pack("!H", websocket._abnf.STATUS_NORMAL),
                websocket._abnf.ABNF.OPCODE_CLOSE,
            )
        except WebSocketConnectionClosedException:
            # Make sure we don't start create back the websocket connection
            self.ws.keep_running = False

    def force_close(self):
        """Close the WS connection abruptly"""
        self.closed = True
        self.ws.close()

    def wait_for_finish(self, timeout=DEFAULT_WS_JOIN_TIMEOUT):
        self.join(timeout)
        if self.is_alive():
            msg = "Websocket connection didn't closed properly after %d seconds"
            LOGGER.warning(msg, timeout)
            return False
        else:
            LOGGER.debug("Websocket connection correctly closed")
            return True

    def _send(self, data):
        if self.ws.sock:
            self.ws.send(data)
            LOGGER.debug("Sending data done, %r", data)
            return True

        else:
            LOGGER.debug("WS not ready for connection")
            self.wait_for_connection()
            return False

    def wait_for_connection(self, num_of_tries=20):
        """
        waits for the server connection
        Args:
            num_of_tries: number of times to try connecting before giving up

        Returns: True if succeeded to connect.

        """
        if not self.is_connected():
            counter = 0

            while not self.is_connected() and counter < num_of_tries:
                time.sleep(0.5)
                counter += 1

            if not self.is_connected():
                LOGGER.debug("Trying to force close the connection")
                self.force_close()

                if self.last_error is not None:
                    # Process potential sources of errors
                    if isinstance(self.last_error[1], ssl.SSLError):
                        LOGGER.error(
                            WS_SSL_ERROR_MSG,
                            exc_info=self.last_error,
                            extra={"show_traceback": True},
                        )

                raise ValueError("Could not connect to server after multiple tries.")

        return True

    def on_open(self, ws):
        LOGGER.debug(WS_ON_OPEN_MSG)

        self.connection.report(event_name=WS_ON_OPEN, err_msg=WS_ON_OPEN_MSG)

    def on_message(self, ws, message):
        if message not in ("got msg", "ok"):
            LOGGER.debug("WS msg: %s", message)

    def on_error(self, ws, error):
        error_type_str = type(error).__name__
        ignores = [
            "WebSocketBadStatusException",
            "error",
            "WebSocketConnectionClosedException",
            "ConnectionRefusedError",
            "BrokenPipeError",
        ]

        self.connection.report(event_name=WS_ON_ERROR, err_msg=repr(error))

        # Ignore some errors for auto-reconnecting
        if error_type_str in ignores:
            LOGGER.debug("Ignore WS error: %r %r", error, self, exc_info=True)
            return

        LOGGER.debug("WS on error: %r %r", error, self, exc_info=True)

        # Save error
        self.last_error = sys.exc_info()

    def on_close(self, *args, **kwargs):
        LOGGER.debug(WS_ON_CLOSE_MSG, self)

        # Report only abnormal WS closing
        if not self.closed:
            self.connection.report(event_name=WS_ON_CLOSE, err_msg=WS_ON_CLOSE_MSG)


def notify_url(server_address):
    return server_address + "notify/event"


def visualization_upload_url():
    """Return the URL to upload visualizations"""
    return "visualizations/upload"


def asset_upload_url():
    return "asset/upload"


def get_git_patch_upload_endpoint():
    return "git-patch/upload"


def log_url(server_address):
    return server_address + "log/add"


def offline_experiment_times_url(server_address):
    return server_address + "status-report/offline-metadata"


def pending_rpcs_url(server_address):
    return server_address + "rpc/get-pending-rpcs"


def add_tags_url(server_address):
    return server_address + "tags/add-tags-to-experiment"


def register_rpc_url(server_address):
    return server_address + "rpc/register-rpc"


def rpc_result_url(server_address):
    return server_address + "rpc/save-rpc-result"


def new_symlink_url(server_address):
    return server_address + "symlink/new"


def get_run_id_url(server_address):
    return server_address + "logger/add/run"


def get_old_run_id_url(server_address):
    # type: (str) -> str
    return url_join(server_address, "logger/get/run")


def copy_experiment_url(server_address):
    return server_address + "logger/copy-steps-from-experiment"


def notification_url(server_address):
    return server_address + "notification/experiment"


def metrics_batch_url(server_address):
    return url_join(server_address, "batch/logger/experiment/metric")


def parameters_batch_url(server_address):
    return url_join(server_address, "batch/logger/experiment/parameter")


UPLOAD_TYPE_URL_MAP = {
    "git-patch": get_git_patch_upload_endpoint(),
    "shap": visualization_upload_url(),
    "prophet": visualization_upload_url(),
    "visualization": visualization_upload_url(),
}


def get_upload_url(server_address, upload_type):
    # Default upload url is asset_upload_url
    return server_address + UPLOAD_TYPE_URL_MAP.get(upload_type, asset_upload_url())


def _debug_proxy_for_http(target_url):
    # type: (str) -> Optional[str]
    """Return the proxy (or None) used by requests for the target url"""
    proxies = requests.utils.get_environ_proxies(target_url)
    return requests.utils.select_proxy(target_url, proxies)


def _debug_proxy_for_ws(target_url):
    # type: (str) -> Optional[str]
    """Return the proxy (or None) used by websocket-url for the target url"""
    # Proxies are retrieved from environ if None
    parsed_url = urlparse(target_url)
    dont_use_proxy = websocket._url._is_no_proxy_host(
        parsed_url.hostname, no_proxy=None
    )
    if dont_use_proxy is True:
        return None

    return get_proxy_for_ws(target_url)


def get_proxy_for_ws(ws_target_url):
    # type: (str) -> Optional[str]
    """Return the proxy to use for the given ws_target_url, use stdlib getproxies() so we read from
    registry on Windows and from MacOSX framework SystemConfiguration on MacOS
    """

    # Parse the target url
    parsed_url = urlparse(ws_target_url)

    proxies = getproxies()
    # Follow websocket-client own logic
    # https://github.com/websocket-client/websocket-client/blob/cf0eb68a63747222434e382e24ea38836201dc30/websocket/_url.py#L165-L167
    proxy_keys = ["http"]
    if parsed_url.scheme == "wss":
        proxy_keys.insert(0, "https")

    for proxy_key in proxy_keys:
        proxy_value = proxies.get(proxy_key)

        if proxy_value is not None:
            return proxy_value

    return None


def get_and_format_proxy_for_ws(target_url):
    # type: (str) -> Tuple[Optional[str], Optional[int], Optional[Tuple[str, str]], Optional[str]]
    """Return the proxy information for the websocket-client in the expected format for the
    library. DOES NOT handle the NOT_PROXY configuration, websocket-client is handling it directly
    in websocket._url._is_no_proxy_host
    https://github.com/websocket-client/websocket-client/blob/cf0eb68a63747222434e382e24ea38836201dc30/websocket/_url.py#L108
    """
    proxy_value_host = None
    proxy_value_port = None
    proxy_value_auth = None
    proxy_type = None

    proxy_value = get_proxy_for_ws(target_url)

    if proxy_value is not None:
        # Parse the proxy address for websocket-client expected input
        proxy_value_obj = urlparse(proxy_value)
        proxy_type = proxy_value_obj.scheme
        if proxy_value_obj.port is not None:
            proxy_value_port = int(proxy_value_obj.port)
        proxy_value_host = proxy_value_obj.hostname
        if proxy_value_obj.username or proxy_value_obj.password:
            proxy_value_auth = (proxy_value_obj.username, proxy_value_obj.password)
        LOGGER.debug("Using WS PROXY: %s:%s", proxy_value_host, proxy_value_port)
        if proxy_value_host is None:
            raise ValueError("Invalid https proxy format `%s`" % proxy_value)

    return proxy_value_host, proxy_value_port, proxy_value_auth, proxy_type


def get_websocket_sslopt(verify_tls):
    # type: (bool) -> Dict[str, Any]
    if verify_tls:
        return {}
    else:
        return {"cert_reqs": ssl.CERT_NONE}


def get_optimizer_api(api_key, server_address=None, headers=None):
    # type: (str, Optional[str], Optional[Dict[str, Any]]) -> OptimizerAPI
    config = get_config()
    if server_address is None:
        server_address = get_optimizer_address(config)

    server_url = get_root_url(server_address)

    low_level_api = LowLevelHTTPClient(
        server_address=server_url,
        default_timeout=config["comet.timeout.http"],
        headers=headers,
        verify_tls=config.get_bool(None, "comet.internal.check_tls_certificate"),
    )

    return OptimizerAPI(api_key, low_level_api, server_address)


class OptimizerAPI(object):
    """
    API for talking to Optimizer Server.
    """

    def __init__(self, api_key, low_level_api_client, server_address):
        # type: (str, LowLevelHTTPClient, str) -> None
        """ """
        self.DEFAULT_VERSION = "v1"
        self.URLS = {"v1": {"SERVER": server_address}}
        self._version = self.DEFAULT_VERSION
        self._api_key = api_key
        self.low_level_api_client = low_level_api_client

    def get_url(self, version=None):
        """
        Returns the URL for this version of the API.
        """
        version = version if version is not None else self._version
        return self.URLS[version]["SERVER"]

    def get_url_server(self, version=None):
        """
        Returns the URL server for this version of the API.
        """
        version = version if version is not None else self._version
        return self.URLS[version]["SERVER"]

    def get_url_end_point(self, end_point, version=None):
        """
        Return the URL + end point.
        """
        return url_join(self.get_url(version), end_point)

    def get_request(self, end_point, params, return_type="json"):
        """
        Given an end point and a dictionary of params,
        return the results.
        """
        from . import __version__

        url = self.get_url_end_point(end_point)
        LOGGER.debug("API.get_request: url = %s, params = %s", url, params)
        headers = {"X-API-KEY": self._api_key, "PYTHON-SDK-VERSION": __version__}

        response = self.low_level_api_client.get(
            url, params=params, headers=headers, retry=False
        )

        raise_exception = None
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as exception:
            if exception.response.status_code == 401:
                raise_exception = ValueError("Invalid COMET_API_KEY")
            else:
                raise
        if raise_exception:
            raise raise_exception
        ### Return data based on return_type:
        if return_type == "json":
            return response.json()
        elif return_type == "binary":
            return response.content
        elif return_type == "text":
            return response.text
        elif return_type == "response":
            return response

    def post_request(self, end_point, json=None, **kwargs):
        """
        Given an end point and a dictionary of json,
        post the json, and return the results.
        """
        from . import __version__

        url = self.get_url_end_point(end_point)
        if json is None:
            json = {}
        LOGGER.debug("API.post_request: url = %s, json = %s", url, json)
        headers = {
            "PYTHON-SDK-VERSION": __version__,
            "X-API-KEY": self._api_key,
            "Content-Type": "application/json",
        }
        if "files" in kwargs:
            del headers["Content-Type"]

        response = self.low_level_api_client.post(
            url, headers=headers, payload=json, retry=False
        )

        raise_exception = None
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as exception:
            if exception.response.status_code == 401:
                raise_exception = ValueError("Invalid COMET_API_KEY")
            else:
                raise
        if raise_exception:
            raise raise_exception
        return response.json()

    def get_version(self):
        """
        Return the default version of the API.
        """
        return self._version

    def optimizer_next(self, id):
        # type: (str) -> Optional[Dict[str, Any]]
        """
        Get the next set of parameters to evaluate.
        """
        ## /next?id=ID
        params = {"id": id}
        results = self.get_request("next", params=params)
        if results["code"] == 200:
            result_params = results["parameters"]  # type: Dict[str, Any]
            return result_params
        else:
            return None

    def optimizer_spec(self, algorithm_name):
        """
        Get the full spec for a optimizer by name.
        """
        if algorithm_name is not None:
            params = {"algorithmName": algorithm_name}
        else:
            raise ValueError("Optimizer must have an algorithm name")

        results = self.get_request("spec", params=params)
        if "code" in results:
            raise ValueError(results["message"])
        return results

    def optimizer_status(self, id=None):
        """
        Get the status of optimizer instance or
        search.
        """
        ## /status
        ## /status?id=ID
        if id is not None:
            params = {"id": id}
        else:
            params = {}
        results = self.get_request("status", params=params)
        return results

    def optimizer_algorithms(self):
        """
        Get a list of algorithms.
        """
        ## /algorithms
        results = self.get_request("algorithms", {})
        return results

    def optimizer_update(self, id, pid, trial, status=None, score=None, epoch=None):
        """
        Post the status of a search.
        """
        ## /update {"pid": PID, "trial": TRIAL,
        ##          "status": STATUS, "score": SCORE, "epoch": EPOCH}
        json = {
            "id": id,
            "pid": pid,
            "trial": trial,
            "status": status,
            "score": score,
            "epoch": epoch,
        }
        results = self.post_request("update", json=json)
        return results

    def optimizer_insert(self, id, p):
        """
        Insert a completed parameter package.
        """
        ## /insert {"pid": PID, "trial": TRIAL,
        ##          "status": STATUS, "score": SCORE, "epoch": EPOCH}
        json = {
            "id": id,
            "pid": p["pid"],
            "trial": p["trial"],
            "status": p["status"],
            "score": p["score"],
            "epoch": p["epoch"],
            "parameters": p["parameters"],
            "tries": p["tries"],
            "startTime": p["startTime"],
            "endTime": p["endTime"],
            "lastUpdateTime": p["lastUpdateTime"],
            "count": p["count"],
        }
        results = self.post_request("insert", json=json)
        return results


def get_rest_api_client(
    version,
    server_address=None,
    headers=None,
    api_key=None,
    use_cache=False,
    check_version=True,
):
    # type: (str, Optional[str], Optional[Dict[str, Any]], Optional[str], bool, bool) -> RestApiClient
    settings = get_config()
    if api_key is None:
        raise ValueError("get_rest_api_client requires an api_key")

    if server_address is None:
        server_address = settings["comet.url_override"]
    server_url = get_root_url(server_address)

    low_level_api = LowLevelHTTPClient(
        server_address=server_url,
        default_timeout=settings["comet.timeout.api"],
        headers=headers,
        verify_tls=settings.get_bool(None, "comet.internal.check_tls_certificate"),
    )

    if use_cache:
        return RestApiClientWithCache(
            server_url=server_url,
            version=version,
            low_level_api_client=low_level_api,
            api_key=api_key,
            config=settings,
            check_version=check_version,
        )
    else:
        return RestApiClient(
            server_url=server_url,
            version=version,
            low_level_api_client=low_level_api,
            api_key=api_key,
            config=settings,
            check_version=check_version,
        )


def _check_response_status(response, method="POST"):
    # type: (Response, str) -> Response
    if response.status_code == 200:
        return response

    if response.status_code == 402:
        raise PaymentRequired(method, response)
    else:
        raise CometRestApiException(method, response)


class BaseApiClient(object):
    """A base api client to centralize how we build urls and treat exceptions"""

    API_KEY_HEADER = "Authorization"

    def __init__(self, server_url, base_url, low_level_api_client, api_key, config):
        # type: (str, List[str], LowLevelHTTPClient, Optional[str], Config) -> None
        self.server_url = server_url
        self.base_url = url_join(server_url, *base_url)
        self.low_level_api_client = low_level_api_client
        self.api_key = api_key
        self.config = config

    def get(self, url, params, headers=None, timeout=None, stream=False):
        # type: (str, Optional[Dict[str, str]], Optional[Dict[str, str]], Optional[int], bool) -> requests.Response
        response = self.low_level_api_client.get(
            url,
            params=params,
            headers=headers,
            timeout=timeout,
            stream=stream,
        )

        if response.status_code != 200:
            if response.status_code == 401:
                raise InvalidRestAPIKey(self.api_key)  # probably
            elif response.status_code == 404:
                raise NotFound("GET", response)
            else:
                raise CometRestApiException("GET", response)

        return response

    def get_from_endpoint(
        self,
        endpoint,
        params,
        return_type="json",
        timeout=None,
        stream=False,
    ):
        # type: (str, Optional[Dict[str, str]], str, Optional[int], bool) -> Any
        url = url_join(self.base_url, endpoint)
        response = self.get(url, params, timeout=timeout, stream=stream)

        ### Return data based on return_type:
        if return_type == "json":
            content_type = response.headers["content-type"]
            # octet-stream is a GZIPPED stream:
            if content_type in ["application/json", "application/octet-stream"]:
                retval = response.json()
            else:
                raise CometRestApiValueError("GET", "data is not json", response)
        elif return_type == "binary":
            retval = response.content
        elif return_type == "text":
            retval = response.text
        elif return_type == "response":
            retval = response
        else:
            raise CometRestApiValueError(
                "GET",
                "invalid return_type %r: should be 'json', 'binary', or 'text'"
                % return_type,
                response,
            )

        return retval

    def post(
        self,
        url,  # type: str
        payload,  # type: Any
        headers=None,  # type: Optional[Mapping[str, Optional[str]]]
        files=None,  # type: Optional[Any]
        params=None,  # type: Optional[Any]
        custom_encoder=None,  # type: Optional[Type[json.JSONEncoder]]
        compress=False,  # type: bool
    ):
        # type: (...) -> requests.Response
        response = self.low_level_api_client.post(
            url,
            payload=payload,
            headers=headers,
            files=files,
            params=params,
            custom_encoder=custom_encoder,
            compress=compress,
        )

        return _check_response_status(response)

    def put(
        self,
        url,  # type: str
        payload,  # type: Any
        headers=None,  # type: Optional[Mapping[str, Optional[str]]]
        files=None,  # type: Optional[Any]
        params=None,  # type: Optional[Any]
        custom_encoder=None,  # type: Optional[Type[json.JSONEncoder]]
        compress=False,  # type: bool
    ):
        # type: (...) -> requests.Response
        response = self.low_level_api_client.put(
            url,
            payload=payload,
            headers=headers,
            files=files,
            params=params,
            custom_encoder=custom_encoder,
            compress=compress,
        )

        return _check_response_status(response)

    def post_from_endpoint(self, endpoint, payload, **kwargs):
        # type: (str, Any, **Any) -> requests.Response
        return self._result_from_http_method(self.post, endpoint, payload, **kwargs)

    def put_from_endpoint(self, endpoint, payload, **kwargs):
        # type: (str, Any, **Any) -> requests.Response
        return self._result_from_http_method(self.put, endpoint, payload, **kwargs)

    def _result_from_http_method(self, method, endpoint, payload, **kwargs):
        url = url_join(self.base_url, endpoint)  # create endpoint URL
        return method(url, payload, **kwargs)


class RestApiClient(BaseApiClient):
    """This API Client is meant to discuss to the REST API and handle, params and payload formatting,
    input validation if necessary, creating the url and parsing the output. All the HTTP
    communication is handled by the low-level HTTP client.

    Inputs must be JSON-encodable, any conversion must be done by the caller.

    One method equals one endpoint and one call"""

    def __init__(
        self, server_url, version, low_level_api_client, api_key, config, check_version
    ):
        # type: (str, str, LowLevelHTTPClient, str, Config, bool) -> None
        super(RestApiClient, self).__init__(
            server_url,
            ["api/rest/", version + "/"],
            low_level_api_client,
            api_key,
            config,
        )
        self._version = version

        self.use_cache = False

        if check_version:
            self._check_version()

    def _check_version(self):
        config_minimal_backend_version = self.config[
            "comet.rest_v2_minimal_backend_version"
        ]
        minimal_backend_version = None
        try:
            # Invalid version will raise exception:
            minimal_backend_version = parse_version_number(
                config_minimal_backend_version
            )
        except Exception:
            LOGGER.warning(
                INVALID_CONFIG_MINIMAL_BACKEND_VERSION, config_minimal_backend_version
            )

        if minimal_backend_version:
            self._check_api_backend_version(minimal_backend_version)

    def get(self, url, params, headers=None, timeout=None, stream=False):
        headers = {self.API_KEY_HEADER: self.api_key}

        return super(RestApiClient, self).get(
            url, params=params, headers=headers, timeout=timeout, stream=stream
        )

    def post(
        self,
        url,
        payload,
        headers=None,
        files=None,
        params=None,
        custom_encoder=None,
        compress=False,
    ):
        headers = {self.API_KEY_HEADER: self.api_key}

        return super(RestApiClient, self).post(
            url,
            payload=payload,
            headers=headers,
            files=files,
            params=params,
            custom_encoder=custom_encoder,
            compress=compress,
        )

    def put(
        self,
        url,
        payload,
        headers=None,
        files=None,
        params=None,
        custom_encoder=None,
        compress=False,
    ):
        headers = {self.API_KEY_HEADER: self.api_key}

        return super(RestApiClient, self).put(
            url,
            payload=payload,
            headers=headers,
            files=files,
            params=params,
            custom_encoder=custom_encoder,
        )

    def reset(self):
        pass

    # Read Experiment methods:

    def get_account_details(self):
        """
        Example: {'userName': 'USERNAME', 'defaultWorkspaceName': 'WORKSPACE'}
        """
        payload = None
        response = self.get_from_endpoint("account-details", payload)
        return response

    def _get_experiment_system_details_single_field(self, experiment_key, field):
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        response = self.get_from_endpoint("experiment/system-details", payload)
        if response:
            return response[field]
        else:
            return None

    def get_experiment_os_packages(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_OS_PACKAGES
        )

    def get_experiment_user(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_USER
        )

    def get_experiment_installed_packages(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_INSTALLED_PACKAGES
        )

    def get_experiment_command(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_COMMAND
        )

    def get_experiment_executable(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_EXECUTABLE
        )

    def get_experiment_hostname(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_HOSTNAME
        )

    def get_experiment_gpu_static_info(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_GPU_STATIC_INFO_LIST
        )

    def get_experiment_additional_system_info(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_ADDITIONAL_SYSTEM_INFO_LIST
        )

    def get_experiment_ip(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_IP
        )

    def get_experiment_max_memory(self, experiment_key):
        # FIXME: always None
        return self._get_experiment_system_details_single_field(
            experiment_key, "maxTotalMemory"
        )

    def get_experiment_network_interface_ips(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, "networkInterfaceIps"
        )

    def get_experiment_os(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_OS
        )

    def get_experiment_os_type(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_OS_TYPE
        )

    def get_experiment_os_release(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_OS_RELEASE
        )

    def get_experiment_pid(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_PID
        )

    def get_experiment_python_version(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_PYTHON_VERSION
        )

    def get_experiment_python_version_verbose(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_PYTHON_VERSION_VERBOSE
        )

    def get_experiment_total_memory(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_TOTAL_RAM
        )

    def get_experiment_machine(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_MACHINE
        )

    def get_experiment_processor(self, experiment_key):
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_PROCESSOR
        )

    def get_experiment_system_info(self, experiment_key):
        """
        Deprecated.
        """
        return self._get_experiment_system_details_single_field(
            experiment_key, PAYLOAD_ADDITIONAL_SYSTEM_INFO_LIST
        )

    def get_experiment_system_metric_names(self, experiment_key):
        """ """
        return self._get_experiment_system_details_single_field(
            experiment_key, "systemMetricNames"
        )

    def get_experiment_model_graph(self, experiment_key):
        """
        Get the associated graph/model description for this
        experiment.
        """
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("experiment/graph", params)

    def get_experiment_tags(self, experiment_key):
        """
        Get the associated tags for this experiment.
        """
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("experiment/tags", params)

    def get_experiment_parameters_summaries(self, experiment_key):
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("experiment/parameters", params)

    def get_experiment_metrics_summaries(self, experiment_key):
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("experiment/metrics/summary", params)

    def get_experiment_metric(self, experiment_key, metric):
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key, "metricName": metric}
        return self.get_from_endpoint("experiment/metrics/get-metric", params)

    def get_experiment_multi_metrics(
        self, experiment_keys, metrics, parameters=None, independent=True, full=True
    ):
        payload = {
            "targetedExperiments": experiment_keys,
            "metrics": metrics,
            "params": parameters,
            "independentMetrics": independent,
            "fetchFull": full,
        }
        return self.post_from_endpoint("experiments/multi-metric-chart", payload)

    def get_experiment_asset_list(self, experiment_key, asset_type=None):
        """
        Get a list of assets associated with the experiment.
        """
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        if asset_type is not None:
            params["type"] = asset_type
        results = self.get_from_endpoint("experiment/asset/list", params)
        if results:
            return results["assets"]

    def _prepare_experiment_asset_request(
        self,
        asset_id,
        experiment_key=None,
        artifact_version_id=None,
    ):
        # type: (str, Optional[str], Optional[str]) -> Tuple[str, Dict[str, Any], Dict[str, Any]]
        params = {"assetId": asset_id}

        if experiment_key is not None:
            params[PAYLOAD_EXPERIMENT_KEY] = experiment_key

        if artifact_version_id is not None:
            params["artifactVersionId"] = artifact_version_id

        url = url_join(self.base_url, "experiment/asset/get-asset")

        headers = {}

        headers[self.API_KEY_HEADER] = self.api_key
        headers.update(self.low_level_api_client.headers)

        return (url, params, headers)

    def get_experiment_asset(
        self,
        asset_id,
        experiment_key=None,
        artifact_version_id=None,
        return_type="binary",
        stream=False,
    ):
        # type: (str, Optional[str], Optional[str], str, bool) -> Any
        _, params, _ = self._prepare_experiment_asset_request(
            asset_id, experiment_key, artifact_version_id
        )

        response = self.get_from_endpoint(
            "experiment/asset/get-asset",
            params,
            return_type=return_type,
            timeout=self.config["comet.timeout.file_download"],
            stream=stream,
        )
        return response

    def get_experiment_others_summaries(self, experiment_key):
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("experiment/log-other", params)

    def get_experiment_system_details(self, experiment_key):
        """
        Return the dictionary of system details.
        """
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("experiment/system-details", params)

    def get_experiment_html(self, experiment_key):
        """
        Get the HTML associated with the experiment.
        """
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("experiment/html", params=params)

    def get_experiment_code(self, experiment_key):
        """
        Get the associated source code for this experiment.
        """
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("experiment/code", params)

    def get_experiment_output(self, experiment_key):
        """
        Get the associated standard output for this experiment.
        """
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("experiment/output", params)

    def get_experiment_metadata(self, experiment_key):
        """
        Returns the JSON metadata for an experiment

        Returns:

        ```python
        {
            'archived': False,
            'durationMillis': 7,
            'endTimeMillis': 1586174765277,
            'experimentKey': 'EXPERIMENT-KEY',
            'experimentName': None,
            'fileName': None,
            'filePath': None,
            'optimizationId': None,
            'projectId': 'PROJECT-ID',
            'projectName': 'PROJECT-NAME',
            'running': False,
            'startTimeMillis': 1586174757596,
            'throttle': False,
            'workspaceName': 'WORKSPACE-NAME',
        }
        ```
        """
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        results = self.get_from_endpoint("experiment/metadata", params)
        return results

    def get_experiment_git_patch(self, experiment_key):
        """
        Get the git-patch associated with this experiment.
        """
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        results = self.get_from_endpoint("experiment/git/patch", params, "binary")
        # NOTE: returns either a binary JSON message or a binary git patch
        if results.startswith(b'{"msg"'):
            return None  # JSON indicates no patch
        else:
            return results

    def get_experiment_git_metadata(self, experiment_key):
        """
        Get the git-metadata associated with this experiment.
        """
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("experiment/git/metadata", params)

    def get_experiments_metadata(self, workspace, project_name):
        """
        Return the names of the projects in a workspace.
        """
        payload = {"workspaceName": workspace, "projectName": project_name}
        results = self.get_from_endpoint("projects", payload)
        return results

    # Other methods:

    def get_workspaces(self):
        """
        Get workspace names.
        """
        return self.get_from_endpoint("workspaces", {})

    def get_project_experiments(self, workspace, project_name):
        """
        Return the metadata JSONs of the experiments in a project.
        """
        payload = {"workspaceName": workspace, "projectName": project_name}
        results = self.get_from_endpoint("experiments", payload)
        # Returns list of experiment dicts
        return results

    def get_project_notes_by_id(self, project_id):
        """
        Return the notes of a project.
        """
        payload = {"projectId": project_id}
        results = self.get_from_endpoint("project/notes", payload)
        if results and "notes" in results:
            return results["notes"]

    def get_project_jsons(self, workspace):
        """
        Return the JSONs of the projects in a workspace.
        """
        payload = {"workspaceName": workspace}
        return self.get_from_endpoint("projects", payload)

    def get_projects(self, workspace):
        """
        Get project names in a workspace.
        """
        payload = {"workspaceName": workspace}
        results = self.get_from_endpoint("projects", payload)
        if results and "projects" in results:
            projects = [
                project_json["projectName"] for project_json in results["projects"]
            ]
            return projects

    def get_project(self, workspace=None, project_name=None, project_id=None):
        """
        Get project details.
        """
        if project_id is not None:
            payload = {"projectId": project_id}
        else:
            payload = {"workspaceName": workspace, "projectName": project_name}
        results = self.get_from_endpoint("project", payload)
        return results

    def get_project_by_id(self, project_id):
        """
        Get project details.
        """
        payload = {"projectId": project_id}
        results = self.get_from_endpoint("project", payload)
        return results

    def get_project_json(self, workspace, project_name):
        """
        Get Project metadata JSON.
        """
        payload = {"workspaceName": workspace}
        results = self.get_from_endpoint("projects", payload)
        if results and "projects" in results:
            projects = [
                project_json
                for project_json in results["projects"]
                if project_json["projectName"] == project_name
            ]
            if len(projects) > 0:
                # Get the first if more than one
                return projects[0]
            # else, return None

    def query_project(self, workspace, project_name, predicates, archived=False):
        """
        Given a workspace, project_name, and predicates, return matching experiments.
        """
        payload = {
            "workspaceName": workspace,
            "projectName": project_name,
            "predicates": predicates,
            "archived": archived,
        }
        return self.post_from_endpoint("project/query", payload)

    def get_project_columns(self, workspace, project_name):
        """
        Given a workspace and project_name return the column names, types, etc.
        """
        payload = {"workspaceName": workspace, "projectName": project_name}
        return self.get_from_endpoint("project/column-names", payload)

    # Write methods:

    def update_project(
        self,
        workspace,
        project_name,
        new_project_name=None,
        description=None,
        public=None,
    ):
        """
        Update the metadata of a project by project_name
        and workspace.

        Args:
            workspace: name of workspace
            project_name: name of project
            new_project_name: new name of project (optional)
            description: new description of project (optional)
            public: new setting of visibility (optional)
        """
        payload = {}
        if project_name is None or workspace is None:
            raise ValueError("update_project requires workspace and project_name")
        payload["projectName"] = project_name
        payload["workspaceName"] = workspace
        if new_project_name is not None:
            payload["newProjectName"] = new_project_name
        if description is not None:
            payload["newProjectDescription"] = description
        if public is not None:
            payload["isPublic"] = public
        response = self.post_from_endpoint("write/project/update", payload)
        return response

    def update_project_by_id(
        self, project_id, new_project_name=None, description=None, public=None
    ):
        """
        Update the metdata of a project by project_id.

        Args:
            project_id: project id
            new_project_name: new name of project (optional)
            description: new description of project (optional)
            public: new setting of visibility (optional)
        """
        if project_id is None:
            raise ValueError("update_project_by_id requires project_id")
        payload = {}
        payload["projectId"] = project_id
        if new_project_name is not None:
            payload["newProjectName"] = new_project_name
        if description is not None:
            payload["newProjectDescription"] = description
        if public is not None:
            payload["isPublic"] = public
        response = self.post_from_endpoint("write/project/update", payload)
        return response

    def create_project_share_key(self, project_id):
        """
        Create a sharable key for a private project.

        Args:
            project_id: project id

        Example:
        ```python
        >>> api = API()
        >>> SHARE_KEY = api.create_project_share_key(PROJECT_ID)
        ```
        You can now share the private project with:
        https://comet.ml/workspace/project?shareable=SHARE_KEY

        See also: get_project_share_keys(), and delete_project_share_key().
        """
        payload = {"projectId": project_id}
        response = self.get_from_endpoint("write/project/add-share-link", payload)
        return response

    def get_project_share_keys(self, project_id):
        """
        Get all sharable keys for a private project.

        Args:
            project_id: project id

        Example:
        ```python
        >>> api = API()
        >>> SHARE_KEYS = api.get_project_share_keys(PROJECT_ID)
        ```

        See also: create_project_share_key(), and delete_project_share_key().
        """
        payload = {"projectId": project_id}
        response = self.get_from_endpoint("project/get-project-share-links", payload)
        return response

    def delete_project_share_key(self, project_id, share_key):
        """
        Delete a sharable key for a private project.

        Args:
            project_id: project id
            share_key: the share key

        Example:
        ```python
        >>> api = API()
        >>> SHARE_KEYS = api.get_project_share_keys(PROJECT_ID)
        >>> api.delete_project_share_key(PROJECT_ID, SHARE_KEYS[0])
        ```

        See also: create_project_share_key(), and get_project_share_keys().
        """
        payload = {
            "projectId": project_id,
            "shareCode": share_key,
        }
        response = self.get_from_endpoint(
            "write/project/delete-project-share-link", payload
        )
        return response

    def add_experiment_gpu_metrics(self, experiment_key, gpu_metrics):
        """
        Add an instance of GPU metrics.

        Args:
            experiment_key: an experiment id
            gpu_metrics: a list of dicts with keys:
                * gpuId: required, Int identifier
                * freeMemory: required, Long
                * usedMemory: required, Long
                * gpuUtilization: required, Int percentage utilization
                * totalMemory: required, Long
        """
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, "gpus": gpu_metrics}
        response = self.post_from_endpoint("write/experiment/gpu-metrics", payload)
        return response

    def add_experiment_cpu_metrics(
        self,
        experiment_key,
        cpu_metrics,
        context=None,
        step=None,
        epoch=None,
        timestamp=None,
    ):
        """
        Add an instance of CPU metrics.

        Args:
            experiment_key: an experiment id
            cpu_metrics: a list of integer percentages, ordered by cpu
            context: optional, a run context
            step: optional, the current step
            epoch: optional, the current epoch
            timestamp: optional": current time, in milliseconds since the Epoch
        """
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            "cpuPercentUtilization": cpu_metrics,
        }
        if context is not None:
            payload["context"] = context
        if step is not None:
            payload["step"] = step
        if epoch is not None:
            payload["epoch"] = epoch
        if timestamp is not None:
            payload["timestamp"] = timestamp
        response = self.post_from_endpoint("write/experiment/cpu-metrics", payload)
        return response

    def add_experiment_ram_metrics(
        self,
        experiment_key,
        total_ram,
        used_ram,
        context=None,
        step=None,
        epoch=None,
        timestamp=None,
    ):
        """
        Add an instance of RAM metrics.

        Args:
            experiment_key: an experiment id
            total_ram: required, total RAM available
            used_ram: required,  RAM used
            context: optional, the run context
            step: optional, the current step
            epoch: optional, the current epoch
            timestamp: optional, the current timestamp
        """
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_TOTAL_RAM: total_ram,
            PAYLOAD_USED_RAM: used_ram,
        }
        if context is not None:
            payload["context"] = context
        if step is not None:
            payload["step"] = step
        if epoch is not None:
            payload["epoch"] = epoch
        if timestamp is not None:
            payload["timestamp"] = timestamp
        response = self.post_from_endpoint("write/experiment/ram-metrics", payload)
        return response

    def add_experiment_load_metrics(
        self,
        experiment_key,
        load_avg,
        context=None,
        step=None,
        epoch=None,
        timestamp=None,
    ):
        """
        Add an instance of system load metrics.

        Args:
            experiment_key: an experiment id
            load_avg: required, the load average
            context: optional, the run context
            step: optional, the current step
            epoch: optional, the current epoch
            timestamp: optional, the current timestamp
        """
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, "loadAverage": load_avg}
        if context is not None:
            payload["context"] = context
        if step is not None:
            payload["step"] = step
        if epoch is not None:
            payload["epoch"] = epoch
        if timestamp is not None:
            payload["timestamp"] = timestamp
        response = self.post_from_endpoint("write/experiment/load-metrics", payload)
        return response

    def set_experiment_git_metadata(
        self, experiment_key, user, root, branch, parent, origin
    ):
        """ """
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            "user": user,
            "root": root,
            "branch": branch,
            "parent": parent,
            "origin": origin,
        }
        response = self.post_from_endpoint("write/experiment/git/metadata", payload)
        return response

    def set_experiment_git_patch(self, experiment_key, contents):
        """ """
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        files = {"file": ("filename", contents)}
        response = self.post_from_endpoint(
            "write/experiment/git/patch", {}, files=files, params=params
        )
        return response

    def set_experiment_code(self, experiment_key, code):
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, "code": code}
        response = self.post_from_endpoint("write/experiment/code", payload)
        return response

    def set_experiment_model_graph(self, experiment_key, graph_str):
        # type: (str, str) -> requests.Response
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_MODEL_GRAPH: graph_str,
        }
        response = self.post_from_endpoint("write/experiment/graph", payload)
        return response

    def set_experiment_os_packages(self, experiment_key, os_packages):
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_OS_PACKAGES: os_packages,
        }
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_user(self, experiment_key, user):
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, PAYLOAD_USER: user}
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_installed_packages(self, experiment_key, installed_packages):
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_INSTALLED_PACKAGES: installed_packages,
        }
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_command(self, experiment_key, command):
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, PAYLOAD_COMMAND: command}
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_gpu_static_info(self, experiment_key, gpu_static_info):
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_GPU_STATIC_INFO_LIST: gpu_static_info,
        }
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_executable(self, experiment_key, executable):
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_EXECUTABLE: executable,
        }
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_filename(self, experiment_key, filename):
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, PAYLOAD_FILE_PATH: filename}
        response = self.post_from_endpoint("write/experiment/file-path", payload)
        return response

    def set_experiment_hostname(self, experiment_key, hostname):
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, PAYLOAD_HOSTNAME: hostname}
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_ip(self, experiment_key, ip):
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, PAYLOAD_IP: ip}
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def log_experiment_system_info(self, experiment_key, system_info):
        if not isinstance(system_info, list):
            raise ValueError("system_info must be a list of {key:..., value:...} dicts")
        for si in system_info:
            if "key" not in si or "value" not in si:
                raise ValueError(
                    "system_info must be a list of {key:..., value:...} dicts"
                )

        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_ADDITIONAL_SYSTEM_INFO_LIST: system_info,
        }
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_network_interface_ips(
        self, experiment_key, network_interface_ips
    ):
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            "networkInterfaceIps": network_interface_ips,
        }
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_os(self, experiment_key, os):
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, PAYLOAD_OS: os}
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_os_type(self, experiment_key, os_type):
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, PAYLOAD_OS_TYPE: os_type}
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_os_release(self, experiment_key, os_release):
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_OS_RELEASE: os_release,
        }
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_pid(self, experiment_key, pid):
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, PAYLOAD_PID: pid}
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_python_version(self, experiment_key, python_version):
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_PYTHON_VERSION: python_version,
        }
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_python_version_verbose(
        self, experiment_key, python_version_verbose
    ):
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_PYTHON_VERSION_VERBOSE: python_version_verbose,
        }
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_machine(self, experiment_key, machine):
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, PAYLOAD_MACHINE: machine}
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_processor(self, experiment_key, processor):
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, PAYLOAD_PROCESSOR: processor}
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def set_experiment_system_details(
        self,
        _os,
        command,
        env,
        experiment_key,
        hostname,
        ip,
        machine,
        os_release,
        os_type,
        pid,
        processor,
        python_exe,
        python_version_verbose,
        python_version,
        user,
    ):
        payload = {
            PAYLOAD_COMMAND: command,
            PAYLOAD_ENV: env,
            PAYLOAD_EXECUTABLE: python_exe,
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_HOSTNAME: hostname,
            PAYLOAD_IP: ip,
            PAYLOAD_MACHINE: machine,
            PAYLOAD_OS: _os,
            PAYLOAD_OS_RELEASE: os_release,
            PAYLOAD_OS_TYPE: os_type,
            PAYLOAD_PID: pid,
            PAYLOAD_PROCESSOR: processor,
            PAYLOAD_PYTHON_VERSION: python_version,
            PAYLOAD_PYTHON_VERSION_VERBOSE: python_version_verbose,
            PAYLOAD_USER: user,
        }
        response = self.post_from_endpoint("write/experiment/system-details", payload)
        return response

    def update_experiment_status(self, experiment_key):
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        response = self.get_from_endpoint("write/experiment/set-status", payload)
        return response

    def set_experiment_start_end(self, experiment_key, start_time, end_time):
        """
        Set the start/end time of an experiment.

        Note: times are in milliseconds.
        """
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            "startTimeMillis": start_time,
            "endTimeMillis": end_time,
        }
        response = self.post_from_endpoint(
            "write/experiment/set-start-end-time", payload
        )
        return response

    def set_project_notes_by_id(self, project_id, notes):
        """
        Set the notes of a project.
        """
        payload = {"projectId": project_id, "notes": notes}
        results = self.post_from_endpoint("write/project/notes", payload)
        if results:
            return results.json()

    def set_experiment_cloud_details(self, experiment_key, provider, cloud_metadata):
        # type: (str, str, Dict[Any, Any]) -> requests.Response
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_PROVIDER: provider,
            PAYLOAD_METADATA: cloud_metadata,
        }
        response = self.post_from_endpoint("write/experiment/cloud-details", payload)
        return response

    def log_experiment_output(
        self, experiment_key, output, context=None, stderr=False, timestamp=None
    ):
        """
        Log an output line.

        Args:
            experiment_key: str, the experiment id
            output: str, the output to log
            context: str, the context of the output
            stderr: bool, if True, then output is stderr
            timestamp: int, time in seconds, since epoch
        """
        if timestamp is None:
            timestamp = get_time_monotonic()

        stdout_lines = []

        for offset, line in enumerate(output.splitlines(True)):
            stdout_lines.append(
                {
                    PAYLOAD_STDERR: stderr,
                    PAYLOAD_OUTPUT: line,
                    PAYLOAD_LOCAL_TIMESTAMP: int(timestamp * 1000),
                    PAYLOAD_OFFSET: offset,
                }
            )

        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_RUN_CONTEXT: context,
            PAYLOAD_OUTPUT_LINES: stdout_lines,
        }
        response = self.post_from_endpoint("write/experiment/output", payload)
        return response

    def send_stdout_batch(
        self, batch_items, experiment_key, compress=True, timestamp=None
    ):
        # type: (List[MessageBatchItem], str, bool, int) -> None

        endpoint_url = "write/experiment/output"

        LOGGER.debug(
            "Sending stdout messages batch, length: %d, compression enabled: %s, endpoint: %s",
            len(batch_items),
            compress,
            endpoint_url,
        )

        if timestamp is None:
            timestamp = get_time_monotonic()

        stderr_flags = [False, True]
        for stderr in stderr_flags:
            payload = format_stdout_message_batch_items(
                batch_items=batch_items,
                timestamp=timestamp,
                experiment_key=experiment_key,
                stderr=stderr,
            )
            if payload is not None:
                self.post_from_endpoint(endpoint_url, payload, compress=compress)

    def log_experiment_other(self, experiment_key, key, value, timestamp=None):
        """
        Set an other key/value pair for an experiment.

        Args:
            experiment_key: str, the experiment id
            key: str, the name of the other value
            value: any, the value of the other key
            timestamp: int, time in seconds, since epoch
        """
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            "key": key,
            "value": value,
        }
        if timestamp is not None:
            payload["timestamp"] = int(timestamp * 1000)
        response = self.post_from_endpoint("write/experiment/log-other", payload)
        return response

    def log_experiment_parameter(
        self, experiment_key, parameter, value, step=None, timestamp=None
    ):
        """
        Set a parameter name/value pair for an experiment.

        Args:
            experiment_key: str, the experiment id
            parameter: str, the name of the parameter
            value: any, the value of the parameter
            step: int, the step number at time of logging
            timestamp: int, time in seconds, since epoch
        """
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            "parameterName": parameter,
            "parameterValue": value,
        }
        if step is not None:
            payload["step"] = step
        if timestamp is not None:
            payload["timestamp"] = int(timestamp * 1000)
        response = self.post_from_endpoint("write/experiment/parameter", payload)
        return response

    def log_experiment_metric(
        self,
        experiment_key,
        metric,
        value,
        step=None,
        epoch=None,
        timestamp=None,
        context=None,
    ):
        """
        Set a metric name/value pair for an experiment.
        """
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            "metricName": metric,
            "metricValue": value,
        }
        if epoch is not None:
            payload["epoch"] = epoch
        if context is not None:
            payload["context"] = context
        if step is not None:
            payload["step"] = step
        if timestamp is not None:
            payload["timestamp"] = int(timestamp * 1000)
        response = self.post_from_endpoint("write/experiment/metric", payload)
        return response

    def log_experiment_html(
        self, experiment_key, html, overwrite=False, timestamp=None
    ):
        """
        Set, or append onto, an experiment's HTML.

        Args:
            experiment_key: str, the experiment id
            html: str, the html string to log
            overwrite: bool, if, true overwrite previously-logged html
            timestamp: int, time in seconds, since epoch
        """
        if timestamp is None:
            timestamp = local_timestamp()
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_HTML: html,
            PAYLOAD_OVERRIDE: overwrite,
            PAYLOAD_TIMESTAMP: int(timestamp * 1000),
        }
        response = self.post_from_endpoint("write/experiment/html", payload)
        return response

    def log_experiment_dependency(self, experiment_key, name, version, timestamp=None):
        if timestamp is None:
            timestamp = local_timestamp()
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            PAYLOAD_TIMESTAMP: int(timestamp * 1000),
            PAYLOAD_DEPENDENCIES: [
                {
                    PAYLOAD_DEPENDENCY_NAME: name,
                    PAYLOAD_DEPENDENCY_VERSION: version,
                }
            ],
        }
        return self.put_from_endpoint(
            endpoint="write/experiment/dependencies",
            payload=payload,
        )

    def add_experiment_tags(self, experiment_key, tags):
        """
        Append onto an experiment's list of tags.
        """
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, "addedTags": tags}
        response = self.post_from_endpoint("write/experiment/tags", payload)
        return response

    def log_experiment_asset(
        self,
        experiment_key,
        file_data,
        step=None,
        overwrite=None,
        context=None,
        ftype=None,
        metadata=None,
        extension=None,
        file_content=None,
        file_name=None,
    ):
        """
        Upload an asset to an experiment.
        """
        if file_name is None:
            if not isinstance(file_data, str):
                LOGGER.warning("logging file-like asset with no name; using `unnamed`")
                file_name = "unnamed"
            else:
                file_name = file_data

        params = {PAYLOAD_EXPERIMENT_KEY: experiment_key, "fileName": file_name}
        if step is not None:
            params["step"] = step
        if overwrite is not None:
            params["overwrite"] = overwrite
        if context is not None:
            params["context"] = context
        if ftype is not None:
            params["type"] = ftype
        if metadata is not None:
            params["metadata"] = metadata
        if extension is not None:
            params["extension"] = extension
        else:
            ext = os.path.splitext(file_name)[1]
            if ext:
                params["extension"] = ext

        headers = {self.API_KEY_HEADER: self.api_key}
        headers.update(self.low_level_api_client.headers)

        url = url_join(self.base_url, "write/experiment/upload-asset")

        if file_content:
            processor = AssetDataUploadProcessor(
                file_content,
                ftype,
                params,
                upload_limit=float("+inf"),
                copy_to_tmp=False,
                error_message_identifier=None,
                metadata=metadata,
                tmp_dir=None,
                critical=False,
            )
            message = processor.process()
        else:
            processor = AssetUploadProcessor(
                file_data,
                ftype,
                params,
                upload_limit=float("+inf"),
                copy_to_tmp=False,
                error_message_identifier=None,
                metadata=metadata,
                tmp_dir=None,
                critical=False,
            )
            message = processor.process()

        # We could get a file-like upload message in case filename is not a file-path or an invalid
        # one
        if isinstance(message, UploadFileMessage):
            response = send_file(
                url,
                message.file_path,
                params=message.additional_params,
                headers=headers,
                timeout=self.config.get_int(None, "comet.timeout.file_upload"),
                metadata=message.metadata,
                session=self.low_level_api_client.session,
            )
        else:
            response = send_file_like(
                url,
                message.file_like,
                params=message.additional_params,
                headers=headers,
                timeout=self.config.get_int(None, "comet.timeout.file_upload"),
                metadata=message.metadata,
                session=self.low_level_api_client.session,
            )

        return _check_response_status(response)

    def log_experiment_image(
        self,
        experiment_key,
        filename,
        image_name=None,
        step=None,
        overwrite=None,
        context=None,
    ):
        """
        Upload an image asset to an experiment.
        """
        _, filename_extension = os.path.splitext(filename)
        with open(filename, "rb") as fp:
            params = {
                PAYLOAD_EXPERIMENT_KEY: experiment_key,
                "type": "image",
                "extension": filename_extension,
            }
            files = {"file": (filename, fp)}
            if image_name is not None:
                params["fileName"] = image_name
            if step is not None:
                params["step"] = step
            if overwrite is not None:
                params["overwrite"] = overwrite
            if context is not None:
                params["context"] = context
            response = self.post_from_endpoint(
                "write/experiment/upload-asset", {}, params=params, files=files
            )
            return response

    def stop_experiment(self, experiment_key):
        """
        Stop a running experiment.
        """
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("write/experiment/stop", payload)

    def get_artifact_list(
        self,
        workspace,
        artifact_type=None,
    ):
        # type: (str, Optional[str]) -> Any
        params = {"workspace": workspace}

        if artifact_type is not None:
            params["type"] = artifact_type

        return self.get_from_endpoint("artifacts/get-all", params)

    def get_artifact_details(self, artifact_id=None, workspace=None, name=None):
        # type: (Optional[str], Optional[str], Optional[str]) -> Any
        params = {
            "artifact_id": artifact_id,
            "workspace": workspace,
            "artifactName": name,
        }

        return self.get_from_endpoint("artifacts/get", params)

    def get_artifact_version_details(
        self,
        workspace=None,
        name=None,
        artifact_id=None,
        version=None,
        alias=None,
        artifact_version_id=None,
        version_or_alias=None,
        experiment_key=None,
        consumer_experiment_key=None,
    ):
        params = {
            "alias": alias,
            "artifactId": artifact_id,
            "artifactName": name,
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            "consumerExperimentKey": consumer_experiment_key,
            "version": version,
            "versionId": artifact_version_id,
            "versionOrAlias": version_or_alias,
            "workspace": workspace,
        }

        return self.get_from_endpoint("artifacts/version", params)

    def get_artifact_files(
        self,
        artifact_id=None,
        workspace=None,
        project=None,
        name=None,
        version=None,
        alias=None,
    ):
        # type: (Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]) -> Any
        params = {
            "artifact_id": artifact_id,
            "workspace": workspace,
            "artifactName": name,
            "version": version,
            "alias": alias,
        }

        return self.get_from_endpoint("artifacts/version/files", params)

    def upsert_artifact(
        self,
        artifact_name=None,  # type: Optional[str]
        artifact_type=None,  # type: Optional[str]
        description=None,  # type: Optional[str]
        experiment_key=None,  # type: Optional[str]
        is_public=None,  # type: Optional[str]
        metadata=None,  # type: Optional[Dict[Any, Any]]
        version=None,  # type: Optional[str]
        aliases=None,  # type: Optional[List[str]]
        version_tags=None,  # type: Optional[List[str]]
    ):
        # type: (...) -> Any
        version_metadata = encode_metadata(metadata)

        payload = {
            "artifactName": artifact_name,
            "artifactType": artifact_type,
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            "versionMetadata": version_metadata,
            "version": version,
            "alias": aliases,
            "versionTags": version_tags,
        }

        return self.post_from_endpoint("write/artifacts/upsert", payload)

    def update_artifact(
        self,
        artifact_id,  # type: str
        artifact_type=None,  # type: Optional[str]
        metadata=None,  # type: Optional[Dict[Any, Any]]
        version=None,  # type: Optional[str]
        aliases=None,  # type: Optional[List[str]]
        tags=None,  # type: Optional[List[str]]
    ):
        # type: (...) -> Any
        artifact_metadata = encode_metadata(metadata)

        payload = {
            "artifactId": artifact_id,
            "artifactType": artifact_type,
            "versionMetadata": artifact_metadata,
            "version": version,
            "tags": tags,
        }

        return self.post_from_endpoint("write/artifacts/details", payload)

    def update_artifact_version(
        self,
        artifact_version_id,  # type: str
        version_aliases=None,  # type: Optional[List[str]]
        version_metadata=None,  # type: Optional[Dict[Any, Any]]
        version_tags=None,  # type: Optional[List[str]]
    ):
        # type: (...) -> Any
        artifact_version_metadata = encode_metadata(version_metadata)

        payload = {
            "alias": version_aliases,
            "artifactVersionId": artifact_version_id,
            "versionMetadata": artifact_version_metadata,
            "versionTags": version_tags,
        }

        return self.post_from_endpoint("write/artifacts/version/labels", payload)

    def update_experiment_error_status(
        self,
        experiment_key,  # type: str
        is_alive,  # type: bool
        has_error,  # type: bool
    ):
        # type: (...) -> Any
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            "isAlive": is_alive,
            "error": has_error,
        }

        try:
            return self.post_from_endpoint(
                "write/experiment/update-status", payload=payload
            )
        except Exception as ex:
            LOGGER.debug(
                "Failed to update experiment error status, reason: %r",
                ex,
                exc_info=True,
            )

    def _prepare_update_artifact_version_state(
        self,
        artifact_version_id,  # type: str
        experiment_key,  # type: str
        state,  # type: str
    ):
        # type: (...) -> Tuple[str, Dict[str, Any], Dict[str, Any]]
        params = {
            "artifactVersionId": artifact_version_id,
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            "state": state,
        }

        url = url_join(self.base_url, "write/artifacts/state")

        headers = {}

        headers[self.API_KEY_HEADER] = self.api_key
        headers.update(self.low_level_api_client.headers)

        return (url, params, headers)

    # Create, Delete, Archive and Move methods:

    def move_experiments(
        self, experiment_keys, target_workspace, target_project_name, symlink=False
    ):
        """
        Move/symlink list of experiments to another workspace/project_name
        """
        payload = {
            "targetWorkspaceName": target_workspace,
            "targetProjectName": target_project_name,
            "experimentKeys": experiment_keys,
            "symlink": symlink,
        }
        return self.post_from_endpoint("write/experiment/move", payload)

    def delete_experiment(self, experiment_key):
        """
        Delete one experiment.
        """
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("write/experiment/delete", payload)

    def delete_experiment_asset(self, experiment_key, asset_id):
        """
        Delete an experiment's asset.
        """
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key, "assetId": asset_id}
        return self.get_from_endpoint("write/experiment/asset/delete", payload)

    def delete_project(
        self,
        workspace=None,
        project_name=None,
        project_id=None,
        delete_experiments=False,
    ):
        """
        Delete a project.
        """
        if project_id is not None:
            payload = {
                "projectId": project_id,
                "deleteAllExperiments": delete_experiments,
            }
        else:
            payload = {
                "workspaceName": workspace,
                "projectName": project_name,
                "deleteAllExperiments": delete_experiments,
            }
        return self.post_from_endpoint("write/project/delete", payload)

    def restore_experiment(self, experiment_key):
        """
        Restore one experiment.
        """
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("write/experiment/restore", payload)

    def delete_experiments(self, experiment_keys):
        """
        Delete list of experiments.
        """
        return [self.delete_experiment(key) for key in experiment_keys]

    def archive_experiment(self, experiment_key):
        """
        Archive one experiment.
        """
        payload = {PAYLOAD_EXPERIMENT_KEY: experiment_key}
        return self.get_from_endpoint("write/experiment/archive", payload)

    def archive_experiments(self, experiment_keys):
        """
        Archive list of experiments.
        """
        return [self.archive_experiment(key) for key in experiment_keys]

    def create_project(
        self, workspace, project_name, project_description=None, public=False
    ):
        """
        Create a project.
        """
        payload = {
            "workspaceName": workspace,
            "projectName": project_name,
            "projectDescription": project_description,
            "isPublic": public,
        }
        response = self.post_from_endpoint("write/project/create", payload)
        return response.json()

    def create_experiment(self, workspace, project_name, experiment_name=None):
        """
        Create an experiment and return its associated APIExperiment.
        """
        payload = {"workspaceName": workspace, "projectName": project_name}
        if experiment_name is not None:
            payload["experimentName"] = experiment_name
        return self.post_from_endpoint("write/experiment/create", payload)

    def create_experiment_symlink(self, experiment_key, project_name):
        """
        Create a copy of this experiment in another project
        in the workspace.
        """
        payload = {
            PAYLOAD_EXPERIMENT_KEY: experiment_key,
            "projectName": project_name,
        }
        return self.get_from_endpoint("write/project/symlink", payload)

    # Experiment model methods:

    def get_experiment_models(self, experiment_id):
        """
        Given an experiment id, return a list of model data associated
        with an experiment.

        Args:
            experiment_id: the experiment's key

        Returns [{'experimentModelId': 'MODEL-ID'
                  'experimentKey': 'EXPERIMENT-KEY',
                  'modelName': 'MODEL-NAME'}, ...]
        """
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_id}
        response = self.get_from_endpoint("experiment/model", params)
        if response:
            return response["models"]

    def get_experiment_model_asset_list(self, experiment_id, model_name):
        """
        Get an experiment model's asset list by model name.

        Args:
            experiment_id: the experiment's key
            model_name: str, the name of the model

        Returns: a list of asset dictionaries with these fields:
            * fileName
            * fileSize
            * runContext
            * step
            * link
            * createdAt
            * dir
            * canView
            * audio
            * histogram
            * image
            * type
            * metadata
            * assetId
        """
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_id, "modelName": model_name}
        response = self.get_from_endpoint("experiment/model/asset/list", params)
        if response:
            return response["assets"]

    def get_experiment_model_zipfile(self, experiment_id, model_name):
        params = {PAYLOAD_EXPERIMENT_KEY: experiment_id, "modelName": model_name}
        return self.get_from_endpoint(
            "experiment/model/download",
            params,
            return_type="binary",
            timeout=self.config.get_int(None, "comet.timeout.file_download"),
        )

    # Registry model methods:

    def get_registry_models(self, workspace):
        """
        Return a list of registered models in workspace.

        Args:
            workspace: the name of workspace
        """
        params = {"workspaceName": workspace}
        response = self.get_from_endpoint("registry-model", params)
        if response:
            return response["registryModels"]

    def get_registry_model_count(self, workspace):
        """
        Return a count of registered models in workspace.

        Args:
            workspace: the name of workspace
        """
        params = {"workspaceName": workspace}
        response = self.get_from_endpoint("registry-model/count", params)
        if response:
            return response["registryModelCount"]

    def get_registry_model_versions(self, workspace, registry_name):
        """
        Return a list of versions of the registered model in the given
        workspace.

        Args:
            workspace: the name of workspace
            registry_name: the name of the registered model
        """
        return [
            m["version"]
            for m in self.get_registry_model_details(workspace, registry_name)[
                "versions"
            ]
        ]

    def get_registry_model_details(self, workspace, registry_name, version=None):
        """
        Return a dictionary of details of the model in given
        workspace.

        Args:
            workspace: the name of workspace
            registry_name: the name of the registered model
            version: optional, the string version number
        """
        params = {"workspaceName": workspace, "modelName": registry_name}
        response = self.get_from_endpoint("registry-model/details", params)
        if response:
            if version is None:
                return response
            else:
                select = [
                    model
                    for model in response["versions"]
                    if model["version"] == version
                ]
                if len(select) > 0:
                    return select[0]
                else:
                    return None

    def get_registry_model_notes(self, workspace, registry_name):
        """
        Return the notes for a registered model.
        """
        params = {"workspaceName": workspace, "modelName": registry_name}
        response = self.get_from_endpoint("registry-model/notes", params)
        if response:
            return response["notes"]

    def get_registry_model_zipfile(self, workspace, registry_name, version, stage):
        # type: (str, str, Optional[str], Optional[str]) -> bytes
        params = {
            "workspaceName": workspace,
            "modelName": registry_name,
        }

        if version and stage:
            raise ValueError(
                "Please specify version OR stage (not both) to download model"
            )

        if version is not None:
            params["version"] = version

        if stage is not None:
            params["stage"] = stage

        return self.get_from_endpoint(
            "registry-model/item/download",
            params,
            return_type="binary",
            timeout=self.config["comet.timeout.file_download"],
        )

    def get_registry_model_items_download_links(
        self, workspace, registry_name, version, stage
    ):
        # type: (str, str, Optional[str], Optional[str]) -> Any
        params = {
            "workspaceName": workspace,
            "modelName": registry_name,
        }

        if version and stage:
            raise ValueError(
                "Please specify version OR stage (not both) to download model"
            )

        if version is not None:
            params["version"] = version

        if stage is not None:
            params["stage"] = stage

        return self.get_from_endpoint(
            "registry-model/item/download-instructions",
            params,
            return_type="json",
        )

    # Write registry methods:

    def register_model(
        self,
        experiment_id,
        model_name,
        version,
        workspace,
        registry_name,
        public,
        description,
        comment,
        stages,
    ):
        """
        Register an experiment model in the workspace registry.

        Args:
            experiment_id: the experiment key
            model_name: the name of the experiment model
            workspace: the name of workspace
            version: a version string
            registry_name: the name of the registered workspace model
            public: if True, then the model will be publically viewable
            description: optional, a textual description of the model
            comment: optional, a textual comment about the model
            stages: optional, a list of textual tags such as ["production", "staging"] etc.

        Returns 200 Response if successful
        """
        models = self.get_experiment_models(experiment_id)
        if len(models) == 0:
            raise ValueError("There are no models for experiment %r" % experiment_id)
        # Look up the model name:
        details = [model for model in models if model["modelName"] == model_name]
        # If model name found:
        if len(details) == 1:
            registry_name = registry_name or proper_registry_model_name(model_name)
            registry_models = [model for model in self.get_registry_models(workspace)]
            model_id = details[0]["experimentModelId"]
            payload = {
                "experimentModelId": model_id,
                "registryModelName": registry_name,
                "version": version,
            }
            if public is not None:
                payload["isPublic"] = public
            if description is not None:
                payload["description"] = description
            if comment is not None:
                payload["comment"] = comment
            if stages is not None:
                if not isinstance(stages, (list, tuple)) or any(
                    not isinstance(s, str) for s in stages
                ):
                    raise ValueError("Invalid stages list: should be a list of strings")
                payload["stages"] = stages

            # Now we create or add a new version:
            if payload["registryModelName"] in [
                model["modelName"] for model in registry_models
            ]:
                # Adding a new version of existing registry model:
                if "description" in payload:
                    del payload["description"]
                    LOGGER.warning(
                        "The argument 'description' was given, but ignored when adding a new registry model version"
                    )
                if "isPublic" in payload:
                    del payload["isPublic"]
                    LOGGER.warning(
                        "The argument 'public' was given, but ignored when adding a new registry model version"
                    )
                # Update:
                response = self.post_from_endpoint(
                    "write/registry-model/item", payload=payload
                )
            else:
                # Create:
                response = self.post_from_endpoint(
                    "write/registry-model", payload=payload
                )
            LOGGER.info(
                "Successfully registered %r, version %r in workspace %r",
                registry_name,
                version,
                workspace,
            )
            return response
        else:
            # Model name not found
            model_names = [model["modelName"] for model in models]
            raise ValueError(
                "Invalid experiment model name: %r; should be one of %r"
                % (model_name, model_names)
            )

    def update_registry_model_version(
        self, workspace, registry_name, version, comment=None, stages=None
    ):
        """
        Updates a registered model version's comments and/or stages.
        """
        details = self.get_registry_model_details(workspace, registry_name, version)
        payload = {"registryModelItemId": details["registryModelItemId"]}
        # update the registry model version: comment and stages
        if comment is not None:
            payload["comment"] = comment
        if stages is not None:
            if not isinstance(stages, (list, tuple)) or any(
                not isinstance(s, str) for s in stages
            ):
                raise ValueError("Invalid stages list: should be a list of strings")
            payload["stages"] = stages
        response = self.post_from_endpoint("write/registry-model/item/update", payload)
        if response:
            return response

    def update_registry_model(
        self, workspace, registry_name, new_name=None, description=None, public=None
    ):
        """
        Updates a registered model's name, description, and/or visibility.
        """
        details = self.get_registry_model_details(workspace, registry_name)
        payload = {"registryModelId": details["registryModelId"]}
        # update the registry model top level: name, description, and public
        if new_name is not None:
            payload["registryModelName"] = new_name
        if description is not None:
            payload["description"] = description
        if public is not None:
            payload["isPublic"] = public
        response = self.post_from_endpoint("write/registry-model/update", payload)
        if response:
            return response

    def delete_registry_model_version(self, workspace, registry_name, version):
        """
        Delete a registered model version
        """
        details = self.get_registry_model_details(workspace, registry_name, version)
        payload = {"modelItemId": details["registryModelItemId"]}
        response = self.get_from_endpoint(
            "write/registry-model/item/delete", payload, return_type="response"
        )
        if response:
            return response

    def delete_registry_model(self, workspace, registry_name):
        """
        Delete a registered model
        """
        params = {"workspaceName": workspace, "modelName": registry_name}
        response = self.get_from_endpoint(
            "write/registry-model/delete", params, return_type="response"
        )
        if response:
            return response

    def update_registry_model_notes(self, workspace, registry_name, notes):
        """
        Update the notes of a registry model.
        """
        payload = {
            "workspaceName": workspace,
            "registryModelName": registry_name,
            "notes": notes,
        }
        response = self.post_from_endpoint("write/registry-model/notes", payload)
        if response:
            return response

    def add_registry_model_version_stage(
        self, workspace, registry_name, version, stage
    ):
        details = self.get_registry_model_details(workspace, registry_name, version)
        params = {"modelItemId": details["registryModelItemId"], "stage": stage}
        response = self.get_from_endpoint(
            "write/registry-model/item/stage", params, return_type="response"
        )
        if response:
            return response

    def delete_registry_model_version_stage(
        self, workspace, registry_name, version, stage
    ):
        details = self.get_registry_model_details(workspace, registry_name, version)
        params = {"modelItemId": details["registryModelItemId"], "stage": stage}
        response = self.get_from_endpoint(
            "write/registry-model/item/stage/delete", params, return_type="response"
        )
        if response:
            return response

    # Other helpers
    def _check_api_backend_version(self, minimal_backend_version):
        # type: (Tuple[int, int, int]) -> None
        version_url = url_join(self.server_url, "clientlib/isAlive/ver")

        # Get the backend version
        try:
            response = self.low_level_api_client.get(
                version_url, check_status_code=True
            )
            # Invalid version will raise exception:
            backend_version = parse_version_number(response.json()["version"])
        except Exception:
            LOGGER.warning(BACKEND_VERSION_CHECK_ERROR, version_url, exc_info=True)
            return None

        # Compare versions
        if backend_version < minimal_backend_version:
            raise BackendVersionTooOld(
                version_url, backend_version, minimal_backend_version
            )

    # General methods:

    def close(self):
        self.low_level_api_client.close()

    def do_not_cache(self, *items):
        """
        Add these items from the do-not-cache list. Ignored
        as this class does not have cache.
        """
        pass

    def do_cache(self, *items):
        """
        Remove these items from the do-not-cache list. Raises
        Exception as this class does not have cache.
        """
        raise Exception("this implementation does not have cache")


class RestApiClientWithCache(RestApiClient):
    """
    Same as RestApiClient, except with optional cache.

    When you post_from_endpoint(write_endpoint) you clear the
    associated read_endpoints.

    When you get_from_endpoint(read_endpoint) you attempt to
    read from cache and save to cache unless in the NOCACHE.

    If you read from a read_endpoint that is not listed, then
    a debug message is shown.
    """

    # map of write endpoints to read endpoints
    # POST-ENDPOINT: [(GET-ENDPOINT, [GET-ARGS]), ...]
    ENDPOINTS = {
        "write/project/symlink": [],  # Nothing to do
        "write/experiment/set-status": [],  # Nothing to do
        "write/experiment/set-start-end-time": [
            ("experiment/metadata", [PAYLOAD_EXPERIMENT_KEY])
        ],
        "write/experiment/file-path": [
            ("experiment/metadata", [PAYLOAD_EXPERIMENT_KEY])
        ],
        "write/experiment/": [("experiment/metadata", [PAYLOAD_EXPERIMENT_KEY])],
        "write/experiment/system-details": [
            ("experiment/system-details", [PAYLOAD_EXPERIMENT_KEY])
        ],
        "write/experiment/code": [("experiment/code", [PAYLOAD_EXPERIMENT_KEY])],
        "write/experiment/graph": [("experiment/graph", [PAYLOAD_EXPERIMENT_KEY])],
        "write/experiment/output": [("experiment/output", [PAYLOAD_EXPERIMENT_KEY])],
        "write/experiment/log-other": [
            ("experiment/log-other", [PAYLOAD_EXPERIMENT_KEY])
        ],
        "write/experiment/parameter": [
            ("experiment/parameters", [PAYLOAD_EXPERIMENT_KEY])
        ],
        "write/experiment/metric": [
            ("experiment/metrics/get-metric", [PAYLOAD_EXPERIMENT_KEY]),
            ("experiment/metrics/summary", [PAYLOAD_EXPERIMENT_KEY]),
        ],
        "write/experiment/html": [("experiment/html", [PAYLOAD_EXPERIMENT_KEY])],
        "write/experiment/tags": [("experiment/tags", [PAYLOAD_EXPERIMENT_KEY])],
        "write/experiment/upload-asset": [
            (
                "experiment/asset/list",
                [PAYLOAD_EXPERIMENT_KEY],
            ),  # not usually cached
            (
                "experiment/asset/get-asset",
                [PAYLOAD_EXPERIMENT_KEY, "assetId"],
            ),  # not usually cached
        ],
        "write/experiment/git/metadata": [
            ("experiment/git/metadata", [PAYLOAD_EXPERIMENT_KEY])
        ],
        "write/experiment/git/patch": [
            ("experiment/git/patch", [PAYLOAD_EXPERIMENT_KEY])
        ],
        "write/experiment/cpu-metrics": [
            ("experiment/system-details", [PAYLOAD_EXPERIMENT_KEY])
        ],
        "write/experiment/gpu-metrics": [
            ("experiment/system-details", [PAYLOAD_EXPERIMENT_KEY])
        ],
        "write/experiment/load-metrics": [
            ("experiment/system-details", [PAYLOAD_EXPERIMENT_KEY])
        ],
        "write/experiment/ram-metrics": [
            ("experiment/system-details", [PAYLOAD_EXPERIMENT_KEY])
        ],
        "write/project/notes": [("project/notes", ["projectId"])],
        # Only read that is really a POST:
        "experiments/multi-metric-chart": [],  # Nothing to do
    }  # type: Dict[str, List[Tuple[str, List[str]]]]

    # Don't use cache on these GET endpoints:
    NOCACHE_ENDPOINTS = set(
        [
            "write/experiment/delete",  # GET
            "write/experiment/archive",  # GET
            "write/experiment/restore",  # GET
            "write/experiment/set-status",  # GET
            "write/project/symlink",  # GET
            "projects",  # GET
            "project/column-names",  # GET
            "experiments",  # GET
            "experiment/metadata",  # GET
            "experiment/asset/get-asset",  # GET
            "experiment/asset/list",  # GET
            # Don't cache model GETS:
            "experiment/model",  # GET
            "experiment/model/asset/list",  # GET
            "experiment/model/download",  # GET
            "registry-model",  # GET
            "registry-model/count",  # GET
            "registry-model/details",  # GET
            "registry-model/notes",  # GET
            "write/registry-model/delete",  # GET
            "write/registry-model/item/delete",  # GET
            "write/registry-model/item/stage",  # GET
            "write/registry-model/item/stage/delete",  # GET
            "write/project/delete-project-share-link",  # GET
            "project/get-project-share-links",  # GET
            "write/project/add-share-link",  # GET
        ]
    )

    # Some read endpoints have additional payload key/values to clear:
    EXTRA_PAYLOAD = {
        "experiment/asset/list": {
            "type": ["all", "histogram_combined_3d", "image", "audio", "video"]
        }
    }

    def __init__(self, *args, **kwargs):
        super(RestApiClientWithCache, self).__init__(*args, **kwargs)
        self.use_cache = True
        self.cache = {}
        self.READ_ENDPOINTS = list(
            itertools.chain.from_iterable(
                [[ep for (ep, items) in self.ENDPOINTS[key]] for key in self.ENDPOINTS]
            )
        )

    # Cache methods:

    def do_not_cache(self, *items):
        """
        Add these items from the do-not-cache list.
        """
        for item in items:
            self.NOCACHE_ENDPOINTS.add(item)

    def do_cache(self, *items):
        """
        Remove these items from the do-not-cache list.
        """
        for item in items:
            if item in self.NOCACHE_ENDPOINTS:
                self.NOCACHE_ENDPOINTS.remove(item)

    def cacheable(self, read_endpoint):
        return read_endpoint not in self.NOCACHE_ENDPOINTS

    def get_hash(self, kwargs):
        key = hash(json.dumps(kwargs, sort_keys=True, default=str))
        return key

    def cache_get(self, **kwargs):
        # Look up in cache
        key = self.get_hash(kwargs)
        if key in self.cache:
            hit = True
            value = self.cache[key]
        else:
            hit = False
            value = None
        return hit, value

    def cache_put(self, value, **kwargs):
        # Put in cache
        if not self.check_read_endpoint(kwargs["endpoint"]):
            LOGGER.debug(
                "this endpoint cannot be cleared from cache: %r", kwargs["endpoint"]
            )
        key = self.get_hash(kwargs)
        LOGGER.debug("cache_put: %s, key: %s", kwargs, key)
        self.cache[key] = value

    def cache_clear_return_types(self, **kwargs):
        # Remove all return_types from cache for this endpoint/params:
        for return_type in ["json", "binary", "text"]:
            kwargs["return_type"] = return_type
            key = self.get_hash(kwargs)
            LOGGER.debug("attempting cache_clear: %s, key: %s", kwargs, key)
            if key in self.cache:
                LOGGER.debug("cache_clear: CLEARED!")
                del self.cache[key]

    def cache_clear(self, **kwargs):
        extra = self.EXTRA_PAYLOAD.get(kwargs["endpoint"])
        if extra:
            # First, without extras:
            self.cache_clear_return_types(**kwargs)
            # If more than one extra, we have to do all combinations:
            for key in extra:
                for value in extra[key]:
                    kwargs["payload"][key] = value
                    self.cache_clear_return_types(**kwargs)
        else:
            self.cache_clear_return_types(**kwargs)

    def get_read_endpoints(self, write_endpoint):
        """
        Return the mapping from a write endpoint to a list
        of tuples of (read-endpoint, [payload keys]) to
        clear the associated read endpoint caches.
        """
        return self.ENDPOINTS.get(write_endpoint, None)

    def check_read_endpoint(self, read_endpoint):
        """
        Check to see if the read_endpoint is in the
        list of known ones, or if it is not cached.
        If it is neither, then there is no way to
        clear it.
        """
        return (read_endpoint in self.READ_ENDPOINTS) or not self.cacheable(
            read_endpoint
        )

    # Overridden methods:

    def reset(self):
        self.cache.clear()

    def cache_clear_read_endpoints(self, endpoint, payload):
        # Clear read cache:
        endpoints = self.get_read_endpoints(endpoint)
        if endpoints:
            for read_endpoint, keys in endpoints:
                # Build read payload:
                read_payload = {}
                for key in keys:
                    if key in payload:
                        read_payload[key] = payload[key]
                self.cache_clear(endpoint=read_endpoint, payload=read_payload)

    def post_from_endpoint(self, endpoint, payload, **kwargs):
        """
        Wrapper that clears the cache after posting.
        """
        response = super(RestApiClientWithCache, self).post_from_endpoint(
            endpoint, payload, **kwargs
        )
        self.cache_clear_read_endpoints(endpoint, payload)
        if "params" in kwargs:
            self.cache_clear_read_endpoints(endpoint, kwargs["params"])
        return response

    def get_from_endpoint(
        self, endpoint, params, return_type="json", timeout=None, stream=False
    ):
        # type: (str, Optional[Dict[str, str]], str, Optional[int], bool) -> Any
        """
        Wrapper around RestApiClient.get_from_endpoint() that adds cache.

        """
        if self.use_cache and self.cacheable(endpoint) and not stream:
            hit, result = self.cache_get(
                endpoint=endpoint, payload=params, return_type=return_type
            )
            if hit:
                # LOGGER.debug(
                #     "RestApiClientWithCache, hit: endpoint = %s, params = %s, return_type = %s",
                #     endpoint,
                #     params,
                #     return_type,
                # )
                return result

        # LOGGER.debug(
        #     "RestApiClientWithCache, miss: endpoint = %s, params = %s, return_type = %s",
        #     endpoint,
        #     params,
        #     return_type,
        # )
        retval = super(RestApiClientWithCache, self).get_from_endpoint(
            endpoint, params, return_type, timeout=timeout, stream=stream
        )

        if (
            self.use_cache
            and self.cacheable(endpoint)
            and retval is not None
            and not stream
        ):
            self.cache_put(
                retval, endpoint=endpoint, payload=params, return_type=return_type
            )

        return retval


def get_comet_api_client(server_address=None, headers=None):
    # type: (Optional[str], Optional[Dict[str, Any]]) -> CometApiClient
    config = get_config()
    if server_address is None:
        server_address = config["comet.url_override"]
    server_url = get_root_url(server_address)

    low_level_api = LowLevelHTTPClient(
        server_address=server_url,
        default_timeout=config["comet.timeout.http"],
        headers=headers,
        verify_tls=config.get_bool(None, "comet.internal.check_tls_certificate"),
    )

    return CometApiClient(server_url, low_level_api, config)


class CometApiClient(BaseApiClient):
    """
    Inputs must be JSON-encodable, any conversion must be done by the caller.

    One method equals one endpoint and one call
    """

    def __init__(self, server_url, low_level_api_client, config):
        # type: (str, LowLevelHTTPClient, Config) -> None
        super(CometApiClient, self).__init__(
            server_url, ["api/auth/"], low_level_api_client, None, config
        )

    def get(self, url, params, headers=None, timeout=None, stream=False):
        # Overrides to suppress exceptions on errors
        response = self.low_level_api_client.get(
            url, params=params, headers=headers, timeout=timeout, stream=stream
        )
        return response

    def post(
        self,
        url,
        payload,
        headers=None,
        files=None,
        params=None,
        custom_encoder=None,
        compress=False,
    ):
        return super(CometApiClient, self).post(
            url,
            payload=payload,
            headers=headers,
            files=files,
            params=params,
            custom_encoder=custom_encoder,
            compress=compress,
        )

    def check_email(self, email, reason):
        # type: (str, str) -> int
        """
        Check if the given email is associated with a user.

        Args:
            email: str, the email of the user
            reason: str, the reason for the check

        Returns: a status code

        * 200: ok, existing user
        * 204: unknown user
        """
        payload = {"email": email, "reason": reason}
        response = self.get_from_endpoint("users", payload, return_type="response")
        status_code = response.status_code  # type: int
        return status_code

    def create_user(self, email, username, signup_source, send_email=True):
        # type: (str, str, str, bool) -> dict
        """
        Creates a temporary user token for the email/username.

        Args:
            email: str, an email address
            username: str, a proper Comet username
            signup_source: str, description of signup source
            send_email: bool, if True (the default), the new user will receive the welcome emails

        Returns: dict (if successful), a JSON response as follows.
            Otherwise, a CometRestApiException with reason for
            failure.

        POST /api/auth/users?sendEmail=true
        ```json
        {
         'cometUserName': 'new-usernane',
         'token': 'MXZRU1WXsEJAzeFn0I235423549',
         'apiKey': 'X6Wr0uKXZwOLnPFpNvV39874857'
        }
        ```
        """
        payload = {
            "email": email,
            "userName": username,
            "signupSource": signup_source,
        }
        # The backend expect boolean to be formatted like in JSON
        params = {"sendEmail": json.dumps(send_email)}
        response = self.post_from_endpoint("users", payload, params=params)
        response_json = response.json()  # type: Dict[Any, Any]
        return response_json


def compress_git_patch(git_patch):
    # Create a zip
    zip_dir = tempfile.mkdtemp()

    zip_path = os.path.join(zip_dir, "patch.zip")
    archive = zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED)
    archive.writestr("git_diff.patch", git_patch)
    archive.close()

    return archive, zip_path


def _get_clientlib_params(experiment_id, project_id, api_key):
    # type: (str, str, str) -> Dict[str, str]
    return {"experimentId": experiment_id, "projectId": project_id, "apiKey": api_key}


def _get_clientlib_headers(experiment_id):
    # type: (str) -> Dict[str, str]
    return {"X-COMET-DEBUG-EXPERIMENT-KEY": experiment_id}


def _send_file(url, data, params, headers, timeout, session):
    # type: (str, Any, Dict[str, str], Dict[str, Any], int, Session) -> Response
    LOGGER.debug("Uploading files %r to %s with params %s", data, url, params)

    r = session.post(
        url,
        params=params,
        data=data,
        timeout=timeout,
        headers=headers,
    )  # type: Response

    LOGGER.debug("Uploading file to %s done", url)

    return r


def send_file(
    post_endpoint,
    file_path,
    params,
    headers,
    timeout,
    session,
    monitor=None,
    metadata=None,
):
    # type: (str, str, Dict[str, str], Dict[str, str], int, Session, Optional[UploadSizeMonitor], Optional[Dict[Any, Any]]) -> Response
    with open(file_path, "rb") as _file:
        fields = {"file": ("file", _file)}  # type: Dict[str, Any]

        if metadata is not None:
            encoded_metadata = encode_metadata(metadata)
            if encoded_metadata:
                fields["metadata"] = encoded_metadata

        encoder = MultipartEncoder(fields=fields)

        if monitor is not None:
            monitor.total_size = encoder.len
            data = MultipartEncoderMonitor(encoder, monitor.monitor_callback)
        else:
            data = encoder

        headers.update({"Content-Type": encoder.content_type})

        return _send_file(
            post_endpoint,
            params=params,
            data=data,
            timeout=timeout,
            headers=headers,
            session=session,
        )


def send_file_like(
    post_endpoint,
    file_like,
    params,
    headers,
    timeout,
    session,
    monitor=None,
    metadata=None,
):
    # type: (str, Any, Dict[str, str], Dict[str, str], int, Session, Optional[UploadSizeMonitor], Optional[Dict[Any, Any]]) -> Response
    fields = {"file": ("file", file_like)}  # type: Dict[str, Any]

    if metadata is not None:
        encoded_metadata = encode_metadata(metadata)
        if encoded_metadata:
            fields["metadata"] = encoded_metadata

    encoder = MultipartEncoder(fields=fields)

    if monitor is not None:
        monitor.total_size = encoder.len
        data = MultipartEncoderMonitor(encoder, monitor.monitor_callback)
    else:
        data = encoder

    headers.update({"Content-Type": encoder.content_type})

    return _send_file(
        post_endpoint,
        params=params,
        data=data,
        timeout=timeout,
        headers=headers,
        session=session,
    )


def send_remote_asset(
    post_endpoint,
    remote_uri,
    params,
    headers,
    timeout,
    session,
    monitor=None,
    metadata=None,
):
    # type: (str, str, Dict[str, str], Dict[str, str], int, Session, Optional[UploadSizeMonitor], Optional[Dict[Any, Any]]) -> Response

    fields = {"link": ("link", remote_uri)}  # type: Dict[str, Any]

    if metadata is not None:
        encoded_metadata = encode_metadata(metadata)
        if encoded_metadata:
            fields["metadata"] = encoded_metadata

    encoder = MultipartEncoder(fields=fields)

    if monitor is not None:
        monitor.total_size = encoder.len
        data = MultipartEncoderMonitor(encoder, monitor.monitor_callback)
    else:
        data = encoder

    headers.update({"Content-Type": encoder.content_type})

    return _send_file(
        post_endpoint,
        params=params,
        data=data,
        timeout=timeout,
        headers=headers,
        session=session,
    )


def upload_file(
    project_id,
    experiment_id,
    file_path,
    upload_endpoint,
    api_key,
    timeout,
    verify_tls,  # type: bool
    _monitor=None,
    additional_params=None,
    metadata=None,
    clean=True,
    on_asset_upload=None,
    on_failed_asset_upload=None,
):
    # type: (...) -> None
    params = _get_clientlib_params(experiment_id, project_id, api_key)

    if additional_params is not None:
        params.update(additional_params)

    headers = _get_clientlib_headers(experiment_id)

    try:
        response = send_file(
            upload_endpoint,
            file_path,
            params=params,
            headers=headers,
            metadata=metadata,
            timeout=timeout,
            session=get_thread_session(
                True, verify_tls=verify_tls, tcp_keep_alive=True
            ),
            monitor=_monitor,
        )

        if clean is True:
            # Cleanup file
            try:
                os.remove(file_path)
            except OSError:
                pass

        if response.status_code != 200:
            raise ValueError(
                "POSTing file failed (%s) on url %r: %r"
                % (response.status_code, upload_endpoint, response.content)
            )

        LOGGER.debug(
            "File successfully uploaded to (%s): %s",
            response.status_code,
            upload_endpoint,
        )

        if on_asset_upload is not None:
            try:
                on_asset_upload(response)
            except Exception:
                LOGGER.warning("Failed to call on_asset_upload", exc_info=True)
    except Exception as e:
        LOGGER.error("File could not be uploaded: %r", e, exc_info=True)
        Reporting.report(
            event_name=FILE_UPLOADED_FAILED,
            experiment_key=experiment_id,
            project_id=project_id,
            api_key=api_key,
            err_msg=str(e),
            config=get_config(),
        )

        if on_failed_asset_upload is not None:
            try:
                on_failed_asset_upload(sys.exc_info())
            except Exception:
                LOGGER.warning("Failed to call on_failed_asset_upload", exc_info=True)
        raise


def upload_file_like(
    project_id,
    experiment_id,
    file_like,
    upload_endpoint,
    api_key,
    timeout,
    verify_tls,  # type: bool
    _monitor=None,
    additional_params=None,
    metadata=None,
    on_asset_upload=None,
    on_failed_asset_upload=None,
):
    # type: (...) -> None
    params = _get_clientlib_params(experiment_id, project_id, api_key)

    if additional_params is not None:
        params.update(additional_params)

    headers = _get_clientlib_headers(experiment_id)

    try:
        response = send_file_like(
            upload_endpoint,
            file_like,
            params=params,
            headers=headers,
            metadata=metadata,
            timeout=timeout,
            session=get_thread_session(
                True, verify_tls=verify_tls, tcp_keep_alive=True
            ),
            monitor=_monitor,
        )

        if response.status_code != 200:
            raise ValueError(
                "POSTing file failed (%s) on url %r: %r"
                % (response.status_code, upload_endpoint, response.content)
            )

        LOGGER.debug(
            "File-like successfully uploaded to (%s): %s",
            response.status_code,
            upload_endpoint,
        )

        if on_asset_upload is not None:
            try:
                on_asset_upload(response)
            except Exception:
                LOGGER.warning("Failed to call on_asset_upload", exc_info=True)
    except Exception as e:
        LOGGER.error("File-like could not be uploaded: %r", e, exc_info=True)
        Reporting.report(
            event_name=FILE_UPLOADED_FAILED,
            experiment_key=experiment_id,
            project_id=project_id,
            api_key=api_key,
            err_msg=str(e),
            config=get_config(),
        )

        if on_failed_asset_upload is not None:
            try:
                on_failed_asset_upload(sys.exc_info())
            except Exception:
                LOGGER.warning("Failed to call on_failed_asset_upload", exc_info=True)

        raise


def upload_remote_asset(
    project_id,
    experiment_id,
    remote_uri,
    upload_endpoint,
    api_key,
    timeout,
    verify_tls,  # type: bool
    _monitor=None,
    additional_params=None,
    metadata=None,
    on_asset_upload=None,
    on_failed_asset_upload=None,
):
    # type: (...) -> None
    params = _get_clientlib_params(experiment_id, project_id, api_key)

    if additional_params is not None:
        params.update(additional_params)

    headers = _get_clientlib_headers(experiment_id)

    try:
        response = send_remote_asset(
            upload_endpoint,
            remote_uri,
            params=params,
            headers=headers,
            metadata=metadata,
            timeout=timeout,
            session=get_thread_session(
                True, verify_tls=verify_tls, tcp_keep_alive=False
            ),
            monitor=_monitor,
        )

        if response.status_code != 200:
            raise ValueError(
                "POSTing file failed (%s) on url %r: %r"
                % (response.status_code, upload_endpoint, response.content)
            )

        LOGGER.debug(
            "Remote Asset successfully uploaded to (%s): %s",
            response.status_code,
            upload_endpoint,
        )

        if on_asset_upload is not None:
            try:
                on_asset_upload(response)
            except Exception:
                LOGGER.warning("Failed to call on_asset_upload", exc_info=True)
    except Exception as e:
        LOGGER.error("Remote Asset could not be uploaded: %r", e, exc_info=True)
        Reporting.report(
            event_name=FILE_UPLOADED_FAILED,
            experiment_key=experiment_id,
            project_id=project_id,
            api_key=api_key,
            err_msg=str(e),
            config=get_config(),
        )

        if on_failed_asset_upload is not None:
            try:
                on_failed_asset_upload(sys.exc_info())
            except Exception:
                LOGGER.warning("Failed to call on_failed_asset_upload", exc_info=True)

        raise


class UploadSizeMonitor(object):
    __slots__ = ["total_size", "bytes_read"]

    def __init__(self):
        self.total_size = None
        self.bytes_read = 0

    def monitor_callback(self, monitor):
        self.bytes_read = monitor.bytes_read


class FileUploadManager(object):
    def __init__(self, worker_cpu_ratio, worker_count=None):
        # type: (int, Optional[int]) -> None
        self.upload_results = []  # type: List[multiprocessing.pool.AsyncResult]

        pool_size, cpu_count, self._pool = get_thread_pool(
            worker_cpu_ratio, worker_count
        )

        LOGGER.debug(
            "FileUploadManager instantiated with %d threads, %d CPUs, %d worker_cpu_ratio, %s worker_count",
            pool_size,
            cpu_count,
            worker_cpu_ratio,
            worker_count,
        )

    def upload_file_thread(self, critical=False, estimated_size=None, **kwargs):
        monitor = UploadSizeMonitor()
        if estimated_size is not None:
            monitor.total_size = estimated_size

        kwargs["_monitor"] = monitor
        async_result = self._pool.apply_async(upload_file, args=[], kwds=kwargs)
        async_result._critical = critical
        async_result._monitor = monitor
        self.upload_results.append(async_result)

    def upload_file_like_thread(self, critical=False, estimated_size=None, **kwargs):
        monitor = UploadSizeMonitor()
        if estimated_size is not None:
            monitor.total_size = estimated_size

        kwargs["_monitor"] = monitor
        async_result = self._pool.apply_async(upload_file_like, args=[], kwds=kwargs)
        async_result._critical = critical
        async_result._monitor = monitor
        self.upload_results.append(async_result)

    def upload_remote_asset_thread(self, critical=False, estimated_size=None, **kwargs):
        monitor = UploadSizeMonitor()
        if estimated_size is not None:
            monitor.total_size = estimated_size

        kwargs["_monitor"] = monitor
        async_result = self._pool.apply_async(upload_remote_asset, args=[], kwds=kwargs)
        async_result._critical = critical
        async_result._monitor = monitor
        self.upload_results.append(async_result)

    def all_done(self):
        # type: () -> bool
        return all(result.ready() for result in self.upload_results)

    def remaining_data(self):
        # type: () -> (Tuple[int, int, int])
        remaining_uploads = 0
        remaining_bytes_to_upload = 0
        total_size = 0
        for result in self.upload_results:
            monitor = result._monitor
            if monitor.total_size is None or monitor.bytes_read is None:
                continue

            total_size += monitor.total_size

            if result.ready() is True:
                continue

            remaining_uploads += 1

            remaining_bytes_to_upload += monitor.total_size - monitor.bytes_read

        return (remaining_uploads, remaining_bytes_to_upload, total_size)

    def remaining_uploads(self):
        # type: () -> int
        return len([1 for result in self.upload_results if result.ready() is False])

    def close(self):
        self._pool.close()

    def join(self):
        # type: () -> None
        self._pool.join()

    def has_failed(self):
        # type: () -> bool
        """Returns True if:
        * at least one critical file uploads has failed
        * at least one critical file upload is not finished yet, caller must handle the timeout itself
        """
        failed = False

        for result in self.upload_results:

            if not result._critical:
                continue

            if not result.ready():
                failed = True
            elif not result.successful():
                failed = True

        return failed


class FileUploadManagerMonitor(object):
    def __init__(self, file_upload_manager):
        # type: (FileUploadManager) -> None
        self.file_upload_manager = file_upload_manager
        self.last_remaining_bytes = 0
        self.last_remaining_uploads_display = None

    def log_remaining_uploads(self):
        # type: () -> None
        uploads, remaining_bytes, total_size = self.file_upload_manager.remaining_data()

        current_time = get_time_monotonic()

        if remaining_bytes == 0:
            LOGGER.info(
                FILE_UPLOAD_MANAGER_MONITOR_WAITING_BACKEND_ANSWER,
            )
        elif self.last_remaining_uploads_display is None:
            LOGGER.info(
                FILE_UPLOAD_MANAGER_MONITOR_FIRST_MESSAGE,
                uploads,
                format_bytes(remaining_bytes),
                format_bytes(total_size),
            )
        else:
            uploaded_bytes = self.last_remaining_bytes - remaining_bytes
            time_elapsed = current_time - self.last_remaining_uploads_display
            upload_speed = uploaded_bytes / time_elapsed

            # Avoid 0 division if no bytes were uploaded in the last period
            if uploaded_bytes <= 0:
                # avoid negative upload speed
                if upload_speed < 0:
                    upload_speed = 0

                LOGGER.info(
                    FILE_UPLOAD_MANAGER_MONITOR_PROGRESSION_UNKOWN_ETA,
                    uploads,
                    format_bytes(remaining_bytes),
                    format_bytes(total_size),
                    format_bytes(upload_speed),
                )

            else:
                # Avoid displaying 0s, also math.ceil returns a float in Python 2.7
                remaining_time = str(int(math.ceil(remaining_bytes / upload_speed)))

                LOGGER.info(
                    FILE_UPLOAD_MANAGER_MONITOR_PROGRESSION,
                    uploads,
                    format_bytes(remaining_bytes),
                    format_bytes(total_size),
                    format_bytes(upload_speed),
                    remaining_time,
                )

        self.last_remaining_bytes = remaining_bytes
        self.last_remaining_uploads_display = current_time

    def all_done(self):
        # type: () -> bool
        return self.file_upload_manager.all_done()


def write_stream_response_to_file(responses, file_object, monitor=None):
    # type: (requests.Response, IO[bytes], Optional[FileDownloadSizeMonitor]) -> None
    for chunk in responses.iter_content(chunk_size=1024 * 1024):
        bytes_written = file_object.write(chunk)
        if monitor:
            monitor.monitor_callback(bytes_written)


def parse_experiment_handshake_response(res_body):
    # type: (Dict[str, Any]) -> ExperimentHandshakeResponse
    run_id = res_body["runId"]  # type: str
    ws_server = res_body["ws_url"]  # type: str

    project_id = res_body.get("project_id", None)  # type: Optional[str]

    is_github = bool(res_body.get("githubEnabled", False))

    focus_link = res_body.get("focusUrl", None)  # type: Optional[str]

    last_offset = res_body.get("lastOffset", 0)  # type: int

    # Upload limit
    upload_limit_server = res_body.get("upload_file_size_limit_in_mb", None)

    if isinstance(upload_limit_server, int) and upload_limit_server > 0:
        # The limit is given in Mb, convert it back in bytes
        upload_limit = upload_limit_server * 1024 * 1024  # type: int
    else:
        LOGGER.debug(
            "Fallback to default upload size limit, %r value is invalid",
            upload_limit_server,
        )
        upload_limit = DEFAULT_UPLOAD_SIZE_LIMIT

    # Asset upload limit
    asset_upload_limit_server = res_body.get("asset_upload_file_size_limit_in_mb", None)

    if isinstance(asset_upload_limit_server, int) and asset_upload_limit_server > 0:
        # The limit is given in Mb, convert it back in bytes
        asset_upload_limit = asset_upload_limit_server * 1024 * 1024  # type: int
    else:
        LOGGER.debug(
            "Fallback to default asset upload size limit, %r value is invalid",
            asset_upload_limit_server,
        )
        asset_upload_limit = DEFAULT_ASSET_UPLOAD_SIZE_LIMIT

    res_msg = res_body.get("msg")
    if res_msg:
        log_once_at_level(logging.INFO, res_msg)

    # Parse feature toggles
    feature_toggles = {}  # type: Dict[str, bool]
    LOGGER.debug("Raw feature toggles %r", res_body.get("featureToggles", []))
    for toggle in res_body.get("featureToggles", []):
        try:
            feature_toggles[toggle["name"]] = bool(toggle["enabled"])
        except (KeyError, TypeError):
            LOGGER.debug("Invalid feature toggle: %s", toggle, exc_info=True)
    LOGGER.debug("Parsed feature toggles %r", feature_toggles)

    # Parse URL prefixes
    web_asset_url = res_body.get("cometWebAssetUrl", None)  # type: Optional[str]
    web_image_url = res_body.get("cometWebImageUrl", None)  # type: Optional[str]
    api_asset_url = res_body.get("cometRestApiAssetUrl", None)  # type: Optional[str]
    api_image_url = res_body.get("cometRestApiImageUrl", None)  # type: Optional[str]

    experiment_name = res_body.get("name", None)  # type: Optional[str]

    return ExperimentHandshakeResponse(
        run_id=run_id,
        ws_server=ws_server,
        project_id=project_id,
        is_github=is_github,
        focus_link=focus_link,
        last_offset=last_offset,
        upload_limit=upload_limit,
        asset_upload_limit=asset_upload_limit,
        feature_toggles=feature_toggles,
        web_asset_url=web_asset_url,
        web_image_url=web_image_url,
        api_asset_url=api_asset_url,
        api_image_url=api_image_url,
        experiment_name=experiment_name,
    )


class ExperimentHandshakeResponse(object):
    def __init__(
        self,
        run_id,  # type: str
        ws_server,  # type: str
        project_id,  # type: Optional[str]
        is_github,  # type: bool
        focus_link,  # type: Optional[str]
        last_offset,  # type: int
        upload_limit,  # type: int
        asset_upload_limit,  # type: int
        feature_toggles,  # type: Dict[str, bool]
        web_asset_url,  # type: Optional[str]
        web_image_url,  # type: Optional[str]
        api_asset_url,  # type: Optional[str]
        api_image_url,  # type: Optional[str]
        experiment_name,  # type: Optional[str]
    ):
        # type: (...) -> None
        self.run_id = run_id
        self.ws_server = ws_server
        self.project_id = project_id
        self.is_github = is_github
        self.focus_link = focus_link
        self.last_offset = last_offset
        self.upload_limit = upload_limit
        self.asset_upload_limit = asset_upload_limit
        self.feature_toggles = feature_toggles
        self.web_asset_url = web_asset_url
        self.web_image_url = web_image_url
        self.api_asset_url = api_asset_url
        self.api_image_url = api_image_url
        self.experiment_name = experiment_name
