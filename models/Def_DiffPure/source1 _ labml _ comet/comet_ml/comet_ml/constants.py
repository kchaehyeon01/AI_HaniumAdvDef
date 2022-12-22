# -*- coding: utf-8 -*-
# *******************************************************
#   ____                     _               _
#  / ___|___  _ __ ___   ___| |_   _ __ ___ | |
# | |   / _ \| '_ ` _ \ / _ \ __| | '_ ` _ \| |
# | |__| (_) | | | | | |  __/ |_ _| | | | | | |
#  \____\___/|_| |_| |_|\___|\__(_)_| |_| |_|_|
#
#  Sign up for free at http://www.comet.ml
#  Copyright (C) 2021 Comet ML INC
#  This file can not be copied and/or distributed without the express
#  permission of Comet ML Inc.
# *******************************************************


# Resume Strategy / Old Offline MODE
RESUME_STRATEGY_GET = "get"
RESUME_STRATEGY_GET_OR_CREATE = "get_or_create"
RESUME_STRATEGY_CREATE = "create"

DEPRECATED_OFFLINE_MODE_CREATE = "create"
DEPRECATED_OFFLINE_MODE_APPEND = "append"

DEPRECATED_OFFLINE_MODE_TO_RESUME_STRATEGY_MAP = {
    DEPRECATED_OFFLINE_MODE_APPEND: RESUME_STRATEGY_GET,
    DEPRECATED_OFFLINE_MODE_CREATE: RESUME_STRATEGY_CREATE,
}

# Conda related assets
CONDA_ENV_FILE_NAME = "conda-environment.yml"
CONDA_ENV_ASSET_TYPE = "conda-environment-definition"

CONDA_SPEC_FILE_NAME = "conda-spec.txt"
CONDA_SPEC_ASSET_TYPE = "conda-specification"

CONDA_INFO_FILE_NAME = "conda-info.yml"
CONDA_INFO_ASSET_TYPE = "conda-info"

# The experiment status response keys
STATUS_RESPONSE_IS_ALIVE_BEAT_DURATION_MILLIS = "is_alive_beat_duration_millis"
STATUS_RESPONSE_GPU_MONITOR_INTERVAL_MILLIS = "gpu_monitor_interval_millis"
STATUS_RESPONSE_CPU_MONITOR_INTERVAL_MILLIS = "cpu_monitor_interval_millis"
STATUS_RESPONSE_PARAMETER_UPDATE_INTERVAL_MILLIS = "parameter_update_interval_millis"
STATUS_RESPONSE_PENDING_RPCS = "pending_rpcs"
STATUS_RESPONSE_CODE = "code"
STATUS_RESPONSE_MESSAGE = "msg"

# The REST API constants
PAYLOAD_EXPERIMENT_KEY = "experimentKey"
PAYLOAD_OS_PACKAGES = "osPackages"
PAYLOAD_MODEL_GRAPH = "graph"
PAYLOAD_COMMAND = "command"
PAYLOAD_EXECUTABLE = "executable"
PAYLOAD_HOSTNAME = "hostname"
PAYLOAD_IP = "ip"
PAYLOAD_OS = "os"
PAYLOAD_OS_TYPE = "osType"
PAYLOAD_OS_RELEASE = "osRelease"
PAYLOAD_PID = "pid"
PAYLOAD_PYTHON_VERSION = "pythonVersion"
PAYLOAD_PYTHON_VERSION_VERBOSE = "pythonVersionVerbose"
PAYLOAD_MACHINE = "machine"
PAYLOAD_PROCESSOR = "processor"
PAYLOAD_ENV = "env"
PAYLOAD_USER = "user"
PAYLOAD_INSTALLED_PACKAGES = "installedPackages"
PAYLOAD_GPU_STATIC_INFO_LIST = "gpuStaticInfoList"
PAYLOAD_TOTAL_RAM = "totalRam"
PAYLOAD_USED_RAM = "usedRam"
PAYLOAD_FILE_PATH = "filePath"
PAYLOAD_HTML = "html"
PAYLOAD_OVERRIDE = "override"
PAYLOAD_TIMESTAMP = "timestamp"
PAYLOAD_PROVIDER = "provider"
PAYLOAD_METADATA = "metadata"
PAYLOAD_ADDITIONAL_SYSTEM_INFO_LIST = "logAdditionalSystemInfoList"
PAYLOAD_DEPENDENCIES = "logDependencies"
PAYLOAD_DEPENDENCY_NAME = "name"
PAYLOAD_DEPENDENCY_VERSION = "version"
PAYLOAD_STDERR = "stderr"
PAYLOAD_OUTPUT = "output"
PAYLOAD_LOCAL_TIMESTAMP = "localTimestamp"
PAYLOAD_OFFSET = "offset"
PAYLOAD_RUN_CONTEXT = "runContext"
PAYLOAD_OUTPUT_LINES = "outputLines"
