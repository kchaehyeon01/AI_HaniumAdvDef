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

This module contains the main components of GPU information logging

"""
import logging
import threading
import time

from comet_ml.utils import metric_name, to_utf8

try:
    import pynvml
except ImportError:
    pynvml = None

LOGGER = logging.getLogger(__name__)


DEFAULT_GPU_MONITOR_INTERVAL = 60


def get_gpu_name(handle):
    """Returns the name of the GPU device

    https://docs.nvidia.com/deploy/nvml-api/group__nvmlDeviceQueries.html#group__nvmlDeviceQueries_1ga5361803e044c6fdf3b08523fb6d1481
    """
    name = pynvml.nvmlDeviceGetName(handle)
    return to_utf8(name)


def get_uuid(handle):
    """Returns the globally unique GPU device UUID

    https://docs.nvidia.com/deploy/nvml-api/group__nvmlDeviceQueries.html#group__nvmlDeviceQueries_1g72710fb20f30f0c2725ce31579832654
    """
    uuid = pynvml.nvmlDeviceGetUUID(handle)
    return to_utf8(uuid)


def get_memory_info(handle):
    """Returns memory info in bytes

    https://docs.nvidia.com/deploy/nvml-api/group__nvmlDeviceQueries.html#group__nvmlDeviceQueries_1g2dfeb1db82aa1de91aa6edf941c85ca8
    """
    return pynvml.nvmlDeviceGetMemoryInfo(handle)


def get_temperature(handle):
    """Returns degrees in the Celsius scale

    https://docs.nvidia.com/deploy/nvml-api/group__nvmlDeviceQueries.html#group__nvmlDeviceQueries_1g92d1c5182a14dd4be7090e3c1480b121
    """
    return pynvml.nvmlDeviceGetTemperature(handle, pynvml.NVML_TEMPERATURE_GPU)


def get_power_usage(handle):
    """Returns power usage in milliwatts

    https://docs.nvidia.com/deploy/nvml-api/group__nvmlDeviceQueries.html#group__nvmlDeviceQueries_1g7ef7dff0ff14238d08a19ad7fb23fc87
    """
    return pynvml.nvmlDeviceGetPowerUsage(handle)


def get_power_limit(handle):
    """Returns max power usage in milliwatts

    https://docs.nvidia.com/deploy/nvml-api/group__nvmlDeviceQueries.html#group__nvmlDeviceQueries_1g263b5bf552d5ec7fcd29a088264d10ad
    """
    try:
        return pynvml.nvmlDeviceGetEnforcedPowerLimit(handle)
    except Exception:
        return None


def get_gpu_utilization(handle):
    """Returns the % of utilization of the kernels during the last sample

    https://docs.nvidia.com/deploy/nvml-api/structnvmlUtilization__t.html#structnvmlUtilization__t
    """
    return pynvml.nvmlDeviceGetUtilizationRates(handle).gpu


def get_compute_mode(handle):
    """Returns the compute mode of the GPU

    https://docs.nvidia.com/deploy/nvml-api/group__nvmlDeviceEnumvs.html#group__nvmlDeviceEnumvs_1gbed1b88f2e3ba39070d31d1db4340233
    """
    return pynvml.nvmlDeviceGetComputeMode(handle)


def get_compute_processes(handle):
    """Returns the list of processes ids having a compute context on the
    device with the memory used

    https://docs.nvidia.com/deploy/nvml-api/group__nvmlDeviceQueries.html#group__nvmlDeviceQueries_1g46ceaea624d5c96e098e03c453419d68
    """
    processes = pynvml.nvmlDeviceGetComputeRunningProcesses(handle)

    return [{"pid": p.pid, "used_memory": p.usedGpuMemory} for p in processes]


def get_graphics_processes(handle):
    """Returns the list of processes ids having a graphics context on the
    device with the memory used

    https://docs.nvidia.com/deploy/nvml-api/group__nvmlDeviceQueries.html#group__nvmlDeviceQueries_1g7eacf7fa7ba4f4485d166736bf31195e
    """
    processes = pynvml.nvmlDeviceGetGraphicsRunningProcesses(handle)

    return [{"pid": p.pid, "used_memory": p.usedGpuMemory} for p in processes]


def get_gpu_static_info():
    try:
        pynvml.nvmlInit()
        deviceCount = pynvml.nvmlDeviceGetCount()
        devices = []
        for i in range(deviceCount):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)

            # Memory
            memory = get_memory_info(handle)

            device_details = {
                "name": get_gpu_name(handle),
                "uuid": get_uuid(handle),
                "total_memory": memory.total,
                "power_limit": get_power_limit(handle),
                "gpu_index": i,
            }
            devices.append(device_details)
        return devices

    except pynvml.NVMLError:
        LOGGER.debug("Failed to retrieve gpu static info", exc_info=True)
        return []


def get_initial_gpu_metric():
    try:
        pynvml.nvmlInit()
        deviceCount = pynvml.nvmlDeviceGetCount()
        devices = []
        for i in range(deviceCount):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)

            # Memory
            memory = get_memory_info(handle)

            device_details = {"total_memory": memory.total}
            devices.append(device_details)
        return devices

    except pynvml.NVMLError:
        LOGGER.debug("Failed to retrieve gpu initial metrics", exc_info=True)
        return []


def get_recurrent_gpu_metric():
    try:
        pynvml.nvmlInit()
        deviceCount = pynvml.nvmlDeviceGetCount()
        devices = []
        for i in range(deviceCount):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)

            # Memory
            memory = get_memory_info(handle)

            device_details = {
                "free_memory": memory.free,
                "gpu_utilization": get_gpu_utilization(handle),
                "power_usage": get_power_usage(handle),
                "temperature": get_temperature(handle),
                "used_memory": memory.used,
            }
            devices.append(device_details)
        return devices

    except pynvml.NVMLError:
        LOGGER.debug("Failed to retrieve gpu recurrent metrics", exc_info=True)
        return []


def get_gpu_details():
    try:
        pynvml.nvmlInit()
        deviceCount = pynvml.nvmlDeviceGetCount()
        devices = []
        for i in range(deviceCount):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)

            # Memory
            memory = get_memory_info(handle)

            device_details = {
                "name": get_gpu_name(handle),
                "uuid": get_uuid(handle),
                "free_memory": memory.free,
                "total_memory": memory.total,
                "used_memory": memory.used,
                "temperature": get_temperature(handle),
                "power_usage": get_power_usage(handle),
                "power_limit": get_power_limit(handle),
                "gpu_utilization": get_gpu_utilization(handle),
                "compute_mode": get_compute_mode(handle),
                "compute_processes": get_compute_processes(handle),
                "graphics_processes": get_graphics_processes(handle),
            }
            devices.append(device_details)
        return devices

    except pynvml.NVMLError:
        LOGGER.debug("Failed to retrieve gpu information", exc_info=True)
        return []


def is_gpu_details_available():
    if pynvml is not None:
        try:
            pynvml.nvmlInit()
            return True

        except pynvml.NVMLError:
            return False
    else:
        return False


class GPULoggingThread(threading.Thread):
    def __init__(self, initial_interval, callback, metrics_prefix=None):
        super(GPULoggingThread, self).__init__()
        self.daemon = True
        self.interval = initial_interval
        self.callback = callback
        self.last_run = 0.0
        self.metrics_prefix = metrics_prefix

        self.closed = False

        LOGGER.debug(
            "GPUThread create with %ds interval, metrics prefix: %s",
            initial_interval,
            metrics_prefix,
        )

    def run(self):
        while not self.closed:
            self._loop()

    def _loop(self):
        LOGGER.debug("GPU MONITOR LOOP %s %s", self.closed, self._should_run())
        if self._should_run():
            # Run
            gpu_details = get_recurrent_gpu_metric()
            self.callback(gpu_details)
            self.last_run = time.time()

        # Don't check the interval every CPU cycle but don't sleep the
        # whole interval in order to be able to change the interval and
        # close it more granularly
        time.sleep(1)

    def _should_run(self):
        next_run = self.last_run + self.interval
        now = time.time()
        result = next_run <= now
        return result

    def update_interval(self, interval):
        LOGGER.debug("Update GPU monitor thread interval to %d", interval)
        self.interval = interval

    def close(self):
        LOGGER.debug("GPU THREAD close")
        self.closed = True


def convert_gpu_details_to_metrics(gpu_details, prefix=None):
    metrics = []
    for i, gpu in enumerate(gpu_details):
        for key, value in gpu.items():
            metric = {
                "name": metric_name("sys.gpu.%d.%s" % (i, key), prefix=prefix),
                "value": value,
            }
            metrics.append(metric)
    return metrics
