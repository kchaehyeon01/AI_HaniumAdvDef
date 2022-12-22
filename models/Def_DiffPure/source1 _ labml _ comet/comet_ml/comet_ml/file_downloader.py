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

""" This module handles asynchronous downloading of the files."""

import logging
import math
import multiprocessing.pool

from ._typing import Any, Callable, List, Optional, Tuple
from .logging_messages import (
    FILE_DOWNLOAD_MANAGER_COMPLETED,
    FILE_DOWNLOAD_MANAGER_MONITOR_FIRST_MESSAGE,
    FILE_DOWNLOAD_MANAGER_MONITOR_PROGRESSION,
    FILE_DOWNLOAD_MANAGER_MONITOR_PROGRESSION_UNKNOWN_ETA,
)
from .parallel_utils import get_thread_pool
from .utils import format_bytes, get_time_monotonic

LOGGER = logging.getLogger(__name__)


class FileDownloadSizeMonitor(object):
    """The monitor callback to maintain the file download progress"""

    __slots__ = ["total_size", "bytes_written"]

    def __init__(self):
        self.total_size = None
        self.bytes_written = 0

    def monitor_callback(self, bytes_written):
        # type: (int) -> None
        if bytes_written is not None:  # Python2.7 compatibility check
            self.bytes_written += bytes_written


class DownloadResult(object):
    """The multiprocessing.pool.AsyncResult wrapper providing additional information about download task"""

    def __init__(self, async_result, monitor):
        # type: (multiprocessing.pool.AsyncResult, FileDownloadSizeMonitor) -> None
        self.async_result = async_result
        self.monitor = monitor

    def ready(self):
        # type: () -> bool
        """Allows to check if wrapped AsyncResult is ready"""
        return self.async_result.ready()

    def get(self, timeout=None):
        # type: (Optional[int]) -> Any
        """Return the result when it arrives from the wrapped multiprocessing.pool.AsyncResult.
        If timeout is not None and the result does not arrive within timeout seconds then
        multiprocessing.TimeoutError is raised. If the remote call raised an exception then
        that exception will be reraised by get()."""
        return self.async_result.get(timeout)


class FileDownloadManager(object):
    """The manager to handle downloading of the files and reporting the progress"""

    def __init__(self, worker_cpu_ratio, worker_count=None):
        # type: (int, Optional[int]) -> None
        """Creates new instance with specified ration of workers per CPU. This will instantiate the thread pool for
        asynchronous downloads processing in multiple processes.
        Args:
            worker_cpu_ratio: int - the number of workers per CPU
        """
        self.download_results = []  # type: List[DownloadResult]

        pool_size, cpu_count, self._pool = get_thread_pool(
            worker_cpu_ratio, worker_count
        )

        LOGGER.debug(
            "FileDownloadManager instantiated with %d threads, %d CPUs, %d worker_cpu_ratio, %s worker_count",
            pool_size,
            cpu_count,
            worker_cpu_ratio,
            worker_count,
        )

    def download_file_async(self, download_func, estimated_size=None, **kwargs):
        # type: (Callable, int, Optional[Any]) -> DownloadResult
        """Registers file to be downloaded asynchronously using specified download processing function.
        Args:
            download_func: the function to maintain file downloading routines.
            estimated_size: Optional, int - the estimated size of the file to be downloaded (bytes).
        """
        monitor = FileDownloadSizeMonitor()
        if estimated_size is not None:
            monitor.total_size = estimated_size

        kwargs["_monitor"] = monitor
        async_result = self._pool.apply_async(download_func, args=[], kwds=kwargs)
        result = DownloadResult(async_result=async_result, monitor=monitor)
        self.download_results.append(result)
        return result

    def all_done(self):
        # type: () -> bool
        """Allows to check if all downloads completed"""
        return all(result.ready() for result in self.download_results)

    def remaining_data(self):
        # type: () -> (Tuple[int, int, int])
        """Calculates the number of remaining files, bytes, and total size to
        be downloaded."""
        remaining_downloads = 0
        remaining_bytes_to_download = 0
        total_size = 0
        for result in self.download_results:
            monitor = result.monitor
            if monitor.total_size is None or monitor.bytes_written is None:
                continue

            total_size += monitor.total_size

            if result.ready() is True:
                continue

            remaining_downloads += 1
            remaining_bytes_to_download += monitor.total_size - monitor.bytes_written

        return remaining_downloads, remaining_bytes_to_download, total_size

    def remaining_downloads(self):
        # type: () -> int
        """Returns number of files to be downloaded"""
        return len([1 for result in self.download_results if result.ready() is False])

    def close(self):
        # type: () -> None
        self._pool.close()

    def join(self):
        # type: () -> None
        self._pool.join()


class FileDownloadManagerMonitor(object):
    """The monitor to log download progress of the associated FileDownloadManager"""

    def __init__(self, file_download_manager):
        # type: (FileDownloadManager) -> None
        self.file_download_manager = file_download_manager
        self.last_remaining_bytes = 0
        self.last_remaining_downloads_log_time = None  # type: Optional[float]

    def log_remaining_downloads(self):
        # type: () -> None
        (
            downloads,
            remaining_bytes,
            total_size,
        ) = self.file_download_manager.remaining_data()

        current_time = get_time_monotonic()

        if remaining_bytes == 0:
            LOGGER.info(FILE_DOWNLOAD_MANAGER_COMPLETED)
        elif self.last_remaining_downloads_log_time is None:
            LOGGER.info(
                FILE_DOWNLOAD_MANAGER_MONITOR_FIRST_MESSAGE,
                downloads,
                format_bytes(remaining_bytes),
                format_bytes(total_size),
            )
        else:
            processed_bytes = self.last_remaining_bytes - remaining_bytes
            time_elapsed = current_time - self.last_remaining_downloads_log_time
            throughput = processed_bytes / time_elapsed

            if processed_bytes <= 0:
                LOGGER.info(
                    FILE_DOWNLOAD_MANAGER_MONITOR_PROGRESSION_UNKNOWN_ETA,
                    downloads,
                    format_bytes(remaining_bytes),
                    format_bytes(total_size),
                    format_bytes(throughput),
                )
            else:
                # Avoid displaying 0s, also math.ceil returns a float in Python 2.7
                remaining_time = str(int(math.ceil(remaining_bytes / throughput)))

                LOGGER.info(
                    FILE_DOWNLOAD_MANAGER_MONITOR_PROGRESSION,
                    downloads,
                    format_bytes(remaining_bytes),
                    format_bytes(total_size),
                    format_bytes(throughput),
                    remaining_time,
                )

        self.last_remaining_bytes = remaining_bytes
        self.last_remaining_downloads_log_time = current_time

    def all_done(self):
        # type: () -> bool
        """Checks if all downloads completed by monitored download manager."""
        return self.file_download_manager.all_done()
