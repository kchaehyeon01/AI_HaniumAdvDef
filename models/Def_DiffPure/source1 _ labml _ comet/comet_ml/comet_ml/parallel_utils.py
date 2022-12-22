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
#  This file can not be copied and/or distributed
#  without the express permission of Comet ML Inc.
# *******************************************************
import logging
import multiprocessing.pool
import os
import threading
from functools import wraps

from ._typing import Callable, Optional, Tuple
from .config import DEFAULT_POOL_RATIO, MAX_POOL_SIZE
from .oscontainer import OSContainer
from .oscontainer.constants import DEFAULT_CPU_COUNT

LOGGER = logging.getLogger(__name__)


def get_thread_pool(worker_cpu_ratio, worker_count=None, os_container=None):
    # type: (int, Optional[int], Optional[OSContainer]) -> Tuple[int, int, multiprocessing.pool.ThreadPool]

    # Use the same max size that concurrent.futures.ThreadPoolExecutor which we should use once
    # Python 2.7 is not supported

    if os_container is None:
        os_container = OSContainer()
    if os_container.is_containerized():
        try:
            cpu_count = os_container.active_processor_count()
        except Exception:
            LOGGER.error(
                "Failed to calculate active processors count. Fall back to default CPU count %d"
                % DEFAULT_CPU_COUNT,
                exc_info=True,
            )
            cpu_count = DEFAULT_CPU_COUNT
    else:
        try:
            cpu_count = multiprocessing.cpu_count() or 1
        except NotImplementedError:
            # os.cpu_count is not available on Python 2 and multiprocessing.cpu_count can raise NotImplementedError
            cpu_count = DEFAULT_CPU_COUNT

    if worker_count is not None:
        pool_size = worker_count
    else:
        if not isinstance(worker_cpu_ratio, int) or worker_cpu_ratio <= 0:
            LOGGER.debug("Invalid worker_cpu_ratio %r", worker_cpu_ratio)
            worker_cpu_ratio = DEFAULT_POOL_RATIO

        pool_size = min(MAX_POOL_SIZE, cpu_count * worker_cpu_ratio)

    return pool_size, cpu_count, multiprocessing.pool.ThreadPool(processes=pool_size)


def synchronised(func):
    # type: (Callable) -> Callable
    """The decorator to make particular function synchronized"""
    func.__lock__ = threading.Lock()  # type: ignore

    @wraps(func)
    def synced_func(*args, **kws):
        with func.__lock__:  # type: ignore
            return func(*args, **kws)

    return synced_func


@synchronised
def makedirs_synchronized(name, exist_ok=False):
    """
    Replacement for Python2's version lacking exist_ok
    """
    if not os.path.exists(name) or not exist_ok:
        os.makedirs(name)
