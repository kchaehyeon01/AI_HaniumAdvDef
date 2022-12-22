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

"""
Author: Douglas Blank

This module contains the main components of CPU information logging

"""
import logging
import threading
import time

try:
    import psutil
except Exception:
    psutil = None

LOGGER = logging.getLogger(__name__)


DEFAULT_CPU_MONITOR_INTERVAL = 60


def is_cpu_info_available():
    return psutil is not None


class CPULoggingThread(threading.Thread):
    def __init__(self, initial_interval, callback):
        super(CPULoggingThread, self).__init__()
        self.daemon = True
        self.interval = initial_interval  # in seconds
        self.callback = callback
        self.last_run = 0.0

        self.closed = False

        LOGGER.debug(
            "CPUThread create with %ds interval",
            initial_interval,
        )

    def run(self):
        while not self.closed:
            try:
                self._loop()
            except Exception:
                LOGGER.debug("CPUThread failed to run", exc_info=True)

    def _loop(self):
        LOGGER.debug("CPU MONITOR LOOP %s %s", self.closed, self._should_run())
        if self._should_run():
            # Run
            cpu_details = self.get_cpu_metrics()
            self.callback(cpu_details)
            self.last_run = time.time()

        # Don't check the interval every CPU cycle but don't sleep the
        # whole interval in order to be able to change the interval and
        # close it more granularly
        time.sleep(1)

    def _should_run(self):
        next_run = self.last_run + self.interval  # seconds
        now = time.time()
        result = next_run <= now
        return result

    def update_interval(self, interval):
        LOGGER.debug("Update CPU monitor thread interval to %d", interval)
        self.interval = interval

    def close(self):
        LOGGER.debug("CPU THREAD close")
        self.closed = True

    def get_cpu_metrics(self):
        vm = psutil.virtual_memory()
        metrics = {}
        percents = psutil.cpu_percent(interval=None, percpu=True)
        # CPU percents:
        if len(percents) > 0:
            metrics["sys.cpu.percent.avg"] = sum(percents) / len(percents)
            for (i, percent) in enumerate(percents):
                metrics["sys.cpu.percent.%02d" % (i + 1)] = percent
        # Load average:
        try:
            # psutil <= 5.6.2 did not have getloadavg:
            if hasattr(psutil, "getloadavg"):
                metrics["sys.load.avg"] = psutil.getloadavg()[0]
            else:
                # Do not log an empty metric
                pass
        except OSError:
            metrics["sys.load.avg"] = None
        # RAM:
        metrics["sys.ram.total"] = vm.total
        metrics["sys.ram.used"] = vm.used
        return metrics
