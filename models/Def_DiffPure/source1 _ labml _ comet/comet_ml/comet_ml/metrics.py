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

This module contains the main components of comet.ml client side

"""
import random
from collections import OrderedDict

from ._typing import Any, Dict, List, Optional


class ReservoirSampler(object):
    def __init__(self, num_samples):
        """Create a new sampler with a certain reservoir size."""
        self.num_samples = num_samples
        self.num_items_seen = 0
        self.reservoir = []  # type: List[float]

    def sample(self, item):
        """Sample an item and store in the reservoir if needed."""
        num_samples = self.num_samples

        if len(self.reservoir) < num_samples:
            # reservoir not yet full, just append
            self.reservoir.append(item)
        else:
            # find a sample to replace
            index = random.randrange(self.num_items_seen + 1)
            if index < num_samples:
                self.reservoir[index] = item

        self.num_items_seen += 1

    def get_samples(self):
        """Get samples collected in the reservoir."""
        return self.reservoir


class BaseOperation(object):
    def process(self, metric):
        # type: (Dict[str, Any]) -> Optional[Dict[str, Any]]
        return metric

    def get_saved_metric(self):
        # type: () -> Optional[Dict[str, Any]]
        return None


class FirstMessageOperation(BaseOperation):
    def __init__(self):
        self.first_message = None

    def process(self, metric):
        if self.first_message is None:
            self.first_message = metric
            return None

        return metric

    def get_saved_metric(self):
        return self.first_message


class LastMessageOperation(BaseOperation):
    def __init__(self):
        self.previous = None

    def process(self, metric):
        previous = self.previous
        self.previous = metric
        return previous

    def get_saved_metric(self):
        return self.previous


class MinMessageOperation(BaseOperation):
    def __init__(self):
        self.min_message = None

    def process(self, metric):
        if self.min_message is None:
            self.min_message = metric
            return

        if metric["metric"]["metricValue"] < self.min_message["metric"]["metricValue"]:
            previous_min_message = self.min_message
            self.min_message = metric
            return previous_min_message

        return metric

    def get_saved_metric(self):
        return self.min_message


class MaxMessageOperation(BaseOperation):
    def __init__(self):
        self.max_message = None

    def process(self, metric):
        if self.max_message is None:
            self.max_message = metric
            return

        if metric["metric"]["metricValue"] > self.max_message["metric"]["metricValue"]:
            previous_max_message = self.max_message
            self.max_message = metric
            return previous_max_message

        return metric

    def get_saved_metric(self):
        return self.max_message


class MetricsSampler(object):
    def __init__(self, sample_size):

        if sample_size < 4:
            # We cannot sample below 4 as we need to keep the min, max, first
            # and last messages
            raise ValueError(
                "Invalid sample size, %d should be greater or equals to 4" % sample_size
            )

        self.sample_size = sample_size
        self.samplers = OrderedDict()  # type: Dict[str, ReservoirSampler]
        self.pipelines = {}  # type: Dict[str, List[BaseOperation]]
        self.all_have_steps = True

    def get_sampler(self, metric_name):
        # type: (str) -> ReservoirSampler
        if metric_name not in self.samplers:
            self.samplers[metric_name] = ReservoirSampler(self.sample_size - 4)

        return self.samplers[metric_name]

    def get_pipeline(self, metric_name):
        # type: (str) -> List[BaseOperation]
        if metric_name not in self.pipelines:
            pipeline = [
                FirstMessageOperation(),
                LastMessageOperation(),
                MinMessageOperation(),
                MaxMessageOperation(),
            ]
            self.pipelines[metric_name] = pipeline

        return self.pipelines[metric_name]

    def sample_metric(self, metric):
        # type: (Dict[str, Any]) -> None
        """Sample the next element of the metric stream for the metric names.
        There are few rules about which messages we should keep at all costs:
        - The first message of the stream
        - The last message of the stream
        - The message with the minimum value
        - The message with the maximum value
        """
        metric_name = metric["metric"]["metricName"]
        metric_step = metric["metric"]["step"]

        # Check that all metrics have a step
        self.all_have_steps = bool(self.all_have_steps and metric_step is not None)

        # Create the sampler anyway, so we don't lose track of metrics with
        # only one value
        sampler = self.get_sampler(metric_name)

        # Process the metric through a pipeline to extract the 4 needed
        # messages, first, last, min and max
        pipeline = self.get_pipeline(metric_name)

        for operation in pipeline:
            new_metric = operation.process(metric)

            metric = new_metric
            # The metric has been "saved" by the pipeline operation, do not
            # process it further
            if metric is None:
                return

        sampler.sample(metric)

    def get_samples(self):
        # type: () -> List[Dict[str, Any]]
        result = []

        for metric_name, sampler in self.samplers.items():
            result.extend(sampler.get_samples())

            # Add the 4 messages that got saved in the pipeline
            pipeline = self.get_pipeline(metric_name)

            for operation in pipeline:
                saved_message = operation.get_saved_metric()

                if saved_message:
                    result.append(saved_message)

        # Finally, sort everything back
        if self.all_have_steps:

            def key(metric):
                return metric["metric"]["step"]

        else:

            def key(metric):
                return metric["local_timestamp"]

        return list(sorted(result, key=key))
