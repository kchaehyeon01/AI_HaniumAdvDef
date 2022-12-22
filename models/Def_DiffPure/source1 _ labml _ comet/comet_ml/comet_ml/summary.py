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

import logging
from collections import defaultdict

from ._typing import Any, Dict, List, Optional
from .utils import format_bytes, log_once_at_level

LOGGER = logging.getLogger(__name__)


class Summary(object):
    def __init__(self, experiment_class_name):
        # type: (str) -> None
        self.experiment_class_name = experiment_class_name
        self.topics = {
            "data": SummaryTopic("Data"),
            "uploads": SummaryTopic("Uploads"),
            "others": SummaryTopic("Others"),
            "downloads": SummaryTopic("Downloads"),
            "parameters": SummaryTopic("Parameters"),
            "metrics": SummaryTopic("Metrics", minmax=True),
            "system-info": SummaryTopic("System Information"),
        }

    def set(self, topic, name, value, framework=None):
        # type: (str, str, Any, Optional[str]) -> None
        if topic in self.topics:
            self.topics[topic].set(name, value, framework=framework)
        else:
            LOGGER.error("no such summary topic: %r", topic)

    def get(self, topic, key):
        # type: (str, str) -> Any
        if topic in self.topics:
            if key in self.topics[topic].details:
                return self.topics[topic].details[key]
        return None

    def increment_section(self, topic, section, size=None, framework=None):
        # type: (str, str, Optional[int], Optional[str]) -> None
        if topic in self.topics:
            self.topics[topic].increment_section(
                section, size=size, framework=framework
            )
        else:
            LOGGER.error("no such summary topic: %r", topic)

    def generate_summary(self, display_summary_level):
        # type: (int) -> Dict[str, Any]
        """
        Generate and optionally display.

        Args:
            display_summary_level: level of details to display.

        Return dictionary is of the form:

        {
            'Data': {
                'url': 'https://comet.ml/workspace/project/2637237464736'
            },
            'Metrics [count] (min, max)': {
                'sys.cpu.percent.01': '2.9',
                'sys.cpu.percent.avg': '9.575000000000001',
                'sys.load.avg': '0.58',
                'sys.ram.total': '16522285056.0',
                'sys.ram.used': '13996814336.0',
                'train_acc [10]': '(0.1639556496115957, 0.9755067788284781)',
                'train_loss [10]': '(0.02660752389019383, 0.9435748153289714)',
                'validate_acc': '0.820739646603997',
                'validate_loss': '0.7258299466381112'},
            'Others': {
                'Name': 'my experiment'
            },
            'Uploads': {
                'asset': '2 (2 MB)',
                'git-patch': '1'
            }
        }
        """
        summary = {}

        for topic_key in self.topics:
            topic_summary = self.topics[topic_key]
            topic_name = topic_summary.name

            details = {}  # type: Dict[str, Any]
            count = 0
            minmax = 0
            empty = True
            for key in topic_summary.details:
                frameworks = topic_summary.get_frameworks(key)
                if "comet" in frameworks:
                    # at least one was logged by the system framework
                    if display_summary_level < 2:
                        # don't show system logged items like cpu, gpu, etc.
                        continue

                empty = False
                detail_name = key

                key_summary = topic_summary.details[key]

                if (
                    key_summary["min"] != float("inf")
                    and key_summary["max"] != float("-inf")
                    and (key_summary["min"] != key_summary["max"])
                ):
                    if key_summary["count"] > 1:
                        detail_name += " [%s]" % key_summary["count"]
                        count += 1

                    minmax += 1
                    details[detail_name] = (key_summary["min"], key_summary["max"])
                else:
                    if key_summary["value"] is not None:
                        details[detail_name] = key_summary["value"]
                    else:  # counts, and maybe size
                        if key_summary["size"]:
                            details[detail_name] = "%s (%s)" % (
                                key_summary["count"],
                                format_bytes(key_summary["size"]),
                            )
                        else:
                            details[detail_name] = key_summary["count"]

            if not empty:
                if count > 0:
                    if minmax > 0:
                        topic_description = "%s [count] (min, max)" % topic_name
                    else:
                        topic_description = "%s [count]" % topic_name
                else:
                    topic_description = "%s" % topic_name

                summary[topic_description] = details

        if display_summary_level > 0:
            title = "Comet.ml %s Summary" % self.experiment_class_name
            LOGGER.info("-" * len(title))
            LOGGER.info(title)
            LOGGER.info("-" * len(title))

            for topic in sorted(summary):
                # Show description
                LOGGER.info("  %s:", topic)

                # First, find maximum size of description:
                max_size = 0
                for desc in summary[topic]:
                    max_size = max(max_size, len(desc) + 1)

                for desc in sorted(summary[topic]):
                    value = summary[topic][desc]
                    LOGGER.info("    %-" + str(max_size) + "s: %s", desc, value)

            LOGGER.info("-" * len(title))
        return summary


class SummaryTopic(object):
    def __init__(self, name, minmax=False):
        # type: (str, bool) -> None
        self.name = name
        self.minmax = minmax

        def default():
            default_value = {
                "value": None,
                "min": float("inf"),
                "max": float("-inf"),
                "count": 0,
                "size": 0,
                "frameworks": [],
            }
            return default_value

        self.details = defaultdict(default)

    def get_frameworks(self, section):
        # type: (str) -> List
        return self.details[section]["frameworks"]

    def append_framework(self, section, framework):
        # type: (str, str) -> None
        self.get_frameworks(section).append(framework)

    def set(self, key, value, framework=None):
        # type: (str, Any, Optional[str]) -> None
        self.details[key]["value"] = value
        self.increment_section(key, framework=framework)
        if self.minmax:
            try:
                min_value = self.details[key]["min"]
                self.details[key]["min"] = min(min_value, value)
                max_value = self.details[key]["max"]
                self.details[key]["max"] = max(max_value, value)
            except Exception:
                log_once_at_level(
                    logging.DEBUG, "summary of %r cannot get min, max values" % key
                )

    def increment_section(self, section, size=None, framework=None):
        # type: (str, Optional[int], Optional[str]) -> None
        self.details[section]["count"] += 1
        if size:
            self.details[section]["size"] += size
        if framework:
            self.append_framework(section, framework)
