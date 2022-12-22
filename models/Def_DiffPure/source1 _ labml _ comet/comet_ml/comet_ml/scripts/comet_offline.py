#!/usr/bin/env python
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
Explore an offline experiment archive.

Display summaries of an offline experiments:

$ comet offline *.zip

Display CSV (Comma-Separated Value) format. Shows an
experiment's data in a row format:

Workspace, Project, Experiment, Level, Section, Name, Value

where:

* level: detail, maximum, or minimum
* section: metric, param, log_other, etc.
* name: name of metric, param, etc.

$ comet offline --csv *.zip

Use --level, --section, or --name to filter the rows.
"""

from __future__ import division, print_function

import argparse
import csv
import json
import logging
import numbers
import os
import sys
import time
import zipfile
from collections import defaultdict

from .._typing import Dict, Optional
from ..config import (
    OFFLINE_EXPERIMENT_JSON_FILE_NAME,
    OFFLINE_EXPERIMENT_MESSAGES_JSON_FILE_NAME,
    get_config,
)
from ..messages import (
    CloudDetailsMessage,
    FileNameMessage,
    GitMetadataMessage,
    InstalledPackagesMessage,
    LogOtherMessage,
    MetricMessage,
    ModelGraphMessage,
    OsPackagesMessage,
    ParameterMessage,
    RemoteAssetMessage,
    StandardOutputMessage,
    SystemDetailsMessage,
    UploadFileMessage,
    WebSocketMessage,
)
from ..utils import format_bytes

LOGGER = logging.getLogger("comet_ml")
ADDITIONAL_ARGS = True


class Size(object):
    """
    Keeps a running size, but able to display nicely for humans.
    """

    def __init__(self, bytes):
        self.bytes = bytes

    def __add__(self, other):
        if isinstance(other, Size):
            return Size(self.bytes + other.bytes)
        else:
            return Size(self.bytes + other)

    def __str__(self):
        return format_bytes(self.bytes)


class ArchivedExperiment(object):
    """
    Object reprenting an Offline Experiment ZIP file.
    """

    def __init__(self, filename, args, show_header=True):
        self.filename = filename
        self.archive = zipfile.ZipFile(self.filename)
        self.args = args
        self.config = get_config()
        self.counts = defaultdict(
            lambda: defaultdict(lambda: 0)
        )  # type: Dict[str, Dict[str, int]]
        self.values = defaultdict(
            lambda: defaultdict(lambda: None)
        )  # type: Dict[str, Dict[str, Optional[float]]]
        self.workspace = None
        self.project = None
        self.experiment = None
        self.displayed_header = not show_header
        if self.args.csv:
            if self.args.output is None:
                self.writer = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
            else:
                self.writer = csv.writer(open(args.output, "a"), quoting=csv.QUOTE_ALL)
        self.process()
        self.display_summary()

    def close(self):
        """
        Close up the archive.
        """
        pass

    def process(self):
        """
        Process the experiment and message JSON files.
        """
        try:
            self.process_experiment()
        except Exception:
            LOGGER.error("Error in processing experiment; skipping...", exc_info=True)
        try:
            self.process_message()
        except Exception:
            LOGGER.error("Error in processing messages; skipping...", exc_info=True)

    def update_summary(self, section, name, value, count_only, sum_only=False):
        """
        Update the summary counts and values.
        """
        self.counts[section][name] += 1
        if sum_only:
            if self.values[section][name] is None:
                self.values[section][name] = value
            else:
                self.values[section][name] += value
            return
        if count_only:
            return
        if isinstance(value, numbers.Number) and not isinstance(value, bool):
            if self.values[section][name] is None:
                self.values[section][name] = (value, value)
            else:
                minimum, maximum = self.values[section][name]
                self.values[section][name] = (min(minimum, value), max(maximum, value))
        else:
            self.values[section][name] = value

    def process_message(self):
        """
        Process the message JSON file.
        """
        with self.archive.open(OFFLINE_EXPERIMENT_MESSAGES_JSON_FILE_NAME) as fp:
            for line in fp:
                message = json.loads(line)
                if message["type"] in [
                    WebSocketMessage.type,
                    ParameterMessage.type,
                    MetricMessage.type,
                    LogOtherMessage.type,
                ]:
                    self.process_payload(message["payload"])
                elif message["type"] == UploadFileMessage.type:
                    payload = message["payload"]
                    file_info = [
                        fi
                        for fi in self.archive.filelist
                        if fi.filename == payload["file_path"]
                    ][0]
                    size = (
                        Size(file_info.file_size)
                        if not self.args.raw_size
                        else file_info.file_size
                    )
                    self.update_summary(
                        "data", "file uploads", size, count_only=False, sum_only=True
                    )
                    self.write(
                        "detail", UploadFileMessage.type, payload["upload_type"], size
                    )
                elif message["type"] == StandardOutputMessage.type:
                    payload = message["payload"]
                    name = "stderr" if payload["stderr"] is True else "stdout"
                    self.update_summary("data", name, payload["output"], True)
                elif message["type"] in [
                    RemoteAssetMessage.type,
                    ModelGraphMessage.type,
                    OsPackagesMessage.type,
                    SystemDetailsMessage.type,
                    CloudDetailsMessage.type,
                    FileNameMessage.type,
                    InstalledPackagesMessage.type,
                    GitMetadataMessage.type,
                ]:
                    continue
                else:
                    LOGGER.error(
                        "Unknown message  type: %s", message["type"], exc_info=True
                    )

    def process_payload(self, payload):
        """
        Process the payload of a message.
        """
        for section in payload:
            if payload[section] is not None and section not in [
                "offset",
                "local_timestamp",
                "stderr",
            ]:
                count_only = False
                if section == "metric":
                    name = payload[section]["metricName"]
                    value = payload[section]["metricValue"]
                elif section == "param":
                    name = payload[section]["paramName"]
                    value = payload[section]["paramValue"]
                elif section == "log_other":
                    name = payload[section]["key"]
                    value = payload[section]["val"]
                elif section == "stdout":
                    value = payload["stdout"]
                    name = "stderr" if payload["stderr"] else "stdout"
                    section = "data"
                    count_only = True
                else:
                    name = section
                    value = payload[section]
                    section = "data"
                    count_only = True
                self.update_summary(section, name, value, count_only)
                ## encode unicode, especially for python2
                self.write("detail", section, name, value)

    def write(self, level, section, name, value, force=False):
        """
        Write a row to the CSV writer.
        """
        if force or (
            self.args.csv
            and (self.args.level is None or level in self.args.level)
            and (self.args.section is None or section in self.args.section)
            and (self.args.name is None or name in self.args.name)
        ):
            if not self.displayed_header:
                self.writer.writerow(
                    [
                        "workspace",
                        "project",
                        "experiment",
                        "level",
                        "section",
                        "name",
                        "value",
                    ]
                )
                self.displayed_header = True
            if isinstance(value, str):
                value = value.replace("\n", "\\n")
            self.writer.writerow(
                [
                    self.workspace,
                    self.project,
                    self.experiment,
                    level,
                    section,
                    name,
                    value,
                ]
            )

    def display_summary(self):
        """
        Display the summary of the Offline ZIP Experiment archive.
        """
        if not self.args.csv:
            print("================================")
            print("Comet Offline Experiment Summary")
            print("================================")
            print("Archive: %s" % self.filename)
        for section in sorted(self.counts.keys()):
            if not self.args.csv:
                print()
                heading = "Section %s [count]: (min, max)" % section
                print(heading)
                print("-" * len(heading))
            for name in sorted(self.counts[section].keys()):
                count = self.counts[section][name]
                if self.values[section][name] is None:
                    value = None
                elif (
                    isinstance(self.values[section][name], tuple)
                    and len(self.values[section][name]) == 2
                ):
                    if self.values[section][name][0] == self.values[section][name][1]:
                        value = self.values[section][name][0]
                    else:
                        value = self.values[section][name]
                    self.write("minimum", section, name, self.values[section][name][0])
                    self.write("maximum", section, name, self.values[section][name][1])
                else:
                    value = self.values[section][name]
                if not self.args.csv:
                    warning = ""
                    if section == "metric":
                        if (
                            self.counts["metric"][name]
                            > self.config["comet.offline_sampling_size"]
                        ):
                            warning = (
                                "--- THIS METRIC WILL BE DOWNSAMPLED TO %s ITEMS: see https://www.comet.ml/docs/python-sdk/warnings-errors/#offline-experiments"
                                % self.config["comet.offline_sampling_size"]
                            )
                    if value is not None:
                        print("    %s [%s]: %s %s" % (name, count, value, warning))
                    else:
                        print("    %s [%s] %s" % (name, count, warning))

    def process_experiment(self):
        """
        Process the experiment JSON file.
        """
        with self.archive.open(OFFLINE_EXPERIMENT_JSON_FILE_NAME) as fp:
            line = fp.readline()
            experiment = json.loads(line)
            self.workspace = experiment["workspace"] or "DEFAULT"
            self.project = experiment["project_name"] or "general"
            self.experiment = experiment["offline_id"]
            for field, value in [
                (
                    "start_time",
                    time.strftime(
                        "%Y-%m-%d %H:%M:%S",
                        time.localtime(experiment["start_time"] / 1000.0),
                    ),
                ),
                (
                    "stop_time",
                    time.strftime(
                        "%Y-%m-%d %H:%M:%S",
                        time.localtime(experiment["stop_time"] / 1000.0),
                    ),
                ),
                ("tags", experiment["tags"]),
                ("url", "/".join([self.workspace, self.project, self.experiment])),
            ]:
                self.update_summary("data", field, value, count_only=False)
                self.write("detail", "data", field, value)


def get_parser_arguments(parser):
    parser.add_argument(
        "archives", nargs="+", help="the offline experiment archives to display"
    )
    parser.add_argument(
        "--csv",
        help="output details in csv format",
        action="store_const",
        const=True,
        default=False,
    )
    parser.add_argument(
        "--section",
        help="output specific section in csv format, including param, metric, log_other, data, etc.",
        action="append",
    )
    parser.add_argument(
        "--level",
        help="output specific summary level in csv format, including minimum, maximum, detail",
        action="append",
    )
    parser.add_argument(
        "--name",
        help="output specific name in csv format, including items like loss, acc, etc.",
        action="append",
    )
    parser.add_argument("--output", help="output filename for csv format", default=None)
    parser.add_argument(
        "--raw-size",
        help="Use bytes for file sizes",
        action="store_const",
        const=True,
        default=False,
    )
    parser.add_argument(
        "--no-header",
        help="Use this flag to suppress CSV header",
        action="store_const",
        const=True,
        default=False,
    )


def offline(args, rest=None):
    # Called via `comet offline EXP.zip`
    # args are parsed_args
    if (
        (args.name is not None)
        or (args.level is not None)
        or (args.section is not None)
    ):
        args.csv = True

    if args.output:
        if os.path.isfile(args.output):
            os.remove(args.output)

    first = not args.no_header
    for filename in args.archives:
        archive = ArchivedExperiment(filename, args, show_header=first)
        archive.close()
        first = False


def main(args):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    get_parser_arguments(parser)
    parsed_args = parser.parse_args(args)
    offline(parsed_args)


if __name__ == "__main__":
    # Called via `python -m comet_ml.scripts.comet_offline EXP.zip`
    main(sys.argv[1:])
