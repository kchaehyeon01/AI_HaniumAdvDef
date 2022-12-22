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

from comet_ml._typing import Any
from comet_ml.oscontainer.constants import NO_LIMIT
from comet_ml.oscontainer.errors import OSContainerError
from comet_ml.oscontainer.scanf import scanf

LOGGER = logging.getLogger(__name__)


def limit_from_str(limit_str):
    # type: (str) -> int
    if limit_str is None:
        raise OSContainerError("limit is None")

    if limit_str == "max":
        return NO_LIMIT

    return int(limit_str)


def load_multiline_scan(path, scan_format, match_line):
    # type: (str, str, str) -> Any
    """Loads content from multiline file using specified match criteria.
    :param path: the path to the file.
    :param scan_format: the format to use with scanf to extract value.
    :param match_line: the line to be matched for value to extract.
    :return: extracted value or None if not found.
    """
    with open(path, "r") as f:
        for line in f:
            if line.__contains__(match_line):
                res = scanf(scan_format, line)
                if res is not None and len(res) == 2:
                    return res[1]
    return None


def load_scan(path, scan_format):
    # type: (str,str) -> Any
    """Loads content of the file using provided scan format"""
    val = load(path)
    return scanf(scan_format, val)


def load(path):
    # type: (str) -> str
    """Loads a file content"""
    with open(path, "r") as f:
        tmp = f.read()
        return tmp.strip()
