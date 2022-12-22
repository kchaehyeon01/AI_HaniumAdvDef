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

from copy import deepcopy

from ._typing import Any, Dict

try:
    from collections import UserDict
except ImportError:
    from UserDict import UserDict


class ExperimentStorage(UserDict):
    """The storage to be used with experiment providing easy access to the defined storage spaces with fallback to
    the default values. The storage space is a dictionary.
    It is safe to access storage with default keys even after it was cleared; in that case the
    default value will be returned without raising KeyError. The storage can be used as replacement for standard
    dictionary."""

    def __init__(self, default):
        # type: (Dict[str, Dict[str, Any]]) -> None
        """Creates new instance with specified default storage spaces"""
        self._default = deepcopy(default)  # type: Dict[str, Dict[str, Any]]
        # populate with default data
        UserDict.__init__(self, default)

    def __getitem__(self, item):
        # type: (str) -> Dict[str, Any]
        storage = self.data.get(item)
        if storage:
            return storage
        elif item in self._default:
            storage = deepcopy(self._default[item])
        else:
            storage = {}

        self.data[item] = storage
        return storage

    def __setitem__(self, key, value):
        # type: (str, Dict[str, Any]) -> None
        self.data[key] = value
