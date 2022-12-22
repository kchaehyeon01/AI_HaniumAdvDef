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

from ._typing import Any, Dict

LOGGER = logging.getLogger()


def validate_metadata(user_metadata, raise_on_invalid=False):
    # type: (Any, bool) -> Dict[Any, Any]
    if user_metadata is None:
        return {}

    if type(user_metadata) is not dict:
        if raise_on_invalid:
            raise ValueError("Invalid metadata, expecting dict type %r" % user_metadata)
        else:
            LOGGER.warning("Invalid metadata, expecting dict type %r", user_metadata)
            return {}

    result = user_metadata  # type: Dict[Any, Any]

    return result
