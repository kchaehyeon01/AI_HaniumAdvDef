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

import imp
import os.path
import sys

import comet_ml  # noqa

if __name__ == "sitecustomize":
    # Import the next sitecustomize.py file

    # Then remove current directory from the search path
    current_dir = os.path.dirname(__file__)
    path = list(sys.path)

    try:
        path.remove(current_dir)
    except KeyError:
        pass

    # Then import any other sitecustomize
    try:
        _file, pathname, description = imp.find_module("sitecustomize", path)
    except ImportError:
        # We might be the only sitecustomize file
        pass
    else:
        imp.load_module("sitecustomize", _file, pathname, description)
