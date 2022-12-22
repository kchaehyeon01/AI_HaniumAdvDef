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

from .api import API, APIExperiment, Metadata, Metric, Other, Parameter, Tag

LOGGER = logging.getLogger(__name__)

__all__ = ["API", "APIExperiment", "Metadata", "Metric", "Other", "Parameter", "Tag"]

LOGGER.warning(
    "You have imported comet_ml.papi; "
    + "this interface is deprecated. Please use "
    + "comet_ml.api instead. For more information, see: "
    + "https://www.comet.ml/docs/python-sdk/releases/#release-300"
)
