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

import sentry_sdk

from ._logging import _setup_sentry_error_handlers
from ._typing import Any, Dict, Optional
from .env_logging import _get_non_pii_env_details


def _setup_sentry_error_tracker(
    sentry_dsn, experiment_id, comet_url, debug, feature_toggles
):
    # type: (str, str, str, bool, Optional[Dict[str, bool]]) -> Any
    sentry_client = sentry_sdk.init(
        sentry_dsn,
        integrations=[],
        default_integrations=False,
        debug=debug,
    )

    sentry_sdk.set_user({"experiment_id": experiment_id})

    sentry_sdk.set_context(
        "python-sdk-context",
        _get_non_pii_env_details(comet_url),
    )

    if feature_toggles:
        sentry_sdk.set_context("python-sdk-FT", feature_toggles)

    root_logger = logging.getLogger("comet_ml")

    _setup_sentry_error_handlers(root_logger)

    return sentry_client
