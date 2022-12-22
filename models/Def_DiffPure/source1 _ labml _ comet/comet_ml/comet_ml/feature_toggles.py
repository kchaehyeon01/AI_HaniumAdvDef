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

This module contains feature toggles related code

"""

from .config import CONFIG_MAP, get_config


class FeatureToggles(object):
    """Feature Toggle helper class, avoid getting a feature toggle without
    fallbacking on the default value. Also read environment variables for
    overrides.
    """

    def __init__(self, raw_toggles, config):
        self.raw_toggles = raw_toggles
        self.config = config

        # Write the toggles in config override, the namespace need to be
        # converted from dots to underscores for everett to be happy
        self.config._set_backend_override(self.raw_toggles, "comet_override_feature")

    def __eq__(self, other):
        if isinstance(other, FeatureToggles):
            return self.raw_toggles == other.raw_toggles

        return False

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "FeatureToggles(%s, %r)" % (self.raw_toggles, self.config)

    def __getitem__(self, name):
        try:
            override_value = self.config["comet.override_feature.%s" % name]
            if override_value is not None:
                return override_value

        except KeyError:
            pass

        return self.raw_toggles.get(name, False)


def get_feature_toggle_overrides():
    result = {}
    config = get_config()
    for ft in FT_LIST:
        config_key = "comet.override_feature.%s" % ft
        override_value = config[config_key]
        default_value = CONFIG_MAP[config_key].get("default", None)
        if override_value != default_value:
            result[ft] = override_value

    return result


# Constants defining feature toggles names to avoid typos disabling a feature

HTTP_LOGGING = "sdk_http_logging"

USE_HTTP_MESSAGES = "sdk_use_http_messages"

FT_LIST = [HTTP_LOGGING, USE_HTTP_MESSAGES]
