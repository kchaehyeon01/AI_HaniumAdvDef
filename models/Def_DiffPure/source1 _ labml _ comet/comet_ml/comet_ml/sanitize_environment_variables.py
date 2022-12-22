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

import json

from ._typing import Any, Dict


def is_json(myjson):
    try:
        json.loads(myjson)
    except ValueError:
        return False
    return True


def sanitize_environment_variables(pipeline_details):
    # type: (Dict[Any]) -> Dict[Any]
    if not isinstance(pipeline_details, dict):
        return pipeline_details

    for key, value in pipeline_details.items():
        if key == "env":
            if isinstance(value, dict):
                pipeline_details[key] = {}
            elif isinstance(value, str):
                pipeline_details[key] = ""
            else:
                pipeline_details[key] = []
        elif isinstance(value, list):
            for i, item in enumerate(value):
                value[i] = sanitize_environment_variables(item)
        elif isinstance(value, str):
            pipeline_details[
                key
            ] = _kubeflow_handle_string_which_is_itself_encoded_json(value)
        sanitize_environment_variables(value)
    return pipeline_details


def _kubeflow_handle_string_which_is_itself_encoded_json(value):
    if is_json(value):
        return json.dumps(sanitize_environment_variables(json.loads(value)))
    else:
        return value
