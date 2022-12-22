# -*- coding: utf-8 -*-
# *******************************************************
#   ____                     _               _
#  / ___|___  _ __ ___   ___| |_   _ __ ___ | |
# | |   / _ \| '_ ` _ \ / _ \ __| | '_ ` _ \| |
# | |__| (_) | | | | | |  __/ |_ _| | | | | | |
#  \____\___/|_| |_| |_|\___|\__(_)_| |_| |_|_|
#
#  Sign up for free at http://www.comet.ml
#  Copyright (C) 2015-2022 Comet ML INC
#  This file can not be copied and/or distributed without the express
#  permission of Comet ML Inc.
# *******************************************************

from types import TracebackType

from comet_ml import get_global_experiment

from ._typing import Any, Callable, Optional, Type

ExceptionHookType = Callable[
    [Type[BaseException], BaseException, Optional[TracebackType]],
    Any,
]


def _create_exception_hook(old_exception_hook):
    # type: (ExceptionHookType) -> ExceptionHookType
    def comet_sdk_exception_hook(exception_type, exception_value, traceback):
        # type: (Type[BaseException], BaseException, Optional[TracebackType]) -> ExceptionHookType
        experiment = get_global_experiment()

        if experiment is not None:
            experiment._report_experiment_error()

        return old_exception_hook(exception_type, exception_value, traceback)

    return comet_sdk_exception_hook
