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
import logging

from ._online import ExistingExperiment, Experiment
from .api import API
from .config import get_config, get_global_experiment, set_global_experiment
from .constants import RESUME_STRATEGY_GET_OR_CREATE
from .exceptions import CometRestApiException
from .experiment import BaseExperiment
from .logging_messages import (
    START_PARAMETERS_DONT_MATCH_EXISTING_EXPERIMENT,
    START_RUNNING_EXPERIMENT_IGNORED_PARAMETER,
)
from .offline import ExistingOfflineExperiment, OfflineExperiment

LOGGER = logging.getLogger(__name__)


def _check_matching_parameters_create(
    existing_experiment,
    api_key,
    disabled,
    distributed_node_identifier,
    distributed_node_type,
    experiment_key,
    offline_directory,
    offline,
    project_name,
    workspace,
):
    # type: (...) -> bool
    """Check if the existing experiment match when the resume strategy "create" is used"""
    if isinstance(existing_experiment, (ExistingExperiment, ExistingOfflineExperiment)):
        return False

    # Offline check
    if (
        isinstance(existing_experiment, (Experiment, ExistingExperiment))
        and offline is True
    ):
        return False
    elif (
        isinstance(existing_experiment, (OfflineExperiment, ExistingOfflineExperiment))
        and offline is False
    ):
        return False
    # TODO: Handle the case where existing_experiment is not of any known class

    if api_key != existing_experiment.api_key:
        return False

    if workspace != existing_experiment.workspace:
        # TODO: Test me
        return False

    if project_name != existing_experiment.project_name:
        # TODO: Test me
        return False

    if experiment_key is not None:
        if experiment_key != existing_experiment.id:
            return False

    if disabled != existing_experiment.disabled:
        return False

    existing_experiment_offline_directory = getattr(
        existing_experiment, "offline_directory", None
    )
    if (
        offline_directory is not None
        and existing_experiment_offline_directory is not None
    ):
        if offline_directory != existing_experiment_offline_directory:
            return False

    # TODO: Test distributed too
    return True


def _check_matching_parameters_get(
    existing_experiment,
    api_key,
    disabled,
    distributed_node_identifier,
    distributed_node_type,
    experiment_key,
    offline_directory,
    offline,
    project_name,
    workspace,
):
    # type: (...) -> bool
    """Check if the existing experiment match when the resume strategy "get" is used"""

    if experiment_key is None:
        raise Exception("experiment_key cannot be None")
    if not isinstance(
        existing_experiment, (ExistingExperiment, ExistingOfflineExperiment)
    ):
        return False

    # Offline check
    if (
        isinstance(existing_experiment, (Experiment, ExistingExperiment))
        and offline is True
    ):
        return False
    elif (
        isinstance(existing_experiment, (OfflineExperiment, ExistingOfflineExperiment))
        and offline is False
    ):
        return False
    # TODO: Handle the case where existing_experiment is not of any known class

    if api_key != existing_experiment.api_key:
        return False

    if workspace:
        LOGGER.debug("IGNORED")

    if project_name:
        LOGGER.debug("IGNORED")

    if experiment_key != existing_experiment.id:
        return False

    if disabled != existing_experiment.disabled:
        return False

    existing_experiment_offline_directory = getattr(
        existing_experiment, "offline_directory", None
    )
    if (
        offline_directory is not None
        and existing_experiment_offline_directory is not None
    ):
        if offline_directory != existing_experiment_offline_directory:
            return False

    # TODO: Test distributed too
    return True


def _check_matching_parameters_get_or_create(
    existing_experiment,
    api_key,
    disabled,
    distributed_node_identifier,
    distributed_node_type,
    experiment_key,
    offline_directory,
    offline,
    project_name,
    workspace,
):
    # type: (...) -> bool
    """Check if the existing experiment match when the resume strategy "get_or_create" is used"""

    if experiment_key is None:
        raise Exception("experiment_key cannot be None")

    # Offline check
    if (
        isinstance(existing_experiment, (Experiment, ExistingExperiment))
        and offline is True
    ):
        return False
    elif (
        isinstance(existing_experiment, (OfflineExperiment, ExistingOfflineExperiment))
        and offline is False
    ):
        return False
    # TODO: Handle the case where existing_experiment is not of any known class

    if api_key != existing_experiment.api_key:
        return False

    if workspace is not None and isinstance(
        existing_experiment, (Experiment, ExistingExperiment)
    ):
        LOGGER.debug(
            START_RUNNING_EXPERIMENT_IGNORED_PARAMETER,
            "workspace",
            workspace,
            existing_experiment,
        )

    if project_name is not None and isinstance(
        existing_experiment, (Experiment, ExistingExperiment)
    ):
        LOGGER.debug(
            START_RUNNING_EXPERIMENT_IGNORED_PARAMETER,
            "project_name",
            project_name,
            existing_experiment,
        )

    if experiment_key != existing_experiment.id:
        return False

    if disabled != existing_experiment.disabled:
        return False

    existing_experiment_offline_directory = getattr(
        existing_experiment, "offline_directory", None
    )
    if (
        offline_directory is not None
        and existing_experiment_offline_directory is not None
    ):
        if offline_directory != existing_experiment_offline_directory:
            return False

    # TODO: Test distributed too
    return True


def _check_matching_parameters(
    existing_experiment,
    resume_strategy,
    api_key,
    disabled,
    distributed_node_identifier,
    distributed_node_type,
    experiment_key,
    offline_directory,
    offline,
    project_name,
    workspace,
):
    # type: (...) -> bool

    if resume_strategy == "create":
        return _check_matching_parameters_create(
            existing_experiment,
            api_key,
            disabled,
            distributed_node_identifier,
            distributed_node_type,
            experiment_key,
            offline_directory,
            offline,
            project_name,
            workspace,
        )
    elif resume_strategy == "get":
        return _check_matching_parameters_get(
            existing_experiment,
            api_key,
            disabled,
            distributed_node_identifier,
            distributed_node_type,
            experiment_key,
            offline_directory,
            offline,
            project_name,
            workspace,
        )
    elif resume_strategy == "get_or_create":
        return _check_matching_parameters_get_or_create(
            existing_experiment,
            api_key,
            disabled,
            distributed_node_identifier,
            distributed_node_type,
            experiment_key,
            offline_directory,
            offline,
            project_name,
            workspace,
        )
    else:
        raise NotImplementedError()


def start(
    api_key=None,
    workspace=None,
    project_name=None,
    experiment_key=None,
    resume_strategy=None,
    offline=False,
    offline_directory=None,
    disabled=False,
    distributed_node_type=None,
    distributed_node_identifier=None,
    **kwargs
):
    # type: (...) -> BaseExperiment

    config = get_config()

    resume_strategy = config.get_string(
        resume_strategy, "comet.resume_strategy", default="create"
    )
    offline = config.get_bool(offline, "comet.offline", not_set_value=False)

    final_experiment_key = config.get_string(experiment_key, "comet.experiment_key")

    existing_experiment = get_global_experiment()
    # Only check non-ended experiments
    if existing_experiment is not None and existing_experiment.ended is False:

        match = _check_matching_parameters(
            existing_experiment,
            resume_strategy=resume_strategy,
            api_key=api_key,
            disabled=disabled,
            distributed_node_identifier=distributed_node_identifier,
            distributed_node_type=distributed_node_type,
            experiment_key=experiment_key,
            offline_directory=offline_directory,
            offline=offline,
            project_name=project_name,
            workspace=workspace,
        )

        if match:
            return existing_experiment
        else:
            LOGGER.warning(
                START_PARAMETERS_DONT_MATCH_EXISTING_EXPERIMENT, existing_experiment
            )
            # TODO: WAIT TRUE?
            existing_experiment.end()

            set_global_experiment(None)

    if resume_strategy == "create":
        if offline is False:
            return Experiment(
                api_key=api_key,
                workspace=workspace,
                project_name=project_name,
                disabled=disabled,
                experiment_key=final_experiment_key,
                **kwargs
            )
        else:
            return OfflineExperiment(
                api_key=api_key,
                workspace=workspace,
                project_name=project_name,
                disabled=disabled,
                experiment_key=final_experiment_key,
                offline_directory=offline_directory,
                **kwargs
            )
    elif resume_strategy == "get":
        # TODO: Better validation
        if final_experiment_key is None:
            raise Exception("experiment_key cannot be None")

        if offline is False:
            return ExistingExperiment(
                api_key=api_key,
                workspace=workspace,
                project_name=project_name,
                disabled=disabled,
                previous_experiment=final_experiment_key,
                **kwargs
            )
        else:
            return ExistingOfflineExperiment(
                api_key=api_key,
                workspace=workspace,
                project_name=project_name,
                disabled=disabled,
                previous_experiment=final_experiment_key,
                offline_directory=offline_directory,
                **kwargs
            )
    elif resume_strategy == "get_or_create":
        # TODO: Better validation
        if final_experiment_key is None:
            raise Exception("experiment_key cannot be None")

        if offline is False:

            # TODO: Find a better way, CometRestApiException could be raised for various reasons and
            # it's probably inneficient
            api = API(api_key=api_key, cache=False)

            try:
                api._get_experiment_metadata(final_experiment_key)

                return ExistingExperiment(
                    api_key=api_key,
                    workspace=workspace,
                    project_name=project_name,
                    disabled=disabled,
                    previous_experiment=final_experiment_key,
                    **kwargs
                )
            except CometRestApiException:
                return Experiment(
                    api_key=api_key,
                    workspace=workspace,
                    project_name=project_name,
                    disabled=disabled,
                    experiment_key=final_experiment_key,
                    **kwargs
                )
        else:
            experiment = OfflineExperiment(
                api_key=api_key,
                workspace=workspace,
                project_name=project_name,
                disabled=disabled,
                experiment_key=final_experiment_key,
                offline_directory=offline_directory,
                **kwargs
            )
            # TODO: Temporary
            experiment.resume_strategy = RESUME_STRATEGY_GET_OR_CREATE
            return experiment

    else:
        raise NotImplementedError()
