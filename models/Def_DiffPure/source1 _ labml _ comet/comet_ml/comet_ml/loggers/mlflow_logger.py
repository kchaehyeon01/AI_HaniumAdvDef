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
from os.path import basename, join

from .._typing import Optional
from ..config import (
    get_api_key,
    get_config,
    get_global_experiment,
    set_global_experiment,
)
from ..experiment import BaseExperiment
from ..logging_messages import (
    MLFLOW_NESTED_RUN_UNSUPPORTED,
    MLFLOW_OFFLINE_EXPERIMENT_FALLBACK,
    MLFLOW_RESUMED_RUN,
)
from ..monkey_patching import check_module
from ..offline import OfflineExperiment

LOGGER = logging.getLogger(__name__)


LOG_MODEL_MODEL_NAME = None
# MLFlow fluent api is NOT thread-safe, so setting an experiment name in a
# thread will impact other threads creating runs
PROJECT_NAME = None

# MLFlow _get_or_start_run calls the same high-level method start_run, do
# distinguish between direct calls and implicit calls by MLFlow itself
IMPLICIT_START_RUN = False


def _reset_global_state():
    global LOG_MODEL_MODEL_NAME
    global PROJECT_NAME
    global IMPLICIT_START_RUN

    LOG_MODEL_MODEL_NAME = None
    PROJECT_NAME = None
    IMPLICIT_START_RUN = False


def _create_experiment(experiment_name=None):
    # type: (Optional[str]) -> BaseExperiment
    LOGGER.debug("Creating new Experiment for MLFlow, implicit? %r", IMPLICIT_START_RUN)

    global PROJECT_NAME

    api_key = get_api_key(None, get_config())

    if api_key:
        from comet_ml import Experiment

        LOGGER.debug("Creating an online Experiment with project name %r", PROJECT_NAME)

        exp = Experiment(api_key, project_name=PROJECT_NAME)
    else:
        LOGGER.info(MLFLOW_OFFLINE_EXPERIMENT_FALLBACK)
        LOGGER.debug(
            "Creating an offline Experiment with project name %r.", PROJECT_NAME
        )

        exp = OfflineExperiment(
            project_name=PROJECT_NAME,
        )

    if experiment_name:
        exp.set_name(experiment_name)

    # Mark the experiment as created implicitly from MLFlow logger
    exp.log_other("Created from", "MLFlow auto-logger")

    return exp


def _get_or_create_experiment(experiment):
    # type: (Optional[BaseExperiment]) -> BaseExperiment
    if experiment:
        return experiment

    # We might have already created an experiment during the execution of the
    # Entrypoint
    global_experiment = get_global_experiment()
    if global_experiment:
        return global_experiment

    return _create_experiment()


def mlflow_log_metric(experiment, original, return_value, key, value, step=None):
    # MLFlow auto-logging is passing epochs in the step parameter
    _get_or_create_experiment(experiment).log_metric(key, value, epoch=step)


def mlflow_log_metrics(experiment, original, return_value, metrics, step=None):
    # MLFlow auto-logging is passing epochs in the step parameter
    _get_or_create_experiment(experiment).log_metrics(metrics, epoch=step)


def mlflow_log_param(experiment, original, return_value, key, value):
    _get_or_create_experiment(experiment).log_parameter(key, value)


def mlflow_log_params(experiment, original, return_value, params):
    _get_or_create_experiment(experiment).log_parameters(params)


def mlflow_set_tag(experiment, original, return_value, key, value):
    _get_or_create_experiment(experiment).log_other(key, value)


def mlflow_set_tags(experiment, original, return_value, tags):
    _get_or_create_experiment(experiment).log_others(tags)


def mlflow_log_artifact(
    experiment, original, return_value, local_path, artifact_path=None
):
    experiment = _get_or_create_experiment(experiment)

    file_name = None
    if artifact_path:
        file_name = join(artifact_path, basename(local_path))

    experiment.log_asset(local_path, file_name=file_name)


def mlflow_log_artifacts(
    experiment, original, return_value, local_dir, artifact_path=None
):
    global LOG_MODEL_MODEL_NAME
    exp = _get_or_create_experiment(experiment)

    if LOG_MODEL_MODEL_NAME:
        exp._log_model(LOG_MODEL_MODEL_NAME, local_dir, folder_name=artifact_path)
    else:
        exp._log_asset_folder(
            local_dir, recursive=True, log_file_name=True, folder_name=artifact_path
        )


def mlflow_model_log_before(
    experiment,
    original,
    cls,
    artifact_path,
    flavor,
    registered_model_name=None,
    **kwargs
):
    global LOG_MODEL_MODEL_NAME

    LOGGER.debug("MLFlow log model called with model name %r", registered_model_name)

    LOG_MODEL_MODEL_NAME = registered_model_name


def mlflow_model_log_after(experiment, original, return_value, *args, **kwargs):
    global LOG_MODEL_MODEL_NAME

    LOG_MODEL_MODEL_NAME = None


def mlflow_set_experiment(experiment, original, return_value, experiment_name):
    global PROJECT_NAME

    PROJECT_NAME = experiment_name


def mlflow_start_run(
    experiment,
    original,
    return_value,
    run_id=None,
    experiment_id=None,
    run_name=None,
    nested=False,
):
    if nested:
        LOGGER.warning(MLFLOW_NESTED_RUN_UNSUPPORTED)
        return

    # Detect continuing runs
    new_run_status = getattr(getattr(return_value, "_info", None), "status", None)
    new_run_id = getattr(getattr(return_value, "_info", None), "run_id", None)

    # The latest MLFlow version, on resuming of the existing experiment, sets the status to the RunStatus.RUNNING
    # and run_id to the run_id value that was returned by previous run.
    if new_run_status == "FINISHED" or (
        new_run_status == "RUNNING" and run_id == new_run_id
    ):
        LOGGER.info(MLFLOW_RESUMED_RUN)

    if IMPLICIT_START_RUN:
        # Create an implicit start only if no experiment existed before
        if experiment is None:
            _create_experiment(run_name)
        else:
            # We have an existing experiment that was likely created by the user
            # before calling mlflow.log_*
            pass
    else:
        _create_experiment(run_name)


def mlflow_end_run(experiment, original, return_value, *args, **kwargs):
    current_experiment = get_global_experiment()

    if current_experiment:
        current_experiment.end()

    set_global_experiment(None)


def mlflow_get_or_start_run_before(experiment, original, *args, **kwargs):
    global IMPLICIT_START_RUN

    IMPLICIT_START_RUN = True


def mlflow_get_or_start_run_after(experiment, original, return_value, *args, **kwargs):
    global IMPLICIT_START_RUN

    IMPLICIT_START_RUN = False


MLFLOW_PATCHING = {
    "log_metric": mlflow_log_metric,
    "log_metrics": mlflow_log_metrics,
    "log_param": mlflow_log_param,
    "log_params": mlflow_log_params,
    "set_tag": mlflow_set_tag,
    "set_tags": mlflow_set_tags,
    "log_artifact": mlflow_log_artifact,
    "log_artifacts": mlflow_log_artifacts,
    "set_experiment": mlflow_set_experiment,
    "start_run": mlflow_start_run,
    "end_run": mlflow_end_run,
}


def patch(module_finder):
    check_module("mlflow")

    for mlflow_function, patcher_function in MLFLOW_PATCHING.items():
        module_finder.register_after(
            "mlflow.tracking.fluent",
            mlflow_function,
            patcher_function,
            allow_empty_experiment=True,
        )

    module_finder.register_before(
        "mlflow.models",
        "Model.log",
        mlflow_model_log_before,
        allow_empty_experiment=True,
    )
    module_finder.register_after(
        "mlflow.models",
        "Model.log",
        mlflow_model_log_after,
        allow_empty_experiment=True,
    )

    module_finder.register_before(
        "mlflow.tracking.fluent",
        "_get_or_start_run",
        mlflow_get_or_start_run_before,
        allow_empty_experiment=True,
    )
    module_finder.register_after(
        "mlflow.tracking.fluent",
        "_get_or_start_run",
        mlflow_get_or_start_run_after,
        allow_empty_experiment=True,
    )


check_module("mlflow")
