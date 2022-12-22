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
import logging
import time

from comet_ml.sanitize_environment_variables import sanitize_environment_variables

from ..._typing import Any, Dict, List, Optional, Sequence, Union
from ...config import get_api_key, get_config
from ...experiment import BaseExperiment
from ...logging_messages import KUBEFLOW_LOGGER_ERROR, KUBEFLOW_LOGGER_IMPORT_ERROR
from .. import KEY_CREATED_FROM, KEY_PIPELINE_TYPE

LOGGER = logging.getLogger(__name__)


KUBEFLOW_LOGGER_ERROR_TAG = "kubeflow_not_initialized"


def _comet_logger_experiment_name(task_name, pipeline_run_name):
    # type: (str, str) -> str
    return "{}-{}".format(task_name, pipeline_run_name)


def _comet_logger_task_id(workflow_uid, pod_name):
    # type: (str, str) -> str
    return "{}-{}".format(workflow_uid, pod_name)


def _comet_logger_sidecar_experiment_name(pipeline_run_name):
    # type: (str) -> str
    return "comet-pipeline-logger-{}".format(pipeline_run_name)


def _update_node_status(pipeline_run_detail_dict, node_display_name, new_phase):
    # type: (Dict[str, Any], str, str) -> Dict[str, Any]
    """Update the status of a single node identified by its display name, return the pipeline run
    details
    """

    workflow_manifest = json.loads(
        pipeline_run_detail_dict["pipeline_runtime"]["workflow_manifest"]
    )

    for i in workflow_manifest["status"]["nodes"].values():
        if i["displayName"] == SIDECAR_COMPONENT_NAME:
            i["phase"] = "Succeeded"

    pipeline_run_detail_dict["pipeline_runtime"]["workflow_manifest"] = json.dumps(
        workflow_manifest
    )

    return pipeline_run_detail_dict


def initialize_comet_logger(experiment, workflow_uid, pod_name):
    # type: (BaseExperiment, str, str) -> BaseExperiment
    """Logs the Kubeflow task identifiers needed to track your pipeline status in Comet.ml. You need
    to call this function from every components you want to track with Comet.ml.

    Args:
        experiment: An already created Experiment object.
        workflow_uid: string. The Kubeflow workflow uid, see below to get it automatically from Kubeflow.
        pod_name: string. The Kubeflow pod name, see below to get it automatically from Kubeflow.

    For example:
    ```python
    def my_component() -> None:
        import comet_ml.integration.kubeflow

        experiment = comet_ml.Experiment()
        workflow_uid = "{{workflow.uid}}"
        pod_name = "{{pod_name}}"

        comet_ml.integration.kubeflow.initialize_comet_logger(experiment, workflow_uid, pod_name)
    ```
    """

    try:
        # First collect everything
        import kfp

        client = kfp.Client()
        pipeline_run_detail = client.get_run(workflow_uid)
        pipeline_run_name = pipeline_run_detail.run.name
        workflow_manifest = json.loads(
            pipeline_run_detail.pipeline_runtime.workflow_manifest
        )
        task_name = workflow_manifest["status"]["nodes"][pod_name]["displayName"]

        experiment_name = _comet_logger_experiment_name(task_name, pipeline_run_name)
        kubeflow_task_id = _comet_logger_task_id(workflow_uid, pod_name)

        # Then log all of them, in case of errors, log none of them to avoid half-break experiment
        # tracking data
        experiment.set_name(experiment_name)
        experiment.log_other("kubeflow_run_name", pipeline_run_name)
        experiment.log_other("kubeflow_task_id", kubeflow_task_id)
        experiment.log_other("kubeflow_task_type", "task")
        experiment.log_other("kubeflow_task_name", task_name)
        experiment.log_other(KEY_PIPELINE_TYPE, "kubeflow")

        return experiment
    except ImportError:
        LOGGER.warning(KUBEFLOW_LOGGER_IMPORT_ERROR, exc_info=True)
        experiment.add_tag(KUBEFLOW_LOGGER_ERROR_TAG)
        return experiment
    except Exception:
        LOGGER.warning(KUBEFLOW_LOGGER_ERROR, KUBEFLOW_LOGGER_ERROR_TAG, exc_info=True)
        experiment.add_tag(KUBEFLOW_LOGGER_ERROR_TAG)
        return experiment


COMET_LOGGER_TIMEOUT = 30


def _count_running_tasks(nodes):
    # type: (Sequence[Dict[str, Any]]) -> int
    """Return the number of running tasks at a single point of time for a Kubeflow pipeline"""
    running_tasks = 0

    for node in nodes:
        # Ignore DAG node type which is always running
        if node["type"] == "DAG":
            continue

        if node["phase"].lower() == "running":
            running_tasks += 1

    return running_tasks


def _comet_logger_implementation(experiment, workflow_uid, timeout):
    # type: (BaseExperiment, str, int) -> None
    """Extracted comet logger implementation to ease testing.

    Collect and logs the current Kubeflow pipeline status every second. When this component is the
    last component to run for TIMEOUT second, logs the status one last time and exit.
    """
    import kfp

    # Get initial pipeline run data
    client = kfp.Client()
    pipeline_run_detail = client.get_run(workflow_uid)
    pipeline_run_name = pipeline_run_detail.run.name

    experiment.set_name(_comet_logger_sidecar_experiment_name(pipeline_run_name))
    experiment.log_other("kubeflow_run_name", pipeline_run_name)
    experiment.log_other("kubeflow_run_id", workflow_uid)
    experiment.log_other("kubeflow_task_type", "pipeline")
    experiment.log_other(KEY_CREATED_FROM, "kubeflow")

    # Need to add a while condition based on the state of the pipeline so that it auto-terminates
    # Check there are
    iterator_nb = 0
    step = 0
    while True:
        pipeline_run_detail = client.get_run(workflow_uid)
        pipeline_run_detail_dict = pipeline_run_detail.to_dict()
        pipeline_run_detail_dict = sanitize_environment_variables(
            pipeline_run_detail_dict,
        )

        experiment._log_asset_data(
            json.dumps(pipeline_run_detail_dict, default=str),
            overwrite=True,
            file_name="kubeflow-pipeline",
            asset_type="kubeflow-pipeline",
        )
        # Simple check that looks at the number of running tasks and if there is only one running for more
        # than X seconds stops monitoring the pipeline
        workflow_manifest = pipeline_run_detail_dict["pipeline_runtime"][
            "workflow_manifest"
        ]
        pipeline_state = json.loads(workflow_manifest)["status"]
        nb_tasks_running = _count_running_tasks(pipeline_state["nodes"].values())
        if nb_tasks_running > 1:
            iterator_nb = 0
        else:
            iterator_nb += 1

        if iterator_nb > timeout:
            break

        step += 1
        time.sleep(1)

    # Manually log the final state of the pipeline as done so that the pipeline is marked as done on the Comet UI
    pipeline_run_detail = client.get_run(workflow_uid)
    pipeline_run_detail_dict = pipeline_run_detail.to_dict()

    # Update sidecar component status to show a nice UI
    pipeline_run_detail_dict = _update_node_status(
        pipeline_run_detail_dict, SIDECAR_COMPONENT_NAME, "Succeeded"
    )

    pipeline_run_detail_dict = sanitize_environment_variables(
        pipeline_run_detail_dict,
    )

    experiment._log_asset_data(
        json.dumps(pipeline_run_detail_dict, default=str),
        overwrite=True,
        file_name="kubeflow-pipeline",
        asset_type="kubeflow-pipeline",
    )

    LOGGER.info(
        "Pipeline has finished running - number tasks running = %d",
        nb_tasks_running,
    )


SIDECAR_COMPONENT_NAME = "comet-logger-component"
# SIDECAR COMPONENT NAME is based on the function name used to create the component, transformed by
# Kubeflow


def _comet_logger_component(
    timeout,  # type: int
    project_name=None,  # type: Optional[str]
    workspace=None,  # type: Optional[str]
):
    # type: (...) -> None
    """The actual top-level code Kubeflow component"""

    # This function code run is copied and ran by Kubeflow. We cannot access anything from outside
    # so we need to re-import everything

    from comet_ml import Experiment
    from comet_ml.integration.kubeflow import _comet_logger_implementation

    # Create an experiment with your api key
    experiment = Experiment(
        project_name=project_name,
        workspace=workspace,
        log_git_metadata=False,
        log_git_patch=False,
    )

    workflow_uid = (
        "{{workflow.uid}}"  # The workflow uid is replaced at run time by Kubeflow
    )

    _comet_logger_implementation(experiment, workflow_uid, timeout)

    return None


def comet_logger_component(
    api_key=None,
    project_name=None,
    workspace=None,
    packages_to_install=None,
    base_image=None,
):
    # type: (Optional[str], Optional[str], Optional[str], Optional[List[str]], Optional[List[str]]) -> Any
    """
    Inject the Comet Logger component which continuously track and report the current pipeline
    status to Comet.ml.

    Args:
        api_key: string, optional. Your Comet API Key, if not provided, the value set in the
            configuration system will be used.

        project_name: string, optional. The project name where all of the pipeline tasks are logged.
            If not provided, the value set in the configuration system will be used.

        workspace: string, optional. The workspace name where all of the pipeline tasks are logged.
            If not provided, the value set in the configuration system will be used.

    Example:

    ```python
    @dsl.pipeline(name='ML training pipeline')
    def ml_training_pipeline():
        import comet_ml.integration.kubeflow

        comet_ml.integration.kubeflow.comet_logger_component()
    ```
    """

    import kfp
    from kubernetes.client.models import V1EnvVar

    # Inject type hints as kfp use them
    _comet_logger_component.__annotations__ = {
        "timeout": int,
        "project_name": Union[str, type(None)],
        "workspace": Union[str, type(None)],
        "return": type(None),
    }

    if packages_to_install is None:
        packages_to_install = ["comet_ml", "kfp"]

    component = kfp.components.create_component_from_func(
        func=_comet_logger_component,
        packages_to_install=packages_to_install,
        base_image=base_image,
    )

    config = get_config()
    # TODO: Should we inject other config keys?
    final_project_name = config.get_string(project_name, "comet.project_name")
    final_workspace = config.get_string(workspace, "comet.workspace")

    kwargs = {"timeout": COMET_LOGGER_TIMEOUT}  # type: Dict[str, Any]

    if final_project_name is not None:
        kwargs["project_name"] = final_project_name
    if final_workspace is not None:
        kwargs["workspace"] = final_workspace

    task = component(**kwargs)

    # Inject api key through environement variable to not log it as a component input
    final_api_key = get_api_key(api_key, config)

    if final_api_key is not None:
        env_var_comet_api_key = V1EnvVar(name="COMET_API_KEY", value=final_api_key)
        task = task.add_env_variable(env_var_comet_api_key)

    return task
