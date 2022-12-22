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
#  This file can not be copied and/or distributed without
#  the express permission of Comet ML Inc.
# *******************************************************

import functools
import hashlib
import inspect
import json
import logging

import wrapt

from ..._online import ExistingExperiment, Experiment
from .. import KEY_CREATED_FROM, KEY_PIPELINE_TYPE

LOGGER = logging.getLogger(__name__)

KEY_METAFLOW_STATUS = "metaflow_status"
KEY_METAFLOW_NAME = "metaflow_flow_name"
KEY_METAFLOW_RUN_ID = "metaflow_run_id"
KEY_METAFLOW_GRAPH_FILE = "metaflow_graph_file"
KEY_METAFLOW_STEP_NAME = "metaflow_step_name"
KEY_METAFLOW_RUN_EXPERIMENT = "metaflow_run_experiment"

KEY_COMET_RUN_ID = "comet_run_id"
KEY_COMET_STEP_ID = "comet_step_id"
KEY_COMET_TASK_ID = "comet_task_id"

VALUE_METAFLOW = "metaflow"

STATUS_RUNNING = "Running"
STATUS_COMPLETED = "Completed"
STATUS_FAILED = "Failed"

STEP_NAME_END = "end"
STEP_NAME_START = "start"


def _get_params_from_flow(flow, parameter_names):
    return {p: getattr(flow, p) for p in parameter_names}


class CometFlowSpecProxy(wrapt.ObjectProxy):
    """Proxy the metaflow FlowSpec object in order to expose the `comet_experiment` and
    `run_comet_experiment_key` without having Metaflow trying to pickle it at the end of each step.
    """

    def __init__(self, wrapped, experiment_key=None):
        super(CometFlowSpecProxy, self).__init__(wrapped)
        self._self_comet_experiment = None
        if experiment_key is None:
            self._self_run_exp_key = get_run_experiment_key()
        else:
            self._self_run_exp_key = experiment_key

    @property
    def comet_experiment(self):
        return self._self_comet_experiment

    @property
    def run_comet_experiment_key(self):
        return self._self_run_exp_key


def _get_step(slf, func):
    if func.is_step:
        step = getattr(slf, slf._current_step)
    else:
        step = None

    return step


def _init_task_exp(self, func, proxy, workspace=None, project_name=None):
    from metaflow import Task, current

    task_meta = Task(current.pathspec)
    comet_experiment = Experiment(workspace=workspace, project_name=project_name)
    _init_task_experiment(
        self=self,
        func=func,
        proxy=proxy,
        metaflow_current=current,
        metaflow_task=task_meta,
        comet_experiment=comet_experiment,
    )


def _init_task_experiment(
    self, func, proxy, metaflow_current, metaflow_task, comet_experiment
):
    step = _get_step(self, func)

    if step:
        env = {
            "current_task_id": metaflow_current.task_id,
            "current_step_name": metaflow_current.step_name,
            "current_flow_name": metaflow_current.flow_name,
            "current_run_id": metaflow_current.run_id,
        }
        task_exp_name = "{current_task_id}/{current_step_name} - {current_flow_name} - {current_run_id}".format(
            **env
        )
        # We should never be accessing a Task-level experiment for a second time--we always create a new experiment
        comet_experiment.set_name(task_exp_name)
        task_others = {
            KEY_COMET_RUN_ID: metaflow_current.run_id,
            KEY_COMET_STEP_ID: "{current_run_id}/{current_step_name}".format(**env),
            KEY_COMET_TASK_ID: "{current_run_id}/{current_step_name}/{current_task_id}".format(
                **env
            ),
            KEY_METAFLOW_NAME: metaflow_current.flow_name,
            KEY_METAFLOW_RUN_ID: metaflow_current.run_id,
            KEY_METAFLOW_STEP_NAME: metaflow_current.step_name,
            KEY_METAFLOW_STATUS: STATUS_RUNNING,
            KEY_METAFLOW_RUN_EXPERIMENT: proxy.run_comet_experiment_key,
            KEY_PIPELINE_TYPE: VALUE_METAFLOW,
            KEY_CREATED_FROM: VALUE_METAFLOW,
        }
        task_tags = ["task", metaflow_current.step_name]
        task_tags.extend(metaflow_task.tags)
        comet_experiment.log_others(task_others)
        params_dict = _get_params_from_flow(proxy, metaflow_current.parameter_names)
        if len(params_dict) > 0:
            comet_experiment.log_parameters(params_dict)
        comet_experiment.add_tags(task_tags)
        proxy._self_comet_experiment = comet_experiment


def get_run_experiment_key():
    from metaflow import current

    return hashlib.sha1(current.run_id.encode("utf-8")).hexdigest()


def _init_run_exp(func, proxy, workspace=None, project_name=None):
    """This method is called once per step"""
    from metaflow import Run, current

    # Create the run experiment in the start step
    if func.name == STEP_NAME_START:
        run_experiment = Experiment(
            experiment_key=proxy.run_comet_experiment_key,
            workspace=workspace,
            project_name=project_name,
            display_summary_level=0,
        )
        run_meta = Run("{}/{}".format(current.flow_name, current.run_id))
        _init_run_experiment(
            proxy=proxy,
            metaflow_run=run_meta,
            metaflow_current=current,
            run_experiment=run_experiment,
        )


def _init_run_experiment(proxy, metaflow_current, metaflow_run, run_experiment):
    env = {
        "current_flow_name": metaflow_current.flow_name,
        "current_run_id": metaflow_current.run_id,
    }
    run_experiment_name = "flow - {current_flow_name} - {current_run_id}".format(**env)

    run_experiment.set_name(run_experiment_name)
    run_graph_filename = "{current_flow_name}-{current_run_id}-graph.json".format(**env)
    run_others = {
        KEY_METAFLOW_NAME: metaflow_current.flow_name,
        KEY_METAFLOW_RUN_ID: metaflow_current.run_id,
        KEY_COMET_RUN_ID: metaflow_current.run_id,
        KEY_METAFLOW_STATUS: STATUS_RUNNING,
        KEY_METAFLOW_GRAPH_FILE: run_graph_filename,
        KEY_PIPELINE_TYPE: VALUE_METAFLOW,
        KEY_CREATED_FROM: VALUE_METAFLOW,
    }
    run_tags = ["run"]
    run_tags.extend(metaflow_run.tags)
    run_graph = json.dumps(metaflow_current.graph)

    run_experiment.log_asset_data(
        data=run_graph, name=run_graph_filename, overwrite=False
    )

    run_experiment.log_others(run_others)
    run_experiment.add_tags(run_tags)
    params_dict = _get_params_from_flow(proxy, metaflow_current.parameter_names)
    if len(params_dict) > 0:
        run_experiment.log_parameters(params_dict)
    run_experiment.end()


def _finish_experiments(proxy, workspace=None, project_name=None, exception=None):
    from metaflow import current

    if exception is None:
        proxy._self_comet_experiment.log_other(KEY_METAFLOW_STATUS, STATUS_COMPLETED)
    else:
        proxy._self_comet_experiment.log_other(KEY_METAFLOW_STATUS, STATUS_FAILED)

    proxy._self_comet_experiment.end()

    # Handle Run-level experiment at the end of the Flow
    if current.step_name == STEP_NAME_END or exception is not None:
        run_experiment = ExistingExperiment(
            experiment_key=proxy.run_comet_experiment_key,
            workspace=workspace,
            project_name=project_name,
            # Disable summary for the run experiment or each step will show the run experiment summary
            display_summary_level=0,
        )
        if exception is None:
            run_experiment.log_other(KEY_METAFLOW_STATUS, STATUS_COMPLETED)
        else:
            run_experiment.log_other(KEY_METAFLOW_STATUS, STATUS_FAILED)

        run_experiment.end()


def comet_flow(func=None, workspace=None, project_name=None):
    @functools.wraps(func)
    def decorator(func):
        # If you decorate a class, apply the decoration to all methods in that class
        if inspect.isclass(func):
            cls = func
            for attr in cls.__dict__:
                if callable(getattr(cls, attr)):
                    if not hasattr(attr, "_base_func"):
                        setattr(cls, attr, decorator(getattr(cls, attr)))
            return cls

        # prefer the earliest decoration (i.e. method decoration overrides class decoration)
        if hasattr(func, "_base_func"):
            return func

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            proxy = CometFlowSpecProxy(self)
            try:
                _init_run_exp(func, proxy, workspace, project_name)
                _init_task_exp(self, func, proxy, workspace, project_name)
            except Exception:
                LOGGER.debug("Error setting up run and task experiments", exc_info=True)

            step_exception = None
            try:
                func(proxy, *args, **kwargs)
            except Exception as ex:
                step_exception = ex

            try:
                _finish_experiments(
                    proxy, workspace, project_name, exception=step_exception
                )
            except Exception:
                LOGGER.debug("Error cleaning up the experiments", exc_info=True)

            if step_exception is not None:
                raise step_exception

        wrapper._base_func = func
        return wrapper

    if func is None:
        return decorator
    else:
        return decorator(func)
