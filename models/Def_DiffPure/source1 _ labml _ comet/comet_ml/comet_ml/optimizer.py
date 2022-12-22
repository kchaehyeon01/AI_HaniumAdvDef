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

import ast
import json
import logging
import os

from ._typing import Any, Dict, Optional
from .config import get_api_key, get_config
from .connection import get_optimizer_api
from .exceptions import InvalidOptimizerParameters, OptimizerException
from .experiment import BaseExperiment
from .logging_messages import OPTIMIZER_KWARGS_CONSTRUCTOR_DEPRECATED

LOGGER = logging.getLogger(__name__)


def convert_shorthand_parameter(parameter_name, parameter_data):
    if isinstance(parameter_data, dict):
        pass
    elif isinstance(parameter_data, list):
        plist = parameter_data
        if isinstance(plist[0], (float, int)):
            parameter_data = {"type": "discrete", "values": plist}
        elif isinstance(plist[0], str):
            parameter_data = {"type": "categorical", "values": plist}
        else:
            raise OptimizerException(
                "invalid type of item in list: should be float, int, or str"
            )
    else:  # single value
        if isinstance(parameter_data, (int, float)):
            parameter_data = {"type": "discrete", "values": [parameter_data]}
        elif isinstance(parameter_data, (str,)):
            parameter_data = {"type": "categorical", "values": [parameter_data]}
        else:
            raise OptimizerException(
                "unknown parameter entry: {'%s': %s}" % (parameter_name, parameter_data)
            )

    return parameter_data


class Optimizer(object):
    """
    The Optimizer class. Used to perform a search for minimum
    or maximum loss for a given set of parameters. Also used
    for a grid or random sweep of parameter space.

    Note that any keyword argument not in the following will
    be passed onto the Experiment constructor. For example,
    you can pass `project_name` and logging arguments by
    listing them here.

    Args:
        config: optional, if COMET_OPTIMIZER_ID is configured,
            otherwise is either a config dictionary, optimizer id,
            or a config filename.
        trials: int (optional, default 1) number of trials
            per parameter set to test.
        verbose: boolean (optional, default 1) verbosity level
            where 0 means no output, and 1 (or greater) means
            to show more detail.
        experiment_class: string or callable (optional, default
            None), class to use (for example, OfflineExperiment).

    Examples:

        ```python
        # Assume COMET_OPTIMIZER_ID is configured:
        >>> opt = Optimizer()

        # An optimizer config dictionary:
        >>> opt = Optimizer({"algorithm": "bayes", ...})

        # An optimizer id:
        >>> opt = Optimizer("73745463463")

        # A filename to a optimizer config file:
        >>> opt = Optimizer("/tmp/mlhacker/optimizer.config")
        ```

    To pass arguments to the `Experiment` constructor, pass them into the
    `opt.get_experiments()` call, like so:

    ```python
    >>> opt = Optimizer("/tmp/mlhacker/optimizer.config")
    >>> for experiment in opt.get_experiments(
    ...     project_name="my-project",
    ...     auto_metric_logging=False,
    ... ):
    ...     loss = fit(model)
    ...     experiment.log_metric("loss", loss")
    ...     experiment.end()
    ```
    """

    def __init__(
        self,
        config=None,
        trials=None,
        verbose=1,
        experiment_class="Experiment",
        api_key=None,
        **kwargs
    ):
        """
        The Optimizer constructor.

        Args:
            config: (optional, if COMET_OPTIMIZER_ID is configured). Can
                an optimizer config id, an optimizer config dictionary, or an
                optimizer config filename.
            trials: int (optional, default 1) number of trials per parameter value set
            verbose: int (optional, default 1) level of details to show; 0 means quiet
            experiment_class: string (optional). Supported values are "Experiment" (the
                default) to use online Experiments or "OfflineExperiment" to use offline
                Experiments. It can also be a callable (a function or a method) that returns an
                instance of Experiment, OfflineExperiment, ExistingExperiment or
                ExistingOfflineExperiment.

        See above for examples.
        """
        self.VALID_TYPES = ["float", "double", "integer", "discrete", "categorical"]
        self.VALID_SCALING_TYPES = [
            "linear",
            "uniform",
            "loguniform",
            "normal",
            "lognormal",
        ]
        self.experiment_class = experiment_class

        if kwargs:
            LOGGER.warning(OPTIMIZER_KWARGS_CONSTRUCTOR_DEPRECATED)

        self.experiment_kwargs = kwargs
        self.config = get_config()
        self.api_key = get_api_key(api_key, self.config)
        self._api = get_optimizer_api(
            self.api_key,
        )
        self.id = None
        if os.environ.get("COMET_OPTIMIZER_ID") is not None:
            self.id = os.environ.get("COMET_OPTIMIZER_ID")
            if verbose > 0:
                LOGGER.info(
                    "Using Optimizer id '%s' overridden from environment, ignoring local optimizer config",
                    self.id,
                )
        elif config is None:
            raise OptimizerException(
                "No config given and COMET_OPTIMIZER_ID is not configured"
            )
        elif isinstance(config, dict):
            try:
                self._set(**config)
            except TypeError:
                err_msg = "Invalid Optimizer configuration given, please check the configuration keys"
                LOGGER.debug(err_msg, exc_info=True)
                raise InvalidOptimizerParameters(err_msg)
            return
        elif not os.path.isfile(config):
            self.id = config
        elif os.path.isfile(config):
            self._load(config, trials=trials, verbose=verbose)
            return
        else:
            raise OptimizerException("unknown config: %s" % config)
        # Check the status for existing optimizer:
        status = self.status()
        if "code" in status:
            raise OptimizerException("invalid optimizer instance status: %s" % status)
        elif verbose > 0:
            LOGGER.info("Using optimizer config: %s", status)

    def get_experiments(self, **kwargs):
        """
        `Optimizer.get_experiments()` will iterate over all possible
        experiments for this sweep or search, `n` at a time. All
        experiments will have a unique set of parameter values
        (unless performing multiple trials per parameter set).

        Example:

            ```python
            >>> for experiment in optimizer.get_experiments():
            ...     loss = fit(x, y)
            ...     experiment.log_metric("loss", loss)
            ```
        """
        while True:
            experiment = self.next(**kwargs)
            if experiment is not None:
                yield experiment
            else:
                break

    def get_parameters(self):
        """
        `Optimizer.get_parameters()` will iterate over all possible parameters
        for this sweep or search. All parameters combinations will be emitted
        once (unless performing multiple trials per parameter set).

        Example:

            ```python
            >>> for parameters in optimizer.get_parameters():
            ...     experiment = comet_ml.Experiment()
            ...     loss = fit(x, y)
            ...     experiment.log_metric("loss", loss)
            ```
        """
        while True:
            data = self.next_data()
            if data is not None:
                yield data
            else:
                break

    def next(self, **kwargs):
        # type: (Dict[str, str]) -> Optional[BaseExperiment]
        """
        `Optimizer.next()` will return the next experiment for this
        sweep or search. All experiments will have a unique set of
        parameter values (unless performing multiple trials per
        parameter set).

        Normally, you would not call this directly, but use the
        generator `Optimizer.get_experiments()`

        Args:
            kwargs: (optional). Any keyword argument will be passed to the
                Experiment class for creation. The API key is passed directly.

        Example:

            ```python
            >>> experiment = optimizer.next()
            >>> if experiment is not None:
            ...     loss = fit(x, y)
            ...     experiment.log_metric("loss", loss)
            ```
        """
        data = self.next_data()
        if data:
            return self._create_experiment(
                data["parameters"],
                data["pid"],
                data["trial"],
                data["count"],
                experiment_kwargs=kwargs,
            )

        return None

    def next_data(self):
        # type: () -> Optional[Dict[str, Any]]
        """
        `Optimizer.next_data()` will return the next parameters in the Optimizer
        sweep.

        Normally, you would not call this directly, but use the generator
        `Optimizer.get_parameters()`

        Example:

            ```python
            >>> experiment = optimizer.next_data()
            ```
        """
        data = self._api.optimizer_next(self.id)
        if data is not None:
            # Include the optimizer ID so Experiment can recreate the Optimizer
            # object if needed
            data["id"] = self.id
            return data
        else:
            # Check the optimizer status
            instance = self._api.optimizer_status(self.id)

            if instance["status"] == "completed":
                LOGGER.info("Optimizer search %s has completed", self.id)
        return None

    def status(self):
        """
        Get the status from the optimizer server for this optimizer.

        Example:

            ```
            >>> opt.status()
            {'algorithm': 'grid',
             'comboCount': 32,
             'configSpaceSize': 10,
             'endTime': None,
             'id': 'c20b90ecad694e71bdb5702778eb8ac7',
             'lastUpdateTime': None,
             'maxCombo': 0,
             'name': 'c20b90ecad694e71bdb5702778eb8ac7',
             'parameters': {'x': {'max': 20,
                                  'min': 0,
                                  'scalingType': 'uniform',
                                  'type': 'integer'}},
             'retryCount': 0,
             'spec': {'gridSize': 10,
              'maxCombo': 0,
              'metric': 'loss',
              'minSampleSize': 100,
              'randomize': False,
              'retryLimit': 20,
              'seed': 2997936454},
             'startTime': 1558793292216,
             'state': {
              'sequence_i': 0,
              'sequence_retry': 11,
              'sequence_trial': 0,
              'total': 32,
             },
             'status': 'running',
             'trials': 1,
             'version': '1.0.0'}
            ```
        """
        results = self._api.optimizer_status(id=self.id)
        return results

    def get_id(self):
        """
        Get the id of this optimizer, with Comet config variable.

        Example:

            ```
            >>> opt.get_id()
            COMET_OPTIMIZER_ID=87463746374647364
            ```
        """
        return "COMET_OPTIMIZER_ID=%s" % self.id

    def end(self, experiment):
        """
        `Optimizer.end()` is called at end of experiment. Usually,
        you would not call this manually, as it is called directly
        when the experiment ends.
        """
        pid = experiment.optimizer["pid"]
        trial = experiment.optimizer["trial"]
        count = experiment.optimizer["count"]
        status = self.status()
        metric = status["spec"]["metric"]
        objective = "minimum"
        version = status["version"]
        score = None
        name = status["name"]
        ## Find correct, min/max metric
        spec_metric = experiment._summary.get("metrics", metric)
        if spec_metric:
            if "objective" not in status["spec"]:
                # Algorithm doesn't actually need score
                # but we send min anyway:
                score = spec_metric["min"]
            elif status["spec"]["objective"] == "minimize":
                score = spec_metric["min"]
            else:
                score = spec_metric["max"]
                # Set again, with whatever is in spec:
                objective = status["spec"]["objective"]
            result = "completed"
        else:
            result = "aborted"
            LOGGER.info(
                "Optimizer metrics is '%s' but no logged values found. Experiment ignored in sweep.",
                metric,
            )
        self._api.optimizer_update(self.id, pid, trial, result, score)
        # Log optimizer results with the experiment:
        experiment.log_other("optimizer_metric", metric)
        experiment.log_other("optimizer_metric_value", score)
        experiment.log_other("optimizer_version", version)
        experiment.log_other("optimizer_process", os.getpid())
        experiment.log_other("optimizer_count", count)
        experiment.log_other("optimizer_objective", objective)
        experiment.log_other("optimizer_name", name)
        experiment.log_other(
            "optimizer_parameters",
            json.dumps(
                {key: experiment.params.get(key, None) for key in status["parameters"]}
            ),
        )

        return False  # don't force experiment to wait until uploading is done

    def _load(self, optimizer_file, trials=None, verbose=1):
        """
        Load optimizer configuration from JSON.
        """
        optimizer_config = self._load_json(optimizer_file)
        ## Allow overrides:
        if trials is not None:
            optimizer_config["trials"] = trials
        optimizer_config["verbose"] = verbose
        self._set(**optimizer_config)

    def _load_json(self, optimizer_file):
        """
        Load the parameters from a JSON file.
        """
        try:
            with open(optimizer_file) as fp:
                optimizer_config = ast.literal_eval(fp.read())
        except Exception:
            LOGGER.debug("Error reading config file", exc_info=True)
            raise OptimizerException(
                "Invalid optimizer config file: '%s'" % optimizer_file
            )
        return optimizer_config

    def _validate_parameters(self, parameters):
        """
        Do the minimum amount of validation for the rest of the code to works correctly
        """
        for param in parameters:
            if "type" not in parameters[param]:
                raise InvalidOptimizerParameters(
                    "Parameter '%s' has missing entry 'type'" % param
                )

    def _fill_defaults(self, parameters):
        """
        Fill in defaults for parameters.
        """
        for param in parameters:
            if parameters[param]["type"] in ["float", "double", "integer"]:
                if "scalingType" not in parameters[param]:
                    parameters[param]["scalingType"] = "uniform"

    def _set(
        self,
        algorithm,
        parameters,
        name=None,
        trials=1,
        spec=None,
        verbose=1,
    ):
        """
        Create or use existing Optimizer.
        """
        if not isinstance(trials, int) or trials <= 0:
            raise InvalidOptimizerParameters(
                "Optimizer trials should be an integer >= 1"
            )

        if len(parameters.keys()) == 0:
            raise InvalidOptimizerParameters("Optimizer needs at least one parameter")

        ## Turn from JSON list shorthand to proper dict format
        converted_parameters = {}

        for key, parameter_data in parameters.items():
            converted_parameters[key] = convert_shorthand_parameter(key, parameter_data)

        self._validate_parameters(converted_parameters)
        self._fill_defaults(converted_parameters)
        # Complete any missing defaults in SPEC:

        if spec is None:
            spec = {}

        spec = self._complete_spec(spec, algorithm)

        if self.id is None:
            ## /create
            payload = {
                "algorithm": algorithm,
                "name": name,
                "parameters": converted_parameters,
                "trials": trials,
                "spec": spec,
            }
            results = self._api.post_request("create", json=payload)
            if results["code"] == 200:
                self.id = results["id"]
                if verbose > 0:
                    LOGGER.info(self.get_id())
            else:
                raise OptimizerException("Optimizer server: " + results["message"])
            ## Is it still None?
            if self.id is None:
                raise OptimizerException("Invalid optimizer instance")

        ## make sure everything matches
        status = self.status()
        request = {
            "algorithm": algorithm,
            "parameters": converted_parameters,
            "trials": trials,
            "spec": spec,
        }
        for key in request.keys():
            if isinstance(status[key], dict):
                for item in status[key]:
                    # Ignore some keys that can be different:
                    if item not in ["seed"]:
                        if self._diff(status[key][item], request[key][item]):
                            raise OptimizerException(
                                "requested optimizer does not match parameter %s:\nstatus : %s\nrequest: %s"
                                % (item, status[key][item], request[key][item])
                            )
            elif status[key] != request[key]:
                raise OptimizerException(
                    "requested optimizer does not match attempted run %s:\nstatus : %s\nrequest: %s"
                    % (key, status[key], request[key])
                )
        if verbose > 0:
            LOGGER.info("Using optimizer config: %s", status)

    def _complete_spec(self, spec, algorithm):
        """
        Get a complete spec, filling in missing values
        with defaults.
        """
        template = self._api.optimizer_spec(algorithm)
        spec_template = self._make_spec_from_template(template)
        spec_template.update(spec)
        return spec_template

    def _make_spec_from_template(self, template):
        """
        Make a spec from the instance's spec template.
        """
        spec = {}
        for param in template:
            spec[param["name"]] = param["default"]
        return spec

    def _diff(self, v1, v2):
        """
        See if two parameter sets are different. Return True if they are
        different and False if they are the same.
        """
        if isinstance(v1, dict) and isinstance(v2, dict):
            if "type" in v1 and "type" in v2:
                if v1["type"] == "discrete" and v2["type"] == "discrete":
                    return set(v1["values"]) != set(v2["values"])
        return v1 != v2

    def _create_experiment(self, parameters, pid, trial, count, experiment_kwargs):
        # type: (Any, Any, Any, Any, Dict[str, Any]) -> BaseExperiment
        """
        Instantiates an Experiment, OfflineExperiment, or
        callable.
        """
        from comet_ml import Experiment, OfflineExperiment

        LOGGER.debug(
            "Creating a %r with %r parameters", self.experiment_class, parameters
        )

        if not experiment_kwargs:
            # Fallback on deprecated experiment kwargs given at Optimizer creation
            experiment_kwargs = self.experiment_kwargs

        # Inject the API Key if not set
        if "api_key" not in experiment_kwargs and self.api_key is not None:
            experiment_kwargs["api_key"] = self.api_key

        if self.experiment_class == "Experiment":
            exp = Experiment(**experiment_kwargs)  # type: BaseExperiment
        elif self.experiment_class == "OfflineExperiment":
            exp = OfflineExperiment(**experiment_kwargs)  # type: BaseExperiment
        elif callable(self.experiment_class):
            exp = self.experiment_class(**experiment_kwargs)
        else:
            raise OptimizerException(
                "Invalid experiment_class: %s" % self.experiment_class
            )

        exp._set_optimizer(self, pid, trial, count)

        exp.log_parameters(parameters)
        # Log optimizer static information:
        exp.log_other("optimizer_id", self.id)
        exp.log_other("optimizer_pid", pid)
        exp.log_other("optimizer_trial", trial)
        return exp
