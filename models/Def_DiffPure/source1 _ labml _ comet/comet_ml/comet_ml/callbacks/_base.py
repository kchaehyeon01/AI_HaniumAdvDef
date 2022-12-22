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

import six

from .._typing import (
    Any,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Set,
    TensorflowInput,
    Tuple,
)
from ..data_structure import Histogram
from ..experiment import BaseExperiment
from ..logging_messages import TF_KERAS_CALLBACK_WARNING_CLOSED_EXPERIMENT
from ..utils import get_time_monotonic, tensor_length

LOGGER = logging.getLogger(__name__)

XGBOOST_PARAMS_EXCLUDE = {"model", "evaluation_result_list", "iteration", "handle"}
XGBOOST_BOOSTER_ATTRS_EXCLUDE = {"handle"}


def _filter_xgboost_attributes(obj, exclude_set):
    # type: (Any, Set[int]) -> Generator[Tuple[str, Any], None, None]
    for attribute_name in dir(obj):
        if attribute_name.startswith("_"):
            continue

        if attribute_name in exclude_set:
            continue

        attribute_value = getattr(obj, attribute_name)

        if callable(attribute_value):
            continue

        if attribute_value is None:
            continue

        yield (attribute_name, attribute_value)


def _get_xgboost_env_params(env):
    # type: (Any) -> Generator[Tuple[str, Any], None, None]
    for attribute in _filter_xgboost_attributes(env, XGBOOST_PARAMS_EXCLUDE):
        yield attribute


def _get_xgboost_env_model_params(booster):
    # type: (Any) -> Generator[Tuple[str, Any], None, None]
    for attribute in _filter_xgboost_attributes(booster, XGBOOST_BOOSTER_ATTRS_EXCLUDE):
        yield attribute


def _get_xgboost_booster_graph(booster):
    # type: (Any) -> str
    from xgboost import to_graphviz

    graphviz_source = to_graphviz(booster).source  # type: str
    return graphviz_source


# TODO:
# Find a better name than obj for the env/model parameter.


def _log_xgboost_step(experiment, xgboost_step):
    # type: (BaseExperiment, int) -> None
    """
    Logs the step of the training to the experiment
    """
    try:
        experiment.set_step(xgboost_step)
    except Exception:
        LOGGER.debug("Failed to log the XGBoost step", exc_info=True)


def _log_xgboost_parameters(experiment, obj):
    # type: (BaseExperiment, Any) -> None
    """
    Logs the parameters of the training to the experiment
    """
    try:
        if experiment.auto_param_logging:
            if not experiment._storage["xgboost"]["env_parameter_set"]:
                for attribute_name, attribute_value in _get_xgboost_env_params(obj):
                    experiment._log_parameter(
                        attribute_name, attribute_value, framework="xgboost"
                    )

                # Set only once the parameters
                experiment._storage["xgboost"]["env_parameter_set"] = True
    except Exception:
        LOGGER.debug("Failed to log the XGBoost params", exc_info=True)


def _log_xgboost_model_attributes(experiment, booster):
    # type: (BaseExperiment, Any) -> None
    """
    Logs the attributes of the model to the experiment
    """
    try:
        if experiment.auto_param_logging:
            if not experiment._storage["xgboost"]["env_model_parameter_set"]:

                for attribute_name, attribute_value in _get_xgboost_env_model_params(
                    booster
                ):
                    experiment._log_parameter(
                        attribute_name, attribute_value, framework="xgboost"
                    )

                # Set only once the model parameters
                experiment._storage["xgboost"]["env_model_parameter_set"] = True
    except Exception:
        LOGGER.debug("Failed to log the XGBoost booster attributes", exc_info=True)


def _log_xgboost_model_metrics(experiment, evals_log):
    # type: (BaseExperiment, Dict) -> None
    """
    Logs the metrics to the experiment
    """
    try:
        if experiment.auto_metric_logging:
            xgboost_metrics = evals_log
            for context, metrics in xgboost_metrics.items():
                with experiment.context_manager(context):
                    experiment._log_metrics(metrics, framework="xgboost")
    except Exception:
        LOGGER.debug("Failed to log the XGBoost metrics", exc_info=True)


def _log_xgboost_model_graph(experiment, booster):
    # type: (BaseExperiment, Any) -> None
    """
    Logs the graph of the model to the experiment
    """
    try:
        if experiment.log_graph:
            if not experiment._storage["xgboost"]["model_graph_set"]:

                booster_graph = _get_xgboost_booster_graph(booster)

                experiment._set_model_graph(booster_graph, framework="xgboost")

                experiment._storage["xgboost"]["model_graph_set"] = True
    # xgboost.to_graphviz can raises ImportError if optional dependencies are not installed
    except ImportError as exc:
        experiment._log_once_at_level(logging.WARNING, str(exc), exc_info=True)
    except Exception:
        LOGGER.debug("Failed to log the XGBoost metrics", exc_info=True)


def get_standardized_layer_set(layer_list_raw, layer_names):
    # type: (List[str], List[str]) -> Set[str]
    """
    Given a raw list of possible layer names or indices,
    return a unique set of valid layer names.
    """
    results = set([])
    for item in layer_list_raw:
        layer_name = None
        try:
            layer_name = layer_names[int(item)]
        except Exception:
            if item in layer_names:
                layer_name = item
            else:
                LOGGER.warning("invalid layer %r; ignoring", item)

        if layer_name is None:
            continue

        if layer_name not in results:
            results.add(layer_name)
        else:
            LOGGER.warning("duplicate use of layer %r; ignoring", item)

    return results


def get_layer_num(layer_names, layer_name):
    # type: (List[str], str) -> int
    """
    Get the layer_num of a layer_name (may have things
    appended after a slash).
    """
    if "/" in layer_name:
        layer_name = layer_name.split("/", 1)[0]

    if layer_name in layer_names:
        return layer_names.index(layer_name)
    else:
        return -1


def format_histogram_prefix(
    prefix_format, num_layers, model_name, layer_names, layer_name
):
    # type: (str, int, str, List[str], str) -> str
    """
    Allow user to format a histogram prefix.
    """
    max_digits = len(str(num_layers))
    layer_num = get_layer_num(layer_names, layer_name) + 1
    try:
        prefix = prefix_format.format(
            model_name=model_name,
            layer_num=layer_num,
            layer_name=layer_name,
            max_digits=max_digits,
        )
    except Exception:
        LOGGER.warning("invalid prefix_format %r; ignoring", prefix_format)
        prefix = ""
    return prefix


def enumerate_tensors(banks_length, tensors, batch_size):
    # type: (int, Any, int) -> Generator[Any, None, None]
    """
    Break up inputs and targets into batch sizes.

    This can be complicated because the format of inputs
    and targets can vary based on the number of banks
    in the input layers, and number of banks in the
    output layers.

    tensors can be:
        * a tuple of lists
        * a tuple of dicts
    """
    if tensor_length(tensors) == 0:
        return

    if banks_length > 1:  # multiple banks
        length = tensor_length(tensors[0])
        multi = True
    else:
        length = tensor_length(tensors)
        multi = False

    current = 0
    while current < length:
        if multi:
            batch = [bank[current : current + batch_size] for bank in tensors]
        else:
            batch = tensors[current : current + batch_size]

        yield batch

        current += batch_size


def enumerate_tensor_list(banks_length, tensors, indices):
    # type: (int, Any, List[int]) -> Generator[Tuple[int, Any], None, None]
    """
    Break up inputs and targets by index.

    This can be complicated because the format of inputs
    and targets can vary based on the number of banks
    in the input layers, and number of banks in the
    output layers.

    tensors can be:
        * a tuple of lists
        * a tuple of dicts
    """
    if tensor_length(tensors) == 0:
        return

    if banks_length > 1:  # multiple banks
        length = tensor_length(tensors[0])
        multi = True
    else:
        length = tensor_length(tensors)
        multi = False

    for i, index in enumerate(indices):
        if index < length:
            if multi:
                batch = [bank[index : index + 1] for bank in tensors]
            else:
                batch = tensors[index : index + 1]
        else:
            batch = None

        yield (i, batch)


def get_trainable_variables(model):
    # type: (Any) -> List[Any]
    if hasattr(model, "trainable_variables"):
        return model.trainable_variables
    elif hasattr(model, "trainable_weights"):
        return model.trainable_weights
    else:
        return []


def get_tensorflow_gradient_histograms(
    model, inputs, targets, batch_size, index_list, layer_set
):
    # type: (Any, Any, Any, int, List[int], Set[str]) -> Optional[Iterator[Tuple[str, TensorflowInput, Histogram]]]
    # Logging gradients does not work with tensorflow 1.*
    try:
        from ..tf_utils import get_gradients
    except ImportError:
        return None

    histograms = None

    if layer_set != set([]):
        weights = [
            weight
            for weight in get_trainable_variables(model)
            if weight.name.split("/", 1)[0] in layer_set
        ]
    else:
        weights = get_trainable_variables(model)

    if len(weights) == 0:
        return None

    if index_list != []:  # single patterns
        input_gen = (
            v for (i, v) in enumerate_tensor_list(len(model.inputs), inputs, index_list)
        )
        target_gen = (
            v
            for (i, v) in enumerate_tensor_list(len(model.outputs), targets, index_list)
        )

        all_weight_names = [weight.name for weight in weights]
        # For each index:
        index = 0
        for ins, targs in six.moves.zip(input_gen, target_gen):
            gradients = get_gradients(model, ins, targs, weights)

            if histograms is None:
                histograms = []
                names = []
                indices = []  # type: List[TensorflowInput]

            for i in range(len(gradients)):
                histogram = Histogram()
                histogram.add(gradients[i])
                histograms.append(histogram)
                names.append(all_weight_names[i])
                indices.append(index_list[index])

            index += 1
    else:
        input_gen = enumerate_tensors(len(model.inputs), inputs, batch_size)
        target_gen = enumerate_tensors(len(model.outputs), targets, batch_size)

        names = [weight.name for weight in weights]
        indices = ["all" for weight in weights]  # type: List[TensorflowInput]

        for batch_input, batch_target in six.moves.zip(input_gen, target_gen):
            gradients = get_gradients(model, batch_input, batch_target, weights)

            if histograms is None:
                histograms = [Histogram() for i in range(len(gradients))]

            for i in range(len(gradients)):
                histograms[i].add(gradients[i])

    # check to see if all are the same length
    return six.moves.zip(names, indices, histograms)


def get_tensorflow_activation_histogram_indices(
    model, output_tensor, inputs, index_list
):
    # type: (Any, Any, Any, List[int]) -> Optional[List[Histogram]]
    # Logging activations does not work with tensorflow 1.11
    try:
        import tensorflow as tf
    except ImportError:
        return None

    histograms = [Histogram() for i in range(len(index_list))]

    try:
        function = tf.keras.backend.function(model.inputs, output_tensor)

        for i, batch_input in enumerate_tensor_list(
            len(model.inputs), inputs, index_list
        ):
            # Batch input is either a one-item input (tensor/ndarray) or a list of one-item
            # input (tensor/ndarray)
            if batch_input is None:
                LOGGER.warning(
                    "index_list[%s] is %r and beyond length of inputs/targets",
                    i,
                    index_list[i],
                )
            else:
                activations = function(batch_input)
                histograms[i].add(activations)
        return histograms
    except tf.errors.InvalidArgumentError:
        LOGGER.debug("Error retrieving activation histograms", exc_info=True)
        return None


def get_tensorflow_activation_histogram_all(model, output_tensor, inputs, batch_size):
    # type: (Any, Any, Any, int) -> Optional[List[Histogram]]
    # Logging activations does not work with tensorflow 1.11
    try:
        import tensorflow as tf
    except ImportError:
        return None

    histogram = Histogram()

    try:
        function = tf.keras.backend.function(model.inputs, output_tensor)

        for batch_input in enumerate_tensors(len(model.inputs), inputs, batch_size):
            # Batch input is either a one-item input (tensor/ndarray) or a list of one-item
            # input (tensor/ndarray)
            activations = function(batch_input)
            histogram.add(activations)
        return [histogram]
    except tf.errors.InvalidArgumentError:
        LOGGER.debug("Error retrieving activation histograms", exc_info=True)
        return None


def build_base_callback(base):
    class CometBaseKerasCallback(base):  # type: ignore
        """
        Base Keras callback.
        """

        def __init__(self):
            super(CometBaseKerasCallback, self).__init__()

        def on_epoch_begin(self, epoch, logs=None):
            pass

        def on_epoch_end(self, epoch, logs=None):
            pass

        def on_batch_begin(self, batch, logs=None):
            pass

        def on_batch_end(self, batch, logs=None):
            pass

        def on_train_begin(self, logs=None):
            pass

        def on_train_end(self, logs=None):
            pass

        def on_train_batch_begin(self, batch, logs=None):
            pass

        def on_train_batch_end(self, batch, logs=None):
            pass

        def on_test_batch_begin(self, batch, logs=None):
            pass

        def on_test_batch_end(self, batch, logs=None):
            pass

        def on_test_begin(self, logs=None):
            pass

        def on_test_end(self, logs=None):
            pass

        def on_predict_begin(self, logs=None):
            pass

        def on_predict_end(self, logs=None):
            pass

        def on_predict_batch_begin(self, batch, logs=None):
            pass

        def on_predict_batch_end(self, batch, logs=None):
            pass

    return CometBaseKerasCallback


def build_empty_keras_callback(base):
    class CometEmptyKerasCallback(base):  # type: ignore
        """
        Empty Keras callback.
        """

    return CometEmptyKerasCallback


def build_keras_callback(base):
    class CometKerasCallback(base):  # type: ignore
        """Keras callback to report params, metrics to Comet.ml Experiment()"""

        def __init__(
            self,
            experiment,  # type: BaseExperiment
            log_params=None,  # type: Optional[bool]
            log_metrics=None,  # type: Optional[bool]
            log_graph=None,  # type: Optional[bool]
            log_histograms=None,  # type: Optional[bool]
            inputs=None,  # type: Optional[Any]
            targets=None,  # type: Optional[Any]
        ):
            # type: (...) -> None
            """
            Create a new experiment and submit source code.
            :param api_key: User's API key. Required.
            """
            super(CometKerasCallback, self).__init__()
            self.inputs = inputs
            self.targets = targets

            # If any log_* parameters are given, give warning and ignore:
            if log_params is not None:
                experiment._log_once_at_level(
                    logging.INFO,
                    "Passing log_params to CometKerasCallback is deprecated; use experiment.auto_param_logging",
                )
            if log_metrics is not None:
                experiment._log_once_at_level(
                    logging.INFO,
                    "Passing log_metrics to CometKerasCallback is deprecated; use experiment.auto_metric_logging",
                )
            if log_graph is not None:
                experiment._log_once_at_level(
                    logging.INFO,
                    "Passing log_graph to CometKerasCallback is deprecated; use experiment.log_graph",
                )
            if log_histograms is not None:
                experiment._log_once_at_level(
                    logging.INFO,
                    "Passing log_histograms to CometKerasCallback is deprecated; use experiment.auto_histogram_*_logging",
                )

            # Inits the experiment with reference to the name of this class. Required for loading the correct
            # script file
            self.experiment = experiment
            self.epoch_start_time = None  # type: Optional[float]
            self.our_step = 0
            self.our_epoch = 0

            self.activation_ignore_list = ["flatten", "dropout", "activation"]

        def on_epoch_begin(self, epoch, logs=None):
            if check_experiment_closed_and_warn(
                self.experiment, callback_method_name="on_epoch_begin"
            ):
                return

            try:
                # This function should only be called during train mode.
                LOGGER.debug("On epoch begin %s %s", epoch, logs)
                self.experiment.set_epoch(self.our_epoch)
                self.epoch_start_time = get_time_monotonic()

                if self.our_epoch == 0:
                    self._log_histograms()
            except Exception:
                LOGGER.warning(
                    "An unknown exception happened in Keras callback on_epoch_begin; ignoring",
                    exc_info=True,
                )

        def on_epoch_end(self, epoch, logs=None):
            if check_experiment_closed_and_warn(
                self.experiment, callback_method_name="on_epoch_end"
            ):
                return

            try:
                # This function should only be called during train mode.
                LOGGER.debug("On epoch end %s %s", epoch, logs)
                if self.experiment.auto_metric_logging:
                    if self.epoch_start_time is not None:
                        self.experiment._log_metric(
                            "epoch_duration",
                            get_time_monotonic() - self.epoch_start_time,
                            step=self.our_step,
                            epoch=self.our_epoch,
                            framework="keras",
                        )
                        self.epoch_start_time = None
                    self.experiment.log_epoch_end(self.our_epoch, step=self.our_step)
                    if logs:
                        for name, val in logs.items():
                            self.experiment._log_metric(
                                name,
                                val,
                                step=self.our_step,
                                epoch=self.our_epoch,
                                framework="keras",
                            )
            except Exception:
                LOGGER.warning(
                    "An unknown exception happened in Keras callback on_epoch_end; ignoring",
                    exc_info=True,
                )

            self.our_epoch += 1

            try:
                if self.experiment._check_histogram_epoch_report_rate(self.our_epoch):
                    self._log_histograms()
            except Exception:
                LOGGER.warning(
                    "An unknown exception happened in Keras callback on_epoch_end; ignoring",
                    exc_info=True,
                )

        def _log_histograms(self):
            prefix_format = self.experiment.config.get_string(
                None, "comet.keras.histogram_name_prefix"
            )
            batch_size = self.experiment.config.get_int(
                None, "comet.keras.histogram_batch_size"
            )

            # Pre-compute some common variables
            num_layers = len(self.model.layers)
            model_name = self.model.name
            layer_names = [layer.name for layer in self.model.layers]

            self._log_weights_histograms(
                prefix_format, num_layers, model_name, layer_names, batch_size
            )

            self._log_gradients_histograms(
                prefix_format, num_layers, model_name, layer_names, batch_size
            )

            self._log_activations_histograms(
                prefix_format, num_layers, model_name, layer_names, batch_size
            )

        def _log_weights_histograms(
            self, prefix_format, num_layers, model_name, layer_names, batch_size
        ):
            # type: (str, int, str, List[str], int) -> None
            if self.experiment.auto_histogram_weight_logging is False:
                return None

            try:
                for layer in self.model.layers:
                    weights = layer.get_weights()
                    if len(weights) == len(layer.weights):
                        for i in range(len(layer.weights)):
                            prefix = format_histogram_prefix(
                                prefix_format,
                                num_layers,
                                model_name,
                                layer_names,
                                layer.weights[i].name,
                            )
                            self.experiment.log_histogram_3d(
                                weights[i],
                                name=layer.weights[i].name,
                                step=self.our_step,
                                epoch=self.our_epoch,
                                metadata={"prefix": prefix},
                            )
                    else:
                        LOGGER.warning(
                            "keras layer.weights and layer.get_weights() are different lengths; ignoring weight histogram"
                        )
            except Exception:
                LOGGER.debug("error attempting to log weights; ignoring", exc_info=True)

        def _log_gradients_histograms(
            self, prefix_format, num_layers, model_name, layer_names, batch_size
        ):
            # type: (str, int, str, List[str], int) -> None
            if self.experiment.auto_histogram_gradient_logging is False:
                return None
            else:
                if self.inputs is None or self.targets is None:
                    self.experiment._log_once_at_level(
                        logging.WARNING,
                        "auto_histogram_gradient_logging is True, but inputs and targets are not available; unable to log gradients",
                    )
                    return None

            try:
                gradient_index_list = self.experiment.config.get_int_list(
                    None, "comet.keras.histogram_gradient_index_list"
                )
            except Exception:
                LOGGER.warning(
                    "malformed `comet.keras.histogram_gradient_index_list`; should be a string of comma-separated integers; ignoring",
                    exc_info=True,
                )
                # If we don't have index, early-return as we won't generate any histogram
                return None

            try:
                gradient_layer_list_raw = self.experiment.config.get_string_list(
                    None, "comet.keras.histogram_gradient_layer_list"
                )
            except Exception:
                LOGGER.warning(
                    "malformed `comet.keras.histogram_gradient_layer_list`; should be a string of comma-separated integers and/or names; ignoring",
                    exc_info=True,
                )
                # If we don't have names, early-return as we won't generate any histogram
                return None

            gradient_layer_set = get_standardized_layer_set(
                gradient_layer_list_raw, layer_names
            )

            try:
                histograms = get_tensorflow_gradient_histograms(
                    self.model,
                    self.inputs,
                    self.targets,
                    batch_size,
                    gradient_index_list,
                    gradient_layer_set,
                )
                if histograms is not None:
                    for layer_name, index, histogram in histograms:
                        prefix = format_histogram_prefix(
                            prefix_format,
                            num_layers,
                            model_name,
                            layer_names,
                            layer_name,
                        )
                        self.experiment.log_histogram_3d(
                            histogram,
                            name="/".join([layer_name, ("gradients:%s" % index)]),
                            step=self.our_step,
                            epoch=self.our_epoch,
                            metadata={"prefix": prefix},
                        )
            except Exception:
                LOGGER.debug(
                    "error attempting to log gradients; ignoring", exc_info=True
                )

        def _log_activations_histograms(
            self, prefix_format, num_layers, model_name, layer_names, batch_size
        ):
            # type: (str, int, str, List[str], int) -> None
            if self.experiment.auto_histogram_activation_logging is False:
                return None
            else:
                if self.inputs is None:
                    self.experiment._log_once_at_level(
                        logging.WARNING,
                        "auto_histogram_activation_logging is True, but inputs are not available; unable to log activations",
                    )
                    return None

            try:
                activation_index_list = self.experiment.config.get_int_list(
                    None, "comet.keras.histogram_activation_index_list"
                )
            except Exception:
                LOGGER.warning(
                    "malformed `comet.keras.histogram_activation_index_list`; should be a string of comma-separated integers; ignoring",
                    exc_info=True,
                )
                # If we don't have index, early-return as we won't generate any histogram
                return None

            try:
                activation_layer_list_raw = self.experiment.config.get_string_list(
                    None, "comet.keras.histogram_activation_layer_list"
                )
            except Exception:
                LOGGER.warning(
                    "malformed `comet.keras.histogram_activation_layer_list`; should be a string of comma-separated integers and/or names; ignoring",
                    exc_info=True,
                )
                # If we don't have names, early-return as we won't generate any histogram
                return None

            activation_layer_set = get_standardized_layer_set(
                activation_layer_list_raw, layer_names
            )

            try:
                for layer in self.model.layers:
                    if activation_layer_set == set([]):
                        if any(
                            (ignore in layer.name)
                            for ignore in self.activation_ignore_list
                        ):
                            continue
                    elif layer.name not in activation_layer_set:
                        continue

                    LOGGER.debug("histogram activation processing %s...", layer.name)
                    if activation_index_list == []:  # all
                        histograms = get_tensorflow_activation_histogram_all(
                            self.model, layer.output, self.inputs, batch_size
                        )
                    else:
                        histograms = get_tensorflow_activation_histogram_indices(
                            self.model,
                            layer.output,
                            self.inputs,
                            activation_index_list,
                        )
                    if histograms is not None:
                        for i, histogram in enumerate(histograms):

                            if activation_index_list == []:
                                name = "all"  # type: TensorflowInput
                            else:
                                name = activation_index_list[i]  # type: TensorflowInput

                            prefix = format_histogram_prefix(
                                prefix_format,
                                num_layers,
                                model_name,
                                layer_names,
                                layer.name,
                            )
                            self.experiment.log_histogram_3d(
                                histogram,
                                name="/".join(
                                    [layer.name, ("activations:%s" % name)]
                                ),  # index of input tensor
                                step=self.our_step,
                                epoch=self.our_epoch,
                                metadata={"prefix": prefix},
                            )

            except Exception:
                LOGGER.debug(
                    "error attempting to log activations; ignoring", exc_info=True
                )

            return None

        def on_batch_begin(self, batch, logs=None):
            if check_experiment_closed_and_warn(self.experiment, show_warning=False):
                return

            try:
                # This function called directly when in train mode.
                LOGGER.debug("On batch begin %s %s", batch, logs)
            except Exception:
                LOGGER.warning(
                    "An unknown exception happened in Keras callback on_batch_begin; ignoring",
                    exc_info=True,
                )

        def on_batch_end(self, batch, logs=None):
            """
            Logs training metrics.
            """
            if check_experiment_closed_and_warn(self.experiment, show_warning=False):
                return

            try:
                # This function called directly when in train mode.
                LOGGER.debug("On batch end %s %s", batch, logs)

                self.our_step += 1

                # Use the batch from keras, as it starts over each epoch:
                if self.experiment._check_metric_step_report_rate(batch):
                    self._send_batch_messages(logs)

            except Exception:
                LOGGER.warning(
                    "An unknown exception happened in Keras callback on_batch_end; ignoring",
                    exc_info=True,
                )

        def on_train_batch_end(self, batch, logs=None):
            try:
                # No context added here, to match previous behavior:
                self.on_batch_end(batch, logs)
            except Exception:
                LOGGER.warning(
                    "An unknown exception happened in Keras callback on_train_batch_end; ignoring",
                    exc_info=True,
                )

        def on_test_batch_end(self, batch, logs=None):
            try:
                with self.experiment.validate():
                    self.on_batch_end(batch, logs)
            except Exception:
                LOGGER.warning(
                    "An unknown exception happened in Keras callback on_test_batch_end; ignoring",
                    exc_info=True,
                )

        def _send_batch_messages(self, logs):
            if logs and self.experiment.auto_metric_logging:
                for name, val in logs.items():
                    self.experiment._log_metric(
                        "batch_" + name, val, step=self.our_step, framework="keras"
                    )

        def on_train_begin(self, logs=None):
            """
            Sets model graph.
            """
            if check_experiment_closed_and_warn(
                self.experiment, callback_method_name="on_train_begin"
            ):
                return

            try:
                LOGGER.debug("On train begin %s", logs)

                if self.experiment.log_graph:
                    model_graph = get_keras_model(self.experiment, self.model)

                    if model_graph:
                        self.experiment._set_model_graph(model_graph, framework="keras")
                    else:
                        LOGGER.debug("Empty graph model, skipping")

                try:
                    trainable_params = self.model.count_params()
                    self.experiment._log_other(
                        "trainable_params", trainable_params, framework="keras"
                    )
                except Exception:
                    LOGGER.debug("Failed to count params in model", exc_info=True)

                if self.experiment.auto_param_logging:
                    if logs:
                        for k, v in logs.items():
                            self.experiment._log_parameter(k, v, framework="keras")

                    # Keras Callback doesn't set this parameter at creation by default
                    if hasattr(self, "params") and self.params:
                        for k, v in self.params.items():
                            if k != "metrics":
                                self.experiment._log_parameter(k, v, framework="keras")

                    try:
                        optimizer_name = self.model.optimizer.__class__.__name__
                        config = self.model.optimizer.get_config()
                        for key, value in config.items():
                            self.experiment._log_parameter(
                                optimizer_name + "_" + key, value, framework="keras"
                            )
                    except Exception:
                        LOGGER.debug(
                            "Failed to extract optimizer information", exc_info=True
                        )
            except Exception:
                LOGGER.warning(
                    "An unknown exception happened in Keras callback on_train_begin; ignoring",
                    exc_info=True,
                )

        def on_train_end(self, *args, **kwargs):
            if check_experiment_closed_and_warn(
                self.experiment, callback_method_name="on_train_end"
            ):
                return

            try:
                LOGGER.debug("On train end %r", locals())
            except Exception:
                LOGGER.warning(
                    "An unknown exception happened in Keras callback on_train_end; ignoring",
                    exc_info=True,
                )

    return CometKerasCallback


def get_keras_model(experiment, model):
    # type: (BaseExperiment, Any) -> Any

    # With multi-gpu models we save the original model in the experiment
    # storage
    storage_key = "gpu_model_%s" % id(model)
    json_model = experiment._storage["keras"]["json_model"].get(storage_key, None)

    if json_model is not None:
        return json_model
    else:
        return model


def check_experiment_closed_and_warn(
    experiment, show_warning=True, callback_method_name="undefined"
):
    # type: (BaseExperiment, Optional[bool], Optional[str]) -> bool
    """Checks if specified experiment is closed and display warning
    Args:
        experiment: BaseExperiment - the experiment to be checked
        show_warning: Optional. bool - the flag to indicate if warning should be displayed [default: True]
        callback_method_name: Optional. string - the name of callback method related to this check.
    """
    if not experiment.alive and show_warning:
        LOGGER.warning(
            TF_KERAS_CALLBACK_WARNING_CLOSED_EXPERIMENT, callback_method_name
        )

    return not experiment.alive
