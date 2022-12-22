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

from ..monkey_patching import check_module
from .logger_utils import _check_callbacks_list, get_argument_bindings

LOGGER = logging.getLogger(__name__)


optimizers_hyper_params = {
    "AdagradDAOptimizer": [
        "_learning_rate",
        "_initial_gradient_squared_accumulator_value",
        "_l1_regularization_strength",
        "_l2_regularization_strength",
    ],
    "ProximalAdagradOptimizer": [
        "_learning_rate",
        "_initial_accumulator_value",
        "_l1_regularization_strength",
        "_l2_regularization_strength",
    ],
    "ProximalGradientDescentOptimizer": [
        "_learning_rate",
        "_l1_regularization_strength",
        "_l2_regularization_strength",
    ],
    "RMSPropOptimizer": ["_learning_rate", "_decay", "_momentum", "_epsilon"],
    "AdadeltaOptimizer": ["_lr", "_rho", "_epsilon"],
    "GradientDescentOptimizer": ["_learning_rate"],
    "MomentumOptimizer": ["_learning_rate", "_momentum", "_use_nesterov"],
    "AdamOptimizer": ["_lr", "_beta1", "_beta2", "_epsilon"],
    "FtrlOptimizer": [
        "_learning_rate",
        "_learning_rate_power",
        "_initial_accumulator_value",
        "_l1_regularization_strength",
        "_l2_regularization_strength",
        "_l2_shrinkage_regularization_strength",
    ],
    "AdagradOptimizer": ["_learning_rate", "_initial_accumulator_value"],
}


def extract_params_from_optimizer(optimizer):
    optimizer_name = optimizer.__class__.__name__
    optimizer_params = optimizers_hyper_params.get(optimizer_name, [])

    hyper_params = {}
    for param in optimizer_params:
        if hasattr(optimizer, param):
            value = getattr(optimizer, param)
            if param[0] == "_":
                hyper_params[param[1:]] = value  # remove underscore prefix
            else:
                hyper_params[param] = value

    hyper_params["Optimizer"] = optimizer_name
    return hyper_params


def optimizer_logger(experiment, original, value, *args, **kwargs):
    if experiment.auto_param_logging:
        try:
            if len(args) > 0:
                LOGGER.debug("TENSORFLOW LOGGER CALLED")
                params = extract_params_from_optimizer(args[0])
                experiment._log_parameters(params, framework="tensorflow")

        except Exception:
            LOGGER.error(
                "Failed to extract parameters from Optimizer.init()", exc_info=True
            )


OPTIMIZER = [
    (
        "tensorflow.python.training.gradient_descent",
        "GradientDescentOptimizer.__init__",
    ),
    ("tensorflow.python.training.momentum", "MomentumOptimizer.__init__"),
    (
        "tensorflow.python.training.proximal_adagrad",
        "ProximalAdagradOptimizer.__init__",
    ),
    (
        "tensorflow.python.training.proximal_gradient_descent",
        "ProximalGradientDescentOptimizer.__init__",
    ),
    ("tensorflow.python.training.adadelta", "AdadeltaOptimizer.__init__"),
    ("tensorflow.python.training.adagrad", "AdagradOptimizer.__init__"),
    ("tensorflow.python.training.adagrad_da", "AdagradDAOptimizer.__init__"),
    ("tensorflow.python.training.adam", "AdamOptimizer.__init__"),
    ("tensorflow.python.training.ftrl", "FtrlOptimizer.__init__"),
    ("tensorflow.python.training.rmsprop", "RMSPropOptimizer.__init__"),
]

OPTIMIZER_V2 = [
    ("tensorflow.python.keras.optimizer_v2.adam", "Adam.__init__"),
    ("tensorflow.python.keras.optimizer_v2.adadelta", "Adadelta.__init__"),
    ("tensorflow.python.keras.optimizer_v2.adagrad", "Adagrad.__init__"),
    ("tensorflow.python.keras.optimizer_v2.adamax", "Adamax.__init__"),
    ("tensorflow.python.keras.optimizer_v2.ftrl", "Ftrl.__init__"),
    ("tensorflow.python.keras.optimizer_v2.gradient_descent", "SGD.__init__"),
    ("tensorflow.python.keras.optimizer_v2.nadam", "Nadam.__init__"),
    ("tensorflow.python.keras.optimizer_v2.rmsprop", "RMSprop.__init__"),
]

# TF 2.6.0 have moved the optimizers code to keras
OPTIMIZER_V2_6 = [
    ("keras.optimizer_v2.adam", "Adam.__init__"),
    ("keras.optimizer_v2.adadelta", "Adadelta.__init__"),
    ("keras.optimizer_v2.adagrad", "Adagrad.__init__"),
    ("keras.optimizer_v2.adamax", "Adamax.__init__"),
    ("keras.optimizer_v2.ftrl", "Ftrl.__init__"),
    ("keras.optimizer_v2.gradient_descent", "SGD.__init__"),
    ("keras.optimizer_v2.nadam", "Nadam.__init__"),
    ("keras.optimizer_v2.rmsprop", "RMSprop.__init__"),
]

ESTIMATOR_V1 = [
    ("tensorflow.python.estimator.canned.baseline", "BaselineClassifier.__init__"),
    ("tensorflow.python.estimator.canned.baseline", "BaselineRegressor.__init__"),
    (
        "tensorflow.python.estimator.canned.boosted_trees",
        "BoostedTreesClassifier.__init__",
    ),
    (
        "tensorflow.python.estimator.canned.boosted_trees",
        "BoostedTreesRegressor.__init__",
    ),
    ("tensorflow.python.estimator.canned.dnn", "DNNClassifier.__init__"),
    ("tensorflow.python.estimator.canned.dnn", "DNNRegressor.__init__"),
    (
        "tensorflow.python.estimator.canned.dnn_linear_combined",
        "DNNLinearCombinedClassifier.__init__",
    ),
    (
        "tensorflow.python.estimator.canned.dnn_linear_combined",
        "DNNLinearCombinedRegressor.__init__",
    ),
    ("tensorflow.python.estimator.canned.linear", "LinearClassifier.__init__"),
    ("tensorflow.python.estimator.canned.linear", "LinearRegressor.__init__"),
]

ESTIMATOR_V2 = [
    # V2 versions goes back at least to tensorflow 1.14
    (
        "tensorflow_estimator.python.estimator.canned.baseline",
        "BaselineClassifierV2.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.baseline",
        "BaselineEstimatorV2.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.baseline",
        "BaselineRegressorV2.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.linear",
        "LinearClassifierV2.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.linear",
        "LinearEstimatorV2.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.linear",
        "LinearRegressorV2.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.dnn_linear_combined",
        "DNNLinearCombinedClassifierV2.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.dnn_linear_combined",
        "DNNLinearCombinedEstimatorV2.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.dnn_linear_combined",
        "DNNLinearCombinedRegressorV2.__init__",
    ),
    ("tensorflow_estimator.python.estimator.canned.dnn", "DNNClassifierV2.__init__"),
    ("tensorflow_estimator.python.estimator.canned.dnn", "DNNEstimatorV2.__init__"),
    ("tensorflow_estimator.python.estimator.canned.dnn", "DNNRegressorV2.__init__"),
    ("tensorflow_estimator.python.estimator.canned.rnn", "RNNEstimator.__init__"),
    (
        "tensorflow_estimator.python.estimator.canned.kmeans",
        "KMeansClustering.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.baseline",
        "BaselineClassifier.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.baseline",
        "BaselineEstimator.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.baseline",
        "BaselineRegressor.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.linear",
        "LinearClassifier.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.linear",
        "LinearEstimator.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.linear",
        "LinearRegressor.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.dnn_linear_combined",
        "DNNLinearCombinedClassifier.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.dnn_linear_combined",
        "DNNLinearCombinedEstimator.__init__",
    ),
    (
        "tensorflow_estimator.python.estimator.canned.dnn_linear_combined",
        "DNNLinearCombinedRegressor.__init__",
    ),
    ("tensorflow_estimator.python.estimator.canned.dnn", "DNNClassifier.__init__"),
    ("tensorflow_estimator.python.estimator.canned.dnn", "DNNEstimator.__init__"),
    ("tensorflow_estimator.python.estimator.canned.dnn", "DNNRegressor.__init__"),
]


def _get_tensorflow_hook(experiment):
    try:
        from ..callbacks._tensorflow_estimator import (
            CometTensorflowEstimatorTrainSessionHook,
        )
    except ImportError:
        return None

    return CometTensorflowEstimatorTrainSessionHook(experiment)


def estimator_train_logger(experiment, original, *args, **kwargs):
    if experiment.log_graph:
        arguments = get_argument_bindings(original, args, kwargs)
        hooks = arguments.get("hooks", None)

        comet_hook = _get_tensorflow_hook(experiment)

        if comet_hook is not None:
            arguments["hooks"] = _check_callbacks_list(
                hooks, comet_hook, copy_list=True
            )

        return ([args[0]], arguments)


def estimator_logger(experiment, original, value, *args, **kwargs):
    if experiment.auto_param_logging:
        try:
            LOGGER.debug("TENSORFLOW LOGGER CALLED")
            params = get_argument_bindings(original, args, kwargs)
            # Add additional items:
            params["Estimator"] = args[0].__class__.__name__
            # Turn all values into strings:
            for key in params:
                params[key] = str(params[key])
            experiment._log_parameters(params, framework="tensorflow")

        except Exception:
            LOGGER.error(
                "Failed to extract parameters from Estimator.init()", exc_info=True
            )


def optimizer_logger_v2(experiment, original, value, *args, **kwargs):
    if experiment.auto_param_logging:
        try:
            if len(args) > 0:
                LOGGER.debug("TENSORFLOW LOGGER CALLED")
                # Tensorflow 1.14 v2 bug: https://github.com/tensorflow/tensorflow/pull/32012
                optimizer = args[0]
                if optimizer.__class__.__name__ == "Ftrl":
                    optimizer._serializer_hyperparameter = (
                        optimizer._serialize_hyperparameter
                    )
                # end bug
                config = args[0].get_config()
                name = config.pop("name")
                params = {name + "_" + key: config[key] for key in config}
                params["Optimizer"] = name
                experiment._log_parameters(params, framework="tensorflow")

        except Exception:
            LOGGER.error(
                "Failed to extract parameters from Optimizer.init()", exc_info=True
            )


def patch(module_finder):
    check_module("tensorflow")

    # Optimizers:
    # Register the methods
    for module, object_name in OPTIMIZER:
        module_finder.register_after(module, object_name, optimizer_logger)
    # Register the v2 methods
    for module, object_name in OPTIMIZER_V2:
        module_finder.register_after(module, object_name, optimizer_logger_v2)

    # Register the v2.6.x methods
    for module, object_name in OPTIMIZER_V2_6:
        module_finder.register_after(module, object_name, optimizer_logger_v2)

    # Estimators:
    for module, object_name in ESTIMATOR_V2:
        module_finder.register_after(module, object_name, estimator_logger)

    for module, object_name in ESTIMATOR_V1:
        module_finder.register_after(module, object_name, estimator_logger)

    module_finder.register_before(
        "tensorflow_estimator.python.estimator.estimator",
        "Estimator.train",
        estimator_train_logger,
    )


check_module("tensorflow")
