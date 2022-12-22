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

""" The Pytorch auto-logger is logging the following elements:
* Loss metric
* Model graph on step 0

A great deal of the code is here to deal with scaling loss libraries and tools which change the
tensor values to speed up training but that means the loss value we receive needs to be unscaled
before logging.

There are two ways of doing loss scaling with Pytorch. In both case, we "save" the original loss in
the experiment localstorage with the id of the scaled_loss and `tensor_backward` check whenever it
receives a loss if it finds the loss id in that mapping and if that is the case, it logs the
original loss instead.

The first historical one is Apex https://github.com/NVIDIA/apex. The gist of its loss scaling code
is:

with amp.scale_loss(loss, optimizer) as scaled_loss:
    scaled_loss.backward()

The source code is here:
https://github.com/NVIDIA/apex/blob/082f999a6e18a3d02306e27482cc7486dab71a50/apex/amp/handle.py#L17

In that case, what we do is hook on the `amp.scale_loss` hookpoint with `scale_loss_hook` and wrap
the returned context manager with `CallableWrapper`. When the resulting context manager is entered,
we add a link to the scaled loss id to the original loss that we saved along the way. This mapping
is kept on the experiment storage using thread locals. Whenever the context manager exits, we clear
that mapping as the scaled loss is likely unusable at this point.

The second more modern way is built-in in Pytorch https://pytorch.org/docs/stable/amp.html. The gist
of its usage is:

# Creates a GradScaler once at the beginning of training.
scaler = GradScaler()

for epoch in epochs:
    for input, target in data:
        optimizer.zero_grad()

        # Runs the forward pass with autocasting.
        with autocast():
            output = model(input)
            loss = loss_fn(output, target)

        # Scales loss.  Calls backward() on scaled loss to create scaled gradients.
        # Backward passes under autocast are not recommended.
        # Backward ops run in the same dtype autocast chose for corresponding forward ops.
        scaler.scale(loss).backward()

We are hooking on the `GradScaler.scale` method where we save the original loss with the id of the
return scaled_loss. But that also means that we are leaking references to the original loss in that
case.
"""

import logging

import wrapt

from .._typing import Any
from ..experiment import BaseExperiment
from ..monkey_patching import check_module

LOGGER = logging.getLogger(__name__)

LOSS_BATCH_SIZE = 10


def _get_loss(loss_backward, experiment):
    # type: (Any, BaseExperiment) -> Any
    """Returns the right loss tensor based on the loss tensor object where
    backward has been called. The right loss might be the unscaled one when
    using APEX or Pytorch 1.6 AMP
    """
    if experiment.alive:
        return experiment._localstorage.pytorch["amp_loss_mapping"].get(
            id(loss_backward), loss_backward
        )
    else:
        # Do not warn the user, it might send too many log messages
        LOGGER.debug("Attempting to get amp_loss_mapping for closed experiment")
        return None


def _add_amp_loss_mapping(scaled_loss, unscaled_loss, experiment):
    # type: (int, Any, BaseExperiment) -> None
    # First argument is the scaled_loss id
    if experiment.alive:
        experiment._localstorage.pytorch["amp_loss_mapping"][
            scaled_loss
        ] = unscaled_loss
    else:
        # No need to save the unscaled_loss if the experiment is not alive
        LOGGER.debug("Attempting to update amp_loss_mapping for closed experiment")


def _clean_amp_loss_mapping(scaled_loss, experiment):
    # type: (int, BaseExperiment) -> None
    # First argument is the scaled_loss id

    # When an experiment is ended, the storage should be cleared. Be defensive and still try to
    # clear it if an experiment is not alive but the storage has not been cleared
    pytorch_storage = getattr(experiment._localstorage, "pytorch", None)
    if pytorch_storage is None:
        return

    pytorch_storage.get("amp_loss_mapping", {}).pop(scaled_loss, None)


def _log_loss_value(arg_loss, experiment):
    # type: (Any, BaseExperiment) -> None
    if not experiment.auto_metric_logging:
        # auto logging is not enabled - just return
        return None

    # log current loss with throttling report to every 10 batch updates
    if experiment.curr_step % LOSS_BATCH_SIZE == 0:
        loss = _get_loss(arg_loss, experiment)

        # We can get empty loss if the experiment is not alive
        if loss is None:
            return None

        if len(loss.data.shape) == 0:
            metric = loss.data.item()
            experiment._log_metric(
                "loss",
                metric,
                framework="pytorch",
            )
        else:
            experiment._log_metric(
                "loss",
                loss.data.mean().item(),
                framework="pytorch",
            )


def tensor_backward(experiment, original, result, *args, **kwargs):
    # args[0] is self, the Tensor (loss):
    try:
        if experiment.curr_step is None:
            experiment.set_step(0)
        else:
            experiment.set_step(experiment.curr_step + 1)

        if experiment.log_graph:
            model = experiment._storage["torch"]["model"]
            if experiment.curr_step == 0 and model is not None:
                experiment._set_model_graph(model, framework="pytorch")

        if len(args) < 1:
            LOGGER.debug("Missing loss, not enough args")
            return result

        arg_loss = args[0]

        try:
            _log_loss_value(arg_loss, experiment)
        finally:
            # Clean the reference to the original unscaled loss to avoid leaking
            # memory
            _clean_amp_loss_mapping(id(arg_loss), experiment)
    except Exception:
        LOGGER.info("Failed to run Tensor.backward logger", exc_info=True)
    return result


def model_constructor(experiment, original, *args, **kwargs):
    ## Assume the first one is the model:
    try:
        model = experiment._storage["torch"]["model"]
        # Save only the first model, it's usually the user-created model/module
        if model is None:
            experiment._storage["torch"]["model"] = args[1]
    except Exception:
        LOGGER.info("Failed to run Module.__init__ logger", exc_info=True)


class CallableWrapper(wrapt.ObjectProxy):
    def __init__(self, wrapped, original_loss, experiment):
        super(CallableWrapper, self).__init__(wrapped)
        self.original_loss = original_loss
        self.experiment = experiment
        self.scaled_loss = None

    def __enter__(self, *args, **kwargs):
        return_value = self.__wrapped__.__enter__(*args, **kwargs)

        try:
            self.scaled_loss = id(return_value)
            _add_amp_loss_mapping(self.scaled_loss, self.original_loss, self.experiment)
        except Exception:
            LOGGER.debug("Error in Apex amp.scale_loss __enter__", exc_info=True)

        return return_value

    def __exit__(self, *args, **kwargs):
        try:
            if self.scaled_loss:
                _clean_amp_loss_mapping(self.scaled_loss, self.experiment)
        except Exception:
            LOGGER.debug("Error in Apex amp.scale_loss __exit__", exc_info=True)

        return self.__wrapped__.__exit__(*args, **kwargs)


def scale_loss_hook(experiment, original, return_value, original_loss, *args, **kwargs):
    return CallableWrapper(return_value, original_loss, experiment)


def amp_scale_loss_hook(
    experiment, original, return_value, scaler, original_loss, *args, **kwargs
):
    # Save the original loss mapped to the scaled one so we can find it back after
    _add_amp_loss_mapping(id(return_value), original_loss, experiment)


def patch(module_finder):
    ## For testing:
    check_module("torch")

    ## For each backpropagation of the gradient:
    # Torch pre-1.9
    module_finder.register_after("torch.tensor", "Tensor.backward", tensor_backward)
    # Torch 1.9
    module_finder.register_after("torch", "Tensor.backward", tensor_backward)
    ## For each model constructor:
    module_finder.register_after(
        "torch.nn.modules.module", "Module.__init__", model_constructor
    )

    module_finder.register_after("apex.amp.handle", "scale_loss", scale_loss_hook)

    module_finder.register_after(
        "torch.cuda.amp.grad_scaler", "GradScaler.scale", amp_scale_loss_hook
    )


check_module("torch")
