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

import tensorflow as tf

from ._typing import Any


@tf.function
def get_gradients(model, batch_input, batch_target, weights):
    # type: (Any, Any, Any, Any) -> Any
    """
    Function to compute gradients of the weights wrt the loss.
    """
    with tf.GradientTape() as tape:
        output = model(batch_input)
        loss_function = tf.keras.losses.get(model.loss)
        loss = loss_function(output, batch_target)

        gradients = tape.gradient(loss, weights)
    return gradients
