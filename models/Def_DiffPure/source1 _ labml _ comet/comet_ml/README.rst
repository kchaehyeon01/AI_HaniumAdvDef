Comet.ml
========

.. image:: https://img.shields.io/pypi/v/comet_ml.svg
    :target: https://pypi.python.org/pypi/comet_ml
    :alt: Latest PyPI version


Documentation
-------------

Full documentation and additional training examples are available on
http://www.comet.ml/docs/

Installation
------------

-  Sign up (free) on comet.ml and obtain an API key at https://www.comet.ml


Getting started: 30 seconds to Comet.ml
---------------------------------------

The core class of Comet.ml is an **Experiment**, a specific run of a
script that generated a result such as training a model on a single set
of hyper parameters. An Experiment will automatically log scripts output (stdout/stderr), code, and command
line arguments on **any** script and for the supported libraries will
also log hyper parameters, metrics and model configuration.

Here is the Experiment object:


    from comet_ml import Experiment
    experiment = Experiment(api_key="YOUR_API_KEY")

    # Your code.



We all strive to be data driven and yet every day valuable experiments
results are just lost and forgotten. Comet.ml provides a dead simple way
of fixing that. Works with any workflow, any ML task, any machine and
any piece of code.

For a more in-depth tutorial about Comet.ml, you can check out or docs http:/www.comet.ml/docs/

License
---------------------------------------

Copyright (C) 2015-2020 Comet ML INC.

This package can not be copied and/or distributed without the express permission of Comet ML Inc.