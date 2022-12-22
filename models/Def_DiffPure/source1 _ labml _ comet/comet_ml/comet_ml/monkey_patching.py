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

""" The import hook which monkey patch modules
"""

import functools
import imp
import inspect
import logging
import sys
import types

import six

from .config import get_config, get_global_experiment
from .logging_messages import COMET_DISABLED_AUTO_LOGGING_MSG

LOGGER = logging.getLogger(__name__)

ALREADY_IMPORTED_MODULES = set()


def check_module(module):
    """
    Check to see if a module has already been loaded.
    This is an error, unless comet.disable_auto_logging == 1
    """
    if get_config("comet.disable_auto_logging"):
        LOGGER.debug(COMET_DISABLED_AUTO_LOGGING_MSG, module)
    elif module in sys.modules:
        ALREADY_IMPORTED_MODULES.add(module)


def is_module_already_imported(module):
    return module in ALREADY_IMPORTED_MODULES


def _reset_already_imported_modules():
    # Modify the set in place to be sure to keep the same reference every
    # where
    ALREADY_IMPORTED_MODULES.clear()


def get_pep_302_importer(path):
    """A pep 302 compatible importer"""
    importer = sys.path_importer_cache.get(path)
    if not importer:
        for hook in sys.path_hooks:
            try:
                importer = hook(path)
                break

            except ImportError:
                pass
    return importer


class CustomFileLoader(object):
    """A Python 3 loader that use a SourceFileLoader to exec an imported
    module and patch it with CometModuleFinder
    """

    def __init__(self, loader, fullname, finder):
        self.loader = loader
        self.fullname = fullname
        self.finder = finder

    def exec_module(self, module):
        # Execute the module source code to define all the objects
        if hasattr(self.loader, "exec_module"):
            self.loader.exec_module(module)
        else:
            # zipimporter doesn't use exec_module
            module = self.loader.load_module(self.fullname)

        return self.finder._patch(module, self.fullname)

    def create_module(self, spec):
        """Mandatory in Python 3.6 as we define the exec_module method"""
        return None


class CometModuleFinder(object):
    def __init__(self):
        self.patcher_functions = {}

        if sys.version_info[0] >= 3:
            from importlib.machinery import PathFinder

            self.pathfinder = PathFinder()

    def register_before(
        self, module_name, object_name, patcher_function, allow_empty_experiment=False
    ):
        self._register(
            "before", module_name, object_name, patcher_function, allow_empty_experiment
        )

    def register_after(
        self, module_name, object_name, patcher_function, allow_empty_experiment=False
    ):
        self._register(
            "after", module_name, object_name, patcher_function, allow_empty_experiment
        )

    def _register(
        self,
        lifecycle,
        module_name,
        object_name,
        patcher_function,
        allow_empty_experiment,
    ):
        module_patchers = self.patcher_functions.setdefault(module_name, {})
        object_patchers = module_patchers.setdefault(
            object_name,
            {
                "before": [],
                "after": [],
                "allow_empty_experiment": allow_empty_experiment,
            },
        )
        object_patchers[lifecycle].append(patcher_function)

    def start(self):
        if self not in sys.meta_path:
            sys.meta_path.insert(0, self)

    def find_module(self, fullname, path=None):
        """Python 2 import hook"""
        if fullname not in self.patcher_functions:
            return

        return self

    def load_module(self, fullname):
        """Python 2 import hook"""
        module = self._get_module(fullname)
        return self._patch(module, fullname)

    def find_spec(self, fullname, path=None, target=None):
        """Python 3 import hook"""
        if fullname not in self.patcher_functions:
            return

        spec = self.pathfinder.find_spec(fullname, path, target)

        if not spec:
            return

        new_loader = CustomFileLoader(spec.loader, fullname, self)
        spec.loader = new_loader

        return spec

    def _get_module(self, fullname):
        splitted_name = fullname.split(".")
        parent = ".".join(splitted_name[:-1])

        if fullname in sys.modules:
            return sys.modules[fullname]

        elif parent in sys.modules:
            parent = sys.modules[parent]
            module_path = imp.find_module(splitted_name[-1], parent.__path__)
            return imp.load_module(fullname, *module_path)

        else:
            try:
                module_path = imp.find_module(fullname)
                return imp.load_module(fullname, *module_path)

            except ImportError:
                for p in sys.path:
                    importer = get_pep_302_importer(p)

                    # Ignore invalid paths
                    if importer is None:
                        continue

                    module_path = importer.find_module(fullname)

                    if module_path:
                        return importer.load_module(fullname)

    def _patch(self, module, fullname):
        objects_to_patch = self.patcher_functions.get(fullname, {})

        for object_name, patcher_callbacks in objects_to_patch.items():
            object_path = object_name.split(".")

            original = self._get_object(module, object_path)

            if original is None:
                # TODO: Send back the error?
                continue

            new_object = Entrypoint(original, **patcher_callbacks)
            self._set_object(module, object_path, original, new_object)

        return module

    def _get_object(self, module, object_path):
        current_object = module

        for part in object_path:
            try:
                current_object = getattr(current_object, part)
            except AttributeError:
                return None

        return current_object

    def _set_object(self, module, object_path, original, new_object):
        object_to_patch = self._get_object(module, object_path[:-1])

        original_self = getattr(original, "__self__", None)

        # Support classmethod
        if original_self and inspect.isclass(original_self):
            new_object = classmethod(new_object)
            # Support staticmethod
        elif (
            six.PY2
            and inspect.isclass(object_to_patch)
            and isinstance(original, types.FunctionType)
        ):
            new_object = staticmethod(new_object)

        setattr(object_to_patch, object_path[-1], new_object)


def valid_new_args_kwargs(callback_return):
    if callback_return is None:
        return False

    try:
        args, kwargs = callback_return
    except (ValueError, TypeError):
        return False

    if not isinstance(args, (list, tuple)):
        return False

    if not isinstance(kwargs, dict):
        return False

    return True


def Entrypoint(original, before=None, after=None, allow_empty_experiment=False):
    """The object that replaces the patched functions / methods.

    Its responsabilites is to:
    - Call the original function with the passed arguments
    - Return the value of the original function
    - Call callbacks before and after the original function execute
    - Only if the experiment is alive and the monkey-patching is not disabled
    - Catch any exception from the callbacks
    """
    if before is None:
        before_callbacks = []
    else:
        before_callbacks = before

    if after is None:
        after_callbacks = []
    else:
        after_callbacks = after

    # Unbound a method if we get a classmethod
    if hasattr(original, "__self__") and inspect.isclass(original.__self__):
        original = original.__func__

    def wrapper(*args, **kwargs):
        experiment = get_global_experiment()

        should_run = True

        if allow_empty_experiment is False and experiment is None:
            should_run = False

        if experiment is not None and experiment.disabled_monkey_patching is True:
            should_run = False

        # Call before callbacks before calling the original method
        if should_run:
            for callback in before_callbacks:
                try:
                    callback_return = callback(experiment, original, *args, **kwargs)

                    if valid_new_args_kwargs(callback_return):
                        LOGGER.debug("New args %r", callback_return)
                        args, kwargs = callback_return
                except Exception:
                    LOGGER.debug(
                        "Exception calling before callback %r", callback, exc_info=True
                    )

        return_value = original(*args, **kwargs)

        # Call after callbacks once we have the return value
        if should_run:
            for callback in after_callbacks:
                try:
                    new_return_value = callback(
                        experiment, original, return_value, *args, **kwargs
                    )
                    if new_return_value is not None:
                        return_value = new_return_value
                except Exception:
                    LOGGER.debug(
                        "Exception calling after callback %r", callback, exc_info=True
                    )

        return return_value

    # Simulate functools.wraps behavior but make it working with mocks
    for attr in functools.WRAPPER_ASSIGNMENTS:
        if hasattr(original, attr):
            setattr(wrapper, attr, getattr(original, attr))

    # Python2 functools.wraps doesn't set the __wrapped__ attribute
    wrapper.__wrapped__ = original
    wrapper.__comet__ = True

    return wrapper
