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

from __future__ import print_function

import io
import logging
import os
import os.path
import shutil

import six.moves
from everett.manager import (
    NO_VALUE,
    ConfigDictEnv,
    ConfigEnvFileEnv,
    ConfigManager,
    ConfigOSEnv,
    ListOf,
    listify,
    parse_bool,
)

from ._jupyter import _in_colab_environment
from ._logging import _get_comet_logging_config
from ._typing import Any, Dict, List, Optional, Tuple, Union
from .exceptions import InvalidRestAPIKey
from .logging_messages import (
    API_KEY_CHECK_FAILED,
    API_KEY_IS_INVALID,
    API_KEY_IS_NOT_SET,
    API_KEY_IS_VALID,
    IN_COLAB_WITHOUT_DRIVE_DIRECTORY1,
    IN_COLAB_WITHOUT_DRIVE_DIRECTORY2,
)
from .utils import (
    clean_string,
    get_api_key_from_user,
    get_root_url,
    is_interactive,
    log_once_at_level,
    sanitize_url,
)

try:  # everett version 1.0.1 or greater
    from everett.ext.inifile import ConfigIniEnv
except ImportError:  # everett version 0.9 for Python 2
    from everett.manager import ConfigIniEnv as UpstreamConfigIniEnv

    class ConfigIniEnv(UpstreamConfigIniEnv):
        """Backport of everett ConfigIniEnv in Python 3 that save the path when found"""

        def __init__(self, possible_paths):
            super(ConfigIniEnv, self).__init__(possible_paths)

            if self.cfg:
                self.path = os.path.abspath(os.path.expanduser(possible_paths.strip()))


LOGGER = logging.getLogger(__name__)

DEBUG = False

# Global experiment placeholder. Should be set by the latest call of Experiment.init()
experiment = None

DEFAULT_UPLOAD_SIZE_LIMIT = 200 * 1024 * 1024  # 200 MebiBytes

DEFAULT_ASSET_UPLOAD_SIZE_LIMIT = 100000 * 1024 * 1024  # 100GB

DEFAULT_STREAMER_MSG_TIMEOUT = 60 * 60  # 1 Hour

ADDITIONAL_STREAMER_UPLOAD_TIMEOUT = 3 * 60 * 60  # 3 hours

DEFAULT_FILE_UPLOAD_READ_TIMEOUT = 900

DEFAULT_ARTIFACT_DOWNLOAD_TIMEOUT = 3 * 60 * 60  # 3 hours

DEFAULT_INITIAL_DATA_LOGGER_JOIN_TIMEOUT = 5 * 60

DEFAULT_WS_JOIN_TIMEOUT = 30

DEFAULT_PARAMETERS_BATCH_INTERVAL_SECONDS = 60

DEFAULT_WAIT_FOR_FINISH_SLEEP_INTERVAL = 15

DEFAULT_OFFLINE_DATA_DIRECTORY = ".cometml-runs"

COLAB_DRIVE_MOUNT = "/content/drive/MyDrive/"

DEFAULT_POOL_RATIO = 4

MAX_POOL_SIZE = 64

MESSAGE_BATCH_USE_COMPRESSION_DEFAULT = True

MESSAGE_BATCH_METRIC_INTERVAL_SECONDS = 2

MESSAGE_BATCH_METRIC_MAX_BATCH_SIZE = 1000

MESSAGE_BATCH_STDOUT_INTERVAL_SECONDS = 5

MESSAGE_BATCH_STDOUT_MAX_BATCH_SIZE = 500

OFFLINE_EXPERIMENT_MESSAGES_JSON_FILE_NAME = "messages.json"

OFFLINE_EXPERIMENT_JSON_FILE_NAME = "experiment.json"


def get_global_experiment():
    global experiment
    return experiment


def set_global_experiment(new_experiment):
    global experiment
    experiment = new_experiment


def _clean_config_path(file_path):
    # type: (str) -> str
    """Apply the usual path cleaning function for config paths"""
    return os.path.abspath(os.path.expanduser(file_path))


def _config_path_from_directory(directory):
    # type: (str) -> str
    return _clean_config_path(os.path.join(directory, ".comet.config"))


def _get_default_config_path():
    # type: () -> str
    config_home = os.environ.get("COMET_CONFIG")
    if config_home is not None:
        if config_home is not None and os.path.isdir(config_home):
            config_home = _config_path_from_directory(config_home)

        return _clean_config_path(config_home)

    elif _in_colab_environment():
        if os.path.isdir(COLAB_DRIVE_MOUNT):
            return _config_path_from_directory(COLAB_DRIVE_MOUNT)
        else:
            LOGGER.warning(IN_COLAB_WITHOUT_DRIVE_DIRECTORY1)
            LOGGER.warning(IN_COLAB_WITHOUT_DRIVE_DIRECTORY2)
            return _config_path_from_directory("~")
    else:
        return _config_path_from_directory("~")


def parse_str_or_identity(_type):
    def parse(value):
        if not isinstance(value, str):
            return value

        return _type(value.strip())

    return parse


class ParseListOf(ListOf):
    """
    Superclass to apply subparser to list items.
    """

    def __init__(self, _type, _parser):
        super(ParseListOf, self).__init__(_type)
        self._type = _type
        self._parser = _parser

    def __call__(self, value):
        f = self._parser(self._type)
        if not isinstance(value, list):
            value = super(ParseListOf, self).__call__(value)
        return [f(v) for v in value]


PARSER_MAP = {
    str: parse_str_or_identity(str),
    int: parse_str_or_identity(int),
    float: parse_str_or_identity(float),
    bool: parse_str_or_identity(parse_bool),
    list: ParseListOf(str, parse_str_or_identity),
    "int_list": ParseListOf(int, parse_str_or_identity(int)),
}


# Vendor generate_uppercase_key for Python 2
def generate_uppercase_key(key, namespace=None):
    """Given a key and a namespace, generates a final uppercase key."""
    if namespace:
        namespace = [part for part in listify(namespace) if part]
        key = "_".join(namespace + [key])

    key = key.upper()
    return key


class Config(object):
    def __init__(self, config_map):
        self.config_map = config_map
        self.override = {}  # type: Dict[str, Any]
        self.backend_override = ConfigDictEnv({})

        config_override = os.environ.get("COMET_INI")
        if config_override is not None:
            log_once_at_level(
                logging.WARNING, "COMET_INI is deprecated; use COMET_CONFIG"
            )
        else:
            config_override = os.environ.get("COMET_CONFIG")

        if config_override is not None and os.path.isdir(config_override):
            config_override = _config_path_from_directory(config_override)

        self.manager = ConfigManager(
            [  # User-defined overrides
                ConfigOSEnv(),
                ConfigEnvFileEnv(".env"),
                ConfigIniEnv(config_override),
                ConfigIniEnv("./.comet.config"),
                ConfigIniEnv("/content/drive/MyDrive/.comet.config"),
                ConfigIniEnv("~/.comet.config"),
                # Comet-defined overrides
                self.backend_override,
            ],
            doc=(
                "See https://comet.ml/docs/python-sdk/getting-started/ for more "
                + "information on configuration."
            ),
        )

    def __setitem__(self, name, value):
        self.override[name] = value

    def _set_backend_override(self, cfg, namespace):
        # Reset the existing overrides
        self.backend_override.cfg = {}

        for key, value in cfg.items():
            namespaced_key = "_".join(namespace.split("_") + [key])
            full_key = generate_uppercase_key(namespaced_key)
            self.backend_override.cfg[full_key] = value

    def keys(self):
        return self.config_map.keys()

    def get_raw(self, user_value, config_name, default=None, not_set_value=None):
        # type: (Any, str, Optional[Any], Optional[Any]) -> Any
        """
        Returns the correct config value based on the following priority list:
        * User_value if set and not None
        * The override value from the Backend
        * The configured value
        * The default value passed in argument if not None
        * The configured value default
        """

        # 1. User value
        if user_value is not not_set_value:
            return user_value

        # 2. Override
        if config_name in self.override:
            override_value = self.override[config_name]

            if override_value is not None:
                return override_value

        # 3. Configured value
        config_type = self.config_map[config_name].get("type", str)
        parser = PARSER_MAP[config_type]

        # Value
        splitted = config_name.split(".")

        config_value = self.manager(
            splitted[-1], namespace=splitted[:-1], parser=parser, raise_error=False
        )

        if config_value != NO_VALUE:
            return config_value

        else:
            # 4. Provided default
            if default is not None:
                return default

            # 5. Config default
            config_default = parser(self.config_map[config_name].get("default", None))
            return config_default

    def get_string(self, user_value, config_name, default=None, not_set_value=None):
        # type: (Any, str, Optional[str], Any) -> str
        """
        Returns the correct config value based on the following priority list:
        * User_value if set and not None
        * The override value from the Backend
        * The configured value
        * The default value passed in argument if not None
        * The configured value default

        In addition make sure the returned value is a string
        """

        value = self.get_raw(
            user_value=user_value,
            config_name=config_name,
            default=default,
            not_set_value=not_set_value,
        )

        return value

    def get_bool(self, user_value, config_name, default=None, not_set_value=None):
        # type: (Any, str, Optional[bool], Any) -> bool
        """
        Returns the correct config value based on the following priority list:
        * User_value if set and not None
        * The override value from the Backend
        * The configured value
        * The default value passed in argument if not None
        * The configured value default

        In addition make sure the returned value is a bool
        """

        value = self.get_raw(
            user_value=user_value,
            config_name=config_name,
            default=default,
            not_set_value=not_set_value,
        )

        return value

    def get_int(self, user_value, config_name, default=None, not_set_value=None):
        # type: (Any, str, Optional[int], int) -> int
        """
        Returns the correct config value based on the following priority list:
        * User_value if set and not None
        * The override value from the Backend
        * The configured value
        * The default value passed in argument if not None
        * The configured value default

        In addition make sure the returned value is an int
        """

        value = self.get_raw(
            user_value=user_value,
            config_name=config_name,
            default=default,
            not_set_value=not_set_value,
        )

        return value

    def get_int_list(self, user_value, config_name, default=None, not_set_value=None):
        # type: (Any, str, Optional[int], int) -> List[int]
        """
        Returns the correct config value based on the following priority list:
        * User_value if set and not None
        * The override value from the Backend
        * The configured value
        * The default value passed in argument if not None
        * The configured value default

        In addition make sure the returned value is a list of int
        """

        value = self.get_raw(
            user_value=user_value,
            config_name=config_name,
            default=default,
            not_set_value=not_set_value,
        )

        return value

    def get_string_list(
        self, user_value, config_name, default=None, not_set_value=None
    ):
        # type: (Any, str, Optional[int], int) -> List[str]
        """
        Returns the correct config value based on the following priority list:
        * User_value if set and not None
        * The override value from the Backend
        * The configured value
        * The default value passed in argument if not None
        * The configured value default

        In addition make sure the returned value is a list of str
        """

        value = self.get_raw(
            user_value=user_value,
            config_name=config_name,
            default=default,
            not_set_value=not_set_value,
        )

        return value

    def get_deprecated_raw(
        self,
        old_user_value,
        old_config_name,
        new_user_value,
        new_config_name,
        new_not_set_value=None,
    ):
        # type: (Any, str, Any, str, Any) -> Any
        """
        Returns the correct value for deprecated config values:
        * New user value
        * Old user value
        * New config value
        * Old config value
        * New config default

        Note: The old config default is not used and should be set to None
        """
        old_config_value = self.get_raw(None, old_config_name, default=NO_VALUE)

        if new_user_value is not new_not_set_value:
            if old_user_value:
                LOGGER.warning(
                    "Deprecated config key %r was set, but ignored as new config key %r is set",
                    old_config_name,
                    new_config_name,
                )
            elif old_config_value:
                LOGGER.warning(
                    "Deprecated config key %r was set in %r, but ignored as new config key %r is set",
                    old_config_name,
                    self.get_config_origin(old_config_name),
                    new_config_name,
                )
            return new_user_value

        # Deprecated parameter default value must be None
        if old_user_value is not None:
            LOGGER.warning(
                "Config key %r is deprecated, please use %r instead",
                old_config_name,
                new_config_name,
            )
            return old_user_value

        new_config_value = self.get_raw(None, new_config_name, default=NO_VALUE)
        if new_config_value is not NO_VALUE:
            return new_config_value

        old_config_value = self.get_raw(None, old_config_name, default=NO_VALUE)
        if old_config_value is not NO_VALUE:
            LOGGER.warning(
                "Config key %r is deprecated (was set in %r), please use %r instead",
                old_config_name,
                self.get_config_origin(old_config_name),
                new_config_name,
            )
            return old_config_value

        config_type = self.config_map[new_config_name].get("type", str)
        parser = PARSER_MAP[config_type]
        return parser(self.config_map[new_config_name].get("default", None))

    def get_deprecated_bool(
        self,
        old_user_value,
        old_config_name,
        new_user_value,
        new_config_name,
        new_not_set_value=None,
    ):
        # type: (Any, str, Any, str, bool) -> bool
        """
        Returns the correct value for deprecated config values:
        * New user value
        * Old user value
        * New config value
        * Old config value
        * New config default

        Note: The old config default is not used and should be set to None
        """
        value = self.get_deprecated_raw(
            old_user_value,
            old_config_name,
            new_user_value,
            new_config_name,
            new_not_set_value=new_not_set_value,
        )

        return value

    def get_subsections(self):
        """
        Return the subsection config names.
        """
        sections = set()
        for key in self.keys():
            parts = key.split(".", 2)
            if len(parts) == 3:
                sections.add(parts[1])
        return sections

    def __getitem__(self, name):
        # type: (str) -> Any
        # Config
        config_type = self.config_map[name].get("type", str)
        parser = PARSER_MAP[config_type]
        config_default = self.config_map[name].get("default", None)

        if name in self.override:
            return self.override[name]

        # Value
        splitted = name.split(".")

        value = self.manager(
            splitted[-1], namespace=splitted[:-1], parser=parser, raise_error=False
        )

        if value == NO_VALUE:
            return parser(config_default)

        return value

    def display(self, display_all=False):
        """
        Show the Comet config variables and values.
        """
        n = 1
        print("=" * 65)
        print("Comet config variables and values, in order of preference:")
        print("    %d) Operating System Variable" % n)
        n += 1
        for path in ["./.env", "~/.comet.config", "./.comet.config"]:
            path = _clean_config_path(path)
            if os.path.exists(path):
                print("    %d) %s" % (n, path))
                n += 1
        print("=" * 65)
        print("Settings:\n")
        last_section = None
        for section, setting in sorted(
            [key.rsplit(".", 1) for key in self.config_map.keys()]
        ):
            key = "%s.%s" % (section, setting)
            value = self[key]
            if "." in section:
                section = section.replace(".", "_")
            if value is None:
                value = "..."
            default_value = self.config_map[key].get("default", None)
            if value == default_value or value == "...":
                if display_all:
                    if section != last_section:
                        if last_section is not None:
                            print()  # break between sections
                        print("[%s]" % section)
                        last_section = section
                    print("%s = %s" % (setting, value))
            else:
                if section != last_section:
                    if last_section is not None:
                        print("")  # break between sections
                    print("[%s]" % section)
                    last_section = section
                print("%s = %s" % (setting, value))
        print("=" * 65)

    def get_setting_key(self, setting):
        # Given a setting short-name, return proper ".comet.config" name
        # eg, given "api_key" return "comet.api_key"
        # eg, given "logging_console" return "comet.logging.console"
        subsections = self.get_subsections()
        key = None
        for prefix in subsections:
            if setting.startswith(prefix + "_"):
                key = ("comet.%s." % prefix) + setting[len(prefix) + 1 :]
                break
        if key is None:
            key = "comet." + setting
        return key

    def get_setting_name(self, setting):
        # Given a setting short-name, return proper env NAME
        # eg, given "api_key" return "COMET_API_KEY"
        # eg, given "logging_console" return "COMET_LOGGING_CONSOLE"
        subsections = self.get_subsections()
        name = None
        for prefix in subsections:
            if setting.startswith(prefix + "_"):
                name = ("COMET_%s_" % prefix.upper()) + (
                    setting[len(prefix) + 1 :].upper()
                )
                break
        if name is None:
            name = "COMET_" + setting.upper()
        return name

    def validate_value(self, key, value):
        # type: (str, Any) -> Tuple[bool, str]
        """
        Validates and converts value to proper type, or
        fails.

        Returns a tuple (valid, reason_if_failed)
        """
        if key in CONFIG_MAP:
            if value in [None, ""]:
                return (False, "invalid value")

            stype = CONFIG_MAP[key]["type"]
            if stype == "int_list":
                if not isinstance(value, list) or not all(
                    [isinstance(v, int) for v in value]
                ):
                    return (False, "not all values in list are integers")

            elif not isinstance(value, stype):  # specific type, like bool, int, str
                return (
                    False,
                    "value is wrong type for setting; type `%s` given but type `%s` expected"
                    % (type(value).__name__, stype.__name__),
                )

            return (True, "valid")

        else:
            return (False, "invalid setting")

    def _set_settings(self, settings, environ=False):
        for setting in settings:
            key = self.get_setting_key(setting)
            value = settings[setting]
            valid, reason = self.validate_value(key, value)
            if valid:
                if environ:
                    name = self.get_setting_name(setting)
                    os.environ[name] = str(value)
                else:
                    self[key] = value
            else:
                LOGGER.warning(
                    "config setting %r failed with value %r: %s", setting, value, reason
                )

    def save(
        self,
        directory=None,
        filename=None,
        save_all=False,
        force=False,
        _prompt_user_confirmation=False,
        **kwargs
    ):
        """
        Save the settings to .comet.config (default) or
        other path/filename. Defaults are commented out.

        Args:
            directory: the path to save the .comet.config config settings.
            save_all: save unset variables with defaults too
            force: force the file to save if it exists; else don't overwrite
            kwargs: key=value pairs to save
        """
        if directory is not None:
            filename = _config_path_from_directory(directory)

        if filename is None:
            filename = _get_default_config_path()

        if os.path.isfile(filename):
            if not force:
                LOGGER.error(
                    "'%s' exists and force is not True; refusing to overwrite", filename
                )
                return
            else:
                # ASK the user and try to make a backup copy
                if _prompt_user_confirmation:
                    overwrite = _confirm_user_config_file_overwriting(filename)
                else:
                    # Assume user consent with only the force flag
                    overwrite = True

                if overwrite:
                    try:
                        shutil.copyfile(filename, filename + ".bak")
                    except Exception:
                        LOGGER.warning(
                            "Unable to make a backup of config file", exc_info=True
                        )
                else:
                    LOGGER.warning(
                        "User refused to overwrite config file %r, aborting", filename
                    )
                    return

        print('Saving config to "%s"...' % filename, end="")
        with io.open(filename, "w", encoding="utf-8") as ini_file:
            ini_file.write(six.u("# Config file for Comet.ml\n"))
            ini_file.write(
                six.u(
                    "# For help see https://www.comet.ml/docs/python-sdk/getting-started/\n"
                )
            )
            last_section = None
            for section, setting in sorted(
                [key.rsplit(".", 1) for key in self.config_map.keys()]
            ):
                key = "%s.%s" % (section, setting)
                key_arg = "%s_%s" % (section, setting)
                if key_arg in kwargs:
                    value = kwargs[key_arg]
                    del kwargs[key_arg]
                elif key_arg.upper() in kwargs:
                    value = kwargs[key_arg.upper()]
                    del kwargs[key_arg.upper()]
                else:
                    value = self[key]
                if len(kwargs) != 0:
                    raise ValueError(
                        "'%s' is not a valid config key" % list(kwargs.keys())[0]
                    )
                if "." in section:
                    section = section.replace(".", "_")
                if value is None:
                    value = "..."
                default_value = self.config_map[key].get("default", None)
                LOGGER.debug("default value for %s is %s", key, default_value)
                if value == default_value or value == "...":
                    # It is a default value
                    # Only save it, if save_all is True:
                    if save_all:
                        if section != last_section:
                            if section is not None:
                                ini_file.write(six.u("\n"))  # break between sections
                            ini_file.write(six.u("[%s]\n" % section))
                            last_section = section
                        if isinstance(value, list):
                            value = ",".join(value)
                        ini_file.write(six.u("# %s = %s\n" % (setting, value)))
                else:
                    # Not a default value; write it out:
                    if section != last_section:
                        if section is not None:
                            ini_file.write(six.u("\n"))  # break between sections
                        ini_file.write(six.u("[%s]\n" % section))
                        last_section = section
                    if isinstance(value, list):
                        value = ",".join([str(v) for v in value])
                    ini_file.write(six.u("%s = %s\n" % (setting, value)))
        print(" done!")

    def get_config_origin(self, name):
        # type: (str) -> Optional[str]
        splitted = name.split(".")

        for env in self.manager.envs:
            value = env.get(splitted[-1], namespace=splitted[:-1])

            if value != NO_VALUE:
                return env

        return None


CONFIG_MAP = {
    "comet.disable_auto_logging": {"type": int, "default": 0},
    "comet.api_key": {"type": str},
    "comet.rest_api_key": {"type": str},
    "comet.offline_directory": {"type": str, "default": DEFAULT_OFFLINE_DATA_DIRECTORY},
    "comet.git_directory": {"type": str},
    "comet.offline_sampling_size": {"type": int, "default": 15000},
    "comet.url_override": {
        "type": str,
        "default": "https://www.comet-ml.com/clientlib/",
    },
    "comet.optimizer_url": {
        "type": str,
        "default": None,
    },
    "comet.ws_url_override": {"type": str, "default": None},
    "comet.experiment_key": {"type": str},
    "comet.project_name": {"type": str},
    "comet.workspace": {"type": str},
    "comet.display_summary_level": {"type": int, "default": 1},
    # Logging
    "comet.logging.file": {"type": str},
    "comet.logging.file_level": {"type": str, "default": "INFO"},
    "comet.logging.file_overwrite": {"type": bool, "default": False},
    "comet.logging.hide_api_key": {"type": bool, "default": True},
    "comet.logging.console": {"type": str},
    "comet.logging.metrics_ignore": {
        "type": list,
        "default": "keras:batch_size,keras:batch_batch",
    },
    "comet.logging.parameters_ignore": {
        "type": list,
        "default": "keras:verbose,keras:do_validation,keras:validation_steps",
    },
    "comet.logging.others_ignore": {"type": list, "default": ""},
    "comet.logging.env_blacklist": {
        "type": list,
        "default": "api_key,apikey,authorization,passwd,password,secret,token,comet",
    },
    # Timeout, unit is seconds
    "comet.timeout.cleaning": {"type": int, "default": DEFAULT_STREAMER_MSG_TIMEOUT},
    "comet.timeout.upload": {
        "type": int,
        "default": ADDITIONAL_STREAMER_UPLOAD_TIMEOUT,
    },
    "comet.timeout.http": {"type": int, "default": 10},
    "comet.timeout.api": {"type": int, "default": 10},
    "comet.timeout.file_upload": {
        "type": int,
        "default": DEFAULT_FILE_UPLOAD_READ_TIMEOUT,
    },
    "comet.timeout.file_download": {"type": int, "default": 600},
    "comet.timeout.artifact_download": {
        "type": int,
        "default": DEFAULT_ARTIFACT_DOWNLOAD_TIMEOUT,
    },
    # HTTP Allow header
    "comet.allow_header.name": {"type": str},
    "comet.allow_header.value": {"type": str},
    # Backend minimal rest V2 version
    "comet.rest_v2_minimal_backend_version": {"type": str, "default": "1.2.78"},
    # Feature flags
    "comet.override_feature.sdk_http_logging": {
        "type": bool
    },  # Leave feature toggle default to None
    "comet.override_feature.sdk_use_http_messages": {
        "type": bool
    },  # Leave feature toggle default to None
    "comet.override_feature.sdk_log_env_variables": {
        "type": bool
    },  # Leave feature toggle default to None
    "comet.override_feature.artifacts": {
        "type": bool
    },  # Leave feature toggle default to None
    # Experiment log controls:
    "comet.auto_log.cli_arguments": {"type": bool},
    "comet.auto_log.code": {"type": bool},
    "comet.auto_log.disable": {"type": bool},
    "comet.auto_log.env_cpu": {"type": bool},
    "comet.auto_log.env_details": {"type": bool},
    "comet.auto_log.env_gpu": {"type": bool},
    "comet.auto_log.env_host": {"type": bool},
    "comet.auto_log.git_metadata": {"type": bool},
    "comet.auto_log.git_patch": {"type": bool},
    "comet.auto_log.graph": {"type": bool},
    "comet.auto_log.metrics": {"type": bool},
    "comet.auto_log.figures": {"type": bool, "default": True},
    "comet.auto_log.output_logger": {"type": str},
    "comet.auto_log.parameters": {"type": bool},
    "comet.auto_log.histogram_tensorboard": {"type": bool, "default": False},
    "comet.auto_log.histogram_epoch_rate": {"type": int, "default": 1},
    "comet.auto_log.histogram_weights": {"type": bool, "default": False},
    "comet.auto_log.histogram_gradients": {"type": bool, "default": False},
    "comet.auto_log.histogram_activations": {"type": bool, "default": False},
    "comet.keras.histogram_name_prefix": {
        "type": str,
        "default": "{layer_num:0{max_digits}d}",
    },
    "comet.keras.histogram_activation_index_list": {"type": "int_list", "default": "0"},
    "comet.keras.histogram_activation_layer_list": {"type": list, "default": "-1"},
    "comet.keras.histogram_batch_size": {"type": int, "default": 1000},
    "comet.keras.histogram_gradient_index_list": {"type": "int_list", "default": "0"},
    "comet.keras.histogram_gradient_layer_list": {"type": list, "default": "-1"},
    "comet.auto_log.metric_step_rate": {"type": int, "default": 10},
    "comet.auto_log.co2": {"type": bool},
    "comet.auto_log.tfma": {"type": bool, "default": False},
    "comet.distributed_node_identifier": {"type": str},
    # Internals:
    "comet.internal.reporting": {"type": bool, "default": True},
    "comet.internal.file_upload_worker_ratio": {
        "type": int,
        "default": DEFAULT_POOL_RATIO,
    },
    "comet.internal.worker_count": {"type": int},
    "comet.internal.check_tls_certificate": {"type": bool, "default": True},
    "comet.internal.sentry_dsn": {"type": str},
    "comet.internal.sentry_debug": {"type": bool, "default": False},
    # Deprecated:
    "comet.display_summary": {"type": bool, "default": None},
    "comet.auto_log.weights": {"type": bool, "default": None},
    # Related to `comet_ml.start`
    "comet.resume_strategy": {"type": str, "default": None},
    "comet.offline": {"type": bool, "default": False},
    # Error tracking
    "comet.error_tracking.enable": {"type": bool, "default": True},
    # Related to message batch processing
    "comet.message_batch.use_compression": {
        "type": bool,
        "default": MESSAGE_BATCH_USE_COMPRESSION_DEFAULT,
    },
    "comet.message_batch.metric_interval": {
        "type": float,
        "default": MESSAGE_BATCH_METRIC_INTERVAL_SECONDS,
    },
    "comet.message_batch.metric_max_size": {
        "type": int,
        "default": MESSAGE_BATCH_METRIC_MAX_BATCH_SIZE,
    },
    "comet.message_batch.parameters_interval": {
        "type": int,
        "default": DEFAULT_PARAMETERS_BATCH_INTERVAL_SECONDS,
    },
    "comet.message_batch.stdout_interval": {
        "type": int,
        "default": MESSAGE_BATCH_STDOUT_INTERVAL_SECONDS,
    },
    "comet.message_batch.stdout_max_size": {
        "type": int,
        "default": MESSAGE_BATCH_STDOUT_MAX_BATCH_SIZE,
    },
}


def get_config(setting=None):
    # type: (Any) -> Union[Config, Any]
    """
    Get a config or setting from the current config
    (os.environment or .env file).

    Note: this is not cached, so every time we call it, it
    re-reads the file. This makes these values always up to date
    at the expense of re-getting the data.
    """
    cfg = Config(CONFIG_MAP)
    if setting is None:
        return cfg
    else:
        return cfg[setting]


def get_api_key(api_key, config):
    if api_key is None:
        final_api_key = config["comet.api_key"]
    else:
        final_api_key = api_key

    # Hide api keys from the log
    if final_api_key and config.get_bool(None, "comet.logging.hide_api_key") is True:
        _get_comet_logging_config().redact_string(final_api_key)

    return final_api_key


def discard_api_key(api_key):
    # type: (str) -> None
    """Discards the provided API key as invalid. After this method invocation the discarded key will not be masked in
    the logger output.
    """
    if api_key is not None:
        _get_comet_logging_config().discard_string_from_redact(api_key)


def get_display_summary_level(display_summary_level, config):
    if display_summary_level is None:
        return config["comet.display_summary_level"]
    else:
        try:
            return int(display_summary_level)
        except Exception:
            LOGGER.warning(
                "invalid display_summary_level %r; ignoring", display_summary_level
            )
            return 1


def get_ws_url(ws_server_from_backend, config):
    """Allow users to override the WS url from the backend using the usual
    config mechanism
    """
    ws_server = config.get_string(
        None, "comet.ws_url_override", default=ws_server_from_backend
    )
    # Ws server can be None if the backend stop returning it or when used in `comet_check` for example
    if ws_server is None:
        return ws_server

    return sanitize_url(
        ws_server,
        ending_slash=False,
    )


def get_previous_experiment(previous_experiment, config):
    if previous_experiment is None:
        return config["comet.experiment_key"]
    else:
        return previous_experiment


def save(directory=None, save_all=False, force=False, **settings):
    """
    An easy way to create a config file.

    Args:
        directory: str (optional), location to save the
            .comet.config file. Typical values are "~/" (home)
            and "./" (current directory). Default is "~/" or
            COMET_CONFIG, if set
        save_all: bool (optional). If True, will create
            entries for all items that are configurable
            with their defaults. Default is False
        force: bool (optional). If True, overwrite pre-existing
            .comet.config file. If False, ask.
        settings: any valid setting and value

    Valid settings include:

    * api_key
    * disable_auto_logging
    * experiment_key
    * offline_directory
    * workspace
    * project_name
    * logging_console
    * logging_file
    * logging_file_level
    * logging_file_overwrite
    * timeout_cleaning
    * timeout_upload

    Examples:

    ```python
    >>> import comet_ml
    >>> comet_ml.config.save(api_key="...")
    >>> comet_ml.config.save(api_key="...", directory="./")
    ```
    """
    cfg = get_config()
    cfg._set_settings(settings)
    cfg.save(directory, save_all=save_all, force=force)


def _api_key_save(config_path, api_key):
    # type: (str, str) -> None
    """
    Low-level function to only change the api_key
    of a .comet.config file in the home directory.
    """
    from configobj import ConfigObj

    if os.path.exists(config_path):
        config = ConfigObj(config_path)
        try:
            shutil.copyfile(config_path, config_path + ".bak")
        except Exception:
            LOGGER.warning("Unable to make a backup of config file", exc_info=True)
    else:
        config = ConfigObj()
        config.filename = config_path

    if "comet" not in config:
        config["comet"] = {}

    config["comet"]["api_key"] = api_key
    config.write()
    LOGGER.info("Comet API key saved in %s", config_path)


def init(directory=None, **settings):
    # type: (Optional[str], **Any) -> None
    """
    An easy, safe, interactive way to set and save your settings.

    Will ask for your api_key if not already set. Your
    api_key will not be shown.

    Args:
        directory: str (optional), location to save the
            .comet.config file. Typical values are "~/" (home)
            and "./" (current directory). Default is "~/" or
            COMET_CONFIG, if set.
        settings: any valid setting and value

    Valid settings include:

    * api_key
    * disable_auto_logging
    * experiment_key
    * offline_directory
    * workspace
    * project_name
    * logging_console
    * logging_file
    * logging_file_level
    * logging_file_overwrite
    * timeout_cleaning
    * timeout_upload

    Examples:

    For use in a notebook, or other interactive code:

    ```python
    >>> import comet_ml
    >>> comet_ml.init()
    ```

    You can also provide values for other settings:

    ```python
    >>> import comet_ml
    >>> comet_ml.init(project_name='colab')
    ```
    """
    _init(directory=directory, settings=settings)


def _init_get_api_key(prompt_user, settings, config):
    # type: (bool, Optional[Dict[str, Any]], Config) -> Tuple[Optional[str], bool]

    if settings is None:
        settings = {}

    need_to_save = False
    if "api_key" in settings:
        api_key = settings.pop("api_key", None)
        if api_key:
            need_to_save = True
    else:
        api_key = config.get_string(None, "comet.api_key")

    if api_key is None:
        if prompt_user:
            api_key = get_api_key_from_user()
            if api_key:
                need_to_save = True

    return api_key, need_to_save


def _check_api_key_validity(api_key):
    # type: (str) -> None
    from .api import API

    API(api_key=api_key).get_workspaces()


def _init(directory=None, should_prompt_user=None, settings=None):
    # type: (Optional[str], Optional[bool], Optional[Dict[str, Any]]) -> None
    # We only save the api_key if given by parameter, or
    # by getpass. We don't save it if in the environment

    config = get_config()

    if should_prompt_user is None:
        should_prompt_user = is_interactive()

    api_key, need_to_save = _init_get_api_key(should_prompt_user, settings, config)

    if api_key is None:
        LOGGER.info(API_KEY_IS_NOT_SET)
    else:
        try:
            _check_api_key_validity(api_key)
        except InvalidRestAPIKey:
            LOGGER.error(
                API_KEY_IS_INVALID,
                config.get_string(None, "comet.url_override"),
                exc_info=True,
            )
            return None
        except Exception:
            LOGGER.error(API_KEY_CHECK_FAILED, exc_info=True)
            return None

        # From here we know that the API Key is valid for the configured Comet installation
        LOGGER.info(API_KEY_IS_VALID)

        if need_to_save:
            try:
                if directory is not None:
                    config_path = _config_path_from_directory(directory)
                else:
                    config_path = _get_default_config_path()

                _api_key_save(config_path, api_key)
            except Exception:
                LOGGER.warning("Unable to save Comet API key to disk", exc_info=True)
                return None

    if settings:
        # Set in environment to save:
        config._set_settings(settings, environ=True)


def init_onprem(force=False):
    # type: (bool) -> None

    if force:
        for var in ["COMET_OPTIMIZER_URL", "COMET_URL_OVERRIDE", "COMET_API_KEY"]:
            if var in os.environ:
                del os.environ[var]

    config = get_config()

    client_url = config.get_string(None, "comet.url_override")

    root_url = sanitize_url(get_root_url(client_url))

    if root_url == "https://www.comet-ml.com/" or force:
        LOGGER.info(
            "For help on these settings, please see: https://www.comet.ml/docs/onprem/"
        )

        client_url = _input_user("Please enter your onprem COMET_URL_OVERRIDE: ")

        # Set the environment variables, config.save will save them in the local config file
        os.environ["COMET_URL_OVERRIDE"] = client_url

    # We currently force the user confirmation but it might hang
    # if not in a TTY
    config.save(_prompt_user_confirmation=True, force=force)

    # Set api key if necessary, and save it too
    try:
        api_key, need_to_save = _init_get_api_key(
            prompt_user=True,
            settings={},
            config=config,
        )

        if api_key is None:
            LOGGER.info(API_KEY_IS_NOT_SET)
        else:
            _check_api_key_validity(api_key)

            # From here we know that the API Key is valid for the configured Comet installation
            LOGGER.info(API_KEY_IS_VALID)

            if need_to_save:
                config_path = _get_default_config_path()

                _api_key_save(config_path, api_key)
    except Exception:
        # TODO: Add more specific error message for individual part of the try block
        raise Exception(
            "invalid url or api key; use comet_ml.init_onprem(force=True) or `comet init --onprem --force` to reset"
        )


def _confirm_user_config_file_overwriting(filename):
    # type: (str) -> bool
    prompt = "Are you sure you want to overwrite your %r file? [y/n] " % filename
    if _input_user(prompt).lower().startswith("y"):
        return True
    else:
        return False


def _input_user(prompt):
    """Independent function to apply clean_string to all responses + make mocking easier"""
    return clean_string(six.moves.input(prompt))
