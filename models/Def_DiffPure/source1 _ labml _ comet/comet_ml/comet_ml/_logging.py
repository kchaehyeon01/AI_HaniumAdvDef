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

"""
Author: Boris Feld

This module contains logging configuration for Comet

"""
import json
import logging
import os
import re
from copy import copy
from datetime import datetime

import requests
import sentry_sdk.integrations.logging

from .json_encoder import NestedEncoder
from .logging_messages import FILE_MSG_FORMAT, MSG_FORMAT
from .utils import get_user, makedirs

LOGGER = logging.getLogger(__name__)


class TracebackLessFormatter(logging.Formatter):

    REDACTED_STRING = "*" * 9 + "REDACTED" + "*" * 9

    def __init__(self, *args, **kwargs):
        hide_traceback = kwargs.pop("hide_traceback", False)
        super(TracebackLessFormatter, self).__init__(*args, **kwargs)

        self.hide_traceback = hide_traceback
        self.strings_to_redact = set()

    def format(self, record):

        if (
            getattr(record, "show_traceback", False) is False
            and self.hide_traceback is True
        ):

            # Make a copy of the record to avoid altering it
            new_record = copy(record)

            # And delete exception information so no traceback could be formatted
            # and displayed
            new_record.exc_info = None
            new_record.exc_text = None
        else:
            new_record = record

        result = super(TracebackLessFormatter, self).format(new_record)
        # If s is not in result, it's faster to check first before doing the substring replacement
        for s in self.strings_to_redact:
            # Avoid redacting strings if they are passed explicitely as log records
            if s in result and s not in record.args:
                result = result.replace(s, self.REDACTED_STRING)
        return result


def shorten_record_name(record_name):
    """Return the first part of the record (which can be None, comet or
    comet.connection)
    """
    if record_name is None:
        return record_name

    return record_name.split(".", 1)[0]


class HTTPHandler(logging.Handler):
    def __init__(self, url, api_key, experiment_key):
        super(HTTPHandler, self).__init__()
        self.url = url
        self.api_key = api_key
        self.experiment_key = experiment_key
        self.session = requests.Session()

    def mapLogRecord(self, record):
        return record.__dict__

    def emit(self, record):
        """
        Emit a record.

        Send the record to the Web server as JSON body
        """
        try:
            payload = {
                "apiKey": self.api_key,
                "record": self.mapLogRecord(record),
                "experimentKey": self.experiment_key,
                "levelname": record.levelname,
                "sender": record.name,
                "shortSender": shorten_record_name(record.name),
            }
            body = json.dumps(payload, cls=NestedEncoder)

            response = self.session.post(
                self.url,
                data=body,
                headers={"Content-Type": "application/json;charset=utf-8"},
                timeout=10,
            )
            response.raise_for_status()
        except Exception:
            self.handleError(record)

    def close(self):
        super(HTTPHandler, self).close()
        self.session.close()

    def handleError(self, record):
        # Hide errors to avoid bad interaction with console logging
        pass


def expand_log_file_path(log_file_path, project_name=None):
    """
    Expand patterns in the file logging path.

    Allowed patterns:
        * {datetime}
        * {pid}
        * {project}
        * {user}
    """

    if project_name is None:
        project_name = "general"

    def make_valid(s):
        # type: (str) -> str
        s = str(s).strip().replace(" ", "_")
        return re.sub(r"(?u)[^-\w.]", "", s)

    user = make_valid(get_user())

    patterns = {
        "datetime": datetime.now().strftime("%Y%m%d-%H%M%S"),
        "pid": os.getpid(),
        "project": project_name,
        "user": user,
    }
    if log_file_path is not None:
        try:
            return log_file_path.format(**patterns)
        except KeyError:
            LOGGER.info(
                "Invalid logging file pattern: '%s'; ignoring",
                log_file_path,
                exc_info=True,
            )
            return log_file_path


class CometLoggingConfig(object):
    def __init__(self):
        self.root = None
        self.console_handler = None
        self.console_formatter = None
        self.file_handler = None
        self.file_formatter = None

    def setup(self, config):
        self.root = logging.getLogger("comet_ml")
        logger_level = logging.CRITICAL

        # Don't send comet-ml to the application logger
        self.root.propagate = False

        # Add handler for console, basic INFO:
        self.console_handler = logging.StreamHandler()
        logging_console = config["comet.logging.console"]

        if logging_console and logging_console.upper() in [
            "DEBUG",
            "ERROR",
            "INFO",
            "CRITICAL",
            "FATAL",
            "WARN",
            "WARNING",
        ]:
            logging_console_level = logging._checkLevel(logging_console.upper())
            self.console_formatter = TracebackLessFormatter(
                MSG_FORMAT, hide_traceback=False
            )
        else:
            logging_console_level = logging.INFO
            self.console_formatter = TracebackLessFormatter(
                MSG_FORMAT, hide_traceback=True
            )

        self.console_handler.setLevel(logging_console_level)
        self.console_handler.setFormatter(self.console_formatter)
        self.root.addHandler(self.console_handler)

        logger_level = min(logger_level, self.console_handler.level)

        # The std* logger might conflicts with the logging if a log record is
        # emitted for each WS message as it would results in an infinite loop. To
        # avoid this issue, all log records after the creation of a message should
        # be at a level lower than info as the console handler is set to info
        # level.

        # Add an additional file handler
        log_file_path = expand_log_file_path(
            config["comet.logging.file"], config["comet.project_name"]
        )
        log_file_level = config["comet.logging.file_level"]
        log_file_overwrite = config["comet.logging.file_overwrite"]

        self.file_handler = None
        self.file_formatter = None

        if log_file_path is not None:

            # Create logfile path, if possible:
            try:
                makedirs(os.path.dirname(log_file_path), exist_ok=True)
            except Exception:
                LOGGER.error(
                    "can't create path to log file %r", log_file_path, exc_info=True
                )

            try:
                # Overwrite file if comet.logging.file_overwrite:
                if log_file_overwrite:
                    self.file_handler = logging.FileHandler(log_file_path, "w+")
                else:
                    self.file_handler = logging.FileHandler(log_file_path)

                if log_file_level is not None:
                    log_file_level = logging._checkLevel(log_file_level.upper())
                else:
                    log_file_level = logging.DEBUG

                self.file_handler.setLevel(log_file_level)
                logger_level = min(logger_level, log_file_level)

                self.file_formatter = TracebackLessFormatter(
                    FILE_MSG_FORMAT, hide_traceback=False
                )
                self.file_handler.setFormatter(self.file_formatter)
                self.root.addHandler(self.file_handler)
            except Exception:
                LOGGER.error(
                    "can't open log file %r; file logging is disabled",
                    log_file_path,
                    exc_info=True,
                )

        self.root.setLevel(logger_level)

    def redact_string(self, string):
        # type: (str) -> None
        """Blacklists specified string from logging. The string will be masked in the logging."""
        if self.console_formatter:
            self.console_formatter.strings_to_redact.add(string)

        if self.file_formatter:
            self.file_formatter.strings_to_redact.add(string)

    def discard_string_from_redact(self, string):
        # type: (str) -> None
        """Discards specified string from the set of blacklisted strings. After this method invocation the string will
        no longer be masked from the logger output.
        """
        if self.console_formatter:
            self.console_formatter.strings_to_redact.discard(string)

        if self.file_formatter:
            self.file_formatter.strings_to_redact.discard(string)


COMET_LOGGING_CONFIG = CometLoggingConfig()


def _get_comet_logging_config():
    return COMET_LOGGING_CONFIG


def _setup_comet_logging(config):
    COMET_LOGGING_CONFIG.setup(config)


def _setup_comet_http_handler(url, api_key, experiment_key):
    root = logging.getLogger("comet_ml")

    http_handler = HTTPHandler(url, api_key, experiment_key)
    http_handler_level = logging.INFO
    http_handler.setLevel(http_handler_level)

    # Remove any other previous HTTPHandlers:
    root.handlers[:] = [
        handler
        for handler in list(root.handlers)
        if not isinstance(handler, HTTPHandler)
    ]

    # Add new HTTPHandler. Only the current experiment should
    # have its logs forwarded to the backend.
    root.addHandler(http_handler)

    # Ensure the logger level is low enough
    root.setLevel(min(root.level, http_handler_level))

    return http_handler


def _setup_sentry_error_handlers(logger):
    # type: (logging.Logger) -> None
    """Add the two Sentry logging handlers to send error messages with their traceback to Sentry
    for error tracking. This is safe to call even if Sentry is not setup.
    """
    # This handler sends log message as errors to Sentry
    error_handler = sentry_sdk.integrations.logging.EventHandler(logging.ERROR)

    # This handler track info log message which are sent as breadcrumbs when an error happens
    breadcrumbs_handler = sentry_sdk.integrations.logging.BreadcrumbHandler(
        logging.INFO
    )

    _add_singleton_logging_handler(logger, error_handler)
    _add_singleton_logging_handler(logger, breadcrumbs_handler)


def _add_singleton_logging_handler(logger, handler):
    handler_type = type(handler)
    already_exists = any(
        isinstance(handler, handler_type) for handler in logger.handlers
    )
    if already_exists:
        LOGGER.debug(
            "A %s logging handler is already in the logger %s handler, skipping...",
            handler_type,
            logger,
        )
        return

    logger.addHandler(handler)
