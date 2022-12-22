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

import atexit
import calendar
import getpass
import json
import logging
import os
import os.path
import random
import re
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime

import six
from pkg_resources import DistributionNotFound, get_distribution
from requests.models import PreparedRequest
from six.moves.urllib.parse import urlparse, urlunparse

from ._typing import IO, Any, Dict, Generator, List, Optional, Set, Tuple
from .compat_utils import json_dump, subprocess_run
from .json_encoder import NestedEncoder
from .logging_messages import MISSING_PANDAS_PROFILING

LOGGER = logging.getLogger(__name__)
LOG_ONCE_CACHE = set()  # type: Set[str]


if hasattr(time, "monotonic"):
    get_time_monotonic = time.monotonic
else:
    # Python2 just won't have accurate time durations
    # during clock adjustments, like leap year, etc.
    get_time_monotonic = time.time


def log_once_at_level(logging_level, message, *args, **kwargs):
    """
    Log the given message once at the given level then at the DEBUG
    level on further calls.

    This is a global log-once-per-session, as opposed to the
    log-once-per-experiment.
    """
    global LOG_ONCE_CACHE

    if message not in LOG_ONCE_CACHE:
        LOG_ONCE_CACHE.add(message)
        LOGGER.log(logging_level, message, *args, **kwargs)
    else:
        LOGGER.debug(message, *args, **kwargs)


def merge_url(url, params):
    # type: (str, Dict[Any, Any]) -> str
    """
    Given an URL that might have query strings,
    combine with additional query strings.

    Args:
        url - a url string (perhaps with a query string)
        params - a dict of additional query key/values

    Returns: a string
    """
    req = PreparedRequest()
    req.prepare_url(url, params)
    return req.url


def is_iterable(value):
    try:
        iter(value)
        return True

    except TypeError:
        return False


def is_list_like(value):
    """Check if the value is a list-like"""
    if is_iterable(value) and not isinstance(value, six.string_types):
        return True

    else:
        return False


def to_utf8(str_or_bytes):
    if hasattr(str_or_bytes, "decode"):
        return str_or_bytes.decode("utf-8", errors="replace")

    return str_or_bytes


def local_timestamp():
    # type: () -> int
    """Return a timestamp in a format expected by the backend (milliseconds)"""
    now = datetime.utcnow()
    timestamp_in_seconds = calendar.timegm(now.timetuple()) + (now.microsecond / 1e6)
    timestamp_in_milliseconds = int(timestamp_in_seconds * 1000)
    return timestamp_in_milliseconds


def wait_for_done(check_function, timeout, progress_callback=None, sleep_time=1):
    """Wait up to TIMEOUT seconds for the check function to return True"""
    end_time = time.time() + timeout
    while check_function() is False and time.time() < end_time:
        if progress_callback is not None:
            progress_callback()
        # Wait a max of sleep_time, but keep checking to see if
        # check_function is done. Allows wait_for_empty to end
        # before sleep_time has elapsed:
        end_sleep_time = time.time() + sleep_time
        while check_function() is False and time.time() < end_sleep_time:
            time.sleep(sleep_time / 20.0)


def read_unix_packages(package_status_file="/var/lib/dpkg/status"):
    # type: (str) -> Optional[List[str]]
    if os.path.isfile(package_status_file):
        package = None
        os_packages = []
        with open(package_status_file, "rb") as fp:
            for binary_line in fp:
                line = binary_line.decode("utf-8", errors="ignore").strip()
                if line.startswith("Package: "):
                    package = line[9:]
                if line.startswith("Version: "):
                    version = line[9:]
                    if package is not None:
                        os_packages.append((package, version))
                    package = None
        os_packages_list = sorted(
            [("%s=%s" % (package, version)) for (package, version) in os_packages]
        )
        return os_packages_list
    else:
        return None


def write_file_like_to_tmp_file(file_like_object, tmpdir):
    # type: (IO, str) -> str
    # Copy of `shutil.copyfileobj` with binary / text detection

    buf = file_like_object.read(1)

    tmp_file = tempfile.NamedTemporaryFile(mode="w+b", dir=tmpdir, delete=False)

    encode = False

    # Detect binary/text
    if not isinstance(buf, bytes):
        encode = True
        buf = buf.encode("utf-8")

    tmp_file.write(buf)

    # Main copy loop
    while True:
        buf = file_like_object.read(16 * 1024)

        if not buf:
            break

        if encode:
            buf = buf.encode("utf-8")

        tmp_file.write(buf)

    tmp_file.close()

    return tmp_file.name


ONE_KBYTE = float(1024)
ONE_MBYTE = float(1024 * 1024)
ONE_GBYTE = float(1024 * 1024 * 1024)


def format_bytes(size):
    # type: (float) -> str
    """
    Given a size in bytes, return a sort string representation.
    """
    if size >= ONE_GBYTE:
        return "%.2f %s" % (size / ONE_GBYTE, "GB")
    elif size >= ONE_MBYTE:
        return "%.2f %s" % (size / ONE_MBYTE, "MB")
    elif size >= ONE_KBYTE:
        return "%.2f %s" % (size / ONE_KBYTE, "KB")
    else:
        return "%d %s" % (size, "bytes")


def get_file_extension(file_path):
    if file_path is None:
        return None

    ext = os.path.splitext(file_path)[1]
    if not ext:
        return None

    # Get rid of the leading "."
    return ext[1::]


def encode_metadata(metadata):
    # type: (Optional[Dict[Any, Any]]) -> Optional[str]
    if metadata is None:
        return None

    if type(metadata) is not dict:
        LOGGER.info("invalid metadata, expecting dict type", exc_info=True)
        return None

    if metadata == {}:
        return None

    try:
        json_encoded = json.dumps(metadata, separators=(",", ":"), sort_keys=True)
        return json_encoded
    except Exception:
        LOGGER.info("invalid metadata, expecting JSON-encodable object", exc_info=True)
        return None


def get_comet_version():
    # type: () -> str
    try:
        return get_distribution("comet_ml").version
    except DistributionNotFound:
        return "Please install comet with `pip install comet_ml`"


def get_user():
    # type: () -> str
    try:
        return getpass.getuser()
    except KeyError:
        # We are in a system with no user, like Docker container with custom UID
        return "unknown"
    except Exception:
        LOGGER.debug(
            "Unknown exception getting the user from the system", exc_info=True
        )
        return "unknown"


def log_asset_folder(folder, recursive=False, extension_filter=None):
    # type: (str, bool, Optional[List[str]]) -> Generator[Tuple[str, str], None, None]
    extension_filter_set = None

    if extension_filter is not None:
        extension_filter_set = set(extension_filter)

    if recursive:
        for dirpath, _, file_names in os.walk(folder):
            for file_name in file_names:

                if extension_filter_set:
                    file_extension = os.path.splitext(file_name)[1]

                    if file_extension not in extension_filter_set:
                        continue

                file_path = os.path.join(dirpath, file_name)
                yield (file_name, file_path)
    else:
        file_names = sorted(os.listdir(folder))
        for file_name in file_names:
            file_path = os.path.join(folder, file_name)
            if os.path.isfile(file_path):

                if extension_filter_set:
                    file_extension = os.path.splitext(file_name)[1]
                    if file_extension not in extension_filter_set:
                        continue

                yield (file_name, file_path)


def parse_version_number(raw_version_number):
    # type: (str) -> Tuple[int, int, int]
    """
    Parse a valid "INT.INT.INT" string, or raise an
    Exception. Exceptions are handled by caller and
    mean invalid version number.
    """
    converted_version_number = [int(part) for part in raw_version_number.split(".")]

    if len(converted_version_number) != 3:
        raise ValueError(
            "Invalid version number %r, parsed as %r",
            raw_version_number,
            converted_version_number,
        )

    # Make mypy happy
    version_number = (
        converted_version_number[0],
        converted_version_number[1],
        converted_version_number[2],
    )
    return version_number


def format_version_number(version_number):
    # type: (Tuple[int, int, int]) -> str
    return ".".join(map(str, version_number))


def valid_ui_tabs(tab=None, preferred=False):
    """
    List of valid UI tabs in browser.
    """
    mappings = {
        "artifacts": "artifacts",
        "assets": "assets",
        "audio": "audio",
        "charts": "chart",
        "code": "code",
        "confusion-matrices": "confusionMatrix",
        "graphics": "images",
        "histograms": "histograms",
        "installed-packages": "installedPackages",
        "metrics": "metrics",
        "notes": "notes",
        "parameters": "params",
        "system-metrics": "systemMetrics",
        "text": "text",
        "output": "stdout",
        "panels": "panels",
        "graph": "graph",
        "other": "other",
        "html": "html",
    }
    preferred_names = list(mappings.keys())
    # Additional keys:
    mappings.update(
        {
            "assetStorage": "assets",
            "chart": "chart",
            "confusion-matrix": "confusionMatrix",
            "confusionMatrix": "confusionMatrix",
            "images": "images",
            "installedPackages": "installedPackages",
            "params": "params",
            "systemMetrics": "systemMetrics",
            "graph-def": "graph",
            "graph-definition": "graph",
            "stdout": "stdout",
        }
    )
    if preferred:
        return preferred_names
    elif tab is None:
        return list(mappings.keys())
    elif tab in mappings:
        return mappings[tab]
    else:
        raise ValueError("invalid tab name; tab should be in %r" % preferred_names)


def shape(data):
    """
    Given a nested list or a numpy array,
    return the shape.
    """
    if hasattr(data, "shape"):
        return list(data.shape)
    else:
        try:
            length = len(data)
            return [length] + shape(data[0])
        except TypeError:
            return []


def tensor_length(data):
    """
    Get the length of a tensor/list.
    """
    if hasattr(data, "shape"):
        return data.shape[0]
    else:
        try:
            length = len(data)
        except TypeError:
            length = 0
    return length


def makedirs(name, exist_ok=False):
    """
    Replacement for Python2's version lacking exist_ok
    """
    if not os.path.exists(name) or not exist_ok:
        os.makedirs(name)


def clean_and_check_root_relative_path(root, relative_path):
    # type: (str, str) -> str
    """
    Given a root and a relative path, resolve the relative path to get an
    absolute path and make sure the resolved path is a child to root. Cases
    where it could not be the case would be if the `relative_path` contains `..`
    or if one part of the relative path is a symlink going above the root.

    Return the absolute resolved path and raises a ValueError if the root path
    is not absolute or if the resolved relative path goes above the root.
    """
    if not os.path.isabs(root):
        raise ValueError("Root parameter %r should an absolute path" % root)

    if not root.endswith(os.sep):
        root = root + os.sep

    real_root = os.path.realpath(root)

    joined_path = os.path.join(real_root, relative_path)
    resolved_path = os.path.realpath(joined_path)

    if not resolved_path.startswith(real_root):
        raise ValueError("Final path %r is outside of %r" % (resolved_path, real_root))

    return resolved_path


def check_if_path_relative_to_root(root, absolute_path):
    # type: (str, str) -> bool
    if not os.path.isabs(root):
        raise ValueError("Root parameter %r should an absolute path" % root)

    root_full_path = os.path.realpath(root) + os.sep
    full_path = os.path.realpath(absolute_path)

    return full_path.startswith(root_full_path)


def verify_data_structure(datatype, data):
    # Raise an error if anything wrong
    if datatype == "curve":
        if (
            ("x" not in data)
            or ("y" not in data)
            or ("name" not in data)
            or (not isinstance(data["name"], str))
            or (len(data["x"]) != len(data["y"]))
        ):
            raise ValueError(
                "'curve' requires lists 'x' and 'y' of equal lengths, and string 'name'"
            )
    else:
        raise ValueError("invalid datatype %r: datatype must be 'curve'" % datatype)


def proper_registry_model_name(name):
    """
    A proper registry model name is:
        * lowercase
        * replaces all non-alphanumeric with dashes
        * removes leading and trailing dashes
        * limited to 1 dash in a row
    """
    name = "".join([(char if char.isalnum() else "-") for char in name])
    while name.startswith("-"):
        name = name[1:]
    while name.endswith("-"):
        name = name[:-1]
    name = name.lower()
    while "--" in name:
        name = name.replace("--", "-")
    return name


def is_interactive():
    """
    Returns True if in interactive mode
    """
    return bool(getattr(sys, "ps1", sys.flags.interactive))


def safe_filename(filename):
    """
    Given a value, turn it into a valid filename.

    1. Remove the spaces
    2. Replace anything not alpha, '-', '_', or '.' with '_'
    3. Remove duplicate '_'
    """
    string = str(filename).strip().replace(" ", "_")
    string = re.sub(r"(?u)[^-\w.]", "_", string)
    return re.sub(r"_+", "_", string)


def get_dataframe_profile_html(dataframe, minimal):
    # type: (Any, Optional[bool]) -> Optional[str]
    """
    Log a pandas dataframe profile.
    """
    try:
        import pandas_profiling

    except ImportError:
        LOGGER.warning(MISSING_PANDAS_PROFILING)
        return None
    except Exception:
        LOGGER.warning(MISSING_PANDAS_PROFILING, exc_info=True)
        return None

    if minimal:  # earlier versions of pandas_profiling did not support minimal
        profile = pandas_profiling.ProfileReport(dataframe, minimal=minimal)
    else:
        profile = pandas_profiling.ProfileReport(dataframe)
    html = profile.to_html()  # type: str
    html = html.replace("http:", "https:")
    return html


def clean_string(string):
    if string:
        return "".join([char for char in string if char not in ["'", '"', " "]])
    else:
        return ""


def get_api_key_from_user():
    """
    Get the Comet API key fromthe user.
    """
    from .config import get_config

    client_url = get_config("comet.url_override")
    root_url = sanitize_url(get_root_url(client_url))

    print(
        "Please enter your Comet API key from {root_url}api/my/settings/".format(
            root_url=root_url
        )
    )
    print("(api key may not show as you type)")
    api_key = clean_string(getpass.getpass("Comet API key: "))
    return api_key


def parse_remote_uri(uri):
    # type: (Optional[str]) -> Optional[str]
    if not uri:
        return None

    try:
        parsed = urlparse(uri)
        if parsed.path:
            # Split the path
            return parsed.path.split("/")[-1]
        else:
            return None
    except Exception:
        LOGGER.debug("Failure parsing URI %r", uri, exc_info=True)
        return None


def make_template_filename(group=None):
    if group is None:
        group = random.randint(0, 10000000)

    return "template_projector_config-%s.json" % group


class ImmutableDict(dict):
    def _immutable(self, *args, **kws):
        raise TypeError("object is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    update = _immutable
    setdefault = _immutable
    pop = _immutable
    popitem = _immutable


class IterationProgressCallback(object):
    def __init__(self, iterable, callback, frequency):
        self._iterable = iterable
        self._callback = callback
        self._frequency = frequency

    def __iter__(self):
        last = get_time_monotonic()

        for obj in self._iterable:
            yield obj

            now = get_time_monotonic()
            diff = now - last
            if diff > self._frequency:
                last = now
                try:
                    self._callback()
                except Exception:
                    LOGGER.debug("Error calling the progress callback", exc_info=True)


def atexit_unregister(func):
    # type: (Any) -> None
    if six.PY2:
        to_delete = []

        # First iterate to detect exithandlers to remove
        for exithandlers in atexit._exithandlers:
            if exithandlers[0] == func:
                to_delete.append(exithandlers)

        # Then remove them but we cannot assume that the list hasn't changed, so uses remove which is inefficient
        for exithandler_to_delete in to_delete:
            try:
                atexit._exithandlers.remove(exithandler_to_delete)
            except ValueError:
                pass
    else:
        return atexit.unregister(func)


def generate_guid():
    # type: () -> str
    """Generate a GUID"""
    return uuid.uuid4().hex


def compact_json_dump(data, fp):
    return json_dump(data, fp, sort_keys=True, separators=(",", ":"), cls=NestedEncoder)


def get_root_url(url):
    # type: (str) -> str
    """Remove the path, params, query and fragment from a given URL"""
    parts = urlparse(url)
    scheme, netloc, path, params, query, fragment = parts

    return urlunparse((scheme, netloc, "", "", "", ""))


def sanitize_url(url, ending_slash=True):
    # type: (str, Optional[bool]) -> str
    """Sanitize an URL, checking that it is a valid URL and ensure it contains an ending slash / or
    that it doesn't contains an ending slash depending on the value of ending_slash"""
    parts = urlparse(url)
    scheme, netloc, path, params, query, fragment = parts

    # TODO: Raise an exception if params, query and fragment are not empty?

    # Ensure the leading slash
    if ending_slash is True:
        if path and not path.endswith("/"):
            path = path + "/"
        elif not path and not netloc.endswith("/"):
            netloc = netloc + "/"
    else:
        if path and path.endswith("/"):
            path = path[:-1]

    return urlunparse((scheme, netloc, path, params, query, fragment))


def metric_name(name, prefix=None):
    # type: (str, Optional[str]) -> str
    if prefix:
        return "%s.%s" % (prefix, name)
    else:
        return name


def subprocess_run_and_check(
    args, timeout, exception_warning_log_message, return_code_warning_log_message
):
    # type: (List[str], int, str, str) -> Optional[bytes]
    """Run the command passed as a List of string with the given timeout (ignored on Python 2.7).

    If the command ran but it's exit code is not 0, show the return_code_warning_log_message as a
    Warning log, logs the stdout/stderr in debug logs and return None.

    If any other error occurs, show the exception_warning_log_message as a Warning log and return
    None.
    """
    try:
        completed_process = subprocess_run(args, timeout=timeout)
    except Exception:
        LOGGER.warning(exception_warning_log_message, exc_info=True)
        return None

    try:
        completed_process.check_returncode()
    except subprocess.CalledProcessError as exception:
        LOGGER.warning(return_code_warning_log_message, exc_info=True)
        LOGGER.debug("Conda env export stdout: %r", exception.output)
        LOGGER.debug("Conda env export stderr: %r", exception.stderr)
        return None
    except Exception:
        LOGGER.warning(exception_warning_log_message, exc_info=True)
        return None

    return completed_process.stdout
