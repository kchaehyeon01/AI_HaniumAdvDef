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

""" This module handles syncing git repos with the backend. Used for pull
request features."""

import io
import json
import logging
import os
import shutil
import tempfile
from collections import namedtuple

import six

from ._typing import (
    IO,
    Any,
    Callable,
    Dict,
    Optional,
    TemporaryFilePath,
    Union,
    UserText,
    ValidFilePath,
)
from .convert_utils import (
    convert_pathlib_path,
    data_to_fp,
    image_data_to_file_like_object,
    write_numpy_array_as_wav,
)
from .exceptions import AssetIsTooBig
from .logging_messages import (
    LOG_AUDIO_TOO_BIG,
    LOG_FIGURE_TOO_BIG,
    LOG_IMAGE_TOO_BIG,
    UPLOAD_ASSET_TOO_BIG,
    UPLOAD_FILE_OS_ERROR,
)
from .messages import RemoteAssetMessage, UploadFileMessage, UploadInMemoryMessage
from .utils import (
    generate_guid,
    get_file_extension,
    parse_remote_uri,
    write_file_like_to_tmp_file,
)
from .validation_utils import validate_metadata

LOGGER = logging.getLogger(__name__)

try:
    import numpy
except ImportError:
    LOGGER.warning("numpy not installed; some functionality will be unavailable")
    pass

try:
    from plotly.graph_objects import Figure as PlotlyFigure
except ImportError:
    PlotlyFigure = None


def is_valid_file_path(file_path):
    # type: (Any) -> bool
    """Check if the given argument is corresponding to a valid file path,
    ready for reading
    """
    try:
        if os.path.isfile(file_path):
            return True
        else:
            return False
    # We can receive lots of things as arguments
    except (TypeError, ValueError):
        return False


def is_user_text(input):
    # type: (Any) -> bool
    return isinstance(input, (six.string_types, bytes))


# Requests accepts either a file-object (IO, StringIO and BytesIO), a file path, string.
# We also accepts specific inputs for each logging method


def check_max_file_size(file_path, max_upload_size, too_big_msg):
    # type: (str, int, str) -> int
    """Check if a file identified by its file path is bigger than the maximum
    allowed upload size. Raises AssetIsTooBig if the file is greater than the
    upload limit.
    """

    # Check the file size before reading it
    try:
        file_size = os.path.getsize(file_path)
        if file_size > max_upload_size:
            raise AssetIsTooBig(file_path, file_size, max_upload_size)

        return file_size

    except OSError:
        LOGGER.error(too_big_msg, file_path, exc_info=True)
        raise


def save_matplotlib_figure(figure=None):
    # type: (Optional[Any]) -> str
    """Try saving either the current global pyplot figure or the given one
    and return None in case of error.
    """
    # Get the right figure to upload
    if figure is None:
        import matplotlib.pyplot

        # Get current global figure
        figure = matplotlib.pyplot.gcf()

    if hasattr(figure, "gcf"):
        # The pyplot module was passed as figure
        figure = figure.gcf()

    if figure.get_axes():
        # Save the file to a tempfile but don't delete it, the file uploader
        # thread will take care of it
        tmpfile = tempfile.NamedTemporaryFile(suffix=".svg", delete=False)
        figure.savefig(tmpfile, format="svg")
        tmpfile.flush()
        tmpfile.close()

        return tmpfile.name
    else:
        # TODO DISPLAY BETTER ERROR MSG
        msg = (
            "Refuse to upload empty figure, please call log_figure before calling show"
        )
        LOGGER.warning(msg)
        raise TypeError(msg)


def save_plotly_figure(figure):
    # type: (Optional[Any]) -> str
    """
    Save the Plotly Figure as an image.
    """
    # Save the file to a tempfile but don't delete it, the file uploader
    # thread will take care of it
    tmpfile = tempfile.NamedTemporaryFile(suffix=".svg", delete=False)
    figure.write_image(tmpfile, format="svg")
    tmpfile.flush()
    tmpfile.close()
    return tmpfile.name


def total_len(o):
    if hasattr(o, "__len__"):
        return len(o)

    if hasattr(o, "getvalue"):
        # e.g. BytesIO, cStringIO.StringIO
        return len(o.getvalue())

    if hasattr(o, "fileno"):
        try:
            fileno = o.fileno()
        except OSError:
            pass
        else:
            return os.fstat(fileno).st_size

    raise NotImplementedError(
        "Don't know how to compute total_len for %r[%r]", o, o.__class__
    )


class AssetUploadUserInput(object):
    def __init__(self, user_input):
        self.user_input = user_input


class FileUpload(AssetUploadUserInput):
    def __init__(self, file_path):
        super(FileUpload, self).__init__(file_path)

        try:
            self.size = os.path.getsize(file_path)
        except OSError:
            LOGGER.debug("Error retrieving file size for %r", file_path)
            self.size = 0


class MemoryFileUpload(AssetUploadUserInput):
    def __init__(self, user_input):
        super(MemoryFileUpload, self).__init__(user_input)

        try:
            self.size = total_len(user_input)
        except Exception:
            LOGGER.debug("Error retrieving size for %r", user_input)
            self.size = 0


class UserTextFileUpload(AssetUploadUserInput):
    def __init__(self, user_input):
        super(UserTextFileUpload, self).__init__(user_input)

        try:
            self.size = total_len(user_input)
        except Exception:
            LOGGER.debug("Error retrieving size for %r", user_input)
            self.size = 0


class ObjectToConvertFileUpload(AssetUploadUserInput):
    def __init__(self, user_input):
        super(ObjectToConvertFileUpload, self).__init__(user_input)
        self.size = 0


def dispatch_user_file_upload(user_input):
    # type: (Any) -> Union[FileUpload, MemoryFileUpload, UserTextFileUpload, ObjectToConvertFileUpload]

    user_input = convert_pathlib_path(user_input)

    if isinstance(user_input, ValidFilePath) or is_valid_file_path(user_input):
        return FileUpload(user_input)
    elif hasattr(user_input, "read"):  # Support Python 2 legacy StringIO
        return MemoryFileUpload(user_input)
    elif is_user_text(user_input):
        return UserTextFileUpload(user_input)
    else:
        return ObjectToConvertFileUpload(user_input)


class BaseUploadProcessor(object):

    TOO_BIG_MSG = ""
    UPLOAD_TYPE = ""

    def __init__(
        self,
        user_input,  # type: Any
        upload_limit,  # type: int
        url_params,  # type: Optional[Dict[str, Optional[Any]]]
        metadata,  # type: Optional[Dict[str, str]]
        copy_to_tmp,  # type: bool
        error_message_identifier,  # type: Any
        tmp_dir,  # type: str
        critical,  # type: bool
        on_asset_upload=None,
        on_failed_asset_upload=None,
    ):
        # type: (...) -> None
        self.user_input = user_input
        self.url_params = url_params
        self.metadata = validate_metadata(metadata)
        self.upload_limit = upload_limit
        self.error_message_identifier = error_message_identifier
        self.tmp_dir = tmp_dir
        self.file_size = None  # type: Optional[int]
        self.critical = critical
        self.on_asset_upload = on_asset_upload
        self.on_failed_asset_upload = on_failed_asset_upload

        self.copy_to_tmp = copy_to_tmp

        LOGGER.debug("%r created with %r", self, self.__dict__)

    def process(self):
        # type: () -> Union[None, UploadInMemoryMessage, UploadFileMessage]

        user_input = convert_pathlib_path(self.user_input)

        if isinstance(user_input, ValidFilePath) or is_valid_file_path(user_input):
            return self.process_upload_by_filepath(user_input)
        elif hasattr(user_input, "read"):  # Support Python 2 legacy StringIO
            return self.process_io_object(user_input)
        elif is_user_text(user_input):
            return self.process_user_text(user_input)
        else:
            return self.process_upload_to_be_converted(user_input)

    # Dispatched user input method, one method per supported type in general. By
    # default those methods raise an exception, implement them for supported
    # input type per upload type

    def process_upload_by_filepath(self, upload_filepath):
        # type: (ValidFilePath) -> Optional[UploadFileMessage]
        raise TypeError("Unsupported upload input %r" % type(upload_filepath))

    def process_upload_to_be_converted(self, user_input):
        # type: (Any) -> Union[None, UploadInMemoryMessage, UploadFileMessage]
        raise TypeError("Unsupported upload input %r" % type(user_input))

    def process_io_object(self, io_object):
        # type: (IO) -> Union[None, UploadInMemoryMessage, UploadFileMessage]
        raise TypeError("Unsupported upload input %r" % type(io_object))

    def process_user_text(self, user_text):
        # type: (UserText) -> Union[None, UploadInMemoryMessage, UploadFileMessage]
        raise TypeError("Unsupported upload input %r" % user_text)

    # Low-level common code, once we have either an IO object or a filepath to upload

    def _process_upload_by_filepath(self, user_filepath):
        # type: (ValidFilePath) -> Optional[UploadFileMessage]
        try:
            self.file_size = check_max_file_size(
                user_filepath, self.upload_limit, self.TOO_BIG_MSG
            )
        except AssetIsTooBig as exc:
            if self.error_message_identifier is None:
                error_message_identifier = exc.file_path
            else:
                error_message_identifier = self.error_message_identifier

            LOGGER.error(
                self.TOO_BIG_MSG, error_message_identifier, exc.file_size, exc.max_size
            )
            return None
        except Exception:
            LOGGER.debug("Error while checking the file size", exc_info=True)
            return None

        upload_filepath = self._handle_in_memory_file_upload(user_filepath)

        # If we failed to copy the file, abort
        if not upload_filepath:
            return None

        LOGGER.debug(
            "File upload message %r, type %r, params %r",
            upload_filepath,
            self.UPLOAD_TYPE,
            self.url_params,
        )

        # Clean only temporary files
        if isinstance(upload_filepath, TemporaryFilePath):
            clean = True
        else:
            clean = False

        if self.copy_to_tmp and not isinstance(upload_filepath, TemporaryFilePath):
            LOGGER.warning(
                "File %s should have been copied to a temporary location but was not",
                upload_filepath,
            )

        upload_message = UploadFileMessage(
            upload_filepath,
            self.UPLOAD_TYPE,
            self.url_params,
            self.metadata,
            size=0,  # TODO: Replace by pre-processing
            clean=clean,
            critical=self.critical,
            on_asset_upload=self.on_asset_upload,
            on_failed_asset_upload=self.on_failed_asset_upload,
        )

        return upload_message

    def _handle_in_memory_file_upload(self, upload_filepath):
        # type: (ValidFilePath) -> Union[None, ValidFilePath, TemporaryFilePath]
        # If we cannot remove the uploaded file or need the file content will
        # be frozen to the time the upload call is made, pass copy_to_tmp with
        # True value
        if self.copy_to_tmp is True and not isinstance(
            upload_filepath, TemporaryFilePath
        ):
            tmpfile = tempfile.NamedTemporaryFile(delete=False)
            tmpfile.close()
            LOGGER.debug(
                "Copying %s to %s because of copy_to_tmp", upload_filepath, tmpfile.name
            )
            try:
                shutil.copyfile(upload_filepath, tmpfile.name)
            except (OSError, IOError):
                LOGGER.error(UPLOAD_FILE_OS_ERROR, upload_filepath, exc_info=True)
                return None
            upload_filepath = TemporaryFilePath(tmpfile.name)

        return upload_filepath

    def _process_upload_io(self, io_object):
        # type: (IO) -> Union[None, UploadInMemoryMessage, UploadFileMessage]
        if self.copy_to_tmp:
            LOGGER.debug("Saving IO to tmp_file because of copy_to_tmp")
            # Convert the file-like to a temporary file on disk
            file_path = write_file_like_to_tmp_file(io_object, self.tmp_dir)
            self.copy_to_tmp = False

            # TODO it would be easier to use the same field name for a file or a figure upload
            if "fileName" in self.url_params and self.url_params["fileName"] is None:
                self.url_params["fileName"] = os.path.basename(file_path)

            if "figName" in self.url_params and self.url_params["figName"] is None:
                self.url_params["figName"] = os.path.basename(file_path)

            return self._process_upload_by_filepath(TemporaryFilePath(file_path))

        LOGGER.debug(
            "File-like upload message %r, type %r, params %r",
            io_object,
            self.UPLOAD_TYPE,
            self.url_params,
        )

        return UploadInMemoryMessage(
            io_object,
            self.UPLOAD_TYPE,
            self.url_params,
            self.metadata,
            size=0,  # TODO: Replace by pre-processing
            critical=self.critical,
            on_asset_upload=self.on_asset_upload,
            on_failed_asset_upload=self.on_failed_asset_upload,
        )

    def _process_upload_text(self, user_text):
        # type: (UserText) -> Union[None, UploadInMemoryMessage, UploadFileMessage]
        if self.copy_to_tmp:
            # TODO: Be more efficient here
            io_object = data_to_fp(user_text)

            if not io_object:
                # We couldn't convert to an io_object
                return None

            file_path = write_file_like_to_tmp_file(io_object, self.tmp_dir)

            return self._process_upload_by_filepath(TemporaryFilePath(file_path))

        LOGGER.debug(
            "Text upload message %r, type %r, params %r",
            user_text,
            self.UPLOAD_TYPE,
            self.url_params,
        )

        return UploadInMemoryMessage(
            user_text,
            self.UPLOAD_TYPE,
            self.url_params,
            self.metadata,
            size=0,  # TODO: Replace by pre-processing
            critical=self.critical,
            on_asset_upload=self.on_asset_upload,
            on_failed_asset_upload=self.on_failed_asset_upload,
        )


class AssetUploadProcessor(BaseUploadProcessor):

    TOO_BIG_MSG = UPLOAD_ASSET_TOO_BIG

    def __init__(
        self,
        user_input,  # type: Any
        upload_type,  # type: str
        url_params,  # type: Dict[str, Optional[Any]]
        metadata,  # type: Optional[Dict[str, str]]
        upload_limit,  # type: int
        copy_to_tmp,  # type: bool
        error_message_identifier,  # type: Any
        tmp_dir,  # type: str
        critical,  # type: bool
        on_asset_upload=None,
        on_failed_asset_upload=None,
    ):
        # type: (...) -> None
        self.UPLOAD_TYPE = upload_type

        super(AssetUploadProcessor, self).__init__(
            user_input,
            upload_limit,
            url_params,
            metadata,
            copy_to_tmp,
            error_message_identifier,
            tmp_dir,
            critical,
            on_asset_upload,
            on_failed_asset_upload,
        )

    def process_upload_by_filepath(self, upload_filepath):
        # type: (ValidFilePath) -> Optional[UploadFileMessage]

        if self.url_params["fileName"] is None:
            self.url_params["fileName"] = os.path.basename(upload_filepath)

        self.url_params["extension"] = get_file_extension(upload_filepath)

        return self._process_upload_by_filepath(upload_filepath)

    def process_io_object(self, io_object):
        # type: (IO) -> Union[None, UploadInMemoryMessage, UploadFileMessage]
        extension = get_file_extension(self.url_params["fileName"])
        if extension is not None:
            self.url_params["extension"] = extension

        return self._process_upload_io(io_object)

    def process_user_text(self, user_text):
        # type: (UserText) -> Union[None, UploadInMemoryMessage, UploadFileMessage]
        LOGGER.error(UPLOAD_FILE_OS_ERROR, user_text)
        return None


class FigureUploadProcessor(BaseUploadProcessor):

    TOO_BIG_MSG = LOG_FIGURE_TOO_BIG
    UPLOAD_TYPE = "visualization"

    def __init__(
        self,
        user_input,  # type: Any
        upload_limit,  # type: int
        url_params,  # type: Dict[str, Optional[Any]]
        metadata,  # type: Optional[Dict[str, str]]
        copy_to_tmp,  # type: bool
        error_message_identifier,  # type: Any
        tmp_dir,  # type: str
        critical,  # type: bool
        upload_type=None,  # type: Optional[str]
        on_asset_upload=None,
        on_failed_asset_upload=None,
    ):
        # type: (...) -> None
        super(FigureUploadProcessor, self).__init__(
            user_input,
            upload_limit,
            url_params,
            metadata,
            copy_to_tmp,
            error_message_identifier,
            tmp_dir,
            critical,
            on_asset_upload,
            on_failed_asset_upload,
        )
        if upload_type is not None:
            self.UPLOAD_TYPE = upload_type

    def process_upload_to_be_converted(self, user_input):
        # type: (Any) -> Optional[UploadFileMessage]
        if PlotlyFigure is not None and isinstance(user_input, PlotlyFigure):
            try:
                filename = save_plotly_figure(user_input)
            except Exception:
                LOGGER.warning(
                    "Failing to save the plotly figure; requires dependencies: see https://plotly.com/python/static-image-export/",
                    exc_info=True,
                )
                # An error occurred
                return None

        else:
            try:
                filename = save_matplotlib_figure(user_input)
            except Exception:
                LOGGER.warning("Failing to save the matplotlib figure", exc_info=True)
                # An error occurred
                return None

        self.url_params["extension"] = get_file_extension(filename)

        return self._process_upload_by_filepath(TemporaryFilePath(filename))


class ImageUploadProcessor(BaseUploadProcessor):

    TOO_BIG_MSG = LOG_IMAGE_TOO_BIG
    UPLOAD_TYPE = "visualization"

    def __init__(
        self,
        user_input,  # type: Any
        name,  # type: Optional[str]
        overwrite,  # type: bool
        image_format,
        image_scale,
        image_shape,
        image_colormap,
        image_minmax,
        image_channels,
        upload_limit,  # type: int
        url_params,  # type: Dict[str, Optional[Any]]
        metadata,  # type: Optional[Dict[str, str]]
        copy_to_tmp,  # type: bool
        error_message_identifier,  # type: Any
        tmp_dir,
        critical,  # type: bool
        on_asset_upload=None,  # type: Callable
        on_failed_asset_upload=None,  # type: Callable
    ):
        # type: (...) -> None
        self.name = name
        self.image_format = image_format
        self.image_scale = image_scale
        self.image_shape = image_shape
        self.image_colormap = image_colormap
        self.image_minmax = image_minmax
        self.image_channels = image_channels
        super(ImageUploadProcessor, self).__init__(
            user_input,
            upload_limit,
            url_params,
            metadata,
            copy_to_tmp,
            error_message_identifier,
            tmp_dir,
            critical,
            on_asset_upload,
            on_failed_asset_upload,
        )

    def process_upload_by_filepath(self, upload_filepath):
        # type: (ValidFilePath) -> Optional[UploadFileMessage]

        if self.url_params["figName"] is None:
            self.url_params["figName"] = os.path.basename(upload_filepath)

        self.url_params["extension"] = get_file_extension(upload_filepath)

        return self._process_upload_by_filepath(upload_filepath)

    def process_upload_to_be_converted(self, user_input):
        # type: (Any) -> Union[None, UploadInMemoryMessage, UploadFileMessage]
        try:
            image_object = image_data_to_file_like_object(
                user_input,
                self.name,
                self.image_format,
                self.image_scale,
                self.image_shape,
                self.image_colormap,
                self.image_minmax,
                self.image_channels,
            )
        except Exception:
            LOGGER.error(
                "Could not convert image_data into an image; ignored", exc_info=True
            )
            return None

        if not image_object:
            LOGGER.error(
                "Could not convert image_data into an image; ignored", exc_info=True
            )
            return None

        return self._process_upload_io(image_object)

    def process_io_object(self, io_object):
        # type: (IO) -> Union[None, UploadInMemoryMessage, UploadFileMessage]
        extension = get_file_extension(self.name)
        if extension is not None:
            self.url_params["extension"] = extension

        return self._process_upload_io(io_object)

    def process_user_text(self, user_text):
        # type: (UserText) -> None
        LOGGER.error(UPLOAD_FILE_OS_ERROR, user_text)
        return None


class AudioUploadProcessor(BaseUploadProcessor):

    TOO_BIG_MSG = LOG_AUDIO_TOO_BIG
    UPLOAD_TYPE = "audio"

    def __init__(
        self,
        user_input,  # type: Any
        sample_rate,  # type: Optional[int]
        overwrite,  # type: bool
        upload_limit,  # type: int
        url_params,  # type: Dict[str, Optional[Any]]
        metadata,  # type: Optional[Dict[str, str]]
        copy_to_tmp,  # type: bool
        error_message_identifier,  # type: Any
        tmp_dir,  # type: str
        critical,  # type: bool
        on_asset_upload=None,  # type: Callable
        on_failed_asset_upload=None,  # type: Callable
    ):
        # type: (...) -> None
        self.sample_rate = sample_rate

        super(AudioUploadProcessor, self).__init__(
            user_input,
            upload_limit,
            url_params,
            metadata,
            copy_to_tmp,
            error_message_identifier,
            tmp_dir,
            critical,
            on_asset_upload,
            on_failed_asset_upload,
        )

    def process_upload_by_filepath(self, upload_filepath):
        # type: (ValidFilePath) -> Optional[UploadFileMessage]
        if self.url_params["fileName"] is None:
            self.url_params["fileName"] = os.path.basename(upload_filepath)

        self.url_params["extension"] = get_file_extension(upload_filepath)

        # The file has not been sampled
        self.url_params["sampleRate"] = None

        return self._process_upload_by_filepath(upload_filepath)

    def process_upload_to_be_converted(self, user_input):
        # type: (Any) -> Union[None, UploadInMemoryMessage, UploadFileMessage]

        try:
            if not isinstance(user_input, numpy.ndarray):
                raise TypeError("Unsupported audio_data type %r" % type(user_input))
        except NameError:
            # Numpy is not available
            raise TypeError("Numpy is needed when passing a numpy array to log_audio")

        extension = get_file_extension(self.url_params["fileName"])
        if extension is not None:
            self.url_params["extension"] = extension

        if self.sample_rate is None:
            raise TypeError("sample_rate cannot be None when logging a numpy array")

        if not self.sample_rate:
            raise TypeError("sample_rate cannot be 0 when logging a numpy array")

        # Send the sampling rate to the backend
        self.url_params["sampleRate"] = self.sample_rate

        # And save it in the metadata too
        self.metadata["sample_rate"] = self.sample_rate

        # Write to a file directly to avoid temporary IO copy when we know it
        # will ends up on the file-system anyway
        if self.copy_to_tmp:
            tmpfile = tempfile.NamedTemporaryFile(delete=False)

            write_numpy_array_as_wav(user_input, self.sample_rate, tmpfile)

            tmpfile.close()

            return self._process_upload_by_filepath(TemporaryFilePath(tmpfile.name))
        else:
            io_object = io.BytesIO()

            write_numpy_array_as_wav(user_input, self.sample_rate, io_object)

            return self._process_upload_io(io_object)

    def process_user_text(self, user_text):
        # type: (UserText) -> None
        LOGGER.error(UPLOAD_FILE_OS_ERROR, user_text)
        return None


class AssetDataUploadProcessor(BaseUploadProcessor):

    TOO_BIG_MSG = UPLOAD_ASSET_TOO_BIG

    def __init__(
        self,
        user_input,  # type: Any
        upload_type,  # type: str
        url_params,  # type: Dict[str, Optional[Any]]
        metadata,  # type: Optional[Dict[str, str]]
        upload_limit,  # type: int
        copy_to_tmp,  # type: bool
        error_message_identifier,  # type: Any
        tmp_dir,  # type: str
        critical,  # type: bool
        on_asset_upload=None,  # type: Callable
        on_failed_asset_upload=None,  # type: Callable
    ):
        # type: (...) -> None
        self.UPLOAD_TYPE = upload_type
        super(AssetDataUploadProcessor, self).__init__(
            user_input,
            upload_limit,
            url_params,
            metadata,
            copy_to_tmp,
            error_message_identifier,
            tmp_dir,
            critical,
            on_asset_upload,
            on_failed_asset_upload,
        )

    def process_upload_to_be_converted(self, user_input):
        # type: (Any) -> Union[None, UploadInMemoryMessage, UploadFileMessage]
        # We have an object which is neither an IO object, neither a str or bytes
        try:
            converted = json.dumps(user_input)
        except Exception:
            LOGGER.error("Failed to log asset data as JSON", exc_info=True)
            return None

        extension = get_file_extension(self.url_params["fileName"])
        if extension is not None:
            self.url_params["extension"] = extension

        return self._process_upload_text(converted)

    def process_user_text(self, user_text):
        # type: (UserText) -> Union[None, UploadInMemoryMessage, UploadFileMessage]
        extension = get_file_extension(self.url_params["fileName"])
        if extension is not None:
            self.url_params["extension"] = extension

        return self._process_upload_text(user_text)


class GitPatchUploadProcessor(BaseUploadProcessor):

    TOO_BIG_MSG = UPLOAD_ASSET_TOO_BIG
    UPLOAD_TYPE = "git-patch"

    def process_upload_by_filepath(self, upload_filepath):
        # type: (ValidFilePath) -> Optional[UploadFileMessage]
        return self._process_upload_by_filepath(upload_filepath)


class PreprocessedRemoteAsset(
    namedtuple(
        "_PreprocessedRemoteAsset",
        [
            "remote_uri",
            "overwrite",
            "upload_type",
            "metadata",
            "step",
            "asset_id",
            "logical_path",
            "size",
        ],
    )
):
    __slots__ = ()

    def to_message(
        self, critical, on_asset_upload, on_failed_asset_upload, experiment_url_params
    ):
        # type: (...) -> RemoteAssetMessage
        return _remote_asset_to_message(
            asset=self,
            critical=critical,
            on_asset_upload=on_asset_upload,
            on_failed_asset_upload=on_failed_asset_upload,
            experiment_url_params=experiment_url_params,
        )


def preprocess_remote_asset(
    remote_uri,  # type: Any
    logical_path,  # type: Optional[str]
    overwrite,  # type: bool
    upload_type,  # type: Optional[str]
    metadata,  # type: Optional[Dict[str, str]]
    asset_id=None,  # type: Optional[str]
    step=None,  # type: Optional[int]
):
    # type: (...) -> PreprocessedRemoteAsset

    if asset_id is None:
        asset_id = generate_guid()
    asset_id = asset_id

    if logical_path is None:
        # Try to parse the URI to see if we can extract a useful file name
        logical_path = parse_remote_uri(remote_uri)
        if not logical_path:
            LOGGER.info(
                "Couldn't parse a file_name from URI %r, defaulting to 'remote'",
                remote_uri,
            )
            logical_path = "remote"

    try:
        size = len(remote_uri)
    except Exception:
        LOGGER.debug("Couldn't compute size for remote uri %r", remote_uri)
        size = 0

    return PreprocessedRemoteAsset(
        remote_uri,
        overwrite,
        upload_type,
        validate_metadata(metadata),
        step,
        asset_id,
        logical_path,
        size,
    )


class PreprocessedSyncedRemoteAsset(
    namedtuple(
        "_PreprocessedSyncedRemoteAsset",
        [
            "remote_uri",
            "overwrite",
            "upload_type",
            "metadata",
            "step",
            "asset_id",
            "logical_path",
            "size",
            "local_path",  # the path to the downloaded file
        ],
    )
):
    """Remote asset which was synced against supported cloud storage platforms"""

    __slots__ = ()

    def to_message(
        self, critical, on_asset_upload, on_failed_asset_upload, experiment_url_params
    ):
        # type: (...) -> RemoteAssetMessage
        return _remote_asset_to_message(
            asset=self,
            critical=critical,
            on_asset_upload=on_asset_upload,
            on_failed_asset_upload=on_failed_asset_upload,
            experiment_url_params=experiment_url_params,
        )


def _remote_asset_to_message(
    asset,  # type: Union[PreprocessedRemoteAsset, PreprocessedSyncedRemoteAsset]
    critical,  # type: bool
    on_asset_upload,  # type: Any
    on_failed_asset_upload,  # type: Any
    experiment_url_params,  # type: Dict[str, Any]
):
    # type: (...) -> RemoteAssetMessage
    url_params = {
        "assetId": asset.asset_id,
        "fileName": asset.logical_path,
        "isRemote": True,
        "overwrite": asset.overwrite,
        "step": asset.step,
    }

    # If the asset type is more specific, include the
    # asset type as "type" in query parameters:
    if asset.upload_type is not None and asset.upload_type != "asset":
        url_params["type"] = asset.upload_type

    url_params.update(experiment_url_params)

    return RemoteAssetMessage(
        remote_uri=asset.remote_uri,
        upload_type=asset.upload_type,
        additional_params=url_params,
        metadata=asset.metadata,
        size=asset.size,
        critical=critical,
        on_asset_upload=on_asset_upload,
        on_failed_asset_upload=on_failed_asset_upload,
    )


class PreprocessedFileAsset(
    namedtuple(
        "_PreprocessedFileAsset",
        [
            "local_path_or_data",
            "upload_type",
            "logical_path",
            "metadata",
            "overwrite",
            "copy_to_tmp",
            "step",
            "asset_id",
            "grouping_name",
            "extension",
            "size",
        ],
    )
):
    __slots__ = ()

    def to_message(
        self,
        critical,
        on_asset_upload,
        on_failed_asset_upload,
        clean,
        experiment_url_params,
    ):
        # type: (...) -> UploadFileMessage
        url_params = {
            "assetId": self.asset_id,
            "extension": self.extension,
            "fileName": self.logical_path,
            "overwrite": self.overwrite,
        }

        # If the asset type is more specific, include the
        # asset type as "type" in query parameters:
        if self.upload_type != "asset":
            url_params["type"] = self.upload_type

        if self.grouping_name is not None:
            url_params["groupingName"] = self.grouping_name

        url_params.update(experiment_url_params)

        return UploadFileMessage(
            self.local_path_or_data,
            self.upload_type,
            url_params,
            self.metadata,
            clean=clean,
            critical=critical,
            size=self.size,
            on_asset_upload=on_asset_upload,
            on_failed_asset_upload=on_failed_asset_upload,
        )

    def copy(self, new_local_path, new_copy_to_tmp):
        return self.__class__(
            local_path_or_data=new_local_path,
            upload_type=self.upload_type,
            logical_path=self.logical_path,
            metadata=self.metadata,
            overwrite=self.overwrite,
            copy_to_tmp=new_copy_to_tmp,
            step=self.step,
            asset_id=self.asset_id,
            grouping_name=self.grouping_name,
            extension=self.extension,
            size=self.size,
        )


def preprocess_asset_file(
    dispatched,  # type: FileUpload
    upload_type,  # type: str
    file_name,  # type: Optional[str]
    metadata,  # type: Optional[Dict[str, str]]
    overwrite,  # type: bool
    copy_to_tmp,  # type: bool
    step=None,  # type: Optional[int]
    grouping_name=None,  # type: Optional[str]
    asset_id=None,  # type: Optional[Any]
):
    # type: (...) -> PreprocessedFileAsset
    upload_filepath = dispatched.user_input

    if asset_id is None:
        asset_id = generate_guid()
    asset_id = asset_id

    if file_name is None:
        file_name = os.path.basename(upload_filepath)

    extension = get_file_extension(upload_filepath)

    size = dispatched.size

    return PreprocessedFileAsset(
        upload_filepath,
        upload_type,
        file_name,
        validate_metadata(metadata),
        overwrite,
        copy_to_tmp,
        step,
        asset_id,
        grouping_name,
        extension,
        size,
    )


class PreprocessedMemoryFileAsset(
    namedtuple(
        "_PreprocessedMemoryFileAsset",
        [
            "local_path_or_data",
            "upload_type",
            "logical_path",
            "metadata",
            "overwrite",
            "copy_to_tmp",
            "step",
            "asset_id",
            "grouping_name",
            "extension",
            "size",
        ],
    )
):
    __slots__ = ()

    def to_message(
        self,
        critical,
        on_asset_upload,
        on_failed_asset_upload,
        experiment_url_params,
        clean,
    ):
        # type: (...) -> UploadInMemoryMessage

        # TODO: Clean is ignored but kept to keep the to_message API consistent with
        # PreprocessedFileAsset.to_message
        url_params = {
            "assetId": self.asset_id,
            "extension": self.extension,
            "fileName": self.logical_path,
            "overwrite": self.overwrite,
        }

        # If the asset type is more specific, include the
        # asset type as "type" in query parameters:
        if self.upload_type != "asset":
            url_params["type"] = self.upload_type

        if self.grouping_name is not None:
            url_params["groupingName"] = self.grouping_name

        url_params.update(experiment_url_params)

        return UploadInMemoryMessage(
            self.local_path_or_data,
            self.upload_type,
            url_params,
            self.metadata,
            size=self.size,
            critical=critical,
            on_asset_upload=on_asset_upload,
            on_failed_asset_upload=on_failed_asset_upload,
        )

    def to_preprocessed_file_asset(self, new_local_path, new_copy_to_tmp):
        return PreprocessedFileAsset(
            local_path_or_data=new_local_path,
            upload_type=self.upload_type,
            logical_path=self.logical_path,
            metadata=self.metadata,
            overwrite=self.overwrite,
            copy_to_tmp=new_copy_to_tmp,
            step=self.step,
            asset_id=self.asset_id,
            grouping_name=self.grouping_name,
            extension=self.extension,
            size=self.size,
        )


def preprocess_asset_memory_file(
    dispatched,  # type: MemoryFileUpload
    upload_type,  # type: str
    file_name,  # type: Optional[str]
    metadata,  # type: Optional[Dict[str, str]]
    overwrite,  # type: bool
    copy_to_tmp,  # type: bool
    step=None,  # type: Optional[int]
    grouping_name=None,  # type: Optional[str]
    asset_id=None,  # type: Optional[Any]
):
    # type: (...) -> PreprocessedMemoryFileAsset
    file_like = dispatched.user_input

    if asset_id is None:
        asset_id = generate_guid()
    asset_id = asset_id

    extension = get_file_extension(file_name)

    size = dispatched.size

    # XXX: Previously when no file_name was given and copy_to_tmp was set to True, we would uses the
    # temporary file basename, given random file name. Now we raises an Exception in all cases
    if file_name is None:
        raise TypeError("file_name shouldn't be None")

    return PreprocessedMemoryFileAsset(
        file_like,
        upload_type,
        file_name,
        validate_metadata(metadata),
        overwrite,
        copy_to_tmp,
        step,
        asset_id,
        grouping_name,
        extension,
        size,
    )


PreprocessedAsset = Union[
    PreprocessedFileAsset,
    PreprocessedMemoryFileAsset,
    PreprocessedRemoteAsset,
    PreprocessedSyncedRemoteAsset,
]


def handle_in_memory_file_upload(tmp_dir, upload_filepath):
    # type: (str, ValidFilePath) -> Union[None, ValidFilePath, TemporaryFilePath]
    # If we cannot remove the uploaded file or need the file content will
    # be frozen to the time the upload call is made, pass copy_to_tmp with
    # True value
    if not isinstance(upload_filepath, TemporaryFilePath):
        tmpfile = tempfile.NamedTemporaryFile(delete=False, dir=tmp_dir)
        tmpfile.close()
        LOGGER.debug(
            "Copying %s to %s because of copy_to_tmp", upload_filepath, tmpfile.name
        )
        try:
            shutil.copyfile(upload_filepath, tmpfile.name)
        except (OSError, IOError):
            LOGGER.error(UPLOAD_FILE_OS_ERROR, upload_filepath, exc_info=True)
            return None
        upload_filepath = TemporaryFilePath(tmpfile.name)

    return upload_filepath
