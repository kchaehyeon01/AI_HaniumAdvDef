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
#  This file can not be copied and/or distributed
#  without the express permission of Comet ML Inc.
# *******************************************************

import io
import json
import os
import tempfile
import threading
from collections import namedtuple
from functools import partial
from logging import getLogger

import requests
import semantic_version
import six
from six.moves.urllib.parse import urlparse

from ._typing import (
    IO,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)
from .api import APIExperiment
from .cloud_storage_utils import (
    META_ERROR_MESSAGE,
    META_FILE_SIZE,
    META_SYNCED,
    META_VERSION_ID,
)
from .config import Config
from .connection import RestApiClient, get_thread_session, write_stream_response_to_file
from .exceptions import (
    ArtifactAssetNotFound,
    ArtifactConflictingAssetLogicalPath,
    ArtifactDownloadException,
    ArtifactNotFinalException,
    ArtifactNotFound,
    CometRestApiException,
    GetArtifactException,
    LogArtifactException,
)
from .experiment import BaseExperiment
from .file_downloader import (
    FileDownloadManager,
    FileDownloadManagerMonitor,
    FileDownloadSizeMonitor,
)
from .file_uploader import (
    FileUpload,
    MemoryFileUpload,
    PreprocessedAsset,
    PreprocessedFileAsset,
    PreprocessedRemoteAsset,
    PreprocessedSyncedRemoteAsset,
    dispatch_user_file_upload,
    preprocess_asset_file,
    preprocess_asset_memory_file,
    preprocess_remote_asset,
)
from .file_utils import file_sha1sum, io_sha1sum
from .gs_bucket_info import download_gs_file, preprocess_remote_gs_assets
from .logging_messages import (
    ARTIFACT_ASSET_DOWNLOAD_FAILED,
    ARTIFACT_ASSET_DOWNLOAD_FAILED_REPR,
    ARTIFACT_ASSET_DOWNLOAD_FAILED_WITH_ERROR,
    ARTIFACT_ASSET_UPLOAD_FAILED,
    ARTIFACT_ASSET_WRITE_ERROR,
    ARTIFACT_DOWNLOAD_FILE_OVERWRITTEN,
    ARTIFACT_DOWNLOAD_FINISHED,
    ARTIFACT_DOWNLOAD_START_MESSAGE,
    ARTIFACT_UPLOAD_FINISHED,
    ARTIFACT_UPLOAD_STARTED,
    ARTIFACT_VERSION_CREATED_WITH_PREVIOUS,
    ARTIFACT_VERSION_CREATED_WITHOUT_PREVIOUS,
    FAILED_TO_ADD_ARTIFACT_REMOTE_SYNC_ASSET,
    LOG_ARTIFACT_IN_PROGESS_MESSAGE,
    SYNC_MODE_IS_NOT_SUPPORTED_FOR_STRING_REMOTE_ARTIFACT,
    UNSUPPORTED_URI_SYNCED_REMOTE_ASSET,
)
from .parallel_utils import makedirs_synchronized
from .s3_bucket_info import download_s3_file, preprocess_remote_s3_assets
from .summary import Summary
from .utils import (
    ImmutableDict,
    IterationProgressCallback,
    format_bytes,
    generate_guid,
    wait_for_done,
)
from .validation_utils import validate_metadata

LOGGER = getLogger(__name__)


def _parse_artifact_name(artifact_name):
    # type: (str) -> Tuple[Optional[str], str, Optional[str]]
    """Parse an artifact_name, potentially a fully-qualified name"""

    splitted = artifact_name.split("/")

    # First parse the workspace
    if len(splitted) == 1:
        workspace = None
        artifact_name_version = splitted[0]
    else:
        workspace = splitted[0]
        artifact_name_version = splitted[1]

    name_version_splitted = artifact_name_version.split(":", 1)

    if len(name_version_splitted) == 1:
        artifact_name = name_version_splitted[0]
        version_or_alias = None
    else:
        artifact_name = name_version_splitted[0]
        version_or_alias = name_version_splitted[1]

    return workspace, artifact_name, version_or_alias


def _log_artifact(artifact, experiment):
    # type: (Artifact, Any) -> LoggedArtifact
    artifact_id, artifact_version_id = _upsert_artifact(
        artifact, experiment.rest_api_client, experiment.id
    )

    success_prepared_request = _prepare_update_artifact_version_state(
        experiment.rest_api_client, artifact_version_id, experiment.id, "CLOSED"
    )
    timeout = experiment.config.get_int(None, "comet.timeout.http")
    verify_tls = experiment.config.get_bool(
        None, "comet.internal.check_tls_certificate"
    )

    logged_artifact = _get_artifact(
        experiment.rest_api_client,
        {"artifact_id": artifact_id, "artifact_version_id": artifact_version_id},
        experiment.id,
        experiment._summary,
        experiment.config,
    )

    if len(artifact._assets) == 0:
        LOGGER.warning(
            "Warning: Artifact %s created without adding any assets, was this the intent?",
            logged_artifact,
        )

        _call_post_prepared_request(
            success_prepared_request, timeout, verify_tls=verify_tls
        )
    else:
        failed_prepared_request = _prepare_update_artifact_version_state(
            experiment.rest_api_client, artifact_version_id, experiment.id, "ERROR"
        )

        _log_artifact_assets(
            artifact,
            experiment,
            artifact_version_id,
            logged_artifact.workspace,
            logged_artifact.name,
            str(logged_artifact.version),
            success_prepared_request,
            failed_prepared_request,
            timeout,
            verify_tls=verify_tls,
        )

        LOGGER.info(
            ARTIFACT_UPLOAD_STARTED,
            logged_artifact.workspace,
            logged_artifact.name,
            logged_artifact.version,
        )

    experiment._summary.increment_section("uploads", "artifacts")

    return logged_artifact


def _log_artifact_assets(
    artifact,  # type: Artifact
    experiment,  # type: BaseExperiment
    artifact_version_id,  # type: str
    logged_artifact_workspace,  # type: str
    logged_artifact_name,  # type: str
    logged_artifact_version,  # type: str
    success_prepared_request,  # type: Tuple[str, Dict[str, Any], Dict[str, Any]]
    failed_prepared_request,  # type: Tuple[str, Dict[str, Any], Dict[str, Any]]
    timeout,  # type: int
    verify_tls,  # type: bool
):
    # type: (...) -> None
    artifact_assets = artifact._assets.values()

    all_asset_ids = {artifact_asset.asset_id for artifact_asset in artifact_assets}

    lock = threading.Lock()

    # At the starts, it's the total numbers but then it's the remaining numbers
    num_assets = len(artifact_assets)
    total_size = sum(asset.size for asset in artifact_assets)

    LOGGER.info(
        "Scheduling the upload of %d assets for a size of %s, this can take some time",
        num_assets,
        format_bytes(total_size),
    )

    def progress_callback():
        LOGGER.info(
            LOG_ARTIFACT_IN_PROGESS_MESSAGE,
            num_assets,
            format_bytes(total_size),
        )

    frequency = 5

    success_log_message = ARTIFACT_UPLOAD_FINISHED
    success_log_message_args = (
        logged_artifact_workspace,
        logged_artifact_name,
        logged_artifact_version,
    )

    error_log_message = ARTIFACT_ASSET_UPLOAD_FAILED
    error_log_message_args = (
        logged_artifact_workspace,
        logged_artifact_name,
        logged_artifact_version,
    )

    for artifact_file in IterationProgressCallback(
        artifact_assets, progress_callback, frequency
    ):
        asset_id = artifact_file.asset_id

        # If the asset id is from a downloaded artifact version asset, generate a new ID here.
        # TODO: Need to find a way to not re-upload it
        if artifact_file.asset_id in artifact._downloaded_asset_ids:
            artifact_file = artifact_file._replace(asset_id=generate_guid())

        if isinstance(artifact_file, PreprocessedRemoteAsset) or isinstance(
            artifact_file, PreprocessedSyncedRemoteAsset
        ):
            experiment._log_preprocessed_remote_asset(
                artifact_file,
                artifact_version_id=artifact_version_id,
                critical=True,
                on_asset_upload=partial(
                    _on_artifact_asset_upload,
                    lock,
                    all_asset_ids,
                    asset_id,
                    success_prepared_request,
                    timeout,
                    success_log_message,
                    success_log_message_args,
                    verify_tls,
                ),
                on_failed_asset_upload=partial(
                    _on_artifact_failed_asset_upload,
                    asset_id,
                    failed_prepared_request,
                    timeout,
                    error_log_message,
                    (asset_id,) + error_log_message_args,
                    verify_tls,
                ),
                return_url=False,
            )
        else:
            experiment._log_preprocessed_asset(
                artifact_file,
                artifact_version_id=artifact_version_id,
                critical=True,
                on_asset_upload=partial(
                    _on_artifact_asset_upload,
                    lock,
                    all_asset_ids,
                    asset_id,
                    success_prepared_request,
                    timeout,
                    success_log_message,
                    success_log_message_args,
                    verify_tls,
                ),
                on_failed_asset_upload=partial(
                    _on_artifact_failed_asset_upload,
                    asset_id,
                    failed_prepared_request,
                    timeout,
                    error_log_message,
                    (asset_id,) + error_log_message_args,
                    verify_tls,
                ),
                return_url=False,
            )
        num_assets -= 1
        total_size -= artifact_file.size


def _upsert_artifact(artifact, rest_api_client, experiment_key):
    # type: (Artifact, RestApiClient, str) -> Tuple[str, str]
    try:

        artifact_version = artifact.version
        if artifact_version is not None:
            artifact_version = str(artifact_version)

        response = rest_api_client.upsert_artifact(
            artifact_name=artifact.name,
            artifact_type=artifact.artifact_type,
            experiment_key=experiment_key,
            metadata=artifact.metadata,
            version=artifact_version,
            aliases=list(artifact.aliases),
            version_tags=list(artifact.version_tags),
        )
    except CometRestApiException as e:
        raise LogArtifactException(e.safe_msg, e.sdk_error_code)
    except requests.RequestException:
        raise LogArtifactException()

    result = response.json()

    artifact_id = result["artifactId"]
    artifact_version_id = result["artifactVersionId"]

    version = result["currentVersion"]
    _previous_version = result["previousVersion"]

    if _previous_version is None:
        LOGGER.info(ARTIFACT_VERSION_CREATED_WITHOUT_PREVIOUS, artifact.name, version)
    else:
        LOGGER.info(
            ARTIFACT_VERSION_CREATED_WITH_PREVIOUS,
            artifact.name,
            version,
            _previous_version,
        )

    return artifact_id, artifact_version_id


def _download_artifact_asset(
    url,  # type: str
    params,  # type: Dict[str, Any]
    headers,  # type: Dict[str, Any]
    timeout,  # type: int
    asset_id,  # type: str
    artifact_repr,  # type: str
    artifact_str,  # type: str
    asset_logical_path,  # type: str
    asset_path,  # type: str
    overwrite,  # type: str
    verify_tls,  # type: bool
    _monitor=None,  # type: Optional[FileDownloadSizeMonitor]
):
    # type: (...) -> None
    try:
        retry_session = get_thread_session(
            retry=True, verify_tls=verify_tls, tcp_keep_alive=False
        )

        response = retry_session.get(
            url=url,
            params=params,
            headers=headers,
            stream=True,
        )  # type: requests.Response

        if response.status_code != 200:
            response.close()
            raise CometRestApiException("GET", response)
    except Exception:
        raise ArtifactDownloadException(
            ARTIFACT_ASSET_DOWNLOAD_FAILED_REPR % (asset_id, artifact_repr)
        )

    try:
        _write_artifact_asset_data_to_disk(
            artifact_str=artifact_str,
            asset_id=asset_id,
            asset_logical_path=asset_logical_path,
            asset_path=asset_path,
            overwrite=overwrite,
            writer=AssetDataWriterFromResponse(response=response, monitor=_monitor),
        )
    finally:
        try:
            response.close()
        except Exception:
            LOGGER.debug(
                "Error closing artifact asset download response", exc_info=True
            )
            pass


def _write_artifact_asset_data_to_disk(
    artifact_str,  # type: str
    asset_id,  # type: str
    asset_logical_path,  # type: str
    asset_path,  # type: str
    overwrite,  # type: str
    writer,  # type: AssetDataWriter
):
    # type: (...) -> None
    if os.path.isfile(asset_path):
        if overwrite == "OVERWRITE":
            LOGGER.warning(
                ARTIFACT_DOWNLOAD_FILE_OVERWRITTEN,
                asset_path,
                asset_logical_path,
                artifact_str,
            )
        elif overwrite == "PRESERVE":
            # TODO: Print LOG message if content is different when we have the SHA1 stored the
            # backend
            return
        else:
            # Download the file to a temporary file
            # TODO: Just compare the checksums
            try:
                existing_file_checksum = file_sha1sum(asset_path)
            except Exception:
                LOGGER.debug("Error computing sha1sum", exc_info=True)
                raise ArtifactDownloadException(
                    "Cannot read file %r to compare content, check logs for details"
                    % asset_path
                )

            try:
                with tempfile.NamedTemporaryFile() as f:
                    writer.write(file=f)

                    # Flush to be sure that everything is written
                    f.flush()
                    f.seek(0)

                    # Compute checksums
                    asset_checksum = io_sha1sum(f)

            except Exception:
                LOGGER.debug("Error write tmpfile to compute checksum", exc_info=True)
                raise ArtifactDownloadException(
                    "Cannot write Asset %r on disk path %r, check logs for details"
                    % (asset_id, asset_path)
                )

            if asset_checksum != existing_file_checksum:
                raise ArtifactDownloadException(
                    "Cannot write Asset %r on path %r, a file already exists"
                    % (
                        asset_id,
                        asset_path,
                    )
                )

            return None
    else:
        try:
            dirpart = os.path.dirname(asset_path)
            makedirs_synchronized(dirpart, exist_ok=True)
        except Exception:
            LOGGER.debug("Error creating directories", exc_info=True)
            raise ArtifactDownloadException(
                ARTIFACT_ASSET_WRITE_ERROR
                % (
                    asset_id,
                    asset_path,
                )
            )

    try:
        with io.open(asset_path, "wb") as f:
            writer.write(file=f)
    except Exception:
        LOGGER.debug("Error writing file on path", exc_info=True)
        raise ArtifactDownloadException(
            ARTIFACT_ASSET_WRITE_ERROR
            % (
                asset_id,
                asset_path,
            )
        )


def _download_cloud_storage_artifact_asset(
    data_writer,  # type: AssetDataWriter
    asset_id,  # type: str
    artifact_repr,  # type: str
    artifact_str,  # type: str
    asset_logical_path,  # type: str
    asset_path,  # type: str
    overwrite,  # type: str
    _monitor=None,  # type: Optional[FileDownloadSizeMonitor]
):
    # type: (...) -> None
    try:
        data_writer.monitor = _monitor
        _write_artifact_asset_data_to_disk(
            artifact_str=artifact_str,
            asset_id=asset_id,
            asset_logical_path=asset_logical_path,
            asset_path=asset_path,
            overwrite=overwrite,
            writer=data_writer,
        )
    except Exception:
        LOGGER.debug("Error writing S3/GCS artifact asset file on path", exc_info=True)
        raise ArtifactDownloadException(
            ARTIFACT_ASSET_DOWNLOAD_FAILED_REPR % (asset_id, artifact_repr)
        )
    return None


class AssetDataWriter(object):
    def write(self, file):
        # type: (IO[bytes]) -> None
        pass


class AssetDataWriterFromResponse(AssetDataWriter):
    def __init__(self, response, monitor=None):
        # type: (requests.Response,Optional[FileDownloadSizeMonitor]) -> None
        self.response = response
        self.monitor = monitor

    def write(self, file):
        # type: (IO[bytes]) -> None
        write_stream_response_to_file(self.response, file, self.monitor)


class AssetDataWriterFromS3(AssetDataWriter):
    def __init__(self, s3_uri, version_id, monitor=None):
        # type: (str,str,Optional[FileDownloadSizeMonitor]) -> None
        self.s3_uri = s3_uri
        self.version_id = version_id
        self.monitor = monitor

    def write(self, file):
        # type: (IO[bytes]) -> None
        callback = None
        if self.monitor is not None:
            callback = self.monitor.monitor_callback

        download_s3_file(
            s3_uri=self.s3_uri,
            file_object=file,
            callback=callback,
            version_id=self.version_id,
        )


class AssetDataWriterFromGCS(AssetDataWriter):
    def __init__(self, gs_uri, version_id, monitor=None):
        # type: (str,str,Optional[FileDownloadSizeMonitor]) -> None
        self.gs_uri = gs_uri
        self.version_id = version_id
        self.monitor = monitor

    def write(self, file):
        # type: (IO[bytes]) -> None
        callback = None
        if self.monitor is not None:
            callback = self.monitor.monitor_callback

        download_gs_file(
            gs_uri=self.gs_uri,
            file_object=file,
            callback=callback,
            version_id=self.version_id,
        )


def _on_artifact_asset_upload(
    lock,  # type: threading.Lock
    all_asset_ids,  # type: Set[str]
    asset_id,  # type: str
    prepared_request,  # type: Tuple[str, Dict[str, Any], Dict[str, Any]]
    timeout,  # type: int
    success_log_message,  # type: str
    success_log_message_args,  # type: Tuple
    verify_tls,  # type: bool
    response,  # type: Any
    *args,  # type: Any
    **kwargs  # type: Any
):
    # type: (...) -> None
    with lock:
        all_asset_ids.remove(asset_id)
        if len(all_asset_ids) == 0:
            try:
                _call_post_prepared_request(
                    prepared_request, timeout, verify_tls=verify_tls
                )
                LOGGER.info(success_log_message, *success_log_message_args)
            except Exception:
                LOGGER.error(
                    "Failed to mark the artifact version as closed", exc_info=True
                )


def _on_artifact_failed_asset_upload(
    asset_id,  # type: str
    prepared_request,  # type: Tuple[str, Dict[str, Any], Dict[str, Any]]
    timeout,  # type: int
    error_log_message,  # type: str
    error_log_message_args,  # type: Tuple
    verify_tls,  # type: bool
    response,  # type: Any
    *args,  # type: Any
    **kwargs  # type: Any
):
    # type: (...) -> None
    LOGGER.error(error_log_message, *error_log_message_args)

    try:
        _call_post_prepared_request(prepared_request, timeout, verify_tls=verify_tls)
    except Exception:
        LOGGER.error("Failed to mark the artifact version as error", exc_info=True)


def _call_post_prepared_request(prepared_request, timeout, verify_tls):
    # type: (Tuple[str, Dict[str, Any], Dict[str, Any]], int, bool) -> requests.Response
    session = get_thread_session(True, verify_tls=verify_tls, tcp_keep_alive=True)

    url, json_body, headers = prepared_request

    LOGGER.debug(
        "POST HTTP Call, url %r, json_body %r, timeout %r",
        url,
        json_body,
        timeout,
    )

    response = session.post(url, json=json_body, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response


def _prepare_update_artifact_version_state(
    rest_api_client, artifact_version_id, experiment_key, state
):
    # type: (RestApiClient, str, str, str) -> Tuple[str, Dict[str, Any], Dict[str, Any]]
    # Extracted to ease the monkey-patching of Experiment.log_artifact
    return rest_api_client._prepare_update_artifact_version_state(
        artifact_version_id, experiment_key, state
    )


def _validate_overwrite_strategy(user_overwrite_strategy):
    # type: (Any) -> str

    if isinstance(user_overwrite_strategy, six.string_types):
        lower_user_overwrite_strategy = user_overwrite_strategy.lower()
    else:
        lower_user_overwrite_strategy = user_overwrite_strategy

    if (
        lower_user_overwrite_strategy is False
        or lower_user_overwrite_strategy == "fail"
    ):
        return "FAIL"

    elif lower_user_overwrite_strategy == "preserve":
        return "PRESERVE"

    elif (
        lower_user_overwrite_strategy is True
        or lower_user_overwrite_strategy == "overwrite"
    ):
        return "OVERWRITE"

    else:
        raise ValueError("Invalid user_overwrite value %r" % user_overwrite_strategy)


class Artifact(object):
    def __init__(
        self,
        name,  # type: str
        artifact_type,  # type: str
        version=None,  # type: Optional[str]
        aliases=None,  # type: Optional[Iterable[str]]
        metadata=None,  # type: Any
        version_tags=None,  # type: Optional[Iterable[str]]
    ):
        # type: (...) -> None
        """
        Comet Artifacts allow keeping track of assets beyond any particular experiment. You can keep
        track of Artifact versions, create many types of assets, manage them, and use them in any
        step in your ML pipelines---from training to production deployment.

        Artifacts live in a Comet Project, are identified by their name and version string number.

        Example how to log an artifact with an asset:

        ```python
        from comet_ml import Artifact, Experiment

        experiment = Experiment()
        artifact = Artifact("Artifact-Name", "Artifact-Type")
        artifact.add("local-file")

        experiment.log_artifact(artifact)
        experiment.end()
        ```

        Example how to get and download an artifact assets:

        ```python
        from comet_ml import Experiment

        experiment = Experiment()
        artifact = experiment.get_artifact("Artifact-Name", WORKSPACE, PROJECT_NAME)

        artifact.download("/data/input")
        ```

        The artifact is created on the frontend only when calling `Experiment.log_artifact`

        Args:
            name: The artifact name.
            artifact_type: The artifact-type, for example `dataset`.
            version: Optional. The version number to create. If not provided, a new version number
                will be created automatically.
            aliases: Optional. Iterable of String. Some aliases to attach to the future Artifact
                Version. The aliases list is converted into a set for de-duplication.
            metadata: Optional. Some additional data to attach to the future Artifact Version. Must
                be a JSON-encodable dict.
        """

        # Artifact fields
        self.artifact_type = artifact_type
        self.name = name

        # Upsert fields
        if version is None:
            self.version = None
        else:
            self.version = semantic_version.Version(version)

        self.version_tags = set()  # type: Set[str]
        if version_tags is not None:
            self.version_tags = set(version_tags)

        self.aliases = set()  # type: Set[str]
        if aliases is not None:
            self.aliases = set(aliases)

        self.metadata = validate_metadata(metadata, raise_on_invalid=True)

        self._assets = {}  # type: Dict[str, PreprocessedAsset]

        # The set of assets IDs that was already downloaded through LoggedArtifact.download
        self._downloaded_asset_ids = set()  # type: Set[str]

        self._download_local_path = None  # type: Optional[str]

    @classmethod
    def _from_logged_artifact(
        cls,
        name,  # type: str
        artifact_type,  # type: str
        assets,  # type: Dict[str, PreprocessedAsset]
        root_path,  # type: str
        asset_ids,  # type: Set[str]
    ):
        # type: (...) -> Artifact
        new_artifact = cls(name, artifact_type)
        new_artifact._assets = assets
        new_artifact._download_local_path = root_path
        new_artifact._downloaded_asset_ids = asset_ids

        return new_artifact

    def add(
        self,
        local_path_or_data,  # type: Any
        logical_path=None,  # type: Optional[str]
        overwrite=False,  # type: bool
        copy_to_tmp=True,  # type: bool # if local_path_or_data is a file pointer
        metadata=None,  # type: Any
    ):
        # type: (...) -> None
        """
        Add a local asset to the current pending artifact object.

        Args:
            local_path_or_data: String or File-like - either the file path of the file you want
                to log, or a file-like asset.
            logical_path: String - Optional. A custom file name to be displayed. If not
                provided the filename from the `local_path_or_data` argument will be used.
            overwrite: if True will overwrite all existing assets with the same name.
            copy_to_tmp: If `local_path_or_data` is a file-like object, then this flag determines
                if the file is first copied to a temporary file before upload. If
                `copy_to_tmp` is False, then it is sent directly to the cloud.
            metadata: Optional. Some additional data to attach to the the audio asset. Must be a
                JSON-encodable dict.
        """
        if local_path_or_data is None:
            raise TypeError("local_path_or_data cannot be None")

        dispatched = dispatch_user_file_upload(local_path_or_data)
        asset_id = generate_guid()

        if not isinstance(dispatched, (FileUpload, MemoryFileUpload)):
            raise ValueError(
                "Invalid file_data %r, must either be a valid file-path or an IO object"
                % local_path_or_data
            )
        elif isinstance(dispatched, FileUpload):
            preprocessed = preprocess_asset_file(
                dispatched=dispatched,
                upload_type="asset",
                file_name=logical_path,
                metadata=metadata,
                overwrite=overwrite,
                asset_id=asset_id,
                copy_to_tmp=copy_to_tmp,
            )
        else:
            preprocessed = preprocess_asset_memory_file(
                dispatched=dispatched,
                upload_type="asset",
                file_name=logical_path,
                metadata=metadata,
                overwrite=overwrite,
                copy_to_tmp=copy_to_tmp,
            )

        self._add_preprocessed(preprocessed)

    def add_remote(
        self,
        uri,  # type: Any
        logical_path=None,  # type: Optional[str]
        overwrite=False,  # type: bool
        asset_type=None,  # type: str
        metadata=None,  # type: Any
        sync_mode=True,  # type: bool
        max_synced_objects=10000,
    ):
        # type: (...) -> None
        """
        Add a remote asset to the current pending artifact object. A Remote Asset is an asset but
        its content is not uploaded and stored on Comet. Rather a link for its location is stored so
        you can identify and distinguish between two experiment using different version of a dataset
        stored somewhere else.

        Args:
            uri: String - the remote asset location, there is no imposed format and it could be a
                private link.
            logical_path: String, Optional. The "name" of the remote asset, could be a dataset
                name, a model file name.
            overwrite: if True will overwrite all existing assets with the same name.
            asset_type: Define the type of the asset - Deprecated.
            metadata: Some additional data to attach to the remote asset.
                Must be a JSON-encodable dict.
            sync_mode: Bool - If True and the URI begins with s3://, Comet attempts to list all
                objects in the given bucket and path. Each object will be logged as a separate
                remote asset. If object versioning is enabled on the S3 bucket, Comet also logs each
                object version to be able to download the exact version. If False, Comet just logs a
                single remote asset with the provided URI as the remote URI. Default is True.
            max_synced_objects: When sync_mode is True and the URI begins with s3://, set the
                maximum number of S3 objects to log. If there are more matching S3 objects than
                max_synced_objects, a warning will be displayed and the provided URI will be logged
                as a single remote asset.
        """
        if asset_type:
            LOGGER.warning("The asset type parameter is deprecated.")
        asset_type = None

        if sync_mode is True:
            url_scheme = None
            try:
                o = urlparse(uri)
                url_scheme = o.scheme
            except Exception:
                LOGGER.warning(
                    "Failed to parse artifact's URI '%s'", uri, exc_info=True
                )

            error_message = None
            success = False
            if url_scheme == "s3":
                success, error_message = self._process_s3_assets(
                    uri=uri,
                    max_synced_objects=max_synced_objects,
                    logical_path=logical_path,
                    overwrite=overwrite,
                    asset_type=asset_type,
                    metadata=metadata,
                )
            elif url_scheme == "gs":
                success, error_message = self._process_gs_assets(
                    uri=uri,
                    max_synced_objects=max_synced_objects,
                    logical_path=logical_path,
                    overwrite=overwrite,
                    asset_type=asset_type,
                    metadata=metadata,
                )
            else:
                # log debug warning
                LOGGER.debug(SYNC_MODE_IS_NOT_SUPPORTED_FOR_STRING_REMOTE_ARTIFACT, uri)

            if success is True:
                # to avoid logging this artifact as plain artifact beneath
                return

            # append error message to the metadata
            if error_message is not None:
                # add to metadata
                if metadata is None:
                    metadata = dict()
                metadata[META_ERROR_MESSAGE] = error_message
                metadata[META_SYNCED] = False

        # process asset as usually
        preprocessed = preprocess_remote_asset(
            remote_uri=uri,
            logical_path=logical_path,
            overwrite=overwrite,
            upload_type=asset_type,
            metadata=metadata,
        )
        self._add_preprocessed(preprocessed)

    def _process_gs_assets(
        self,
        uri,  # type: Any
        max_synced_objects,  # type: int
        logical_path,  # type: Optional[str]
        overwrite,  # type: bool
        asset_type,  # type: Optional[str]
        metadata,  # type: Any
    ):
        try:
            preprocessed_assets = preprocess_remote_gs_assets(
                remote_uri=uri,
                logical_path=logical_path,
                overwrite=overwrite,
                upload_type=asset_type,
                metadata=metadata,
                max_synced_objects=max_synced_objects,
            )
            for asset in preprocessed_assets:
                self._add_preprocessed(asset)

            # success - no error
            return True, None

        except LogArtifactException as lax:
            LOGGER.warning(lax.backend_err_msg, exc_info=True)
            error_message = lax.backend_err_msg
        except Exception:
            LOGGER.warning(FAILED_TO_ADD_ARTIFACT_REMOTE_SYNC_ASSET, uri, exc_info=True)
            error_message = FAILED_TO_ADD_ARTIFACT_REMOTE_SYNC_ASSET % uri

        return False, error_message

    def _process_s3_assets(
        self,
        uri,  # type: Any
        max_synced_objects,  # type: int
        logical_path,  # type: Optional[str]
        overwrite,  # type: bool
        asset_type,  # type: Optional[str]
        metadata,  # type: Any
    ):
        try:
            preprocessed_assets = preprocess_remote_s3_assets(
                remote_uri=uri,
                logical_path=logical_path,
                overwrite=overwrite,
                upload_type=asset_type,
                metadata=metadata,
                max_synced_objects=max_synced_objects,
            )
            for asset in preprocessed_assets:
                self._add_preprocessed(asset)

            # success - no error
            return True, None

        except LogArtifactException as lax:
            LOGGER.warning(lax.backend_err_msg, exc_info=True)
            error_message = lax.backend_err_msg
        except Exception:
            LOGGER.warning(FAILED_TO_ADD_ARTIFACT_REMOTE_SYNC_ASSET, uri, exc_info=True)
            error_message = FAILED_TO_ADD_ARTIFACT_REMOTE_SYNC_ASSET % uri

        return False, error_message

    def _preprocessed_user_input(self, preprocessed):
        # type: (PreprocessedAsset) -> Any
        if isinstance(preprocessed, PreprocessedRemoteAsset):
            return preprocessed.remote_uri
        else:
            return preprocessed.local_path_or_data

    def _add_preprocessed(self, preprocessed):
        # type: (PreprocessedAsset) -> None
        preprocessed_logical_path = preprocessed.logical_path

        if preprocessed_logical_path in self._assets:
            # Allow the overriding of an asset inherited from a downloaded version
            if (
                self._assets[preprocessed_logical_path].asset_id
                in self._downloaded_asset_ids
            ):
                self._downloaded_asset_ids.remove(
                    self._assets[preprocessed_logical_path].asset_id
                )
                self._assets[preprocessed_logical_path] = preprocessed
            else:
                raise ArtifactConflictingAssetLogicalPath(
                    self._preprocessed_user_input(
                        self._assets[preprocessed_logical_path]
                    ),
                    self._preprocessed_user_input(preprocessed),
                    preprocessed_logical_path,
                )
        else:
            self._assets[preprocessed_logical_path] = preprocessed

    def __str__(self):
        return "%s(%r, artifact_type=%r)" % (
            self.__class__.__name__,
            self.name,
            self.artifact_type,
        )

    def __repr__(self):
        return (
            "%s(name=%r, artifact_type=%r, version=%r, aliases=%r, version_tags=%s)"
            % (
                self.__class__.__name__,
                self.name,
                self.artifact_type,
                self.version,
                self.aliases,
                self.version_tags,
            )
        )

    @property
    def assets(self):
        """
        The list of `ArtifactAssets` that have been logged with this `Artifact`.
        """
        artifact_version_assets = []

        for asset in self._assets.values():

            if isinstance(asset, PreprocessedRemoteAsset):
                artifact_version_assets.append(
                    ArtifactAsset(
                        remote=True,
                        logical_path=asset.logical_path,
                        # Semantically remote files have a 0 size, but we are still counting
                        # the size for upload progress
                        size=0,
                        link=asset.remote_uri,
                        metadata=asset.metadata,
                        asset_type=asset.upload_type,
                        local_path_or_data=None,
                    )
                )
            elif isinstance(asset, PreprocessedSyncedRemoteAsset):
                artifact_version_assets.append(
                    ArtifactAsset(
                        remote=True,
                        logical_path=asset.logical_path,
                        size=asset.size,
                        link=asset.remote_uri,
                        metadata=asset.metadata,
                        asset_type=asset.upload_type,
                        local_path_or_data=asset.local_path,
                    )
                )
            else:
                artifact_version_assets.append(
                    ArtifactAsset(
                        remote=False,
                        logical_path=asset.logical_path,
                        size=asset.size,
                        link=None,
                        metadata=asset.metadata,
                        asset_type=None,
                        local_path_or_data=asset.local_path_or_data,
                    )
                )

        return artifact_version_assets

    @property
    def download_local_path(self):
        # type: () -> Optional[str]
        """If the Artifact object was returned by `LoggedArtifact.download`, returns the root path
        where the assets has been downloaded. Else, returns None.
        """
        return self._download_local_path


def _get_artifact(rest_api_client, get_artifact_params, experiment_id, summary, config):
    # type: (RestApiClient, Dict[str, Optional[str]], str, Summary, Config) -> LoggedArtifact

    try:
        result = rest_api_client.get_artifact_version_details(**get_artifact_params)
    except CometRestApiException as e:
        if e.sdk_error_code == 624523:
            raise ArtifactNotFound("Artifact not found with %r" % (get_artifact_params))
        if e.sdk_error_code == 90403 or e.sdk_error_code == 90402:
            raise ArtifactNotFinalException(
                "Artifact %r is not in a finalized state and cannot be accessed"
                % (get_artifact_params)
            )

        raise
    except Exception:
        raise GetArtifactException(
            "Get artifact failed with an error, check the logs for details"
        )

    artifact_name = result["artifact"]["artifactName"]
    artifact_version = result["artifactVersion"]
    artifact_metadata = result["metadata"]
    if artifact_metadata:
        try:
            artifact_metadata = json.loads(artifact_metadata)
        except Exception:
            LOGGER.warning(
                "Couldn't decode metadata for artifact %r:%r"
                % (artifact_name, artifact_version)
            )
            artifact_metadata = None

    return LoggedArtifact(
        aliases=result["alias"],
        artifact_id=result["artifact"]["artifactId"],
        artifact_name=artifact_name,
        artifact_tags=result["artifact"]["tags"],
        artifact_type=result["artifact"]["artifactType"],
        artifact_version_id=result["artifactVersionId"],
        config=config,
        experiment_key=experiment_id,  # TODO: Remove ME
        metadata=artifact_metadata,
        rest_api_client=rest_api_client,
        size=result["sizeInBytes"],
        source_experiment_key=result["experimentKey"],
        summary=summary,
        version_tags=result["tags"],
        version=artifact_version,
        workspace=result["artifact"]["workspaceName"],
    )


class LoggedArtifact(object):
    def __init__(
        self,
        artifact_name,
        artifact_type,
        artifact_id,
        artifact_version_id,
        workspace,
        rest_api_client,  # type: RestApiClient
        experiment_key,
        version,
        aliases,
        artifact_tags,
        version_tags,
        size,
        metadata,
        source_experiment_key,  # type: str
        summary,
        config,  # type: Config
    ):
        # type: (...) -> None
        """
        You shouldn't try to create this object by hand, please use
        [Experiment.get_artifact()](/docs/python-sdk/Experiment/#experimentget_artifact) instead to
        retrieve an artifact.
        """
        # Artifact fields
        self._artifact_type = artifact_type
        self._name = artifact_name
        self._artifact_id = artifact_id
        self._artifact_version_id = artifact_version_id

        self._version = semantic_version.Version(version)
        self._aliases = frozenset(aliases)
        self._rest_api_client = rest_api_client
        self._workspace = workspace
        self._artifact_tags = frozenset(artifact_tags)
        self._version_tags = frozenset(version_tags)
        self._size = size
        self._source_experiment_key = source_experiment_key
        self._experiment_key = experiment_key  # TODO: Remove ME
        self._summary = summary
        self._config = config

        if metadata is not None:
            self._metadata = ImmutableDict(metadata)
        else:
            self._metadata = ImmutableDict()

    def _raw_assets(self):
        """Returns the artifact version ID assets"""
        return self._rest_api_client.get_artifact_files(
            workspace=self._workspace,
            name=self._name,
            version=str(self.version),
        )["files"]

    def _to_logged_artifact(self, raw_artifact_asset):
        # type: (Dict[str, Any]) -> LoggedArtifactAsset

        if "remote" in raw_artifact_asset:
            remote = raw_artifact_asset["remote"]
        else:
            remote = (
                raw_artifact_asset["link"] is not None
            )  # TODO: Remove me after October 1st

        return LoggedArtifactAsset(
            remote,
            raw_artifact_asset["fileName"],
            raw_artifact_asset["fileSize"],
            raw_artifact_asset["link"],
            raw_artifact_asset["metadata"],
            raw_artifact_asset["type"],
            raw_artifact_asset["assetId"],
            self._artifact_version_id,
            self._artifact_id,
            self._source_experiment_key,
            verify_tls=self._config.get_bool(
                None, "comet.internal.check_tls_certificate"
            ),
            rest_api_client=self._rest_api_client,
            download_timeout=self._config.get_int(None, "comet.timeout.file_download"),
            logged_artifact_repr=self.__repr__(),
            logged_artifact_str=self.__str__(),
            experiment_key=self._experiment_key,
        )

    @property
    def assets(self):
        # type: () -> List[LoggedArtifactAsset]
        """
        The list of `LoggedArtifactAsset` that have been logged with this `LoggedArtifact`.
        """
        artifact_version_assets = []

        for asset in self._raw_assets():
            artifact_version_assets.append(self._to_logged_artifact(asset))

        return artifact_version_assets

    @property
    def remote_assets(self):
        # type: () -> List[LoggedArtifactAsset]
        """
        The list of remote `LoggedArtifactAsset` that have been logged with this `LoggedArtifact`.
        """
        artifact_version_assets = []

        for asset in self._raw_assets():
            if "remote" in asset:
                remote = asset["remote"]
            else:
                remote = asset["link"] is not None  # TODO: Remove me after October 1st

            if not remote:
                continue

            artifact_version_assets.append(self._to_logged_artifact(asset))

        return artifact_version_assets

    def get_asset(self, asset_logical_path):
        # type: (str) -> LoggedArtifactAsset
        """
        Returns the LoggedArtifactAsset object matching the given asset_logical_path or raises an Exception
        """
        for asset in self._raw_assets():
            if asset["fileName"] == asset_logical_path:
                return self._to_logged_artifact(asset)

        raise ArtifactAssetNotFound(asset_logical_path, self)

    def download(
        self,
        path=None,
        overwrite_strategy=False,
        sync_mode=True,
    ):
        # type: (Optional[str], Union[bool, str], bool) -> Artifact
        """
        Download the current Artifact Version assets to a given directory (or the local directory by
        default). This downloads only non-remote assets. You can access remote assets link with the
        `artifact.assets` property.

        Args:
            path: String, Optional. Where to download artifact version assets. If not provided,
                a temporary path will be used, the root path can be accessed through the Artifact object
                which is returned by download under the `.download_local_path` attribute.
            overwrite_strategy: String or Boolean. One of the three possible strategies to handle
                conflict when trying to download an artifact version asset to a path with an existing
                file. See below for allowed values. Default is False or "FAIL".
            sync_mode: Boolean. Enables download of remote assets from the cloud storage platforms (AWS S3, GCP GS).

        Overwrite strategy allowed values:

            * False or "FAIL": If a file already exists and its content is different, raise the
            `comet_ml.exceptions.ArtifactDownloadException`.
            * "PRESERVE": If a file already exists and its content is different, show a WARNING but
            preserve the existing content.
            * True or "OVERWRITE": If a file already exists and its content is different, replace it
            by the asset version asset.

        Returns: Artifact object
        """

        if path is None:
            root_path = tempfile.mkdtemp()
        else:
            root_path = path

        overwrite_strategy = _validate_overwrite_strategy(overwrite_strategy)

        new_artifact_assets = {}  # type: Dict[str, PreprocessedAsset]
        new_artifact_asset_ids = set()

        try:
            raw_assets = self._raw_assets()
        except Exception:
            raise ArtifactDownloadException(
                "Cannot get asset list for Artifact %r" % self
            )

        worker_cpu_ratio = self._config.get_int(
            None, "comet.internal.file_upload_worker_ratio"
        )
        worker_count = self._config.get_raw(None, "comet.internal.worker_count")
        download_manager = FileDownloadManager(
            worker_cpu_ratio=worker_cpu_ratio, worker_count=worker_count
        )

        file_download_timeout = self._config.get_int(
            None, "comet.timeout.file_download"
        )
        verify_tls = self._config.get_bool(None, "comet.internal.check_tls_certificate")

        download_result_holder = namedtuple(
            "_download_result_holder",
            [
                "download_result",
                "asset_filename",
                "asset_path",
                "asset_metadata",
                "asset_id",
                "asset_synced",
                "asset_type",
                "asset_overwrite_strategy",
                "asset_remote_uri",
            ],
        )
        results = list()  # type: List[download_result_holder]

        self_repr = repr(self)
        self_str = str(self)

        for asset in raw_assets:
            asset_metadata = asset["metadata"]
            if asset_metadata is not None:
                asset_metadata = json.loads(asset["metadata"])

            if "remote" in asset:
                asset_remote = asset["remote"]
            else:
                asset_remote = (
                    asset["link"] is not None
                )  # TODO: Remove me after October 1st

            remote_uri = asset.get("link", None)
            asset_filename = asset["fileName"]
            asset_id = asset["assetId"]
            asset_path = os.path.join(root_path, asset_filename)
            asset_synced = False
            asset_sync_error = None
            asset_type = asset.get("type", "asset")
            if asset_metadata is not None:
                if META_SYNCED in asset_metadata:
                    asset_synced = asset_metadata[META_SYNCED]
                if META_ERROR_MESSAGE in asset_metadata:
                    asset_sync_error = asset_metadata[META_ERROR_MESSAGE]

            if asset_remote is True:
                # check if sync_mode is not enabled or asset was not synced properly
                if sync_mode is False or asset_synced is False:
                    # check if error is in metadata - failed to sync during upload due cloud storage error
                    if asset_sync_error is not None and sync_mode is True:
                        # raise error only if sync_mode==True
                        raise ArtifactDownloadException(
                            ARTIFACT_ASSET_DOWNLOAD_FAILED_WITH_ERROR
                            % (asset_filename, asset_sync_error)
                        )

                    # We don't download plain remote assets
                    new_artifact_assets[asset_filename] = PreprocessedRemoteAsset(
                        remote_uri=remote_uri,
                        overwrite=False,
                        upload_type=asset_type,
                        metadata=asset_metadata,
                        step=None,
                        asset_id=asset_id,
                        logical_path=asset_filename,
                        size=len(asset["link"]),
                    )
                    new_artifact_asset_ids.add(asset_id)
                    self._summary.increment_section("downloads", "artifact assets")
                else:
                    # check that asset is from supported cloud storage if sync_mode enabled
                    # and asset was synced during Artifact upload
                    o = urlparse(remote_uri)
                    if o.scheme == "s3" or o.scheme == "gs":
                        # register download from AWS S3 or GCS
                        if META_FILE_SIZE in asset_metadata:
                            asset_file_size = asset_metadata[META_FILE_SIZE]
                        else:
                            asset_file_size = 0

                        version_id = None
                        if META_VERSION_ID in asset_metadata:
                            version_id = asset_metadata[META_VERSION_ID]

                        if o.scheme == "s3":
                            data_writer = AssetDataWriterFromS3(
                                s3_uri=remote_uri, version_id=version_id
                            )
                        else:
                            data_writer = AssetDataWriterFromGCS(
                                gs_uri=remote_uri, version_id=version_id
                            )

                        result = download_manager.download_file_async(
                            _download_cloud_storage_artifact_asset,
                            data_writer=data_writer,
                            estimated_size=asset_file_size,
                            asset_id=asset_id,
                            artifact_repr=self_repr,
                            artifact_str=self_str,
                            asset_logical_path=asset_filename,
                            asset_path=asset_path,
                            overwrite=overwrite_strategy,
                        )
                        results.append(
                            download_result_holder(
                                download_result=result,
                                asset_filename=asset_filename,
                                asset_path=asset_path,
                                asset_metadata=asset_metadata,
                                asset_id=asset_id,
                                asset_synced=asset_synced,
                                asset_type=asset_type,
                                asset_overwrite_strategy=overwrite_strategy,
                                asset_remote_uri=remote_uri,
                            )
                        )
                    else:
                        # unsupported URI scheme for synced asset
                        raise ArtifactDownloadException(
                            UNSUPPORTED_URI_SYNCED_REMOTE_ASSET % remote_uri
                        )
            else:
                prepared_request = (
                    self._rest_api_client._prepare_experiment_asset_request(
                        asset_id, self._experiment_key, asset["artifactVersionId"]
                    )
                )
                url, params, headers = prepared_request

                # register asset to be downloaded
                result = download_manager.download_file_async(
                    _download_artifact_asset,
                    url=url,
                    params=params,
                    headers=headers,
                    timeout=file_download_timeout,
                    verify_tls=verify_tls,
                    asset_id=asset_id,
                    artifact_repr=self_repr,
                    artifact_str=self_str,
                    asset_logical_path=asset_filename,
                    asset_path=asset_path,
                    overwrite=overwrite_strategy,
                    estimated_size=asset["fileSize"],
                )

                results.append(
                    download_result_holder(
                        download_result=result,
                        asset_filename=asset_filename,
                        asset_path=asset_path,
                        asset_metadata=asset_metadata,
                        asset_id=asset_id,
                        asset_synced=asset_synced,
                        asset_type=asset_type,
                        asset_overwrite_strategy=overwrite_strategy,
                        asset_remote_uri=remote_uri,
                    )
                )

        # Forbid new usage
        download_manager.close()

        # Wait for download manager to complete registered file downloads
        if not download_manager.all_done():
            monitor = FileDownloadManagerMonitor(download_manager)

            LOGGER.info(
                ARTIFACT_DOWNLOAD_START_MESSAGE,
                self._workspace,
                self._name,
                self._version,
            )

            wait_for_done(
                check_function=monitor.all_done,
                timeout=self._config.get_int(None, "comet.timeout.artifact_download"),
                progress_callback=monitor.log_remaining_downloads,
                sleep_time=15,
            )

        # iterate over download results and create file assets descriptors
        try:
            for result in results:
                try:
                    result.download_result.get(file_download_timeout)

                    new_asset_size = os.path.getsize(result.asset_path)
                except Exception:
                    # display failed message
                    LOGGER.error(
                        ARTIFACT_ASSET_DOWNLOAD_FAILED,
                        result.asset_filename,
                        self._workspace,
                        self._name,
                        self._version,
                        exc_info=True,
                    )

                    raise ArtifactDownloadException(
                        "Cannot download Asset %s for Artifact %s"
                        % (result.asset_filename, self_repr)
                    )

                self._summary.increment_section(
                    "downloads",
                    "artifact assets",
                    size=new_asset_size,
                )

                if result.asset_synced is False:
                    # downloaded local asset
                    new_artifact_assets[result.asset_filename] = PreprocessedFileAsset(
                        local_path_or_data=result.asset_path,
                        upload_type=result.asset_type,
                        logical_path=result.asset_filename,
                        metadata=result.asset_metadata,
                        overwrite=result.asset_overwrite_strategy,
                        step=None,
                        asset_id=result.asset_id,
                        grouping_name=None,  # TODO: FIXME?
                        extension=None,  # TODO: FIXME?
                        size=new_asset_size,
                        copy_to_tmp=False,
                    )
                else:
                    # downloaded synced remote asset from cloud storage (AWS S3, GCS)
                    new_artifact_assets[
                        result.asset_filename
                    ] = PreprocessedSyncedRemoteAsset(
                        remote_uri=result.asset_remote_uri,
                        overwrite=result.asset_overwrite_strategy,
                        upload_type=result.asset_type,
                        metadata=result.asset_metadata,
                        step=None,
                        asset_id=result.asset_id,
                        logical_path=result.asset_filename,
                        size=new_asset_size,
                        local_path=result.asset_path,
                    )

                new_artifact_asset_ids.add(result.asset_id)

            # display success message
            LOGGER.info(
                ARTIFACT_DOWNLOAD_FINISHED, self._workspace, self._name, self._version
            )
        finally:
            download_manager.join()

        return Artifact._from_logged_artifact(
            name=self._name,
            artifact_type=self._artifact_type,
            assets=new_artifact_assets,
            root_path=root_path,
            asset_ids=new_artifact_asset_ids,
        )

    def get_source_experiment(
        self,
        api_key=None,
        cache=True,
    ):
        # type: (Optional[str], bool) -> APIExperiment
        """
        Returns an APIExperiment object pointing to the experiment that created this artifact version, assumes that the API key is set else-where.
        """
        return APIExperiment(
            api_key=api_key,
            cache=cache,
            previous_experiment=self._source_experiment_key,
        )

    def update_artifact_tags(self, new_artifact_tags):
        # type: (Sequence[str]) -> None
        """
        Update the logged artifact tags
        """
        new_artifact_tags_list = list(new_artifact_tags)

        self._rest_api_client.update_artifact(
            self._artifact_id,
            tags=new_artifact_tags,
        )

        self._artifact_tags = frozenset(new_artifact_tags_list)

    def update_version_tags(self, new_version_tags):
        # type: (Sequence[str]) -> None
        """
        Update the logged artifact version tags
        """
        new_version_tags_list = list(new_version_tags)

        self._rest_api_client.update_artifact_version(
            self._artifact_version_id,
            version_tags=new_version_tags_list,
        )

        self._version_tags = frozenset(new_version_tags_list)

    def update_aliases(self, new_aliases):
        # type: (Sequence[str]) -> None
        """
        Update the logged artifact tags
        """
        new_aliases_list = list(new_aliases)

        self._rest_api_client.update_artifact_version(
            self._artifact_version_id,
            version_aliases=new_aliases_list,
        )

        self._aliases = frozenset(new_aliases_list)

    # Public properties
    @property
    def name(self):
        """
        The logged artifact name.
        """
        return self._name

    @property
    def artifact_type(self):
        """
        The logged artifact type.
        """
        return self._artifact_type

    @property
    def version(self):
        """
        The logged artifact version, as a SemanticVersion. See
        https://python-semanticversion.readthedocs.io/en/latest/reference.html#semantic_version.Version
        for reference
        """
        return self._version

    @property
    def workspace(self):
        """
        The logged artifact workspace name.
        """
        return self._workspace

    @property
    def aliases(self):
        """
        The set of logged artifact aliases.
        """
        return self._aliases

    @property
    def metadata(self):
        """
        The logged artifact metadata.
        """
        return self._metadata

    @property
    def version_tags(self):
        """
        The set of logged artifact version tags.
        """
        return self._version_tags

    @property
    def artifact_tags(self):
        """
        The set of logged artifact tags.
        """
        return self._artifact_tags

    @property
    def size(self):
        """
        The total size of logged artifact version; it is the sum of all the artifact version assets.
        """
        return self._size

    @property
    def source_experiment_key(self):
        """
        The experiment key of the experiment that created this LoggedArtifact.
        """
        return self._source_experiment_key

    def __str__(self):
        return "<%s '%s/%s:%s'>" % (
            self.__class__.__name__,
            self._workspace,
            self._name,
            self._version,
        )

    def __repr__(self):
        return (
            self.__class__.__name__
            + "(artifact_name=%r, artifact_type=%r, workspace=%r, version=%r, aliases=%r, artifact_tags=%r, version_tags=%r, size=%r, source_experiment_key=%r)"
            % (
                self._name,
                self._artifact_type,
                self._workspace,
                self._version,
                self._aliases,
                self._artifact_tags,
                self._version_tags,
                self._size,
                self._source_experiment_key,
            )
        )


class ArtifactAsset(object):
    """ArtifactAsset(remote, logical_path, size, link, metadata, asset_type, local_path_or_data):
    represent local and remote assets added to an Artifact object but not yet uploaded"""

    __slots__ = (
        "_remote",
        "_logical_path",
        "_size",
        "_link",
        "_metadata",
        "_asset_type",
        "_local_path_or_data",
    )

    def __init__(
        self,
        remote,  # type: bool
        logical_path,  # type: str
        size,  # type: int
        link,  # type: Optional[str]
        metadata,  # type: Optional[Dict[Any, Any]]
        asset_type,  # type: Optional[str]
        local_path_or_data,  # type: Optional[Any]
    ):
        # type: (...) -> None
        self._remote = remote
        self._logical_path = logical_path
        self._size = size
        self._link = link
        self._metadata = metadata
        self._asset_type = asset_type
        self._local_path_or_data = local_path_or_data

    @property
    def remote(self):
        """Is the asset a remote asset or not, boolean"""
        return self._remote

    @property
    def logical_path(self):
        """Asset relative logical_path, str or None"""
        return self._logical_path

    @property
    def size(self):
        """Asset size if the asset is a non-remote asset, int"""
        return self._size

    @property
    def link(self):
        """Asset remote link if the asset is remote, str or None"""
        return self._link

    @property
    def metadata(self):
        """Asset metadata, dict"""
        return self._metadata

    @property
    def asset_type(self):
        """Asset type, str"""
        return self._asset_type

    @property
    def local_path_or_data(self):
        """Asset local path or in-memory file if the asset is non-remote, str, memory-file or None"""
        return self._local_path_or_data

    def __repr__(self):
        return (
            "%s(remote=%r, logical_path=%r, size=%r, link=%r, metadata=%r, asset_type=%r, local_path_or_data=%r)"
            % (
                self.__class__.__name__,
                self._remote,
                self._logical_path,
                self._size,
                self._link,
                self._metadata,
                self._asset_type,
                self._local_path_or_data,
            )
        )

    def __eq__(self, other):
        return (
            self._remote == other._remote
            and self._logical_path == other._logical_path
            and self._size == other._size
            and self._link == other._link
            and self._metadata == other._metadata
            and self._asset_type == other._asset_type
            and self._local_path_or_data == other._local_path_or_data
        )

    def __lt__(self, other):
        return self._logical_path < other._logical_path


class LoggedArtifactAsset(object):
    """
    LoggedArtifactAsset(remote, logical_path, size, link, metadata, asset_type, id,
    artifact_version_id, artifact_id, source_experiment_key): represent assets logged to an Artifact
    """

    __slots__ = (
        "_remote",
        "_logical_path",
        "_size",
        "_link",
        "_metadata",
        "_asset_type",
        "_id",
        "_artifact_version_id",
        "_artifact_id",
        "_source_experiment_key",
        "_rest_api_client",
        "_download_timeout",
        "_logged_artifact_repr",
        "_logged_artifact_str",
        "_experiment_key",
        "_verify_tls",
    )

    def __init__(
        self,
        remote,
        logical_path,
        size,
        link,
        metadata,
        asset_type,
        id,
        artifact_version_id,
        artifact_id,
        source_experiment_key,
        verify_tls,  # type: bool
        rest_api_client=None,
        download_timeout=None,
        logged_artifact_repr=None,
        logged_artifact_str=None,
        experiment_key=None,
    ):
        # type: (...) -> None
        self._remote = remote
        self._logical_path = logical_path
        self._size = size
        self._link = link
        self._metadata = metadata
        self._asset_type = asset_type
        self._id = id
        self._artifact_version_id = artifact_version_id
        self._artifact_id = artifact_id
        self._source_experiment_key = source_experiment_key

        self._rest_api_client = rest_api_client
        self._download_timeout = download_timeout
        self._verify_tls = verify_tls
        self._logged_artifact_repr = logged_artifact_repr
        self._logged_artifact_str = logged_artifact_str
        self._experiment_key = experiment_key

    @property
    def remote(self):
        "Is the asset a remote asset or not, boolean"
        return self._remote

    @property
    def logical_path(self):
        "Asset relative logical_path, str or None"
        return self._logical_path

    @property
    def size(self):
        "Asset size if the asset is a non-remote asset, int"
        return self._size

    @property
    def link(self):
        "Asset remote link if the asset is remote, str or None"
        return self._link

    @property
    def metadata(self):
        "Asset metadata, dict"
        return self._metadata

    @property
    def asset_type(self):
        "Asset type, str"
        return self._asset_type

    @property
    def id(self):
        "Asset unique id, str"
        return self._id

    @property
    def artifact_version_id(self):
        "Artifact version id, str"
        return self._artifact_version_id

    @property
    def artifact_id(self):
        "Artifact id, str"
        return self._artifact_id

    @property
    def source_experiment_key(self):
        "The experiment key of the experiment that logged this asset, str"
        return self._source_experiment_key

    def __repr__(self):
        return (
            "%s(remote=%r, logical_path=%r, size=%r, link=%r, metadata=%r, asset_type=%r, id=%r, artifact_version_id=%r, artifact_id=%r, source_experiment_key=%r)"
            % (
                self.__class__.__name__,
                self._remote,
                self._logical_path,
                self._size,
                self._link,
                self._metadata,
                self._asset_type,
                self._id,
                self._artifact_version_id,
                self._artifact_id,
                self._source_experiment_key,
            )
        )

    def __eq__(self, other):
        return (
            self._remote == other._remote
            and self._logical_path == other._logical_path
            and self._size == other._size
            and self._link == other._link
            and self._metadata == other._metadata
            and self._asset_type == other._asset_type
            and self._id == other._id
            and self._artifact_version_id == other._artifact_version_id
            and self._artifact_id == other._artifact_id
            and self._source_experiment_key == other._source_experiment_key
        )

    def __lt__(self, other):
        return self._logical_path < other._logical_path

    def download(
        self,
        local_path=None,  # if None, downloads to a tmp path
        logical_path=None,
        overwrite_strategy=False,
    ):
        """
        Download the asset to a given full path or directory

        Returns: ArtifactAsset object

        Args:
          local_path: the root folder to which to download.
            if None, will download to a tmp path
            if str, will be either a root local path or a full local path

          logical_path: the path relative to the root local_path to use
            if None and local_path==None then no relative path is used,
              file would just be a tmp path on local disk
            if None and local_path!=None then the local_path will be treated
              as a root path, and the asset's logical_path will be appended
              to the root path to form a full local path
            if "" or False then local_path will be used as a full path
              (local_path can also be None)

            overwrite_strategy: can be False, "FAIL", "PRESERVE" or "OVERWRITE"
              and follows the same semantics for overwrite strategy as artifact.download()
        """
        if local_path is None:
            root_path = tempfile.mkdtemp()
        else:
            root_path = local_path

        if logical_path is None:
            asset_filename = self._logical_path
        else:
            asset_filename = logical_path

        result_asset_path = os.path.join(root_path, asset_filename)

        prepared_request = self._rest_api_client._prepare_experiment_asset_request(
            self._id, self._experiment_key, self._artifact_version_id
        )
        url, params, headers = prepared_request

        _download_artifact_asset(
            url,
            params,
            headers,
            self._download_timeout,
            self._id,
            self._logged_artifact_repr,
            self._logged_artifact_str,
            asset_filename,
            result_asset_path,
            _validate_overwrite_strategy(overwrite_strategy),
            verify_tls=self._verify_tls,
        )

        return ArtifactAsset(
            remote=False,
            logical_path=self._logical_path,
            size=self._size,
            link=None,
            metadata=self._metadata,
            asset_type=self._asset_type,
            local_path_or_data=result_asset_path,
        )
