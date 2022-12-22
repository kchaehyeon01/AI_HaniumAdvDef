# -*- coding: utf-8 -*-
# *******************************************************
#   ____                     _               _
#  / ___|___  _ __ ___   ___| |_   _ __ ___ | |
# | |   / _ \| '_ ` _ \ / _ \ __| | '_ ` _ \| |
# | |__| (_) | | | | | |  __/ |_ _| | | | | | |
#  \____\___/|_| |_| |_|\___|\__(_)_| |_| |_|_|
#
#  Sign up for free at http://www.comet.ml
#  Copyright (C) 2015-2022 Comet ML INC
#  This file can not be copied and/or distributed
#  without the express permission of Comet ML Inc.
# *******************************************************
import datetime
from copy import deepcopy
from logging import getLogger

from comet_ml._typing import IO, Any, Dict, List, Optional
from comet_ml.cloud_storage_utils import (
    META_CHECKSUM,
    META_FILE_SIZE,
    META_GS_PATH,
    META_SYNCED,
    META_VERSION_ID,
    _create_cloud_storage_obj_logical_path,
    _parse_cloud_storage_uri,
)
from comet_ml.exceptions import LogArtifactException
from comet_ml.file_uploader import PreprocessedRemoteAsset
from comet_ml.logging_messages import (
    FAILED_TO_ADD_ARTIFACT_GS_ASSETS_TOO_MANY_KEYS,
    GCP_CLIENT_IMPORT_FAILED,
)
from comet_ml.utils import generate_guid
from comet_ml.validation_utils import validate_metadata

LOGGER = getLogger(__name__)


class GSFileObject(object):
    """Represents GCP GS file object"""

    __slots__ = (
        "key",
        "bucket",
        "size",
        "modified",
        "content_type",
        "etag",
        "version_id",
        "public_url",
    )

    def __init__(
        self,
        key,
        bucket,
        size,
        modified,
        content_type,
        etag,
        version_id,
        public_url=None,
    ):
        # type: (str, str, int, datetime.datetime, str, str, str, Optional[str]) -> None
        """Creates GCP GS object with given parameters"""
        self.key = key
        self.bucket = bucket
        self.size = size
        self.modified = modified
        self.content_type = content_type
        self.etag = etag
        self.version_id = version_id
        self.public_url = public_url

    def is_folder(self):
        # type: () -> bool
        """Checks if this object represents GS folder."""
        return self.key.endswith("/")

    def __str__(self):
        return (
            "GS object -> key: '%s', bucket: '%s', modified: %s, size: %d, ETag: %s, "
            "content type: %s, version ID: %s, public url: %s"
            % (
                self.key,
                self.bucket,
                self.modified,
                self.size,
                self.etag,
                self.content_type,
                self.version_id,
                self.public_url,
            )
        )


def preprocess_remote_gs_assets(
    remote_uri,  # type: Any
    logical_path,  # type: Optional[str]
    overwrite,  # type: bool
    upload_type,  # type: Optional[str]
    metadata,  # type: Optional[Dict[Any, Any]]
    max_synced_objects,  # type: int
    step=0,
):
    # type: (...) -> List[PreprocessedRemoteAsset]
    if not remote_uri.startswith("gs://"):
        raise ValueError("Remote asset URI is not GS bucket related: %s" % remote_uri)

    bucket_name, object_prefix = _parse_cloud_storage_uri(remote_uri)

    try:
        from google.cloud import storage
    except ImportError as ex:
        LOGGER.warning(
            GCP_CLIENT_IMPORT_FAILED, exc_info=True, extra={"show_traceback": True}
        )
        raise ex

    storage_client = storage.Client()
    bucket_objects, is_truncated = _list_blobs_with_prefix(
        storage_client=storage_client,
        bucket_name=bucket_name,
        prefix=object_prefix,
        max_keys=max_synced_objects,
    )
    if is_truncated is True:
        raise LogArtifactException(
            backend_err_msg=FAILED_TO_ADD_ARTIFACT_GS_ASSETS_TOO_MANY_KEYS
            % (remote_uri, max_synced_objects)
        )

    metadata = validate_metadata(metadata)

    assets = list()
    for obj in bucket_objects:
        if not obj.is_folder():
            assets.append(
                _preprocess_gs_asset_object(
                    gs_object=obj,
                    object_prefix=object_prefix,
                    logical_path=logical_path,
                    overwrite=overwrite,
                    upload_type=upload_type,
                    metadata=metadata,
                    step=step,
                )
            )

    return assets


def _preprocess_gs_asset_object(
    gs_object,  # type: GSFileObject
    object_prefix,  # type: str
    logical_path,  # type: Optional[str]
    overwrite,  # type: bool
    upload_type,  # type: Optional[str]
    metadata,  # type: Optional[Dict[Any, Any]]
    step,  # type: int
):
    asset_id = generate_guid()
    logical_path = _create_cloud_storage_obj_logical_path(
        key=gs_object.key, folder=object_prefix, logical_path=logical_path
    )

    return PreprocessedRemoteAsset(
        remote_uri=_create_gs_path(bucket=gs_object.bucket, key=gs_object.key),
        overwrite=overwrite,
        upload_type=upload_type,
        metadata=_fill_gs_asset_object_metadata(
            gs_object=gs_object, orig_metadata=metadata
        ),
        asset_id=asset_id,
        logical_path=logical_path,
        size=gs_object.size,
        step=step,
    )


def _list_blobs_with_prefix(
    storage_client, bucket_name, prefix=None, delimiter=None, max_keys=10000
):
    # type: (Any, str, Optional[str], Optional[str], int) -> (List[GSFileObject], bool)
    """Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:
        a/1.txt
        a/b/2.txt
    If you specify prefix ='a/', without a delimiter, you'll get back:
        a/1.txt
        a/b/2.txt
    However, if you specify prefix='a/' and delimiter='/', you'll get back
    only the file directly under 'a/':
        a/1.txt
    As part of the response, you'll also get back a blobs.prefixes entity
    that lists the "subfolders" under `a/`:
        a/b/
    """
    # There are no easy way to get count of objects in the bucket.
    # Thus, we try to get max_keys + 1 and check if it is greater that max_keys. In that case we assume that
    # number of objects returned was truncated
    max_keys_plus_one = max_keys + 1
    blobs = storage_client.list_blobs(
        bucket_name, prefix=prefix, max_results=max_keys_plus_one, delimiter=delimiter
    )
    blob_objects = list()
    for blob in blobs:
        blob_objects.append(
            GSFileObject(
                key=blob.name,
                bucket=bucket_name,
                size=blob.size,
                modified=blob.updated,
                etag=blob.etag,
                content_type=blob.content_type,
                version_id=str(blob.generation),
                public_url=blob.public_url,
            )
        )
    # the storage_client.list_blobs returns iterable, so we are unable to get its size before actually iterating
    if len(blob_objects) > max_keys:
        # the list of objects was truncated
        return None, True

    return blob_objects, False


def _get_gcp_object(storage_client, bucket_name, blob_name, version_id=None):
    # type: (Any, str, str, Optional[str]) -> GSFileObject
    bucket = storage_client.bucket(bucket_name)
    # Retrieve a blob, and its metadata, from Google Cloud Storage.
    if version_id is not None:
        blob = bucket.get_blob(blob_name=blob_name, generation=int(version_id))
    else:
        blob = bucket.get_blob(blob_name=blob_name)

    if blob is None:
        raise ValueError("gcp object not found")

    return GSFileObject(
        key=blob.name,
        bucket=bucket_name,
        size=blob.size,
        modified=blob.updated,
        etag=blob.etag,
        content_type=blob.content_type,
        version_id=str(blob.generation),
        public_url=blob.public_url,
    )


def download_gs_file(gs_uri, file_object, callback, version_id=None):
    # type: (str, IO[bytes], Any, Optional[str]) -> None
    """Downloads the GS object into provided file_object with optional version_id. The callback will be notified
    after download completed with the size of downloaded object."""
    size = _download_gs_file(
        gs_uri=gs_uri, file_object=file_object, version_id=version_id
    )
    # notify callback after download finishes - there is no way to get intermediate download results from GCP library
    callback(size)


def _download_gs_file(gs_uri, file_object, version_id=None):
    # type: (str, IO[bytes], Optional[str]) -> int
    """Downloads the GS object into provided file_object with optional version_id"""
    try:
        from google.cloud import storage
    except ImportError as ex:
        LOGGER.warning(
            GCP_CLIENT_IMPORT_FAILED, exc_info=True, extra={"show_traceback": True}
        )
        raise ex
    client = storage.Client()
    bucket_name, blob_name = _parse_cloud_storage_uri(gs_uri)
    bucket = client.bucket(bucket_name)
    if version_id is not None:
        blob = bucket.get_blob(blob_name=blob_name, generation=int(version_id))
    else:
        blob = bucket.get_blob(blob_name=blob_name)

    if blob is None:
        raise ValueError(
            "gcp object %r with version '%s' not found" % (gs_uri, version_id)
        )

    if version_id is not None:
        blob.download_to_file(
            file_obj=file_object, client=client, if_generation_match=int(version_id)
        )
    else:
        blob.download_to_file(file_obj=file_object, client=client)

    return blob.size


def _fill_gs_asset_object_metadata(gs_object, orig_metadata):
    # type: (GSFileObject, Dict[Any, Any]) -> Dict[Any, Any]
    if orig_metadata is not None:
        metadata = deepcopy(orig_metadata)
    else:
        metadata = dict()

    metadata[META_SYNCED] = True
    metadata[META_CHECKSUM] = gs_object.etag
    metadata[META_FILE_SIZE] = gs_object.size
    metadata[META_GS_PATH] = _create_gs_path(bucket=gs_object.bucket, key=gs_object.key)
    if gs_object.version_id is not None and int(gs_object.version_id) > 0:
        metadata[META_VERSION_ID] = gs_object.version_id

    return metadata


def _create_gs_path(bucket, key):
    # type: (str, str) -> str
    return "gs://%s/%s" % (bucket, key)
