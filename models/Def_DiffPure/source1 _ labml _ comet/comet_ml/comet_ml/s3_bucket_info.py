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
    META_OBJECT_S3_PLATFORM,
    META_S3_PATH,
    META_SYNCED,
    META_VALUE_S3_PLATFORM_AWS,
    META_VERSION_ID,
    _create_cloud_storage_obj_logical_path,
    _parse_cloud_storage_uri,
)
from comet_ml.exceptions import LogArtifactException
from comet_ml.file_uploader import PreprocessedRemoteAsset
from comet_ml.logging_messages import (
    BOTO3_IMPORT_FAILED,
    FAILED_TO_ADD_ARTIFACT_S3_ASSETS_TOO_MANY_KEYS,
)
from comet_ml.utils import generate_guid
from comet_ml.validation_utils import validate_metadata

AWS_CONTENTS = "Contents"
AWS_CONTENT_LENGTH = "ContentLength"
AWS_CONTENT_TYPE = "ContentType"
AWS_OBJ_KEY = "Key"
AWS_OBJ_LAST_MODIFIED = "LastModified"
AWS_OBJ_SIZE = "Size"
AWS_OBJ_ETAG = "ETag"
AWS_OBJ_VERSION_ID = "VersionId"
AWS_LIST_TRUNCATED = "IsTruncated"

AWS_NULL_VALUE = "null"

CONTENT_TYPE_DIRECTORY = "application/x-directory"

LOGGER = getLogger(__name__)


class S3FileObject(object):
    """Represents S3 file object stored in particular bucket."""

    __slots__ = (
        "key",
        "bucket",
        "size",
        "modified",
        "content_type",
        "etag",
        "version_id",
    )

    def __init__(self, key, bucket, size, modified, content_type, etag, version_id):
        # type: (str, str, int, datetime.datetime, str, str, str) -> None
        """Creates S3 object with given parameters"""
        self.key = key
        self.bucket = bucket
        self.size = size
        self.modified = modified
        self.content_type = content_type
        self.etag = etag
        self.version_id = version_id

    def is_folder(self):
        # type: () -> bool
        """Checks if this object represents S3 folder."""
        parts = self.content_type.split(
            ";"
        )  # usually: application/x-directory; charset=UTF-8
        return parts[0].strip() == CONTENT_TYPE_DIRECTORY

    def __str__(self):
        return (
            "S3 object -> key: '%s', bucket: '%s', modified: %s, size: %d, ETag: %s, "
            "content type: %s, version ID: %s"
            % (
                self.key,
                self.bucket,
                self.modified,
                self.size,
                self.etag,
                self.content_type,
                self.version_id,
            )
        )


def preprocess_remote_s3_assets(
    remote_uri,  # type: Any
    logical_path,  # type: Optional[str]
    overwrite,  # type: bool
    upload_type,  # type: Optional[str]
    metadata,  # type: Optional[Dict[Any, Any]]
    max_synced_objects,  # type: int
    step=0,
):
    # type: (...) -> List[PreprocessedRemoteAsset]
    if not remote_uri.startswith("s3://"):
        raise ValueError("Remote asset URI is not S3 bucket related: %s" % remote_uri)

    bucket_name, object_prefix = _parse_cloud_storage_uri(remote_uri)

    try:
        import boto3
    except ImportError as ex:
        LOGGER.warning(
            BOTO3_IMPORT_FAILED, exc_info=True, extra={"show_traceback": True}
        )
        raise ex

    s3_client = boto3.client("s3")
    bucket_objects, is_truncated = _list_bucket_objects(
        s3_client=s3_client,
        bucket=bucket_name,
        prefix=object_prefix,
        max_keys=max_synced_objects,
    )

    if is_truncated is True:
        raise LogArtifactException(
            backend_err_msg=FAILED_TO_ADD_ARTIFACT_S3_ASSETS_TOO_MANY_KEYS
            % (remote_uri, max_synced_objects)
        )

    metadata = validate_metadata(metadata)

    assets = list()
    for obj in bucket_objects:
        if not obj.is_folder():
            assets.append(
                _preprocess_s3_asset_object(
                    s3_object=obj,
                    object_prefix=object_prefix,
                    logical_path=logical_path,
                    overwrite=overwrite,
                    upload_type=upload_type,
                    metadata=metadata,
                    step=step,
                )
            )

    return assets


def _preprocess_s3_asset_object(
    s3_object,  # type: S3FileObject
    object_prefix,  # type: str
    logical_path,  # type: Optional[str]
    overwrite,  # type: bool
    upload_type,  # type: Optional[str]
    metadata,  # type: Optional[Dict[Any, Any]]
    step,  # type: int
):
    # type: (...) -> PreprocessedRemoteAsset
    asset_id = generate_guid()
    logical_path = _create_cloud_storage_obj_logical_path(
        key=s3_object.key, folder=object_prefix, logical_path=logical_path
    )

    return PreprocessedRemoteAsset(
        remote_uri=_create_s3_path(bucket=s3_object.bucket, key=s3_object.key),
        overwrite=overwrite,
        upload_type=upload_type,
        metadata=_fill_s3_asset_object_metadata(
            s3_object=s3_object, orig_metadata=metadata
        ),
        asset_id=asset_id,
        logical_path=logical_path,
        size=s3_object.size,
        step=step,
    )


def _fill_s3_asset_object_metadata(s3_object, orig_metadata):
    # type: (S3FileObject, Dict[Any, Any]) -> Dict[Any, Any]
    if orig_metadata is not None:
        metadata = deepcopy(orig_metadata)
    else:
        metadata = dict()

    metadata[META_OBJECT_S3_PLATFORM] = META_VALUE_S3_PLATFORM_AWS
    metadata[META_SYNCED] = True
    metadata[META_CHECKSUM] = s3_object.etag
    metadata[META_FILE_SIZE] = s3_object.size
    metadata[META_S3_PATH] = _create_s3_path(bucket=s3_object.bucket, key=s3_object.key)
    if s3_object.version_id is not None:
        metadata[META_VERSION_ID] = s3_object.version_id

    return metadata


def _create_s3_path(bucket, key):
    # type: (str, str) -> str
    return "s3://%s/%s" % (bucket, key)


def _list_bucket_objects(s3_client, bucket, prefix=None, max_keys=10000):
    # type: (Any, str, Optional[str], int) -> (List[S3FileObject], bool)
    """Lists objects in specific bucket limited by max_keys entities."""
    if prefix is None:
        prefix = ""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
    objects = response[AWS_CONTENTS]
    is_truncated = response[AWS_LIST_TRUNCATED]

    s3_objects = list()
    for obj in objects:
        key = obj[AWS_OBJ_KEY]
        obj_info = _get_s3_object(s3_client=s3_client, bucket=bucket, key=key)
        s3_objects.append(obj_info)

    return s3_objects, is_truncated


def _get_s3_object(s3_client, bucket, key, version_id=None):
    # type: (Any, str, str, Optional[str]) -> S3FileObject
    """Acquires information about specific object in the S3 bucket."""
    if version_id is not None:
        obj_info = s3_client.get_object(Bucket=bucket, Key=key, VersionId=version_id)
    else:
        obj_info = s3_client.get_object(Bucket=bucket, Key=key)

    return S3FileObject(
        key=key,
        bucket=bucket,
        size=obj_info[AWS_CONTENT_LENGTH],
        modified=obj_info[AWS_OBJ_LAST_MODIFIED],
        etag=_normalize_etag(obj_info[AWS_OBJ_ETAG]),
        content_type=_extract_aws_value(obj_info, AWS_CONTENT_TYPE),
        version_id=_extract_aws_value(obj_info, AWS_OBJ_VERSION_ID),
    )


def _extract_aws_value(obj_info, key):
    value = obj_info.get(key, None)
    if value is None or value == AWS_NULL_VALUE:
        return None
    return value


def download_s3_file(s3_uri, file_object, callback, version_id=None):
    # type: (str, IO[bytes], Any, Optional[str]) -> None
    try:
        import boto3
    except ImportError as ex:
        LOGGER.warning(
            BOTO3_IMPORT_FAILED, exc_info=True, extra={"show_traceback": True}
        )
        raise ex
    s3_client = boto3.client("s3")
    download_s3_file_uri(
        s3_client=s3_client,
        s3_uri=s3_uri,
        file_object=file_object,
        callback=callback,
        version_id=version_id,
    )


def download_s3_file_uri(s3_client, s3_uri, file_object, callback, version_id=None):
    # type: (Any, str, IO[bytes], Any, Optional[str]) -> None
    """Downloads S3 object from given URI into specified path"""
    bucket_name, obj_key = _parse_cloud_storage_uri(storage_uri=s3_uri)
    s3_file_obj = _get_s3_object(
        s3_client=s3_client, bucket=bucket_name, key=obj_key, version_id=version_id
    )
    download_s3_file_object(
        s3_client=s3_client,
        s3_file_obj=s3_file_obj,
        file_object=file_object,
        callback=callback,
    )


def download_s3_file_object(s3_client, s3_file_obj, file_object, callback):
    # type: (Any, S3FileObject, IO[bytes], Any) -> None
    """Downloads S3 object defined by s3_file_obj into specified path"""
    extra_args = dict()
    if s3_file_obj.version_id is not None:
        extra_args[AWS_OBJ_VERSION_ID] = s3_file_obj.version_id

    s3_client.download_fileobj(
        Bucket=s3_file_obj.bucket,
        Key=s3_file_obj.key,
        Fileobj=file_object,
        Callback=callback,
        ExtraArgs=extra_args,
    )


def _normalize_etag(etag):
    # type: (str) -> str
    """Due to some reason AWS returns ETag in double quotes. This will normalize it by removing extra quotes."""
    return etag.replace('"', "")
