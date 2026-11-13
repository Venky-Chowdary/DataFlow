"""Iceberg file IO that can address a Windows local warehouse.

pyiceberg keeps table locations as URIs and resolves them scheme-first, which
leaves a local Windows warehouse unreachable in *either* spelling:

* ``C:\\warehouse`` — the drive letter reads as the scheme, refused with
  *Unrecognized filesystem type in URI: c*.
* ``file:///C:/warehouse`` — the scheme is recognised, but the path handed to
  ``pyarrow.fs.LocalFileSystem`` keeps the URI's leading slash (``/C:/...``),
  and Windows refuses it with *WinError 123* (bad path syntax).

Only the second is a spelling problem rather than a missing feature, so the
warehouse is written as a ``file://`` URI (see ``iceberg_catalog``) and this IO
drops the slash the URI grammar requires and the filesystem does not accept.
POSIX locations are returned untouched.

Every FileIO here also refuses to open Google Cloud or Azure storage without
the table's own credential (follow-up review 1): pyarrow would use the API
host's identity (``services.platform_gcp_azure_credentials``).

And it never opens, creates or deletes a file on the API host that a tenant's
catalog named (follow-up review 2, local files): a REST, Glue or Hive catalog
decides every metadata, manifest, data and delete file location, so a local one
from it is refused (the guarded catalog sets :data:`LOCAL_FILES` to
``refuse``); any other local location must pass the Iceberg jail
(``iceberg_catalog._jail_local_path``).
"""

from __future__ import annotations

import os
import re
from typing import Any

from pyiceberg.io import EMPTY_DICT
from pyiceberg.io.pyarrow import PyArrowFileIO

_DRIVE_ROOTED = re.compile(r"^/+([A-Za-z]:[/\\].*)$")


def strip_uri_drive_slash(path: str) -> str:
    """``/C:/warehouse`` -> ``C:/warehouse``; anything else unchanged."""
    match = _DRIVE_ROOTED.match(path or "")
    return match.group(1) if match else path


#: FileIO property the guarded catalog sets to ``refuse`` for a remote catalog's tables.
LOCAL_FILES = "dataflow.local-files"
REFUSE = "refuse"


def local_location_refusal(properties: Any, path: str) -> str:
    """Why this FileIO may not touch the local ``path``, else ``""``."""
    if str((properties or {}).get(LOCAL_FILES) or "").strip().lower() == REFUSE:
        return (
            "This Iceberg catalog named a local file on the Datawrap server (a metadata, "
            "manifest, data or delete file). Datawrap does not read or write the server's own "
            "files for a remote catalog; its tables must live in object storage."
        )
    from connectors.iceberg_catalog import _jail_local_path

    try:
        _jail_local_path(path, "data or metadata file")
    except ValueError:
        return (
            "This Iceberg table named a local file outside the Iceberg warehouse folder "
            "(DATAFLOW_ICEBERG_ROOT, else the data folder's iceberg area); Datawrap does not open it."
        )
    return ""


class LocalUriPyArrowFileIO(PyArrowFileIO):
    """``PyArrowFileIO`` that hands the local filesystem a path it accepts."""

    def _check_local(self, location: Any) -> None:
        raw = getattr(location, "location", location)  # delete() takes a file object too
        scheme, netloc, path = self.parse_location(str(raw), self.properties)
        if scheme == "file":
            # ``file://app.env`` is netloc + path, a RELATIVE path pyarrow opens in the
            # process's working directory, which the jail would anchor at its root
            # instead (follow-up review 3, M1): judge exactly what pyarrow opens.
            if netloc or not os.path.isabs(path):
                _refuse(_NOT_ABSOLUTE)
            _refuse(local_location_refusal(self.properties, path))

    def new_input(self, location: str) -> Any:
        self._check_local(location)
        return super().new_input(location)

    def new_output(self, location: str) -> Any:
        self._check_local(location)
        return super().new_output(location)

    def delete(self, location: Any) -> None:
        self._check_local(location)
        super().delete(location)

    @staticmethod
    def parse_location(location: str, properties: Any = EMPTY_DICT) -> tuple[str, str, str]:
        scheme, netloc, path = PyArrowFileIO.parse_location(location, properties)
        if scheme == "file":
            return scheme, netloc, strip_uri_drive_slash(path)
        return scheme, netloc, path

    def _initialize_gcs_fs(self) -> Any:
        from services.platform_gcp_azure_credentials import gcs_fileio_refusal

        _refuse(gcs_fileio_refusal(self.properties))
        return super()._initialize_gcs_fs()

    def _initialize_azure_fs(self) -> Any:
        from services.platform_gcp_azure_credentials import azure_fileio_refusal

        _refuse(azure_fileio_refusal(self.properties))
        return super()._initialize_azure_fs()


_NOT_ABSOLUTE = (
    "This Iceberg table named a local file with a host part or a relative path (file://name); "
    "Datawrap opens only absolute paths inside the Iceberg warehouse folder."
)


def _refuse(refusal: str) -> None:
    if refusal:
        raise ValueError(refusal)


class KeyedS3PyArrowFileIO(LocalUriPyArrowFileIO):
    """Refuses to open S3 with the API process's AWS credentials (follow-up review 1, H2).

    pyarrow's S3 filesystem falls back to the process's credential chain when
    the properties carry no keys. The properties are the merged ones the
    catalog hands the FileIO, so keys a REST catalog vends with a table count.
    pyiceberg opens ``oss://`` with the same S3 filesystem.
    """

    def _initialize_s3_fs(self, netloc: str | None) -> Any:
        from services.platform_aws_credentials import s3_fileio_refusal

        _refuse(s3_fileio_refusal(self.properties))
        return super()._initialize_s3_fs(netloc)

    def _initialize_oss_fs(self) -> Any:
        from services.platform_aws_credentials import s3_fileio_refusal

        _refuse(s3_fileio_refusal(self.properties))
        return super()._initialize_oss_fs()
