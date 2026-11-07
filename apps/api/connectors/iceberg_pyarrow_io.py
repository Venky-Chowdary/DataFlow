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
"""

from __future__ import annotations

import re
from typing import Any

from pyiceberg.io import EMPTY_DICT
from pyiceberg.io.pyarrow import PyArrowFileIO

_DRIVE_ROOTED = re.compile(r"^/+([A-Za-z]:[/\\].*)$")


def strip_uri_drive_slash(path: str) -> str:
    """``/C:/warehouse`` -> ``C:/warehouse``; anything else unchanged."""
    match = _DRIVE_ROOTED.match(path or "")
    return match.group(1) if match else path


class LocalUriPyArrowFileIO(PyArrowFileIO):
    """``PyArrowFileIO`` that hands the local filesystem a path it accepts."""

    @staticmethod
    def parse_location(location: str, properties: Any = EMPTY_DICT) -> tuple[str, str, str]:
        scheme, netloc, path = PyArrowFileIO.parse_location(location, properties)
        if scheme == "file":
            return scheme, netloc, strip_uri_drive_slash(path)
        return scheme, netloc, path
