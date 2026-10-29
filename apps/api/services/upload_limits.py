"""Byte ceilings for uploaded files and for gzip payloads inflated in memory.

Two limits, both read from the environment on every call so an operator can
tune them without a code change:

* ``DATAFLOW_MAX_UPLOAD_BYTES`` (default 256 MiB, the ``client_max_body_size``
  in ``deploy/nginx.conf``) bounds what an upload route reads into memory.
* ``DATAFLOW_MAX_DECOMPRESSED_BYTES`` (default 1 GiB) bounds what a gzip
  payload may expand to when a reader needs the whole image (parse, JSON,
  Excel, XML, Parquet/ORC footers). Streaming readers are not capped.

Overflow raises ``PayloadTooLarge``; ``src.main`` maps it to HTTP 413 and the
message names the limit, the size when it is known, and the setting an
administrator changes. It is
deliberately not a ``ValueError`` so parse/decline handlers that turn
``ValueError`` into a 400 or a slower fallback path let it through.
"""

from __future__ import annotations

import gzip
import io
import tempfile
from typing import Any

from services.brand_env import getenv_brand

DEFAULT_MAX_UPLOAD_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_DECOMPRESSED_BYTES = 1024 * 1024 * 1024
_CHUNK_BYTES = 1024 * 1024
_SPOOL_MEMORY_BYTES = 32 * 1024 * 1024


UPLOAD_SETTING = "MAX_UPLOAD_BYTES"
DECOMPRESSED_SETTING = "MAX_DECOMPRESSED_BYTES"


class PayloadTooLarge(Exception):
    """A payload crossed its configured byte ceiling.

    ``setting`` is the environment variable suffix an administrator changes
    (``DATAFLOW_`` or ``DATAWRAP_`` prefix); ``actual`` is the size in bytes
    when it is known, ``None`` when the read stopped at the limit.
    """

    def __init__(
        self,
        what: str,
        limit: int,
        *,
        setting: str = UPLOAD_SETTING,
        actual: int | None = None,
    ) -> None:
        self.what = what
        self.limit = int(limit)
        self.setting = setting
        self.actual = int(actual) if actual is not None else None
        size = (
            f"is {_describe(self.actual)}; the limit is {_describe(self.limit)}"
            if self.actual is not None
            else f"is larger than the {_describe(self.limit)} limit"
        )
        super().__init__(
            f"{what} {size}. A server administrator can raise it with "
            f"DATAFLOW_{setting} (or DATAWRAP_{setting}), currently {self.limit} bytes."
        )


def _describe(size: int) -> str:
    mib = size / (1024 * 1024)
    return f"{round(mib, 1):g} MB" if mib >= 1 else f"{size} bytes"


def _env_bytes(name: str, default: int) -> int:
    raw = getenv_brand(name)
    if raw is None:
        return default
    try:
        value = int(str(raw).strip())
    except ValueError:
        return default
    return value if value > 0 else default


def max_upload_bytes() -> int:
    return _env_bytes("MAX_UPLOAD_BYTES", DEFAULT_MAX_UPLOAD_BYTES)


def max_decompressed_bytes() -> int:
    return _env_bytes("MAX_DECOMPRESSED_BYTES", DEFAULT_MAX_DECOMPRESSED_BYTES)


async def read_upload(file: Any, limit: int | None = None) -> bytes:
    """Read an ``UploadFile`` body, refusing it once it passes ``limit`` bytes."""
    cap = max_upload_bytes() if limit is None else int(limit)
    declared = getattr(file, "size", None)
    if isinstance(declared, int) and declared > cap:
        raise PayloadTooLarge("Uploaded file", cap, actual=declared)
    buf = bytearray()
    while True:
        chunk = await file.read(min(_CHUNK_BYTES, cap + 1 - len(buf)))
        if not chunk:
            break
        buf += chunk
        if len(buf) > cap:
            raise PayloadTooLarge("Uploaded file", cap)
    return bytes(buf)


def read_capped(
    handle: Any, limit: int | None = None, *, what: str = "Decompressed file"
) -> bytes:
    """Read ``handle`` to EOF, never holding more than ``limit + 1`` bytes."""
    cap = max_decompressed_bytes() if limit is None else int(limit)
    buf = bytearray()
    while True:
        chunk = handle.read(min(_CHUNK_BYTES, cap + 1 - len(buf)))
        if not chunk:
            break
        buf += chunk
        if len(buf) > cap:
            raise PayloadTooLarge(what, cap, setting=DECOMPRESSED_SETTING)
    return bytes(buf)


def read_all_bounded(handle: Any, limit: int | None = None) -> bytes:
    """Read a whole binary handle. A gzip stream is held to the decompressed cap.

    A plain handle is the stored file itself, already bounded by its size on
    disk; only inflation can turn a small input into an unbounded image.
    """
    if isinstance(handle, gzip.GzipFile):
        return read_capped(handle, limit)
    return handle.read()


def gunzip_capped(data: bytes, limit: int | None = None) -> bytes:
    """``gzip.decompress`` that stops at the decompressed cap (multi-member safe)."""
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
        return read_capped(gz, limit)


def spool_capped(handle: Any, limit: int | None = None) -> Any:
    """Copy ``handle`` into a seekable spool (RAM, then disk), bounded by ``limit``.

    Parquet/ORC need random access; spooling keeps a large legitimate
    ``.parquet.gz`` off the heap and a gzip bomb from filling the disk.
    """
    cap = max_decompressed_bytes() if limit is None else int(limit)
    spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL_MEMORY_BYTES)
    written = 0
    try:
        while True:
            chunk = handle.read(min(_CHUNK_BYTES, cap + 1 - written))
            if not chunk:
                break
            written += len(chunk)
            if written > cap:
                raise PayloadTooLarge("Decompressed file", cap, setting=DECOMPRESSED_SETTING)
            spool.write(chunk)
    except BaseException:
        spool.close()
        raise
    spool.seek(0)
    return spool
