"""Spill object-store payloads to disk and read them without loading full bytes into RAM.

This module replaces the previous `Body.read()` / `download_as_bytes()` pattern for
S3, GCS, ADLS and SFTP with a streaming download to a temporary file, followed by
disk-backed parsing for CSV/JSONL/JSON/Excel/Parquet. Binary file types that cannot
be streamed (Avro/ORC/XML) still fall back to reading the spilled file into memory,
but at least the network payload is no longer held as a single byte buffer.
"""

from __future__ import annotations

import io
import logging
import os
from services.brand_env import getenv_brand
import re
import tempfile
import threading
from pathlib import Path
from urllib.parse import unquote, urlparse
from typing import Any, Callable, NamedTuple

from services.value_serializer import cell_to_string

try:
    from services.platform_config import data_dir
except ImportError:  # pragma: no cover - tests launched with src on path
    from src.services.platform_config import data_dir

try:
    from services.file_parser import FileParser
except ImportError:
    from src.services.file_parser import FileParser

_logger = logging.getLogger(__name__)

_SPILL_DIR: Path | None = None
_SPILL_CACHE: dict[str, tuple[Path, float]] = {}
_SPILL_LOCK = threading.Lock()

# Default TTL for cached spilled objects (seconds).  A TTL defends against
# stale data without forcing a re-download on every call; callers that need a
# guaranteed fresh copy (e.g. per-transfer runs) can pass force=True.
_SPILL_TTL_SECONDS = int(getenv_brand("SPILL_TTL", "300"))

_STREAMABLE = {"csv", "tsv", "jsonl", "ndjson", "json", "excel", "parquet", "avro", "orc"}

_S3_URI_SCHEMES = frozenset({"s3", "s3a", "s3n"})
_GCS_URI_SCHEMES = frozenset({"gs", "gcs"})
_ADLS_URI_SCHEMES = frozenset({"abfs", "abfss", "wasb", "wasbs"})
_HDFS_URI_SCHEMES = frozenset({"hdfs", "webhdfs", "swebhdfs"})


class ObjectStoreLocation(NamedTuple):
    """Object-store address of one blob. ``account`` is ADLS account from the URI."""

    kind: str
    bucket: str
    key: str
    account: str = ""


def parse_object_store_uri(uri: str) -> ObjectStoreLocation | None:
    """``s3://`` / ``gs://`` / ``abfss://`` / ``hdfs://`` → (kind, bucket, key).

    Iceberg catalog ``file_path`` is typically an object URI. Dest COUNT
    Range-GETs that blob through the same opener as S3/GCS/ADLS dest.
    ``hdfs://`` / ``webhdfs://`` parse so warehouse joins work; Range-GET
    still requires a configured WebHDFS HTTP endpoint (RPC ``:8020`` is
    not HTTP). ``file:`` / relative paths are not this helper.
    """
    raw = str(uri or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    scheme = (parsed.scheme or "").lower()
    if scheme in _S3_URI_SCHEMES:
        bucket = unquote(parsed.netloc or "")
        key = unquote((parsed.path or "").lstrip("/"))
        if not bucket or not key:
            return None
        return ObjectStoreLocation("s3", bucket, key)
    if scheme in _GCS_URI_SCHEMES:
        bucket = unquote(parsed.netloc or "")
        key = unquote((parsed.path or "").lstrip("/"))
        if not bucket or not key:
            return None
        return ObjectStoreLocation("gcs", bucket, key)
    if scheme in _ADLS_URI_SCHEMES:
        container = unquote(parsed.username or "")
        host = (parsed.hostname or "").strip()
        account = host.split(".")[0] if host else ""
        if not container:
            container = unquote((parsed.netloc or "").split("@", 1)[0])
        key = unquote((parsed.path or "").lstrip("/"))
        if not container or not key:
            return None
        return ObjectStoreLocation("adls", container, key, account)
    if scheme in {"http", "https"}:
        host = (parsed.hostname or "").lower()
        if host.endswith(".blob.core.windows.net") or host.endswith(".dfs.core.windows.net"):
            account = host.split(".")[0]
            parts = [unquote(p) for p in (parsed.path or "").split("/") if p]
            if len(parts) < 2:
                return None
            return ObjectStoreLocation("adls", parts[0], "/".join(parts[1:]), account)
    if scheme in _HDFS_URI_SCHEMES:
        authority = unquote(parsed.netloc or "")
        key = unquote((parsed.path or "").lstrip("/"))
        if not authority or not key:
            return None
        return ObjectStoreLocation("hdfs", authority, key, scheme)
    return None


def _spill_directory() -> Path:
    global _SPILL_DIR
    if _SPILL_DIR is None:
        base = Path(data_dir()) if "data_dir" in globals() else Path(tempfile.gettempdir())
        _SPILL_DIR = base / "object_spill"
        _SPILL_DIR.mkdir(parents=True, exist_ok=True)
    return _SPILL_DIR


def _sanitize_cache_key(key: str) -> str:
    """Turn any cache key into a safe filesystem name."""
    return re.sub(r"[^a-zA-Z0-9._-]", "_", key)[:120]


def spill_path(cache_key: str) -> Path:
    """Return a deterministic temporary path for an object cache key."""
    return _spill_directory() / f"{_sanitize_cache_key(cache_key)}.tmp"


def clear_spill_cache() -> None:
    with _SPILL_LOCK:
        _SPILL_CACHE.clear()


def _cache_is_fresh(key: str, now: float) -> bool:
    with _SPILL_LOCK:
        entry = _SPILL_CACHE.get(key)
        if not entry:
            return False
        path, expires_at = entry
        return path.exists() and now < expires_at


def download_object(
    cache_key: str,
    downloader: Callable[[Path], None],
    *,
    force: bool = False,
    ttl_seconds: int = _SPILL_TTL_SECONDS,
) -> Path:
    """Download an object to a spilled temp file unless a fresh copy is cached.

    ``downloader`` receives a temporary target ``Path`` and must write the full
    object bytes to it.  On success the temp file is atomically renamed to the
    deterministic spill path; on failure the partial temp file is removed and the
    cache entry is invalidated so the next call re-downloads.

    Use ``force=True`` when the caller needs a guaranteed fresh copy (e.g. each
    transfer run) and ``force=False`` when repeated reads of the same object are
    expected within the TTL.
    """
    import time

    now = time.monotonic()
    if not force and _cache_is_fresh(cache_key, now):
        with _SPILL_LOCK:
            path, _ = _SPILL_CACHE[cache_key]
        _logger.info("Reusing fresh spilled object %s at %s", cache_key, path)
        return path

    final_path = spill_path(cache_key)
    spill_dir = _spill_directory()
    tmp_path: Path | None = None
    try:
        fd, tmp_name = tempfile.mkstemp(dir=spill_dir, prefix=".spill_", suffix=".part")
        os.close(fd)
        tmp_path = Path(tmp_name)
        _logger.info("Spilling object %s to temp %s", cache_key, tmp_path)
        downloader(tmp_path)
        # Atomic rename so other readers never see a partially written file.
        os.replace(tmp_path, final_path)
        expires_at = now + ttl_seconds
        with _SPILL_LOCK:
            _SPILL_CACHE[cache_key] = (final_path, expires_at)
        _logger.info("Spilled object %s atomically to %s", cache_key, final_path)
        return final_path
    except Exception:
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        with _SPILL_LOCK:
            _SPILL_CACHE.pop(cache_key, None)
            if final_path.exists():
                try:
                    final_path.unlink()
                except Exception as exc:
                    logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        raise


def _records_to_matrix(records: list[dict[str, Any]], columns: list[str]) -> list[list[str]]:
    return [[cell_to_string(record.get(c)) for c in columns] for record in records]


def _collect_records(path: Path, file_type: str, offset: int, limit: int) -> list[dict[str, Any]]:
    """Stream rows from the spilled file until ``limit`` are collected."""
    try:
        from src.transfer.file_stream import _batch_iterator_for_type
    except ImportError:  # pragma: no cover
        from transfer.file_stream import _batch_iterator_for_type

    records: list[dict[str, Any]] = []
    batch_size = max(limit, 1000)
    skipped = 0
    for batch in _batch_iterator_for_type(file_type, path, batch_size):
        for record in batch:
            if skipped < offset:
                skipped += 1
                continue
            records.append(record)
            if len(records) >= limit:
                return records
    return records


def read_rows_from_spill(
    path: Path,
    filename: str,
    *,
    offset: int = 0,
    limit: int = 500,
    known_total: int | None = None,
) -> tuple[list[str], list[list[str]], int]:
    """Read a row window from a spilled object file.

    Returns ``(headers, rows, total_rows)``.  CSV/JSONL/JSON/Excel/Parquet are
    parsed from disk; other formats fall back to reading the whole spilled file.
    """
    file_type = FileParser.detect_file_type(filename, b"")

    if file_type in _STREAMABLE:
        try:
            from src.transfer.file_stream import peek_file_source
        except ImportError:  # pragma: no cover
            from transfer.file_stream import peek_file_source

        headers, _schema, total, sample = peek_file_source(path, filename)
        if offset == 0 and limit <= len(sample):
            records = sample[:limit]
        else:
            records = _collect_records(path, file_type, offset, limit)
        rows = _records_to_matrix(records, headers)
        return headers, rows, known_total if known_total is not None else total

    # Fallback for Avro / ORC / XML / unknown: read the spilled file and parse.
    raw = path.read_bytes()
    result = FileParser.parse(raw, filename)
    if not result.success:
        raise ValueError(result.error or f"Cannot parse object `{filename}`")
    headers = result.columns
    records = result.data[offset : offset + limit]
    rows = _records_to_matrix(records, headers)
    return headers, rows, known_total if known_total is not None else len(result.data)


class _ChunkReader(io.RawIOBase):
    """File-like over an iterator of byte chunks (Azure ``download_blob().chunks()``).

    Dest COUNT and spill both need ``read(n)``. The iterator is one-shot —
    the same contract as boto3 ``StreamingBody``. One chunk is buffered;
    the object is not concatenated in RAM.
    """

    def __init__(self, chunks: Any) -> None:
        super().__init__()
        self._chunks = iter(chunks)
        self._buf = b""

    def readable(self) -> bool:
        return True

    def readinto(self, b: Any) -> int:
        mv = memoryview(b)
        n = len(mv)
        if n == 0:
            return 0
        while len(self._buf) < n:
            try:
                nxt = next(self._chunks)
            except StopIteration:
                break
            if not nxt:
                continue
            self._buf += bytes(nxt)
        if not self._buf:
            return 0
        take = min(n, len(self._buf))
        mv[:take] = self._buf[:take]
        self._buf = self._buf[take:]
        return take


class RangeGetSource(io.RawIOBase):
    """Hadoop ``FSDataInputStream`` analogue: seek + sized Range GET.

    ``read(n)`` fetches ``[pos, pos+n)``. Unsized ``read()`` fetches the
    remainder ``[pos, size)`` — never a second copy of the object from
    byte 0. Parquet/ORC footer COUNT issues a handful of small ranges
    (magic, tail length, footer). Gzip Parquet/ORC is not this path
    (Hadoop GzipCodec is not splittable). Gate-8 checksum still needs
    data pages and stays on sequential GET.
    """

    def __init__(self, size: int, fetch: Callable[[int, int], bytes]) -> None:
        super().__init__()
        if int(size) < 0:
            raise ValueError("object size must be >= 0")
        self._size = int(size)
        self._pos = 0
        self._fetch = fetch

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            pos = int(offset)
        elif whence == io.SEEK_CUR:
            pos = self._pos + int(offset)
        elif whence == io.SEEK_END:
            pos = self._size + int(offset)
        else:
            raise ValueError("invalid whence")
        if pos < 0:
            raise OSError(22, "negative seek position")
        self._pos = pos
        return self._pos

    def read(self, size: int | None = -1) -> bytes:
        if self.closed:
            raise ValueError("read of closed RangeGetSource")
        if self._pos >= self._size:
            return b""
        if size is None or int(size) < 0:
            length = self._size - self._pos
        else:
            length = int(size)
        if length <= 0:
            return b""
        if self._pos + length > self._size:
            length = self._size - self._pos
        data = self._fetch(self._pos, length)
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("Range GET must return bytes")
        chunk = bytes(data[:length])
        self._pos += len(chunk)
        return chunk

    def readinto(self, b: Any) -> int:
        mv = memoryview(b)
        chunk = self.read(len(mv))
        n = len(chunk)
        if n:
            mv[:n] = chunk
        return n


def _object_missing_from_client_error(exc: BaseException) -> bool:
    response = getattr(exc, "response", None) or {}
    code = str((response.get("Error") or {}).get("Code") or "")
    http = str((response.get("ResponseMetadata") or {}).get("HTTPStatusCode") or "")
    if code in {"404", "NoSuchKey", "NotFound"} or http == "404":
        return True
    name = type(exc).__name__
    msg = str(exc).lower()
    return (
        "NotFound" in name
        or "NoSuchKey" in name
        or "ResourceNotFound" in name
        or "404" in str(exc)
        or "no such key" in msg
        or "not found" in msg
        or "blobnotfound" in msg
    )


def _webhdfs_endpoint(cfg: dict[str, Any], bucket: str) -> str | None:
    """HTTP namenode for WebHDFS. ``hdfs://nn:8020`` RPC is not this URL.

    ``webhdfs://`` / ``swebhdfs://`` authorities are HTTP(S). Bare
    ``hdfs://`` requires ``webhdfs.endpoint`` / ``hdfs.webhdfs``.
    """
    for key in (
        "webhdfs_endpoint",
        "webhdfs.endpoint",
        "hdfs.webhdfs",
        "hdfs.webhdfs.endpoint",
        "hdfs_webhdfs",
    ):
        raw = str(cfg.get(key) or "").strip()
        if raw:
            return raw.rstrip("/")
    scheme = str(cfg.get("hdfs_scheme") or "").strip().lower()
    authority = str(bucket or "").strip()
    if scheme in {"webhdfs", "swebhdfs"} and authority:
        proto = "https" if scheme == "swebhdfs" else "http"
        return f"{proto}://{authority}"
    return None


def _webhdfs_path(key: str) -> str:
    return "/" + str(key or "").lstrip("/")


def _webhdfs_params(cfg: dict[str, Any], extra: dict[str, Any] | None = None) -> dict[str, Any]:
    params = dict(extra or {})
    user = str(
        cfg.get("webhdfs_user")
        or cfg.get("hdfs.user")
        or cfg.get("username")
        or ""
    ).strip()
    if user:
        params["user.name"] = user
    return params


def _webhdfs_missing(status_code: int, payload: Any) -> bool:
    if int(status_code) == 404:
        return True
    if not isinstance(payload, dict):
        return False
    remote = payload.get("RemoteException") or payload.get("remoteException") or {}
    if not isinstance(remote, dict):
        return False
    name = str(remote.get("exception") or remote.get("Exception") or "")
    return "FileNotFound" in name


def _webhdfs_request(
    cfg: dict[str, Any],
    bucket: str,
    key: str,
    params: dict[str, Any],
    *,
    stream: bool = False,
) -> Any:
    import requests

    endpoint = _webhdfs_endpoint(cfg, bucket)
    if not endpoint:
        raise OSError(
            "WebHDFS endpoint unset — hdfs:// RPC authority is not HTTP; "
            "set webhdfs.endpoint / hdfs.webhdfs"
        )
    url = f"{endpoint}/webhdfs/v1{_webhdfs_path(key)}"
    return requests.get(
        url,
        params=_webhdfs_params(cfg, params),
        timeout=30,
        allow_redirects=True,
        stream=stream,
    )


def object_store_content_length(
    kind: str, cfg: dict[str, Any], bucket: str, key: str
) -> int | bool | None:
    """HEAD size in bytes. ``False`` if missing, ``None`` if unknowable.

    Same clients as sequential GET / version tokens. Missing HEAD
    permission is unknowable so COUNT can fall back to a sequential GET.
    """
    try:
        if kind == "s3":
            from botocore.exceptions import ClientError
            from connectors.aws_common import boto3_client

            try:
                head = boto3_client("s3", cfg).head_object(Bucket=bucket, Key=key)
            except ClientError as exc:
                if _object_missing_from_client_error(exc):
                    return False
                raise
            size = head.get("ContentLength")
            if size is None:
                return None
            return int(size)
        if kind == "gcs":
            from connectors.gcs_common import gcs_client

            blob = gcs_client(cfg).bucket(bucket).get_blob(key)
            if blob is None:
                return False
            size = getattr(blob, "size", None)
            if size is None:
                blob.reload()
                size = getattr(blob, "size", None)
            if size is None:
                return None
            return int(size)
        if kind == "adls":
            from connectors.adls_common import blob_service_client

            try:
                props = (
                    blob_service_client(cfg)
                    .get_blob_client(bucket, key)
                    .get_blob_properties()
                )
            except Exception as exc:
                if _object_missing_from_client_error(exc):
                    return False
                raise
            size = getattr(props, "size", None)
            if size is None:
                return None
            return int(size)
        if kind == "hdfs":
            if _webhdfs_endpoint(cfg, bucket) is None:
                return None
            response = _webhdfs_request(
                cfg, bucket, key, {"op": "GETFILESTATUS"}
            )
            payload: Any = None
            try:
                payload = response.json()
            except Exception:
                payload = None
            if _webhdfs_missing(response.status_code, payload):
                return False
            if response.status_code >= 400:
                return None
            status = (payload or {}).get("FileStatus") or (payload or {}).get("fileStatus") or {}
            size = status.get("length") if isinstance(status, dict) else None
            if size is None:
                return None
            return int(size)
    except Exception as exc:
        _logger.info(
            "object-store HEAD failed for dest COUNT (%s/%s): %s",
            bucket,
            key,
            exc,
        )
        return None
    return None


def range_get_object_bytes(
    kind: str, cfg: dict[str, Any], bucket: str, key: str, start: int, length: int
) -> bytes:
    """Sized Range GET ``[start, start+length)``. Never unsized Body.read() of the object."""
    if int(length) <= 0:
        return b""
    start = int(start)
    length = int(length)
    end = start + length - 1
    if kind == "s3":
        from connectors.aws_common import boto3_client

        body = boto3_client("s3", cfg).get_object(
            Bucket=bucket, Key=key, Range=f"bytes={start}-{end}"
        )["Body"]
        closer = getattr(body, "close", None)
        try:
            return bytes(body.read())
        finally:
            if callable(closer):
                closer()
    if kind == "gcs":
        from connectors.gcs_common import gcs_client

        # ``end`` is the last byte (inclusive), same as HTTP Range / S3.
        # Range bytes are not the object MD5 — skip checksum of the whole blob.
        return bytes(
            gcs_client(cfg)
            .bucket(bucket)
            .blob(key)
            .download_as_bytes(start=start, end=end, checksum=None)
        )
    if kind == "adls":
        from connectors.adls_common import blob_service_client

        downloader = (
            blob_service_client(cfg)
            .get_blob_client(bucket, key)
            .download_blob(offset=start, length=length)
        )
        reader = getattr(downloader, "readall", None)
        if callable(reader):
            return bytes(reader())
        return bytes(downloader.read())
    if kind == "hdfs":
        response = _webhdfs_request(
            cfg,
            bucket,
            key,
            {"op": "OPEN", "offset": start, "length": length},
        )
        payload: Any = None
        try:
            payload = response.json()
        except Exception:
            payload = None
        if _webhdfs_missing(response.status_code, payload):
            raise FileNotFoundError(f"WebHDFS object missing: {key}")
        if response.status_code >= 400:
            raise OSError(
                f"WebHDFS OPEN failed for {key}: HTTP {response.status_code}"
            )
        return bytes(response.content)
    raise OSError(f"Range GET is not implemented for store {kind!r}")


def open_object_store_seekable(
    kind: str, cfg: dict[str, Any], bucket: str, key: str
) -> tuple[Any, Any] | bool | None:
    """Seekable Range GET view. ``False`` if missing, ``None`` if unknowable.

    Uncompressed Parquet/ORC dest COUNT uses this instead of spooling the
    GET. Sequential ``open_object_store_binary`` stays the path for
    CSV/JSON/Avro/gzip/Excel and for Gate-8 checksum (data pages). HEAD
    or Range setup failure is unknowable so COUNT can fall back to a
    sequential GET — still correct, more RAM.
    """
    size = object_store_content_length(kind, cfg, bucket, key)
    if size is False or size is None:
        return size
    try:

        def _fetch(start: int, length: int) -> bytes:
            return range_get_object_bytes(kind, cfg, bucket, key, start, length)

        source = RangeGetSource(int(size), _fetch)
        return source, source.close
    except Exception as exc:
        _logger.info(
            "object-store Range GET setup failed for dest COUNT (%s/%s): %s",
            bucket,
            key,
            exc,
        )
        return None


def open_object_store_binary(
    kind: str, cfg: dict[str, Any], bucket: str, key: str
) -> tuple[Any, Any] | bool | None:
    """Readable sequential GET body + closer. ``False`` if missing, ``None`` if unknowable.

    Does not ``Body.read()`` / ``download_as_bytes()`` / ``readall()`` the
    object. Dest COUNT of CSV/JSON/JSONL/XML/Avro (including gzip) walks
    this stream. Gate-8 cell checksum of those same kinds walks the same
    handle. Gzip Excel/Parquet/ORC still spool one decompressed image
    (codec is not splittable). Uncompressed Parquet/ORC dest COUNT uses
    ``open_object_store_seekable`` (footer Range GET) instead of this
    path. Spill downloaders stay the ingest path (disk), not dest COUNT.
    """
    try:
        if kind == "s3":
            from botocore.exceptions import ClientError
            from connectors.aws_common import boto3_client

            try:
                body = boto3_client("s3", cfg).get_object(Bucket=bucket, Key=key)["Body"]
            except ClientError as exc:
                code = str((exc.response or {}).get("Error", {}).get("Code") or "")
                http = str(
                    (exc.response or {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
                    or ""
                )
                if code in {"404", "NoSuchKey", "NotFound"} or http == "404":
                    return False
                raise
            closer = getattr(body, "close", None)
            return body, closer if callable(closer) else None
        if kind == "gcs":
            from connectors.gcs_common import gcs_client

            blob = gcs_client(cfg).bucket(bucket).get_blob(key)
            if blob is None:
                return False
            handle = blob.open("rb")
            closer = getattr(handle, "close", None)
            return handle, closer if callable(closer) else None
        if kind == "adls":
            from connectors.adls_common import blob_service_client

            try:
                downloader = (
                    blob_service_client(cfg).get_blob_client(bucket, key).download_blob()
                )
            except Exception as exc:
                name = type(exc).__name__
                if "NotFound" in name or "404" in str(exc):
                    return False
                raise
            stream = _ChunkReader(downloader.chunks())
            return stream, stream.close
        if kind == "hdfs":
            if _webhdfs_endpoint(cfg, bucket) is None:
                return None
            response = _webhdfs_request(
                cfg, bucket, key, {"op": "OPEN"}, stream=True
            )
            payload: Any = None
            if response.status_code >= 400:
                try:
                    payload = response.json()
                except Exception:
                    payload = None
                if _webhdfs_missing(response.status_code, payload):
                    return False
                raise OSError(
                    f"WebHDFS OPEN failed for {key}: HTTP {response.status_code}"
                )
            def _close() -> None:
                try:
                    response.close()
                except Exception:
                    pass

            return response.raw, _close
    except Exception as exc:
        _logger.info(
            "object-store GET stream failed for dest COUNT (%s/%s): %s",
            bucket,
            key,
            exc,
        )
        return None
    return None


def open_sftp_binary(cfg: dict[str, Any]) -> tuple[Any, Any] | bool | None:
    """Readable SFTP file + closer. ``False`` if missing, ``None`` if unknowable.

    Does not ``fh.read()`` the remote file. Dest COUNT, Gate-8 checksum,
    and dest sample walk this handle under the same host-key pin the
    write trusted. Auth / host-key failure is unknowable, not dest=0.
    """
    from connectors.sftp_common import (
        connect_sftp,
        host_key_settings,
        parse_sftp_config,
    )

    try:
        parsed = parse_sftp_config(
            connection_string=str(cfg.get("connection_string") or ""),
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 22),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            database=str(cfg.get("database") or ""),
            table=str(cfg.get("table") or cfg.get("path") or ""),
            **host_key_settings(cfg),
        )
        if not parsed.host or not parsed.path:
            return None
        transport, sftp = connect_sftp(parsed)
    except Exception as exc:
        _logger.info("SFTP GET stream failed to connect: %s", exc)
        return None
    try:
        handle = sftp.file(parsed.path, "rb")
    except Exception as exc:
        try:
            sftp.close()
        except Exception:
            pass
        try:
            transport.close()
        except Exception:
            pass
        name = type(exc).__name__
        msg = str(exc).lower()
        if (
            "NoSuchFile" in name
            or "FileNotFound" in name
            or "enoent" in msg
            or "no such file" in msg
            or "not found" in msg
        ):
            return False
        _logger.info("SFTP GET stream failed for %s: %s", parsed.path, exc)
        return None

    def _close() -> None:
        try:
            handle.close()
        except Exception:
            pass
        try:
            sftp.close()
        except Exception:
            pass
        try:
            transport.close()
        except Exception:
            pass

    return handle, _close


def download_s3_object(path: Path, cfg: dict[str, Any], bucket: str, key: str) -> None:
    from connectors.aws_common import boto3_client

    obj = boto3_client("s3", cfg).get_object(Bucket=bucket, Key=key)
    with open(path, "wb") as f:
        for chunk in obj["Body"].iter_chunks(chunk_size=8 * 1024 * 1024):
            if chunk:
                f.write(chunk)


def download_gcs_object(path: Path, cfg: dict[str, Any], bucket: str, key: str) -> None:
    from connectors.gcs_common import gcs_client, gcs_emulator_kwargs

    gcs_client(cfg).bucket(bucket).blob(key).download_to_filename(
        str(path), **gcs_emulator_kwargs(cfg)
    )


def download_adls_object(path: Path, cfg: dict[str, Any], bucket: str, key: str) -> None:
    from connectors.adls_common import blob_service_client

    blob = blob_service_client(cfg).get_blob_client(bucket, key)
    with open(path, "wb") as f:
        for chunk in blob.download_blob().chunks():
            if chunk:
                f.write(chunk)


def download_sftp_object(path: Path, cfg: dict[str, Any], bucket: str, key: str) -> None:
    from connectors.sftp_common import (
        connect_sftp,
        parse_sftp_config,
    )

    merged = dict(cfg)
    if bucket:
        merged["database"] = bucket
    if key:
        merged["table"] = key
    sftp_cfg = parse_sftp_config(**merged)
    if not sftp_cfg.host:
        raise ValueError("SFTP host is required")
    if not sftp_cfg.path:
        raise ValueError("SFTP remote path is required")

    remote_path = sftp_cfg.path
    transport, sftp = connect_sftp(sftp_cfg)
    try:
        with open(path, "wb") as f:
            sftp.getfo(remote_path, f)
    finally:
        sftp.close()
        transport.close()


def download_for_object_store(src_type: str, path: Path, cfg: dict[str, Any], bucket: str, key: str) -> None:
    """Download an object to a spilled path based on the source type."""
    dispatch = {
        "s3": download_s3_object,
        "gcs": download_gcs_object,
        "adls": download_adls_object,
        "sftp": download_sftp_object,
    }
    downloader = dispatch.get(src_type)
    if not downloader:
        raise ValueError(f"No downloader implemented for source type '{src_type}'")
    downloader(path, cfg, bucket, key)


def stream_spilled_file_to_database(
    path: Path,
    filename: str,
    destination: Any,
    mappings: list[dict[str, Any]],
    schema: dict[str, str],
    *,
    sync_mode: str = "full_refresh_overwrite",
    stream_contracts: list[dict] | None = None,
    job_id: str | None = None,
    checkpoint: Any | None = None,
    checkpoint_service: Any | None = None,
    retry_budget: Any | None = None,
    backfill_new_fields: bool = False,
    validation_mode: str = "strict",
    source_filter: dict[str, Any] | None = None,
    skip_preflight: bool = False,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """Stream a spilled object file to a database destination without loading it."""
    try:
        from src.transfer.file_stream import stream_file_to_database
    except ImportError:  # pragma: no cover
        from transfer.file_stream import stream_file_to_database

    return stream_file_to_database(
        content=path,
        filename=filename,
        destination=destination,
        mappings=mappings,
        schema=schema,
        sync_mode=sync_mode,
        stream_contracts=stream_contracts,
        job_id=job_id,
        checkpoint=checkpoint,
        checkpoint_service=checkpoint_service,
        retry_budget=retry_budget,
        backfill_new_fields=backfill_new_fields,
        validation_mode=validation_mode,
        source_filter=source_filter,
        skip_preflight=skip_preflight,
    )
