"""Canonical endpoint kinds before a route is checked or run (QA round 11).

Two "not yet live" refusals in Addendum A were spelling, not capability:

* ``PG → file (csv/json/excel/parquet/sqlite)``: the matrix knows destination
  kind ``file_export``; ``kind`` is a free string, so ``file`` (what a caller
  naturally writes) was refused as "Combination database → file/* not yet
  live". ``file`` + an export format is ``file_export``; ``file`` / ``file_export``
  + ``sqlite`` is a SQLite *database* written to a file in the exports folder;
  anything else is refused by name.
* ``SQLite file upload → PG``: the catalog's live SQLite is the server-path
  database driver, and an uploaded ``.sqlite`` had no reader. The upload is now
  staged and read through that same ``database/sqlite`` route. The bytes are
  untrusted: magic-byte sniff, a size cap, a private read-only temp file,
  validated through a read-only ``immutable`` URI with extension loading off,
  rollback-journal files only (a WAL file would need a writable ``-shm``).
"""

from __future__ import annotations

import logging
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

from .models import EndpointConfig

logger = logging.getLogger(__name__)

SQLITE_MAGIC = b"SQLite format 3\x00"
SQLITE_EXTENSIONS = (".sqlite", ".sqlite3", ".db")
_STAGE_DIRNAME = "datawrap_sqlite_uploads"


def is_sqlite_bytes(content: bytes | None) -> bool:
    return bool(content) and bytes(content[:16]) == SQLITE_MAGIC


def _is_sqlite_format(fmt: str) -> bool:
    from .connector_capabilities import resolve_driver_type

    return resolve_driver_type(fmt or "") == "sqlite"


def normalize_destination(dest: EndpointConfig, *, table_hint: str = "") -> EndpointConfig:
    """``file`` → ``file_export``; a SQLite export → ``database/sqlite``. Raises on nonsense."""
    from .registry import LIVE_DEST_FILE_FORMATS

    kind = (dest.kind or "").strip().lower()
    if kind not in {"file", "file_export"}:
        return dest
    fmt = (dest.format or "").strip().lower()
    if _is_sqlite_format(fmt):
        from services.export_paths import resolve_export_path

        name = (dest.output_path or "").strip() or f"{dest.table or table_hint or 'export'}.sqlite"
        path = resolve_export_path(name, default_ext="sqlite")
        dest.kind = "database"
        dest.format = "sqlite"
        dest.database = str(path)
        dest.output_path = str(path)
        dest.table = dest.table or table_hint or "export"
        return dest
    if kind == "file":
        if fmt in LIVE_DEST_FILE_FORMATS:
            dest.kind = "file_export"
            return dest
        raise ValueError(
            f"Destination kind 'file' with format {fmt or '(none)'!r} is not a "
            "destination. Use kind 'file_export' with one of "
            + ", ".join(sorted(LIVE_DEST_FILE_FORMATS))
            + ", format 'sqlite' for a SQLite file, or kind 'database'."
        )
    return dest


def canonical_destination(kind: str, fmt: str) -> tuple[str, str]:
    """The (kind, format) pair the route matrix judges, without side effects."""
    k = (kind or "").strip().lower()
    if k in {"file", "file_export"} and _is_sqlite_format(fmt):
        return "database", "sqlite"
    if k == "file":
        from .registry import LIVE_DEST_FILE_FORMATS

        if (fmt or "").strip().lower() in LIVE_DEST_FILE_FORMATS:
            return "file_export", fmt
    return kind, fmt


def _max_bytes() -> int:
    from services.upload_limits import max_upload_bytes

    return int(max_upload_bytes())


def _stage_dir() -> Path:
    path = Path(tempfile.gettempdir()) / _STAGE_DIRNAME
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    return path


def _read_upload(request: Any) -> bytes:
    content = bytes(getattr(request, "source_content", b"") or b"")
    if content:
        return content
    src_path = str(getattr(request, "source_path", "") or "")
    if not src_path:
        return b""
    size = os.path.getsize(src_path)
    if size > _max_bytes():
        raise ValueError(f"SQLite upload is {size} bytes; the upload cap is {_max_bytes()} bytes.")
    with open(src_path, "rb") as fh:
        return fh.read()


def open_untrusted_sqlite(path: str) -> sqlite3.Connection:
    """Read-only, immutable, extension loading off: nothing in the file runs or writes."""
    uri = Path(path).resolve().as_uri() + "?mode=ro&immutable=1"
    conn = sqlite3.connect(uri, uri=True, timeout=2)
    try:
        conn.enable_load_extension(False)
    except AttributeError:  # Python built without extension support: already off
        pass
    return conn


def stage_sqlite_upload(request: Any, job_id: str = "") -> bool:
    """Turn an uploaded SQLite file into a ``database/sqlite`` source. True when it did."""
    src = request.source
    if (src.kind or "").strip().lower() != "file":
        return False
    fmt = (src.format or "").strip().lower()
    name = str(getattr(request, "source_filename", "") or "").lower()
    if not (_is_sqlite_format(fmt) or (not fmt and name.endswith(SQLITE_EXTENSIONS))):
        return False
    content = _read_upload(request)
    if len(content) > _max_bytes():
        raise ValueError(
            f"SQLite upload is {len(content)} bytes; the upload cap is {_max_bytes()} bytes."
        )
    if not is_sqlite_bytes(content):
        raise ValueError(
            "The uploaded file is not a SQLite database (its header is not "
            "'SQLite format 3'). Upload a .sqlite/.db file, or pick the file's real format."
        )
    # Header bytes 18/19 are the read/write format versions: 2 means WAL, which
    # an immutable read-only open cannot honour without a writable -shm file.
    if content[18:20] == b"\x02\x02":
        raise ValueError(
            "The uploaded SQLite database is in WAL mode. Checkpoint it into a "
            "single file (PRAGMA journal_mode=DELETE) and upload it again."
        )
    fd, staged = tempfile.mkstemp(
        prefix=f"{job_id or 'upload'}_", suffix=".sqlite", dir=str(_stage_dir())
    )
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(content)
        os.chmod(staged, 0o400)
        conn = open_untrusted_sqlite(staged)
        try:
            ok = conn.execute("PRAGMA quick_check").fetchone()
            if not ok or str(ok[0]).lower() != "ok":
                raise ValueError(f"The uploaded SQLite database is damaged: {ok[0] if ok else 'no answer'}")
            tables = [
                str(r[0])
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name NOT LIKE 'sqlite_%' ORDER BY name"
                ).fetchall()
            ]
        finally:
            conn.close()
    except sqlite3.DatabaseError as exc:
        _discard(staged)
        raise ValueError(f"The uploaded file could not be opened as SQLite: {exc}") from exc
    except Exception:
        _discard(staged)
        raise
    table = (src.table or "").strip() or str((src.extra or {}).get("sqlite_table") or "").strip()
    if not table:
        if len(tables) != 1:
            _discard(staged)
            raise ValueError(
                "The uploaded SQLite database holds "
                + (f"{len(tables)} tables ({', '.join(tables[:10])})" if tables else "no tables")
                + "; name the one to load as the source table."
            )
        table = tables[0]
    elif table not in tables:
        _discard(staged)
        raise ValueError(f"The uploaded SQLite database has no table {table!r}.")
    request.source = EndpointConfig(
        kind="database",
        format="sqlite",
        database=staged,
        table=table,
        extra={**dict(src.extra or {}), "staged_sqlite_upload": True},
    )
    request.source_content = b""
    request.source_path = ""
    return True


def _discard(path: str) -> None:
    try:
        os.chmod(path, 0o600)
        os.remove(path)
    except OSError:
        pass


def discard_staged_upload(request: Any) -> None:
    src = getattr(request, "source", None)
    if src is not None and (getattr(src, "extra", None) or {}).get("staged_sqlite_upload"):
        _discard(str(src.database or ""))
