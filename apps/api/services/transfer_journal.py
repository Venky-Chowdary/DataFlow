"""Capture-once transfer journal — extract once, replay to many destinations.

A run opted into capture (``journal_capture`` on the source or destination
``extra``, or ``DATAFLOW_JOURNAL_CAPTURE``) writes every page the engine read
from the source to a per-run journal. A later transfer replays that journal as
its source — to the same destination (re-backfill) or to any number of other
destinations — without touching the live source again.

Contract (what the journal does and does not promise):
- Capture point: database sources are captured as read — after the incremental
  cursor bound, before row filters and transform recipes, which the replaying
  transfer applies itself. File sources are captured as the exact source bytes.
- Replay is a new verified transfer: mapping, preflight, quarantine and Gate-8
  all run against the journal file. It is never a blind copy.
- Commit is all-or-nothing. Pages stream to ``<stream>.partial`` files; only a
  successful run whose captured rows cover every row it delivered renames them
  and writes ``manifest.json`` (atomically, last). A failed, cancelled, resumed
  or row-path-bypassing run leaves no replayable journal and says why.
- Integrity: the manifest pins each stream's SHA-256, byte size and row count;
  replay re-hashes the file and refuses on any mismatch.
- Fidelity: JSON cannot carry every source type. Decimals keep exact digits as
  text, temporals are ISO-8601, bytes are base64 and non-finite floats are
  text. Each such encoding is counted per column in the manifest — nothing is
  dropped and nothing is stringified silently.
- Isolation: journals live under their workspace; replay refuses a journal the
  caller's workspace scope does not cover.
- Durability: files are fsynced before the rename. They are as durable as the
  volume behind ``DATAFLOW_JOURNAL_DIR`` (default ``<upload_dir>/journals``);
  point it at replicated storage for production. Retention is
  ``DATAFLOW_JOURNAL_RETENTION_DAYS`` (default 7), enforced by ``prune_journals``.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import math
import os
import re
import shutil
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from services.brand_env import getenv_brand

logger = logging.getLogger(__name__)

MANIFEST_NAME = "manifest.json"
FORMAT_VERSION = 1
_TRUTHY = {"1", "true", "yes", "on"}
_JOURNAL_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_UNSCOPED = "_unscoped"


class JournalError(ValueError):
    """A journal cannot be replayed (missing, foreign, expired or corrupt)."""


# ---------------------------------------------------------------- config ----


def journal_root() -> Path:
    raw = str(getenv_brand("JOURNAL_DIR", "") or "").strip()
    if raw:
        return Path(raw)
    from services.platform_config import upload_dir

    return Path(upload_dir()) / "journals"


def retention_seconds() -> float:
    raw = str(getenv_brand("JOURNAL_RETENTION_DAYS", "") or "").strip()
    try:
        days = float(raw) if raw else 7.0
    except ValueError:
        days = 7.0
    return max(days, 0.0) * 86400.0


def _truthy(raw: Any) -> bool | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in _TRUTHY


def capture_requested(request: Any) -> bool:
    for endpoint in (getattr(request, "source", None), getattr(request, "destination", None)):
        extra = getattr(endpoint, "extra", None)
        if isinstance(extra, dict):
            flag = _truthy(extra.get("journal_capture"))
            if flag is not None:
                return flag
    return bool(_truthy(getenv_brand("JOURNAL_CAPTURE", "")))


def _workspace_dir(workspace_id: str) -> str:
    ws = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(workspace_id or "").strip()).strip("._")
    return ws or _UNSCOPED


def _safe_stream_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(name or "stream")).strip("._")
    return (cleaned or "stream")[:120]


# -------------------------------------------------------------- encoding ----


def _encode_value(value: Any, column: str, encodings: dict[str, dict[str, int]]) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value

    def note(kind: str) -> None:
        bucket = encodings.setdefault(column, {})
        bucket[kind] = bucket.get(kind, 0) + 1

    if isinstance(value, float):
        if math.isfinite(value):
            return value
        note("non_finite_float_text")
        return repr(value)
    if isinstance(value, Decimal):
        note("decimal_text")
        return str(value)
    if isinstance(value, datetime):
        note("datetime_iso")
        return value.isoformat()
    if isinstance(value, date):
        note("date_iso")
        return value.isoformat()
    if isinstance(value, dtime):
        note("time_iso")
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        note("bytes_base64")
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, uuid.UUID):
        note("uuid_text")
        return str(value)
    if isinstance(value, dict):
        return {str(k): _encode_value(v, column, encodings) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_encode_value(v, column, encodings) for v in value]
    note(f"stringified:{type(value).__name__}")
    return str(value)


# ---------------------------------------------------------------- writer ----


@dataclass
class _StreamState:
    name: str
    path: Path
    fmt: str
    columns: list[str] = field(default_factory=list)
    schema: dict[str, str] = field(default_factory=dict)
    rows: int = 0
    bytes: int = 0
    sha: Any = field(default_factory=hashlib.sha256)
    encodings: dict[str, dict[str, int]] = field(default_factory=dict)
    handle: Any = None
    source_filename: str = ""


class JournalWriter:
    """One run's journal. Thread-safe: parallel page workers share it."""

    def __init__(self, *, job_id: str, workspace_id: str, request: Any) -> None:
        self.job_id = str(job_id)
        self.workspace_id = str(workspace_id or "")
        self.journal_id = f"{_safe_stream_name(self.job_id)[:64]}.{int(time.time() * 1000)}"
        self.dir = journal_root() / _workspace_dir(self.workspace_id) / self.journal_id
        self.dir.mkdir(parents=True, exist_ok=True)
        self.started_at = time.time()
        self.request = request
        self._streams: dict[str, _StreamState] = {}
        self._lock = threading.Lock()
        self.broken: str = ""

    # -- capture -------------------------------------------------------------

    def _stream(self, name: str, fmt: str = "jsonl") -> _StreamState:
        key = _safe_stream_name(name)
        state = self._streams.get(key)
        if state is None:
            suffix = "jsonl" if fmt == "jsonl" else fmt
            state = _StreamState(name=key, path=self.dir / f"{key}.{suffix}", fmt=fmt)
            state.handle = open(self.dir / f"{key}.{suffix}.partial", "wb")
            self._streams[key] = state
        return state

    def _write_line(self, state: _StreamState, payload: dict[str, Any]) -> None:
        line = (json.dumps(payload, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
        state.handle.write(line)
        state.sha.update(line)
        state.bytes += len(line)
        state.rows += 1

    def capture_matrix(
        self,
        stream: str,
        headers: list[str],
        rows: Iterable[Iterable[Any]],
        schema: dict[str, str] | None = None,
    ) -> int:
        cols = [str(h) for h in (headers or [])]
        with self._lock:
            if self.broken:
                return 0
            state = self._stream(stream)
            self._adopt_columns(state, cols, schema)
            before = state.rows
            for row in rows or []:
                values = list(row)
                payload = {
                    (cols[i] if i < len(cols) else f"_c{i}"): _encode_value(
                        v, cols[i] if i < len(cols) else f"_c{i}", state.encodings
                    )
                    for i, v in enumerate(values)
                }
                self._write_line(state, payload)
            return state.rows - before

    def capture_records(
        self,
        stream: str,
        columns: list[str],
        records: Iterable[dict[str, Any]],
        schema: dict[str, str] | None = None,
    ) -> int:
        cols = [str(c) for c in (columns or [])]
        with self._lock:
            if self.broken:
                return 0
            state = self._stream(stream)
            self._adopt_columns(state, cols, schema)
            before = state.rows
            for record in records or []:
                if not isinstance(record, dict):
                    self.broken = (
                        f"stream {state.name} handed a {type(record).__name__} record — "
                        "the journal records dict rows only"
                    )
                    return state.rows - before
                payload = {
                    str(k): _encode_value(v, str(k), state.encodings) for k, v in record.items()
                }
                self._write_line(state, payload)
            return state.rows - before

    def capture_file(self, stream: str, source: bytes | str | Path, filename: str, rows: int | None = None) -> None:
        ext = (Path(filename or "").suffix.lstrip(".").lower() or "bin")[:12]
        with self._lock:
            state = self._stream(stream, fmt=ext)
            state.source_filename = filename
            if isinstance(source, (bytes, bytearray)):
                chunks: Iterable[bytes] = [bytes(source)]
            else:
                chunks = _iter_file_chunks(Path(source))
            for chunk in chunks:
                state.handle.write(chunk)
                state.sha.update(chunk)
                state.bytes += len(chunk)
            state.rows = int(rows or 0)

    @staticmethod
    def _adopt_columns(state: _StreamState, cols: list[str], schema: dict[str, str] | None) -> None:
        for col in cols:
            if col not in state.columns:
                state.columns.append(col)
        if isinstance(schema, dict):
            for k, v in schema.items():
                state.schema.setdefault(str(k), str(v))

    def mark_broken(self, reason: str) -> None:
        with self._lock:
            self.broken = self.broken or reason

    @property
    def captured_rows(self) -> int:
        return sum(s.rows for s in self._streams.values() if s.fmt == "jsonl")

    @property
    def has_file_stream(self) -> bool:
        return any(s.fmt != "jsonl" for s in self._streams.values())

    # -- lifecycle -----------------------------------------------------------

    def _close_handles(self) -> None:
        for state in self._streams.values():
            if state.handle is not None and not state.handle.closed:
                state.handle.flush()
                os.fsync(state.handle.fileno())
                state.handle.close()

    def abort(self, reason: str) -> dict[str, Any]:
        with self._lock:
            for state in self._streams.values():
                if state.handle is not None and not state.handle.closed:
                    state.handle.close()
            shutil.rmtree(self.dir, ignore_errors=True)
        logger.info("transfer journal %s not kept: %s", self.journal_id, reason)
        return {"captured": False, "reason": reason}

    def commit(self, *, delivered_rows: int) -> dict[str, Any]:
        with self._lock:
            if self.broken:
                reason = self.broken
            elif not self._streams:
                reason = "the run read no source page through the journaled read path"
            elif not self.has_file_stream and self.captured_rows < int(delivered_rows or 0):
                reason = (
                    f"captured {self.captured_rows} row(s) but the run delivered "
                    f"{int(delivered_rows)} — this route moved rows outside the journaled "
                    "read path (native bulk copy), so the journal would be incomplete"
                )
            else:
                reason = ""
        if reason:
            return self.abort(reason)
        with self._lock:
            self._close_handles()
            streams = []
            for state in self._streams.values():
                os.replace(f"{state.path}.partial", state.path)
                streams.append({
                    "name": state.name,
                    "file": state.path.name,
                    "format": state.fmt,
                    "source_filename": state.source_filename,
                    "rows": state.rows,
                    "bytes": state.bytes,
                    "sha256": state.sha.hexdigest(),
                    "columns": state.columns,
                    "source_schema": state.schema,
                    "encodings": state.encodings,
                })
            now = time.time()
            src = getattr(self.request, "source", None)
            manifest = {
                "format_version": FORMAT_VERSION,
                "journal_id": self.journal_id,
                "job_id": self.job_id,
                "workspace_id": self.workspace_id,
                "source": {
                    "kind": str(getattr(src, "kind", "") or ""),
                    "format": str(getattr(src, "format", "") or ""),
                    "connector_id": str(getattr(src, "connector_id", "") or ""),
                    "object": str(getattr(src, "table", "") or getattr(src, "collection", "") or ""),
                    "filename": str(getattr(self.request, "source_filename", "") or ""),
                },
                "sync_mode": str(getattr(self.request, "sync_mode", "") or ""),
                "delivered_rows": int(delivered_rows or 0),
                "streams": streams,
                "captured_at": self.started_at,
                "committed_at": now,
                "expires_at": now + retention_seconds(),
            }
            _atomic_write_json(self.dir / MANIFEST_NAME, manifest)
        logger.info(
            "transfer journal %s committed: %d stream(s), %d row(s)",
            self.journal_id, len(streams), self.captured_rows,
        )
        return {
            "captured": True,
            "journal_id": self.journal_id,
            "streams": [
                {"name": s["name"], "rows": s["rows"], "sha256": s["sha256"]} for s in streams
            ],
            "expires_at": manifest["expires_at"],
        }


def _iter_file_chunks(path: Path, size: int = 1 << 20):
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(size)
            if not chunk:
                return
            yield chunk


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex[:8]}.tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


# -------------------------------------------------------- run registry ----

_ACTIVE: dict[str, JournalWriter] = {}
_ACTIVE_LOCK = threading.Lock()


def begin_capture(request: Any, job_id: str, *, resume: bool = False) -> tuple[JournalWriter | None, str]:
    """Open the run's journal, or say why this run is not captured."""
    if not capture_requested(request):
        return None, ""
    src = getattr(request, "source", None)
    extra = getattr(src, "extra", None) or {}
    if isinstance(extra, dict) and extra.get("journal_id"):
        return None, "this run replays a journal — it is not captured again"
    if resume:
        return None, "a resumed run reads only the uncommitted tail — capture a fresh run instead"
    mode = str(getattr(request, "sync_mode", "") or "").lower()
    if "cdc" in mode:
        return None, "CDC runs are change streams with their own durable log — not journaled"
    kind = str(getattr(src, "kind", "") or "")
    if kind not in {"file", "database"}:
        return None, f"source kind {kind!r} is not journaled"
    writer = JournalWriter(
        job_id=job_id,
        workspace_id=str(getattr(request, "workspace_id", "") or ""),
        request=request,
    )
    if kind == "file":
        content = getattr(request, "source_content", None) or b""
        path = str(getattr(request, "source_path", "") or "")
        name = str(getattr(request, "source_filename", "") or "upload.csv")
        if content:
            writer.capture_file(name, content, name)
        elif path and Path(path).is_file():
            writer.capture_file(name, path, name)
        else:
            writer.abort("the file source had no bytes to capture")
            return None, "the file source had no bytes to capture"
    with _ACTIVE_LOCK:
        _ACTIVE[str(job_id)] = writer
    return writer, ""


def active_journal(job_id: Any) -> JournalWriter | None:
    if not job_id:
        return None
    return _ACTIVE.get(str(job_id))


def finish_capture(job_id: str, result: Any) -> dict[str, Any] | None:
    with _ACTIVE_LOCK:
        writer = _ACTIVE.pop(str(job_id), None)
    if writer is None:
        return None
    if result is None or not getattr(result, "success", False):
        return writer.abort("the run did not succeed")
    delivered = int(getattr(result, "records_transferred", 0) or 0)
    try:
        return writer.commit(delivered_rows=delivered)
    except OSError as exc:
        logger.exception("transfer journal %s commit failed", writer.journal_id)
        return writer.abort(f"journal commit failed: {exc}")


def abort_capture(job_id: str, reason: str) -> None:
    with _ACTIVE_LOCK:
        writer = _ACTIVE.pop(str(job_id), None)
    if writer is not None:
        writer.abort(reason)


def capture_batch(job_id: Any, stream: str, batch: Any) -> None:
    """Streamed-read hook: capture one page exactly once."""
    writer = active_journal(job_id)
    if writer is None or not batch:
        return
    if getattr(batch, "_journal_captured", False):
        return
    try:
        batch._journal_captured = True
    except AttributeError:
        writer.mark_broken("a read page cannot be marked captured, so it could be journaled twice")
        return
    writer.capture_matrix(stream, list(getattr(batch, "headers", None) or []), getattr(batch, "rows", None) or [])


def capture_records(job_id: Any, stream: str, columns: list[str], records: list[dict[str, Any]],
                    schema: dict[str, str] | None = None) -> None:
    """Buffered-read hook (database sources; file sources capture their bytes)."""
    writer = active_journal(job_id)
    if writer is None or writer.has_file_stream:
        return
    writer.capture_records(stream, columns, records, schema)


# ---------------------------------------------------------------- reads ----


def _workspace_allows(manifest_ws: str) -> bool:
    from services.workspace_filter import bound_allows

    return bool(bound_allows(manifest_ws or None))


def _load_manifest(journal_dir: Path) -> dict[str, Any] | None:
    path = journal_dir / MANIFEST_NAME
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _journal_dirs() -> Iterable[Path]:
    root = journal_root()
    if not root.is_dir():
        return []
    return [d for ws in root.iterdir() if ws.is_dir() for d in ws.iterdir() if d.is_dir()]


def find_journal(journal_id: str) -> tuple[Path, dict[str, Any]]:
    if not _JOURNAL_ID_RE.match(str(journal_id or "")):
        raise JournalError("Journal id is malformed.")
    for journal_dir in _journal_dirs():
        if journal_dir.name != journal_id:
            continue
        manifest = _load_manifest(journal_dir)
        if manifest is None:
            continue
        if not _workspace_allows(str(manifest.get("workspace_id") or "")):
            break
        return journal_dir, manifest
    raise JournalError(f"Journal {journal_id} was not found in this workspace.")


def list_journals() -> list[dict[str, Any]]:
    out = []
    for journal_dir in _journal_dirs():
        manifest = _load_manifest(journal_dir)
        if manifest is None or not _workspace_allows(str(manifest.get("workspace_id") or "")):
            continue
        out.append(manifest)
    out.sort(key=lambda m: float(m.get("committed_at") or 0), reverse=True)
    return out


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    for chunk in _iter_file_chunks(path):
        digest.update(chunk)
    return digest.hexdigest()


def resolve_replay(journal_id: str, stream: str = "") -> dict[str, Any]:
    """Verify a journal stream and describe it as a file source."""
    journal_dir, manifest = find_journal(journal_id)
    if float(manifest.get("expires_at") or 0) and time.time() > float(manifest["expires_at"]):
        raise JournalError(f"Journal {journal_id} expired; capture a fresh run.")
    streams = [s for s in manifest.get("streams") or [] if isinstance(s, dict)]
    if stream:
        streams = [s for s in streams if s.get("name") == _safe_stream_name(stream)]
    if not streams:
        raise JournalError(f"Journal {journal_id} has no stream {stream!r}.")
    if len(streams) > 1:
        names = ", ".join(str(s.get("name")) for s in streams)
        raise JournalError(f"Journal {journal_id} holds several streams ({names}); name one.")
    entry = streams[0]
    path = (journal_dir / str(entry.get("file") or "")).resolve()
    if journal_dir.resolve() not in path.parents or not path.is_file():
        raise JournalError(f"Journal {journal_id} stream file is missing.")
    if path.stat().st_size != int(entry.get("bytes") or -1) or _sha256_file(path) != entry.get("sha256"):
        raise JournalError(
            f"Journal {journal_id} failed its integrity check (SHA-256/size mismatch) — refusing to replay."
        )
    fmt = str(entry.get("format") or "jsonl")
    filename = str(entry.get("source_filename") or "") or f"{entry.get('name')}.{fmt}"
    return {"path": str(path), "filename": filename, "format": fmt, "manifest": manifest, "stream": entry}


def apply_replay_source(request: Any) -> bool:
    """Point a file-source request at its journal. True when it is a replay."""
    src = getattr(request, "source", None)
    extra = getattr(src, "extra", None)
    if not isinstance(extra, dict) or not extra.get("journal_id"):
        return False
    resolved = resolve_replay(str(extra["journal_id"]), str(extra.get("journal_stream") or ""))
    src.kind = "file"
    src.format = resolved["format"] if resolved["format"] != "bin" else (src.format or "")
    request.source_path = resolved["path"]
    request.source_filename = resolved["filename"]
    request.source_content = b""
    extra["journal_replay"] = {
        "journal_id": resolved["manifest"]["journal_id"],
        "stream": resolved["stream"]["name"],
        "rows": resolved["stream"]["rows"],
        "sha256": resolved["stream"]["sha256"],
    }
    return True


def delete_journal(journal_id: str) -> None:
    journal_dir, _ = find_journal(journal_id)
    shutil.rmtree(journal_dir, ignore_errors=False)


def prune_journals(now: float | None = None, *, orphan_grace_seconds: float = 86400.0) -> dict[str, int]:
    """Drop expired journals and abandoned (manifest-less) capture dirs."""
    now = time.time() if now is None else now
    counts = {"expired": 0, "orphaned": 0}
    for journal_dir in _journal_dirs():
        manifest = _load_manifest(journal_dir)
        if manifest is None:
            if journal_dir.name in {w.journal_id for w in list(_ACTIVE.values())}:
                continue
            try:
                age = now - journal_dir.stat().st_mtime
            except OSError:
                continue
            if age > orphan_grace_seconds:
                shutil.rmtree(journal_dir, ignore_errors=True)
                counts["orphaned"] += 1
            continue
        if float(manifest.get("expires_at") or 0) and now > float(manifest["expires_at"]):
            shutil.rmtree(journal_dir, ignore_errors=True)
            counts["expired"] += 1
    return counts
