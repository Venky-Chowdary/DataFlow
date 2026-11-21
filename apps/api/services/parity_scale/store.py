"""Scan state: the checkpoint a scan resumes from.

The driver saves its whole state after every completed unit of work, so a
worker that dies loses at most the unit in flight, and a new worker resumes
from the last checkpoint and reaches the same result. Writes fail closed: a
save that raises stops the scan rather than letting it run ahead of what is
recorded.

Phase 1 ships an in-memory store (tests, a single worker) and a file store
(atomic replace + fsync). Phase 3 adds the job-store ``parity_scans``
collection under the Mongo-leased worker fleet; the interface stays the same.
"""

from __future__ import annotations

import base64
import copy
import json
import os
import tempfile
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol


class ScanStore(Protocol):
    def save(self, scan_id: str, state: dict[str, Any]) -> None: ...

    def load(self, scan_id: str) -> dict[str, Any] | None: ...


class MemoryScanStore:
    """Keeps a deep copy per save, so the caller cannot mutate a checkpoint."""

    def __init__(self) -> None:
        self._states: dict[str, str] = {}
        self.saves = 0

    def save(self, scan_id: str, state: dict[str, Any]) -> None:
        self._states[scan_id] = json.dumps(state, sort_keys=True)
        self.saves += 1

    def load(self, scan_id: str) -> dict[str, Any] | None:
        raw = self._states.get(scan_id)
        return None if raw is None else json.loads(raw)

    def snapshot(self, scan_id: str) -> dict[str, Any] | None:
        return copy.deepcopy(self.load(scan_id))


class FileScanStore:
    """One JSON file per scan, replaced atomically."""

    def __init__(self, directory: str | os.PathLike[str]) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def _path(self, scan_id: str) -> Path:
        safe = "".join(ch for ch in scan_id if ch.isalnum() or ch in "-_")
        if not safe or safe != scan_id:
            raise ValueError(f"unsafe scan id {scan_id!r}")
        return self.directory / f"{safe}.json"

    def save(self, scan_id: str, state: dict[str, Any]) -> None:
        path = self._path(scan_id)
        fd, tmp = tempfile.mkstemp(dir=self.directory, prefix=".scan-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(state, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def load(self, scan_id: str) -> dict[str, Any] | None:
        path = self._path(scan_id)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))


# ── typed JSON for key values held in checkpoints ───────────────────────────


def encode_value(value: Any) -> Any:
    """A key value as JSON that decodes to the same Python value and type."""
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return {"$i": str(value)}
    if isinstance(value, float):
        return {"$f": repr(value)}
    if isinstance(value, Decimal):
        return {"$d": str(value)}
    if isinstance(value, datetime):
        return {"$dt": value.isoformat()}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, time):
        return {"$t": value.isoformat()}
    if isinstance(value, timedelta):
        return {"$td": [value.days, value.seconds, value.microseconds]}
    if isinstance(value, uuid.UUID):
        return {"$u": str(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"$b": base64.b64encode(bytes(value)).decode("ascii")}
    raise TypeError(f"cannot checkpoint a key value of type {type(value).__name__}")


def decode_value(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    ((tag, raw),) = value.items()
    if tag == "$i":
        return int(raw)
    if tag == "$f":
        return float(raw)
    if tag == "$d":
        return Decimal(raw)
    if tag == "$dt":
        return datetime.fromisoformat(raw)
    if tag == "$date":
        return date.fromisoformat(raw)
    if tag == "$t":
        return time.fromisoformat(raw)
    if tag == "$td":
        return timedelta(days=raw[0], seconds=raw[1], microseconds=raw[2])
    if tag == "$u":
        return uuid.UUID(raw)
    if tag == "$b":
        return base64.b64decode(raw)
    raise ValueError(f"unknown checkpoint tag {tag!r}")


def encode_key(key: tuple[Any, ...] | None) -> list[Any] | None:
    return None if key is None else [encode_value(v) for v in key]


def decode_key(key: list[Any] | None) -> tuple[Any, ...] | None:
    return None if key is None else tuple(decode_value(v) for v in key)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
