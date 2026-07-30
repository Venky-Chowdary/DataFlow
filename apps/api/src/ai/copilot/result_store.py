"""Durable Pilot query/sample result references.

Follow-ups ("analyze that", "filter where email is null") resolve against
real stored rows — never invented fixtures. Single-process TTL store with
optional file spill so short API restarts do not wipe open chat refs.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)

_DEFAULT_TTL_SEC = 60 * 60  # 1 hour
_MAX_ROWS = 500
_MAX_ENTRIES = 200


def _default_path() -> Path:
    override = os.environ.get("DATAFLOW_PILOT_RESULTS_PATH", "").strip()
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[3] / "data" / "pilot_results.json"


def _now() -> float:
    return time.time()


def _new_id() -> str:
    return f"pr_{uuid.uuid4().hex[:16]}"


class PilotResultStore:
    """Thread-safe TTL map of sampled/query result sets."""

    def __init__(self, path: Path | None = None, ttl_sec: int = _DEFAULT_TTL_SEC):
        self.path = path or _default_path()
        self.ttl_sec = max(60, int(ttl_sec))
        self._lock = threading.RLock()
        self._entries: dict[str, dict[str, Any]] = {}
        self._latest_by_session: dict[str, str] = {}
        self._latest_global: str | None = None
        self._load()

    def _load(self) -> None:
        try:
            if not self.path.exists():
                return
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            entries = raw.get("entries") or {}
            now = _now()
            for rid, doc in entries.items():
                if float(doc.get("expires_at") or 0) > now:
                    self._entries[str(rid)] = doc
            self._latest_by_session = {
                str(k): str(v)
                for k, v in (raw.get("latest_by_session") or {}).items()
                if str(v) in self._entries
            }
            lg = raw.get("latest_global")
            self._latest_global = str(lg) if lg and str(lg) in self._entries else None
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            _log.warning("pilot result store load failed: %s", exc)

    def _persist(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "entries": self._entries,
                "latest_by_session": self._latest_by_session,
                "latest_global": self._latest_global,
            }
            tmp = self.path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
            tmp.write_text(json.dumps(payload, default=str), encoding="utf-8")
            tmp.replace(self.path)
        except OSError as exc:
            _log.warning("pilot result store persist failed: %s", exc)

    def _gc_locked(self) -> None:
        now = _now()
        dead = [k for k, v in self._entries.items() if float(v.get("expires_at") or 0) <= now]
        for k in dead:
            self._entries.pop(k, None)
        self._latest_by_session = {
            k: v for k, v in self._latest_by_session.items() if v in self._entries
        }
        if self._latest_global and self._latest_global not in self._entries:
            self._latest_global = None
        # Cap size — drop oldest first
        if len(self._entries) > _MAX_ENTRIES:
            ordered = sorted(
                self._entries.items(),
                key=lambda kv: float(kv[1].get("created_at") or 0),
            )
            for rid, _ in ordered[: len(self._entries) - _MAX_ENTRIES]:
                self._entries.pop(rid, None)

    def put(
        self,
        *,
        rows: list[dict[str, Any]],
        columns: list[str],
        meta: dict[str, Any] | None = None,
        column_schema: dict[str, Any] | None = None,
        session_id: str = "",
        source: str = "",
        ttl_sec: int | None = None,
    ) -> str:
        """Persist a capped row set; return result_id."""
        capped = list(rows[:_MAX_ROWS])
        cols = list(columns or [])
        if not cols and capped:
            cols = list(capped[0].keys())
        rid = _new_id()
        ttl = max(60, int(ttl_sec if ttl_sec is not None else self.ttl_sec))
        now = _now()
        doc = {
            "result_id": rid,
            "created_at": now,
            "expires_at": now + ttl,
            "source": source or (meta or {}).get("source") or "",
            "session_id": (session_id or "").strip(),
            "columns": cols,
            "column_schema": column_schema or {},
            "row_count": len(capped),
            "truncated_store": len(rows) > len(capped),
            "rows": capped,
            "meta": dict(meta or {}),
        }
        with self._lock:
            self._gc_locked()
            self._entries[rid] = doc
            self._latest_global = rid
            sid = (session_id or "").strip()
            if sid:
                self._latest_by_session[sid] = rid
            self._persist()
        return rid

    def get(self, result_id: str) -> dict[str, Any] | None:
        rid = (result_id or "").strip()
        if not rid:
            return None
        with self._lock:
            self._gc_locked()
            doc = self._entries.get(rid)
            if not doc:
                return None
            if float(doc.get("expires_at") or 0) <= _now():
                self._entries.pop(rid, None)
                self._persist()
                return None
            return dict(doc)

    def resolve(
        self,
        result_id: str = "",
        session_id: str = "",
    ) -> dict[str, Any] | None:
        """Resolve explicit id or this session's latest. Never cross-session."""
        sid = (session_id or "").strip()
        if result_id:
            doc = self.get(result_id)
            if not doc:
                return None
            owner = str(doc.get("session_id") or "").strip()
            # Session-scoped rows require a matching session_id.
            if owner and owner != sid:
                return None
            return doc
        if not sid:
            return None
        with self._lock:
            self._gc_locked()
            rid = self._latest_by_session.get(sid)
            if not rid:
                return None
            doc = self._entries.get(rid)
            if doc and float(doc.get("expires_at") or 0) > _now():
                return dict(doc)
        return None

    def clear_for_tests(self) -> None:
        with self._lock:
            self._entries.clear()
            self._latest_by_session.clear()
            self._latest_global = None
            self._persist()


_store: PilotResultStore | None = None
_store_lock = threading.Lock()


def get_result_store() -> PilotResultStore:
    global _store
    with _store_lock:
        if _store is None:
            _store = PilotResultStore()
        return _store
