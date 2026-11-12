"""Durable Pilot query/sample result references.

Follow-ups ("analyze that", "filter where email is null") resolve against
real stored rows — never invented fixtures. Single-process TTL store with
optional file spill so short API restarts do not wipe open chat refs.

A reference is not a credential. Each result is stamped with the workspace and
actor that produced it and is readable only by that actor in a workspace that
may read it; to anyone else it does not exist. The spill file holds the rows
sealed by the connector-secret vault (``sealed_state``) — never in plaintext —
and a deployment whose vault cannot seal keeps them in memory only (audit H5).
"""

from __future__ import annotations

import json
import logging
import os
from services.brand_env import getenv_brand
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from .sealed_state import owner_stamp, readable_by_caller, seal, unseal

_log = logging.getLogger(__name__)

_DEFAULT_TTL_SEC = 60 * 60  # 1 hour
_MAX_ROWS = 500
_MAX_ENTRIES = 200


def _default_path() -> Path:
    override = getenv_brand("PILOT_RESULTS_PATH", "").strip()
    if override:
        return Path(override)
    from services.platform_config import data_dir
    return data_dir() / "pilot_results.json"


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
        # result_id -> ciphertext of its rows and meta, so a persist does not re-seal.
        self._seal_cache: dict[str, tuple[str, str]] = {}
        self._load()

    def _load(self) -> None:
        try:
            if not self.path.exists():
                return
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            _log.warning("pilot result store load failed: %s", exc)
            return
        self._merge_payload_locked(raw)
        entries = raw.get("entries") if isinstance(raw, dict) else None
        if isinstance(entries, dict) and any(
            isinstance(doc, dict) and "rows" in doc for doc in entries.values()
        ):
            # A spill written before sealing holds rows in clear: rewrite it now
            # (sealed, or without them) rather than at the next write (review).
            self._persist()

    def _merge_payload_locked(self, raw: dict[str, Any]) -> None:
        """Fold a disk snapshot in. A row already in memory stays if it is newer."""
        entries = raw.get("entries") or {}
        now = _now()
        for rid, doc in entries.items():
            if not isinstance(doc, dict):
                continue
            if float(doc.get("expires_at") or 0) <= now:
                continue
            if "rows_sealed" in doc:
                rows = unseal(doc.get("rows_sealed"))
                if not isinstance(rows, list):
                    continue  # sealed under a key this process does not hold
                meta = unseal(doc.get("meta_sealed")) if "meta_sealed" in doc else {}
                doc = {k: v for k, v in doc.items() if k not in ("rows_sealed", "meta_sealed")} | {
                    "rows": rows,
                    # Query text names literals: never read back from clear text.
                    "meta": meta if isinstance(meta, dict) else {},
                }
            elif doc.get("rows"):
                continue  # a pre-seal plaintext spill is not read back in
            key = str(rid)
            current = self._entries.get(key)
            if current is not None and float(current.get("created_at") or 0) > float(
                doc.get("created_at") or 0
            ):
                continue
            self._entries[key] = doc
        for k, v in (raw.get("latest_by_session") or {}).items():
            if str(v) in self._entries:
                self._latest_by_session[str(k)] = str(v)
        lg = raw.get("latest_global")
        if lg and str(lg) in self._entries:
            self._latest_global = str(lg)

    def _reload_disk_locked(self) -> None:
        """Another worker may have written the file after this process loaded it."""
        try:
            if not self.path.exists():
                return
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            _log.warning("pilot result store reload failed: %s", exc)
            return
        if isinstance(raw, dict):
            self._merge_payload_locked(raw)

    def _persist(self) -> None:
        from .spill_lock import read_spill, spill_lock

        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with spill_lock(self.path):
                entries = {
                    rid: spilled
                    for rid, doc in self._entries.items()
                    if (spilled := self._sealed_doc(rid, doc)) is not None
                }
                # Another worker's results on the same file are kept, not
                # overwritten by this worker's view (review 2, low).
                disk = read_spill(self.path)
                now = _now()
                for rid, stored in (disk.get("entries") or {}).items():
                    if (
                        rid not in entries and rid not in self._entries and isinstance(stored, dict)
                        and "rows" not in stored and float(stored.get("expires_at") or 0) > now
                    ):
                        entries[rid] = stored
                if len(entries) > _MAX_ENTRIES:
                    keep = sorted(entries, key=lambda r: float(entries[r].get("created_at") or 0))
                    for rid in keep[: len(entries) - _MAX_ENTRIES]:
                        entries.pop(rid, None)
                latest = {
                    sid: rid for sid, rid in (disk.get("latest_by_session") or {}).items() if rid in entries
                } | self._latest_by_session
                payload = {
                    "entries": entries,
                    "latest_by_session": latest,
                    "latest_global": self._latest_global or disk.get("latest_global"),
                }
                tmp = self.path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
                tmp.write_text(json.dumps(payload, default=str), encoding="utf-8")
                tmp.replace(self.path)
        except OSError as exc:
            _log.warning("pilot result store persist failed: %s", exc)

    def _sealed_doc(self, rid: str, doc: dict[str, Any]) -> dict[str, Any] | None:
        """The on-disk form of one entry: metadata in clear, rows sealed.

        None when the vault cannot seal: that entry stays in this process only.
        """
        tokens = self._seal_cache.get(rid)
        if tokens is None:
            # The query text and filters (``meta``) carry literals too (review M7).
            rows_token, meta_token = seal(doc.get("rows") or []), seal(doc.get("meta") or {})
            if rows_token is None or meta_token is None:
                return None
            tokens = self._seal_cache[rid] = (rows_token, meta_token)
        return {k: v for k, v in doc.items() if k not in ("rows", "meta")} | {
            "rows_sealed": tokens[0],
            "meta_sealed": tokens[1],
        }

    def _gc_locked(self) -> bool:
        """Drop expired and over-cap entries; True when anything was dropped."""
        now = _now()
        before = len(self._entries)
        dead = [k for k, v in self._entries.items() if float(v.get("expires_at") or 0) <= now]
        for k in dead:
            self._entries.pop(k, None)
        for k in [k for k in self._seal_cache if k not in self._entries]:
            self._seal_cache.pop(k, None)
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
        return len(self._entries) != before

    def _gc_and_purge_locked(self) -> None:
        """GC, and take what expired off disk now rather than at the next write."""
        if self._gc_locked():
            self._persist()

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
            # Who may read this back: the staging actor, in a workspace that
            # may read the staging workspace.
            **owner_stamp(),
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
            self._gc_and_purge_locked()
            doc = self._entries.get(rid)
            if not doc:
                self._reload_disk_locked()
                doc = self._entries.get(rid)
            if not doc:
                return None
            if float(doc.get("expires_at") or 0) <= _now():
                self._entries.pop(rid, None)
                self._persist()
                return None
            if not readable_by_caller(doc, same_actor=True):
                # Another workspace's or another actor's result does not exist here.
                return None
            return dict(doc)

    def resolve(
        self,
        result_id: str = "",
        session_id: str = "",
    ) -> dict[str, Any] | None:
        """The result named by ``result_id``, or this session's latest — if the caller owns it."""
        sid = (session_id or "").strip()
        if (result_id or "").strip():
            return self.get(result_id)
        if not sid:
            return None
        with self._lock:
            self._gc_and_purge_locked()
            rid = self._latest_by_session.get(sid)
            if not rid:
                self._reload_disk_locked()
                rid = self._latest_by_session.get(sid)
            if not rid:
                return None
            doc = self._entries.get(rid)
            if (
                doc
                and float(doc.get("expires_at") or 0) > _now()
                and readable_by_caller(doc, same_actor=True)
            ):
                return dict(doc)
        return None

    def explain_miss(self, result_id: str = "", session_id: str = "") -> str:
        """Distinct operator text for a bad id versus a session that never sampled."""
        rid = (result_id or "").strip()
        if rid:
            return (
                f"Stored result '{rid}' is not in the store. "
                "It expires after one hour, belongs to another session, or the id is wrong. "
                "Sample the table or run the query again."
            )
        return (
            "This session has no stored sample. "
            "Pass the result_id from the sample or query, or sample a table first."
        )

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
