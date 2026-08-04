"""Per-session working memory for Datawrap Pilot — the state a follow-up turn edits.

Conversational data questions are not self-contained. The multi-turn text-to-SQL
literature (SParC, CoSQL) catalogues exactly the phenomena operators produce:

* **coreference** — "how many rows in *it*", "profile *that* table";
* **ellipsis** — "and by region?" (metric, table and connector all omitted);
* **constraint / aggregation change** — "average instead of total", "top 3";
* **clarification answers** — the system asked "which connector?", the user
  replies with a bare name that means nothing on its own.

The pilot previously threw all of this away: ``_local_agent`` accepted ``history``
and never read it, so every turn was parsed from scratch and "and by region?"
matched no tool at all.

This module holds the *resolved* state of the last successful data turn as
structured key-values (the "working memory" layer of a multi-layer agent memory),
kept separate from the raw chat transcript. Follow-up parsing then treats the new
turn as an **edit of that state** rather than a fresh parse — the CoE-SQL
"query evolution" framing — which is both cheaper and far more predictable than
re-deriving intent from prose.

Two kinds of state are tracked:

``PilotFocus``
    What we last aggregated/sampled: connector, table, real column names, metric,
    measure column, grouping, ordering, and the stored ``result_id``.

``PendingSlot``
    A question the pilot asked and the tool call it was holding. The next turn's
    bare answer ("Local Postgres") fills the slot and re-runs the original call
    instead of restarting from zero.

Session scoping matches ``result_store``: state is only ever readable by the
session that produced it, so one operator's table focus can never leak into
another's chat.
"""

from __future__ import annotations

import json
import logging
import os
from services.brand_env import getenv_brand
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)

_DEFAULT_TTL_SEC = 30 * 60  # focus goes stale faster than stored rows
_MAX_SESSIONS = 200
_MAX_COLUMNS = 200

# Slots the producing tool fully owns: an empty value means "cleared", not
# "unchanged". Identity fields (connector, table) are never blanked this way.
_AUTHORITATIVE_SLOTS = frozenset({"metric", "column", "group_by", "grain"})


def _default_path() -> Path:
    override = getenv_brand("PILOT_MEMORY_PATH", "").strip()
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[3] / "data" / "pilot_memory.json"


def _now() -> float:
    return time.time()


@dataclass
class PilotFocus:
    """The resolved subject of the last data turn in one session."""

    connector_id: str = ""
    connector_name: str = ""
    connector_type: str = ""
    table: str = ""
    columns: list[str] = field(default_factory=list)
    metric: str = ""
    column: str = ""
    group_by: str = ""
    grain: str = ""
    limit: int = 0
    descending: bool = True
    result_id: str = ""
    tool: str = ""
    where: str = ""
    updated_at: float = 0.0

    def has_target(self) -> bool:
        """True when we know a table to talk about."""
        return bool(self.table)

    def describe(self) -> str:
        if not self.table:
            return ""
        if self.connector_name:
            return f"{self.connector_name}.{self.table}"
        return self.table


@dataclass
class PendingSlot:
    """A clarification the pilot asked, plus the call it was holding."""

    tool: str = ""
    args: dict[str, Any] = field(default_factory=dict)
    missing: str = ""
    question: str = ""
    candidates: list[str] = field(default_factory=list)
    created_at: float = 0.0


class PilotWorkingMemory:
    """Thread-safe, TTL'd, session-scoped focus + pending-slot store."""

    def __init__(self, path: Path | None = None, ttl_sec: int = _DEFAULT_TTL_SEC):
        self.path = path or _default_path()
        self.ttl_sec = max(60, int(ttl_sec))
        self._lock = threading.RLock()
        self._focus: dict[str, dict[str, Any]] = {}
        self._pending: dict[str, dict[str, Any]] = {}
        self._load()

    # ------------------------------------------------------------------ io

    def _load(self) -> None:
        try:
            if not self.path.exists():
                return
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            now = _now()
            for sid, doc in (raw.get("focus") or {}).items():
                if float(doc.get("updated_at") or 0) + self.ttl_sec > now:
                    self._focus[str(sid)] = doc
            for sid, doc in (raw.get("pending") or {}).items():
                if float(doc.get("created_at") or 0) + self.ttl_sec > now:
                    self._pending[str(sid)] = doc
        except (OSError, json.JSONDecodeError, TypeError, AttributeError) as exc:
            _log.warning("pilot working memory load failed: %s", exc)

    def _persist(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {"focus": self._focus, "pending": self._pending}
            tmp = self.path.with_suffix(f".tmp.{os.getpid()}.{uuid.uuid4().hex[:8]}")
            tmp.write_text(json.dumps(payload, default=str), encoding="utf-8")
            tmp.replace(self.path)
        except OSError as exc:
            _log.warning("pilot working memory persist failed: %s", exc)

    def _gc_locked(self) -> None:
        now = _now()
        for store, stamp in ((self._focus, "updated_at"), (self._pending, "created_at")):
            for sid in [
                k for k, v in store.items() if float(v.get(stamp) or 0) + self.ttl_sec <= now
            ]:
                store.pop(sid, None)
        if len(self._focus) > _MAX_SESSIONS:
            ordered = sorted(
                self._focus.items(), key=lambda kv: float(kv[1].get("updated_at") or 0)
            )
            for sid, _ in ordered[: len(self._focus) - _MAX_SESSIONS]:
                self._focus.pop(sid, None)
                self._pending.pop(sid, None)

    # --------------------------------------------------------------- focus

    def get_focus(self, session_id: str) -> PilotFocus | None:
        sid = (session_id or "").strip()
        if not sid:
            return None
        with self._lock:
            self._gc_locked()
            doc = self._focus.get(sid)
            if not doc:
                return None
            try:
                return PilotFocus(**doc)
            except TypeError:
                # Shape drifted across versions — drop rather than crash a turn.
                self._focus.pop(sid, None)
                return None

    def remember_focus(self, session_id: str, focus: PilotFocus) -> None:
        sid = (session_id or "").strip()
        if not sid or not focus.has_target():
            return
        focus.updated_at = _now()
        focus.columns = [str(c) for c in (focus.columns or [])][:_MAX_COLUMNS]
        with self._lock:
            self._gc_locked()
            self._focus[sid] = asdict(focus)
            self._persist()

    def update_focus(self, session_id: str, **changes: Any) -> PilotFocus | None:
        """Merge changes into existing focus, or create it when a table is given.

        Query slots are *authoritative*: passing ``group_by=""`` clears it. Without
        that, "no grouping" could never be remembered — the next turn would
        resurrect the dropped GROUP BY from stale state.
        """
        sid = (session_id or "").strip()
        if not sid:
            return None
        current = self.get_focus(sid) or PilotFocus()
        for key, value in changes.items():
            if not hasattr(current, key):
                continue
            if value in (None, ""):
                if key in _AUTHORITATIVE_SLOTS:
                    setattr(current, key, "" if isinstance(getattr(current, key), str) else 0)
                continue
            setattr(current, key, value)
        if not current.has_target():
            return None
        self.remember_focus(sid, current)
        return current

    # ------------------------------------------------------------- pending

    def get_pending(self, session_id: str) -> PendingSlot | None:
        sid = (session_id or "").strip()
        if not sid:
            return None
        with self._lock:
            self._gc_locked()
            doc = self._pending.get(sid)
            if not doc:
                return None
            try:
                return PendingSlot(**doc)
            except TypeError:
                self._pending.pop(sid, None)
                return None

    def remember_pending(self, session_id: str, slot: PendingSlot) -> None:
        sid = (session_id or "").strip()
        if not sid or not slot.tool or not slot.missing:
            return
        slot.created_at = _now()
        with self._lock:
            self._gc_locked()
            self._pending[sid] = asdict(slot)
            self._persist()

    def clear_pending(self, session_id: str) -> None:
        sid = (session_id or "").strip()
        if not sid:
            return
        with self._lock:
            if self._pending.pop(sid, None) is not None:
                self._persist()

    def clear_for_tests(self) -> None:
        with self._lock:
            self._focus.clear()
            self._pending.clear()
            self._persist()


_memory: PilotWorkingMemory | None = None
_memory_lock = threading.Lock()


def get_working_memory() -> PilotWorkingMemory:
    global _memory
    with _memory_lock:
        if _memory is None:
            _memory = PilotWorkingMemory()
        return _memory
