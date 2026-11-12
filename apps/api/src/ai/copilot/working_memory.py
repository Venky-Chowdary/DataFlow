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
    from services.platform_config import data_dir
    return data_dir() / "pilot_memory.json"


def _now() -> float:
    return time.time()


# ---- at rest (review 2, M1) --------------------------------------------------
#
# A focus holds the WHERE clause a follow-up edits ("email is a@b.com"), and a
# pending slot or proposal the arguments it will re-run. On disk and in the
# shared collection each slot is sealed with the Pilot state key; when nothing
# can seal it, it is persisted without its WHERE text.

_STAMPS = ("updated_at", "created_at")


def _without_where(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: ("" if k == "where" else _without_where(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [_without_where(v) for v in value]
    return value


def _at_rest(doc: dict[str, Any]) -> dict[str, Any]:
    """``doc`` as it may be written down: sealed, or else without its WHERE text."""
    from .sealed_state import seal

    token = seal(doc)
    stamps = {k: doc[k] for k in _STAMPS if k in doc}
    if token is not None:
        return {"sealed": token, **stamps}
    return _without_where(doc)


def _from_rest(doc: Any) -> dict[str, Any] | None:
    """A slot read back from disk or the collection (legacy plaintext included)."""
    if not isinstance(doc, dict):
        return None
    if "sealed" not in doc:
        return doc
    from .sealed_state import unseal

    value = unseal(doc["sealed"])
    return value if isinstance(value, dict) else None


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

    def has_scope(self) -> bool:
        """True when we know *where* to look, even if not yet what.

        Listing a connector's tables settles the connector and nothing else. That
        is worth remembering: without it, "how many rows in the first one" right
        after "what tables are on Demo Orders" reached the aggregator with a real
        table name and no connector, and answered "Connector not found". Only
        ``has_target`` gates the elliptical-edit layer, so a connector-only focus
        fills omitted slots without ever claiming a table the operator never named.
        """
        return bool(self.table or self.connector_id or self.connector_name)

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


@dataclass
class PilotProposal:
    """The last thing Pilot offered to do, and everything needed to act on it.

    Focus remembers what was *read*; nothing remembered what was *offered*. So
    "run it" one turn after a transfer plan was parsed from scratch, matched no
    tool and was refused as undocumented — the operator had to retype the whole
    route to run the plan they were just shown. Worse, "actually cancel that"
    after a staged schedule fell through to a job listing, which reads as though
    the cancel worked while the Confirm row was still live.

    ``staged`` separates the two states that matter. A plan is a rehearsal Pilot
    can promote to a run on consent. A staged mutation already holds an ack in
    the ledger, and chat must never self-approve it — Confirm stays the
    operator's action, so consent can only point at the button.
    """

    tool: str = ""
    args: dict[str, Any] = field(default_factory=dict)
    label: str = ""
    ack_id: str = ""
    staged: bool = False
    created_at: float = 0.0


class PilotWorkingMemory:
    """Thread-safe, TTL'd, session-scoped focus + pending-slot store."""

    def __init__(self, path: Path | None = None, ttl_sec: int = _DEFAULT_TTL_SEC):
        self.path = path or _default_path()
        self.ttl_sec = max(60, int(ttl_sec))
        self._lock = threading.RLock()
        self._focus: dict[str, dict[str, Any]] = {}
        self._pending: dict[str, dict[str, Any]] = {}
        self._proposals: dict[str, dict[str, Any]] = {}
        self._load()

    # ------------------------------------------------------------------ io

    def _load(self) -> None:
        try:
            if not self.path.exists():
                return
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            now = _now()
            legacy = False
            for key, store, stamp in (
                ("focus", self._focus, "updated_at"),
                ("pending", self._pending, "created_at"),
                ("proposals", self._proposals, "created_at"),
            ):
                for sid, stored in (raw.get(key) or {}).items():
                    legacy = legacy or (isinstance(stored, dict) and "sealed" not in stored)
                    doc = _from_rest(stored)
                    if doc and float(doc.get(stamp) or 0) + self.ttl_sec > now:
                        store[str(sid)] = doc
        except (OSError, json.JSONDecodeError, TypeError, AttributeError) as exc:
            _log.warning("pilot working memory load failed: %s", exc)
            return
        if legacy:
            # Plaintext written by an earlier release is resealed now, not at
            # the next write, which may never come.
            self._persist()

    def _persist(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                key: {sid: _at_rest(doc) for sid, doc in store.items()}
                for key, store in (
                    ("focus", self._focus), ("pending", self._pending), ("proposals", self._proposals),
                )
            }
            tmp = self.path.with_suffix(f".tmp.{os.getpid()}.{uuid.uuid4().hex[:8]}")
            tmp.write_text(json.dumps(payload, default=str), encoding="utf-8")
            tmp.replace(self.path)
        except OSError as exc:
            _log.warning("pilot working memory persist failed: %s", exc)

    def _gc_locked(self) -> None:
        now = _now()
        for store, stamp in (
            (self._focus, "updated_at"),
            (self._pending, "created_at"),
            (self._proposals, "created_at"),
        ):
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
                self._proposals.pop(sid, None)

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
        if not sid or not focus.has_scope():
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
        if not current.has_scope():
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

    # ------------------------------------------------------------ proposal

    def get_proposal(self, session_id: str) -> PilotProposal | None:
        sid = (session_id or "").strip()
        if not sid:
            return None
        with self._lock:
            self._gc_locked()
            doc = self._proposals.get(sid)
            if not doc:
                return None
            try:
                return PilotProposal(**doc)
            except TypeError:
                self._proposals.pop(sid, None)
                return None

    def remember_proposal(self, session_id: str, proposal: PilotProposal) -> None:
        sid = (session_id or "").strip()
        if not sid or not proposal.tool:
            return
        proposal.created_at = _now()
        with self._lock:
            self._gc_locked()
            self._proposals[sid] = asdict(proposal)
            self._persist()

    def clear_proposal(self, session_id: str) -> None:
        sid = (session_id or "").strip()
        if not sid:
            return
        with self._lock:
            if self._proposals.pop(sid, None) is not None:
                self._persist()

    def clear_for_tests(self) -> None:
        with self._lock:
            self._focus.clear()
            self._pending.clear()
            self._proposals.clear()
            self._persist()


class _MongoSlots:
    """One slot kind (focus / pending / proposal) in the shared collection.

    Offers the few dict operations :class:`PilotWorkingMemory` uses, so the
    focus, pending and proposal logic stays in one place for both backends.
    """

    def __init__(self, coll: Any, kind: str, stamp: str):
        self._coll = coll
        self._kind = kind
        self._stamp = stamp

    def _id(self, sid: str) -> str:
        return f"{self._kind}:{sid}"

    def get(self, sid: str, default: Any = None) -> Any:
        row = self._coll.find_one({"_id": self._id(sid)})
        doc = _from_rest(row.get("doc")) if row else None
        return dict(doc) if doc else default

    def __setitem__(self, sid: str, doc: dict[str, Any]) -> None:
        self._coll.replace_one(
            {"_id": self._id(sid)},
            {
                "_id": self._id(sid),
                "kind": self._kind,
                "sid": sid,
                "stamp": float(doc.get(self._stamp) or 0),
                "doc": _at_rest(json.loads(json.dumps(doc, default=str))),
            },
            upsert=True,
        )

    def pop(self, sid: str, default: Any = None) -> Any:
        row = self._coll.find_one_and_delete({"_id": self._id(sid)})
        return (_from_rest(row.get("doc")) or default) if row else default

    def clear(self) -> None:
        self._coll.delete_many({"kind": self._kind})

    def expire(self, cutoff: float) -> None:
        self._coll.delete_many({"kind": self._kind, "stamp": {"$lte": cutoff}})


class MongoWorkingMemory(PilotWorkingMemory):
    """Working memory shared by every API replica.

    With a pod-local file, a follow-up ("the second one", "do it") that lands
    on another replica loses its focus and staged proposal. Expired slots are
    deleted on a short interval and ignored on read until then.
    """

    _GC_INTERVAL_SEC = 30.0

    def __init__(self, collection: Any | None = None, ttl_sec: int = _DEFAULT_TTL_SEC):
        self.path = None  # file-backend attribute; unused here
        self.ttl_sec = max(60, int(ttl_sec))
        self._lock = threading.RLock()
        if collection is None:
            from services.control_plane_store import mongo_collection

            collection = mongo_collection("pilot_memory")
        if collection is None:
            raise RuntimeError(
                "Shared Pilot memory requires the pilot_memory Mongo collection. "
                "Refusing a pod-local file store."
            )
        self._coll = collection
        self._focus = _MongoSlots(collection, "focus", "updated_at")  # type: ignore[assignment]
        self._pending = _MongoSlots(collection, "pending", "created_at")  # type: ignore[assignment]
        self._proposals = _MongoSlots(collection, "proposal", "created_at")  # type: ignore[assignment]
        self._last_gc = 0.0

    def _persist(self) -> None:
        return None

    def _gc_locked(self) -> None:
        now = _now()
        if now - self._last_gc < self._GC_INTERVAL_SEC:
            return
        self._last_gc = now
        try:
            for slots in (self._focus, self._pending, self._proposals):
                slots.expire(now - self.ttl_sec)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 - a missed sweep only delays deletion; reads still honour the TTL
            _log.debug("pilot memory gc skipped", exc_info=True)

    def _fresh(self, doc: Any, stamp: str) -> Any:
        if doc and float(doc.get(stamp) or 0) + self.ttl_sec > _now():
            return doc
        return None

    def get_focus(self, session_id: str) -> PilotFocus | None:
        sid = (session_id or "").strip()
        if not sid or not self._fresh(self._focus.get(sid), "updated_at"):
            return None
        return super().get_focus(sid)

    def get_pending(self, session_id: str) -> PendingSlot | None:
        sid = (session_id or "").strip()
        if not sid or not self._fresh(self._pending.get(sid), "created_at"):
            return None
        return super().get_pending(sid)

    def get_proposal(self, session_id: str) -> PilotProposal | None:
        sid = (session_id or "").strip()
        if not sid or not self._fresh(self._proposals.get(sid), "created_at"):
            return None
        return super().get_proposal(sid)


_memory: PilotWorkingMemory | None = None
_memory_lock = threading.Lock()


def get_working_memory() -> PilotWorkingMemory:
    """Return the process working memory.

    It follows the confirm ledger's backend: when acks are shared in Mongo
    (multi-replica), focus and staged proposals are too, so a follow-up turn
    routed to another replica keeps its context.
    """
    global _memory
    with _memory_lock:
        from services.process_role import ack_backend

        if ack_backend() == "mongo":
            if not isinstance(_memory, MongoWorkingMemory):
                _memory = MongoWorkingMemory()
            return _memory
        if _memory is None or isinstance(_memory, MongoWorkingMemory):
            _memory = PilotWorkingMemory()
        return _memory
