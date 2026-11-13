"""Persisted transfer plans — immutable mapping revisions for map → preflight → run.

Concurrency (PS-1): every change to a plan is one read-modify-write of the
store file, made while holding the store exclusively — a thread lock for this
process and an ``fcntl`` lock on ``<store>.lock`` for other processes (API
workers, or replicas sharing one data volume) — and written by atomic replace.
So two edits that overlap are applied one after the other and neither is lost;
a writer that cannot get the store within ``LOCK_TIMEOUT_SECONDS`` raises
:class:`PlanStoreBusy` and writes nothing. Each plan carries ``doc_version``,
bumped by every write (a legacy plan without one reads as 0 and gets one on
its first write). A UI save may name the mapping revision it was based on
(``base_version``); when the plan has gained a newer revision since, the save
raises :class:`PlanVersionConflict` instead of replacing that revision.

Fail closed (PS-2): an absent store file is an empty store, but a present one
that cannot be read or parsed raises :class:`PlanStoreUnreadable` for every
read and write — answering "no plans" would let the next write replace the
file with only the plan in hand. Each write first keeps the file it replaces
as ``<store>.bak`` (one generation) for an operator to restore; reads never
fall back to it, since it may hold plans deleted since.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.atomic_file import write_json_atomic
from services.platform_config import data_dir
from services.schema_fingerprint import fingerprint_mappings, fingerprint_schema
from services.value_serializer import json_default
from services.workspace_filter import WorkspaceFilter, bound_allows, current_workspace_stamp

logger = logging.getLogger(__name__)

STORE_PATH = data_dir() / "transfer_plans.json"
#: How long a plan write waits for another writer to finish before it gives up.
LOCK_TIMEOUT_SECONDS = 30.0
_LOCK_POLL_SECONDS = 0.005
_PROCESS_LOCK = threading.RLock()
_HELD = threading.local()


class PlanStoreBusy(TimeoutError):
    """Another writer held the plan store past ``LOCK_TIMEOUT_SECONDS``; nothing was written.

    ``str()`` names the store file only (it can reach clients); ``path`` has
    the full location for logs.
    """

    def __init__(self, path: Path) -> None:
        self.path = str(path)
        super().__init__(
            f"Transfer plan store {Path(path).name} is busy: another writer held it past "
            "the lock timeout, so nothing was written. Retry shortly."
        )


class PlanStoreUnreadable(RuntimeError):
    """The store file exists but cannot be read or parsed; nothing was read or written.

    ``str()`` names files only, never the data directory (it can reach clients,
    e.g. as an async Validate job's error); ``path`` and ``backup_path`` keep
    the full locations for the operator log.
    """

    def __init__(self, path: Path, error: BaseException) -> None:
        self.path = str(path)
        self.backup_path = str(_backup_path(path))
        self.error_class = type(error).__name__
        super().__init__(
            f"Transfer plan store {Path(path).name} cannot be read ({self.error_class}); nothing "
            f"was read or written. Repair it, or restore the previous good copy "
            f"{_backup_path(Path(path)).name} in the data directory, then retry."
        )


#: Path -> error class already logged, so a damaged store is reported once, not per request.
_REPORTED: dict[str, str] = {}


def _backup_path(path: Path) -> Path:
    return path.with_name(path.name + ".bak")


class PlanVersionConflict(RuntimeError):
    """A save was based on an older mapping revision than the plan now holds."""

    def __init__(self, plan_id: str, *, base_version: int, current_version: int) -> None:
        super().__init__(
            f"Plan {plan_id} changed: it is at mapping version {current_version}, "
            f"this save was based on version {base_version}"
        )
        self.plan_id = plan_id
        self.base_version = base_version
        self.current_version = current_version


def _resolve_endpoint_in_plan(endpoint: dict[str, Any] | None) -> dict[str, Any] | None:
    """Canonicalize a saved connector endpoint so its format is authoritative."""
    if not endpoint:
        return endpoint
    connector_id = endpoint.get("connector_id")
    if not connector_id:
        return endpoint
    try:
        from src.transfer.adapters import resolve_endpoint_dict
        return resolve_endpoint_dict(endpoint)
    except Exception:
        return endpoint


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _revision_document_model(
    plan: TransferPlanRecord, document_model: dict[str, Any] | None
) -> tuple[dict[str, Any], str]:
    """The model a new revision stores, and its hash.

    ``None`` keeps the active revision's model, so a mapping edit never drops
    it; ``{}`` clears it. A malformed model raises ``DocumentModelError``
    before anything is written.
    """
    if document_model is None:
        active = plan.active_revision()
        if active is None:
            return {}, ""
        return dict(active.document_model), active.document_model_hash
    if not document_model:
        return {}, ""
    from services.document_model import model_hash

    return dict(document_model), model_hash(document_model)


@dataclass
class PlanRevision:
    version: int
    mappings: list[dict[str, Any]]
    transforms: list[dict[str, Any]]
    validation: dict[str, Any]
    mapping_hash: str
    source_schema_hash: str
    target_schema_hash: str
    agents_used: list[str]
    plan_summary: dict[str, Any] = field(default_factory=dict)
    mapping_proof: dict[str, Any] = field(default_factory=dict)
    llm: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now)
    approved: bool = False
    # Snapshot at revision time — required for additive vs breaking classify.
    source_columns: list[str] = field(default_factory=list)
    source_schema: dict[str, str] = field(default_factory=dict)
    # Document model at this revision (services.document_model payload) and its
    # identity; empty for plans that do not nest tables into documents.
    document_model: dict[str, Any] = field(default_factory=dict)
    document_model_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PlanRevision:
        return cls(
            version=int(data["version"]),
            mappings=list(data.get("mappings") or []),
            transforms=list(data.get("transforms") or []),
            validation=dict(data.get("validation") or {}),
            mapping_hash=data.get("mapping_hash", ""),
            source_schema_hash=data.get("source_schema_hash", ""),
            target_schema_hash=data.get("target_schema_hash", ""),
            agents_used=list(data.get("agents_used") or []),
            plan_summary=dict(data.get("plan_summary") or {}),
            mapping_proof=dict(data.get("mapping_proof") or {}),
            llm=dict(data.get("llm") or {}),
            created_at=data.get("created_at", _now()),
            approved=bool(data.get("approved", False)),
            source_columns=[str(c) for c in (data.get("source_columns") or [])],
            source_schema={
                str(k): str(v) for k, v in (data.get("source_schema") or {}).items()
            },
            document_model=(
                dict(data["document_model"])
                if isinstance(data.get("document_model"), dict)
                else {}
            ),
            document_model_hash=(
                data["document_model_hash"]
                if isinstance(data.get("document_model_hash"), str)
                else ""
            ),
        )


@dataclass
class TransferPlanRecord:
    id: str
    name: str
    status: str  # draft | mapped | preflight_passed | preflight_failed | approved | running | completed | failed
    source: dict[str, Any]
    destination: dict[str, Any]
    source_columns: list[str]
    source_schema: dict[str, str]
    target_columns: list[str]
    target_schema: dict[str, str]
    row_count_estimate: int = 0
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    policies: dict[str, Any] = field(default_factory=dict)
    revisions: list[PlanRevision] = field(default_factory=list)
    preflight_runs: list[dict[str, Any]] = field(default_factory=list)
    active_version: int = 0
    job_ids: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)
    #: Owning workspace (D154); "" = unscoped legacy plan.
    workspace_id: str = ""
    #: Bumped by every write of this plan (PS-1); 0 = stored before it existed.
    doc_version: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "revisions": [r.to_dict() for r in self.revisions],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TransferPlanRecord:
        revisions = [PlanRevision.from_dict(r) for r in data.get("revisions") or []]
        return cls(
            id=data["id"],
            name=data.get("name", "Transfer plan"),
            status=data.get("status", "draft"),
            source=dict(data.get("source") or {}),
            destination=dict(data.get("destination") or {}),
            source_columns=list(data.get("source_columns") or []),
            source_schema=dict(data.get("source_schema") or {}),
            target_columns=list(data.get("target_columns") or []),
            target_schema=dict(data.get("target_schema") or {}),
            row_count_estimate=int(data.get("row_count_estimate") or 0),
            sample_rows=list(data.get("sample_rows") or []),
            policies=dict(data.get("policies") or {}),
            revisions=revisions,
            preflight_runs=list(data.get("preflight_runs") or []),
            active_version=int(data.get("active_version") or 0),
            job_ids=list(data.get("job_ids") or []),
            created_at=data.get("created_at", _now()),
            updated_at=data.get("updated_at", _now()),
            workspace_id=str(data.get("workspace_id") or ""),
            doc_version=int(data.get("doc_version") or 0),
        )

    def head_version(self) -> int:
        """Version of the newest revision (0 for a plan with none)."""
        return self.revisions[-1].version if self.revisions else 0

    def active_revision(self) -> PlanRevision | None:
        if not self.revisions:
            return None
        for rev in reversed(self.revisions):
            if rev.version == self.active_version:
                return rev
        return self.revisions[-1]


def _unreadable(path: Path, error: BaseException) -> PlanStoreUnreadable:
    refusal = PlanStoreUnreadable(path, error)
    if _REPORTED.get(refusal.path) != refusal.error_class:
        _REPORTED[refusal.path] = refusal.error_class
        # Path and error class only: the error text can quote the file's bytes.
        logger.error(
            "Transfer plan store %s cannot be read (%s): every plan read and write is "
            "refused until it is repaired or restored from %s",
            refusal.path,
            refusal.error_class,
            refusal.backup_path,
        )
    return refusal


def _load_all() -> list[TransferPlanRecord]:
    """Every stored plan; ``[]`` only when the store file does not exist.

    A file that exists but cannot be read or parsed raises
    :class:`PlanStoreUnreadable` — never ``[]``, which the next write would
    persist over every plan in it.
    """
    path = STORE_PATH
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    except (OSError, UnicodeDecodeError) as exc:
        raise _unreadable(path, exc) from exc
    try:
        raw = json.loads(text)
        rows = raw.get("plans", []) if isinstance(raw, dict) else None
        if not isinstance(rows, list):
            raise TypeError("not a transfer plan store document")
        plans = [TransferPlanRecord.from_dict(p) for p in rows]
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise _unreadable(path, exc) from exc
    _REPORTED.pop(str(path), None)
    return plans


def _keep_backup() -> None:
    """Keep the store file about to be replaced as ``<store>.bak`` (inside :func:`_exclusive`).

    The file was just read and parsed by this write, so the backup is a good
    copy. A hard link costs no space; a copy is the fallback where links fail.
    """
    if not STORE_PATH.exists():
        return
    backup = _backup_path(STORE_PATH)
    staging = backup.with_name(f".{backup.name}.{os.getpid()}.staging")
    staging.unlink(missing_ok=True)
    try:
        os.link(STORE_PATH, staging)
    except OSError:
        shutil.copy2(STORE_PATH, staging)
    os.replace(staging, backup)


def _save_all(plans: list[TransferPlanRecord]) -> None:
    _keep_backup()
    write_json_atomic(
        STORE_PATH,
        {"plans": [p.to_dict() for p in plans]},
        indent=2,
        default=json_default,
    )


@contextlib.contextmanager
def _store_file_lock(deadline: float) -> Iterator[None]:
    """Exclusive ``fcntl`` lock on ``<store>.lock``, waited for until ``deadline``."""
    try:
        import fcntl
    except ImportError:  # pragma: no cover - non-POSIX: the process lock only
        yield
        return
    lock_path = STORE_PATH.with_name(STORE_PATH.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+") as fh:
        while True:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise PlanStoreBusy(STORE_PATH) from None
                time.sleep(_LOCK_POLL_SECONDS)
        try:
            yield
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


@contextlib.contextmanager
def _exclusive() -> Iterator[None]:
    """Hold the plan store for one read-modify-write (re-entrant per thread)."""
    deadline = time.monotonic() + LOCK_TIMEOUT_SECONDS
    if not _PROCESS_LOCK.acquire(timeout=LOCK_TIMEOUT_SECONDS):
        raise PlanStoreBusy(STORE_PATH)
    try:
        depth = getattr(_HELD, "depth", 0)
        _HELD.depth = depth + 1
        try:
            if depth:
                yield
            else:
                with _store_file_lock(deadline):
                    yield
        finally:
            _HELD.depth = depth
    finally:
        _PROCESS_LOCK.release()


def _commit(plans: list[TransferPlanRecord], *changed: TransferPlanRecord) -> None:
    """Write ``plans`` (inside :func:`_exclusive`), bumping each changed plan's version."""
    for plan in changed:
        plan.doc_version += 1
    _save_all(plans)


def _mutate(
    plan_id: str, change: Callable[[TransferPlanRecord], None]
) -> TransferPlanRecord | None:
    """Apply ``change`` to the caller's plan ``plan_id`` and save it, atomically.

    The plan is read, changed and written while the store is held, so a
    concurrent edit is never overwritten by a stale copy. ``change`` may raise
    to abandon the write; nothing is then saved.
    """
    with _exclusive():
        plans = _load_all()
        for plan in plans:
            if not _in_scope(plan, plan_id):
                continue
            change(plan)
            _commit(plans, plan)
            return plan
    return None


def list_plans(*, limit: int = 50) -> list[TransferPlanRecord]:
    visible = [p for p in _load_all() if bound_allows(p.workspace_id)]
    return sorted(visible, key=lambda p: p.updated_at, reverse=True)[:limit]


def get_plan(plan_id: str) -> TransferPlanRecord | None:
    """A plan of the caller's tenant scope; another workspace's does not exist."""
    for p in _load_all():
        if p.id == plan_id:
            return p if bound_allows(p.workspace_id) else None
    return None


def _in_scope(plan: TransferPlanRecord, plan_id: str) -> bool:
    """The plan a mutator may change: this id, inside the caller's tenant scope.

    The same rule as :func:`get_plan`, so another workspace's plan is "not
    found" to every write as well as every read (audit H-1). Nothing bound
    (workers, schedulers) is system scope and reaches every plan.
    """
    return plan.id == plan_id and bound_allows(plan.workspace_id)


def assign_unscoped_workspaces(workspace_for, *, apply: bool) -> dict[str, int]:
    """Stamp legacy unscoped plans with ``workspace_for(plan.to_dict())`` (D154 backfill)."""
    unscoped = WorkspaceFilter.only("")
    counts = {"assigned": 0, "skipped": 0}
    with _exclusive():
        plans = _load_all()
        assigned: list[TransferPlanRecord] = []
        for plan in plans:
            if not unscoped.matches(plan.workspace_id):
                continue
            ws = (workspace_for(plan.to_dict()) or "").strip()
            counts["assigned" if ws else "skipped"] += 1
            if ws:
                plan.workspace_id = ws
                assigned.append(plan)
        if apply and assigned:
            _commit(plans, *assigned)
    return counts


def create_plan(data: dict[str, Any]) -> TransferPlanRecord:
    source = _resolve_endpoint_in_plan(data.get("source")) or {}
    destination = _resolve_endpoint_in_plan(data.get("destination")) or {}
    plan = TransferPlanRecord(
        id=str(uuid.uuid4()),
        name=data.get("name") or "Transfer plan",
        status="draft",
        source=source,
        destination=destination,
        source_columns=list(data.get("source_columns") or []),
        source_schema={k: str(v) for k, v in (data.get("source_schema") or {}).items()},
        target_columns=list(data.get("target_columns") or []),
        target_schema={k: str(v) for k, v in (data.get("target_schema") or {}).items()},
        row_count_estimate=int(data.get("row_count_estimate") or 0),
        sample_rows=list(data.get("sample_rows") or [])[:100],
        policies=dict(data.get("policies") or {}),
        workspace_id=current_workspace_stamp(),
    )
    with _exclusive():
        plans = _load_all()
        plans.append(plan)
        _commit(plans, plan)
    return plan


def add_mapping_revision(
    plan_id: str,
    pipeline_result: dict[str, Any],
    *,
    source_columns: list[str] | None = None,
    source_schema: dict[str, str] | None = None,
    target_columns: list[str] | None = None,
    target_schema: dict[str, str] | None = None,
    document_model: dict[str, Any] | None = None,
) -> TransferPlanRecord | None:
    def change(plan: TransferPlanRecord) -> None:
        model, model_digest = _revision_document_model(plan, document_model)
        src_cols = source_columns or plan.source_columns
        src_schema = source_schema or plan.source_schema
        tgt_cols = target_columns or plan.target_columns
        tgt_schema = target_schema or plan.target_schema
        mappings = list(pipeline_result.get("mappings") or [])
        version = plan.head_version() + 1
        from services.mapping_proof import mapping_proof_or_build

        dest = plan.destination or {}
        src = plan.source or {}
        mapping_proof = mapping_proof_or_build(
            mappings,
            existing=dict(pipeline_result.get("mapping_proof") or {}),
            target_columns=tgt_cols,
            destination_db_type=str(dest.get("format") or dest.get("type") or ""),
            source_kind=str(src.get("kind") or ""),
            source_db_type=str(src.get("format") or src.get("type") or ""),
            dest_kind=str(dest.get("kind") or ""),
            sync_mode=str((plan.policies or {}).get("sync_mode") or ""),
            destination_table_exists=(
                dest.get("table_exists") if isinstance(dest.get("table_exists"), bool) else None
            ),
        )
        rev = PlanRevision(
            version=version,
            mappings=mappings,
            transforms=list(pipeline_result.get("transforms") or []),
            validation=dict(pipeline_result.get("validation") or {}),
            mapping_hash=fingerprint_mappings(mappings),
            source_schema_hash=fingerprint_schema(src_cols, src_schema),
            target_schema_hash=fingerprint_schema(tgt_cols, tgt_schema),
            agents_used=list(pipeline_result.get("agents_used") or []),
            plan_summary=dict(pipeline_result.get("plan_summary") or {}),
            mapping_proof=mapping_proof,
            llm=dict(pipeline_result.get("llm") or {}),
            source_columns=list(src_cols),
            source_schema={k: str(v) for k, v in (src_schema or {}).items()},
            document_model=model,
            document_model_hash=model_digest,
        )
        plan.revisions.append(rev)
        plan.active_version = version
        plan.source_columns = src_cols
        plan.source_schema = src_schema
        plan.target_columns = tgt_cols
        plan.target_schema = tgt_schema
        plan.status = "mapped" if mappings else "draft"
        plan.updated_at = _now()

    return _mutate(plan_id, change)


def add_preflight_run(plan_id: str, preflight_result: dict[str, Any]) -> TransferPlanRecord | None:
    def change(plan: TransferPlanRecord) -> None:
        run = {
            # Reuse the caller's run id when it carries one, so the id the
            # operator was handed is the id in plan history.
            "id": str(preflight_result.get("run_id") or uuid.uuid4()),
            "time": _now(),
            "passed": bool(preflight_result.get("passed")),
            "readiness_score": preflight_result.get("readiness_score"),
            "gates": preflight_result.get("gates", []),
            "blockers": preflight_result.get("blockers", []),
            "mapping_version": plan.active_version,
            "mapping_hash": (plan.active_revision() or PlanRevision(
                version=0, mappings=[], transforms=[], validation={},
                mapping_hash="", source_schema_hash="", target_schema_hash="", agents_used=[],
            )).mapping_hash,
        }
        plan.preflight_runs.append(run)
        plan.status = "preflight_passed" if run["passed"] else "preflight_failed"
        plan.updated_at = _now()

    return _mutate(plan_id, change)


def update_plan(plan_id: str, data: dict[str, Any]) -> TransferPlanRecord | None:
    """Merge destination, policies, and schema snapshots onto an existing plan."""
    # Canonicalize endpoints before taking the store: it may look up connectors.
    source = (_resolve_endpoint_in_plan(data["source"]) or {}) if "source" in data else None
    destination = (
        (_resolve_endpoint_in_plan(data["destination"]) or {}) if "destination" in data else None
    )

    def change(plan: TransferPlanRecord) -> None:
        if "name" in data and data["name"]:
            plan.name = str(data["name"])
        if source is not None:
            plan.source = source
        if destination is not None:
            plan.destination = destination
        if "source_columns" in data:
            plan.source_columns = list(data["source_columns"] or [])
        if "source_schema" in data:
            plan.source_schema = {k: str(v) for k, v in (data["source_schema"] or {}).items()}
        if "target_columns" in data:
            plan.target_columns = list(data["target_columns"] or [])
        if "target_schema" in data:
            plan.target_schema = {k: str(v) for k, v in (data["target_schema"] or {}).items()}
        if "row_count_estimate" in data:
            plan.row_count_estimate = int(data["row_count_estimate"] or 0)
        if "sample_rows" in data:
            plan.sample_rows = list(data["sample_rows"] or [])[:100]
        if "policies" in data:
            plan.policies = {**plan.policies, **dict(data["policies"] or {})}
        plan.updated_at = _now()

    return _mutate(plan_id, change)


def sync_ui_mappings(
    plan_id: str,
    mappings: list[dict[str, Any]],
    *,
    transforms: list[dict[str, Any]] | None = None,
    document_model: dict[str, Any] | None = None,
    base_version: int | None = None,
) -> TransferPlanRecord | None:
    """Persist user-approved mapping edits as a new revision.

    ``document_model`` is stored with the revision, so each model edit is a
    revision of its own; ``None`` keeps the current model (see
    :func:`_revision_document_model`).

    ``base_version`` is the newest revision version the edit started from.
    When the plan has gained a newer revision since, the edit would replace
    work the caller never saw: :class:`PlanVersionConflict` is raised and
    nothing is written. ``None`` (callers that do not track it) appends.
    """

    def change(plan: TransferPlanRecord) -> None:
        if base_version is not None and base_version != plan.head_version():
            raise PlanVersionConflict(
                plan_id, base_version=base_version, current_version=plan.head_version()
            )
        model, model_digest = _revision_document_model(plan, document_model)
        version = plan.head_version() + 1
        from services.mapping_proof import mapping_proof_or_build

        dest = plan.destination or {}
        src = plan.source or {}
        mapping_rows = list(mappings)
        mapping_proof = mapping_proof_or_build(
            mapping_rows,
            target_columns=plan.target_columns,
            destination_db_type=str(dest.get("format") or dest.get("type") or ""),
            source_kind=str(src.get("kind") or ""),
            source_db_type=str(src.get("format") or src.get("type") or ""),
            dest_kind=str(dest.get("kind") or ""),
            sync_mode=str((plan.policies or {}).get("sync_mode") or ""),
            destination_table_exists=(
                dest.get("table_exists") if isinstance(dest.get("table_exists"), bool) else None
            ),
        )
        rev = PlanRevision(
            version=version,
            mappings=mapping_rows,
            transforms=list(transforms or []),
            validation={"passed": True, "issues": [], "source": "ui_sync"},
            mapping_hash=fingerprint_mappings(mapping_rows),
            source_schema_hash=fingerprint_schema(plan.source_columns, plan.source_schema),
            target_schema_hash=fingerprint_schema(plan.target_columns, plan.target_schema),
            agents_used=["user_review"],
            plan_summary={"edited_in_ui": True, "mapping_count": len(mapping_rows)},
            mapping_proof=mapping_proof,
            document_model=model,
            document_model_hash=model_digest,
        )
        plan.revisions.append(rev)
        plan.active_version = version
        plan.status = "mapped"
        plan.updated_at = _now()

    return _mutate(plan_id, change)


def approve_plan_version(plan_id: str, version: int | None = None) -> TransferPlanRecord | None:
    def change(plan: TransferPlanRecord) -> None:
        target_version = version or plan.active_version
        for rev in plan.revisions:
            rev.approved = rev.version == target_version
        plan.active_version = target_version
        plan.status = "approved"
        plan.updated_at = _now()

    return _mutate(plan_id, change)


def attach_job(plan_id: str, job_id: str, *, status: str = "running") -> TransferPlanRecord | None:
    def change(plan: TransferPlanRecord) -> None:
        if job_id not in plan.job_ids:
            plan.job_ids.append(job_id)
        plan.status = status
        plan.updated_at = _now()

    return _mutate(plan_id, change)
