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
file with only the plan in hand. Each write keeps the file it replaces as
``<store>.bak`` and rotates the older ones to ``.bak.1`` and ``.bak.2`` for an
operator to restore; reads never fall back to them, since they may hold plans
deleted since. Writes are durable (``write_json_atomic`` fsyncs the data and
the directory).
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

from services.atomic_file import fsync_directory, write_json_atomic
from services.platform_config import data_dir
from services.schema_fingerprint import fingerprint_mappings, fingerprint_schema
from services.value_serializer import json_default
from services.workspace_filter import WorkspaceFilter, bound_allows, current_workspace_stamp

logger = logging.getLogger(__name__)

STORE_PATH = data_dir() / "transfer_plans.json"
#: How long a plan write waits for another writer to finish before it gives up.
LOCK_TIMEOUT_SECONDS = 30.0
_LOCK_POLL_SECONDS = 0.005
_PROCESS_LOCK = threading.Lock()
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
        backup = _newest_readable_backup(Path(path))
        self.path = str(path)
        self.backup_path = str(backup or _backup_path(Path(path)))
        #: File name of the newest backup generation that parses; "" when none does.
        self.readable_backup = backup.name if backup else ""
        self.error_class = type(error).__name__
        if backup:
            remedy = f"restore the newest readable backup {backup.name} in the data directory"
        else:
            remedy = (
                f"no readable backup was found ({', '.join(p.name for p in _backup_paths(Path(path)))}); "
                "restore it from your own backups"
            )
        super().__init__(
            f"Transfer plan store {Path(path).name} cannot be read ({self.error_class}); nothing "
            f"was read or written. Repair it, or {remedy}, then retry."
        )


#: Path -> error class already logged, so a damaged store is reported once, not per request.
_REPORTED: dict[str, str] = {}


#: Backup generations kept: ``.bak`` (newest), ``.bak.1``, ``.bak.2``.
BACKUP_GENERATIONS = 3


def _backup_path(path: Path) -> Path:
    return path.with_name(path.name + ".bak")


def _backup_paths(path: Path) -> list[Path]:
    """Every backup generation, newest first."""
    newest = _backup_path(path)
    return [newest] + [newest.with_name(f"{newest.name}.{i}") for i in range(1, BACKUP_GENERATIONS)]


#: One backup generation's identity: (name, inode, ctime_ns, mtime_ns, size).
_BackupStat = tuple[str, int, int, int, int]

#: store path -> (identity per backup generation, answer): a damaged store is
#: refused on every request; its backups are parsed once per change, not per
#: request.
_READABLE_BACKUP_CACHE: dict[str, tuple[tuple[_BackupStat | None, ...], Path | None]] = {}


def _backup_signature(path: Path) -> tuple[_BackupStat | None, ...]:
    # Inode and ctime as well as mtime/size: a replacement copied with
    # ``cp -p`` (or within a coarse clock tick) can keep both of those.
    signature: list[_BackupStat | None] = []
    for candidate in _backup_paths(path):
        try:
            st = candidate.stat()
            signature.append((candidate.name, st.st_ino, st.st_ctime_ns, st.st_mtime_ns, st.st_size))
        except OSError:
            signature.append(None)
    return tuple(signature)


def _newest_readable_backup(path: Path) -> Path | None:
    """The newest backup generation the store's own loader would accept, if any.

    Cached on every generation's (inode, ctime_ns, mtime_ns, size): unchanged
    backups are not parsed again for each refused request.
    """
    signature = _backup_signature(path)
    cached = _READABLE_BACKUP_CACHE.get(str(path))
    if cached is not None and cached[0] == signature:
        return cached[1]
    found: Path | None = None
    for candidate in _backup_paths(path):
        try:
            _parse_store_text(candidate.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeDecodeError, *_PARSE_ERRORS):
            continue
        found = candidate
        break
    _READABLE_BACKUP_CACHE[str(path)] = (signature, found)
    return found


class UnknownPlanVersion(LookupError):
    """An operation named a mapping revision the plan does not have."""

    def __init__(self, plan_id: str, version: int, versions: list[int]) -> None:
        self.plan_id = plan_id
        self.version = version
        self.versions = versions
        have = f"its revisions are {versions[0]}..{versions[-1]}" if versions else "it has no revisions"
        super().__init__(f"Plan {plan_id} has no mapping revision {version} ({have})")


class PlanStoreLockUnavailable(RuntimeError):
    """The store's lock file cannot be opened for writing (permissions, read-only volume).

    Not transient like :class:`PlanStoreBusy`: an operator has to fix the file.
    ``str()`` names the file only; ``path`` has the full location for the log.
    """

    def __init__(self, path: Path, error: BaseException) -> None:
        self.path = str(path)
        self.error_class = type(error).__name__
        super().__init__(
            f"Transfer plan store lock file {Path(path).name} cannot be opened for writing "
            f"({self.error_class}); nothing was written. An operator must make it writable by "
            "the API's user (it is never written to; mode 0600) or fix the volume."
        )


class PlanStoreReentry(RuntimeError):
    """A plan-store write was started inside another on the same thread.

    The outer write saves the plans it read before the inner one ran, so the
    inner write would be silently overwritten; it is refused instead.
    """


class PlanVersionConflict(RuntimeError):
    """A write was based on plan state that has changed since it was read.

    ``compared`` says which: ``"head"`` — the newest revision version
    (``base_version``) —, ``"selection"`` — the active / approved revision an
    internal caller read (``base_selection``) — or ``"active"`` — the revision
    an approval expected to be active (``expected_version``). ``base_version`` and
    ``current_version`` are the head versions, or the active versions for a
    selection conflict.
    """

    def __init__(
        self, plan_id: str, *, base_version: int, current_version: int, compared: str = "head"
    ) -> None:
        if compared == "head":
            text = (
                f"Plan {plan_id}: the newest mapping revision is {current_version}, not "
                f"{base_version} — a newer mapping revision exists (or {base_version} was never one)"
            )
        elif compared == "active":
            text = (
                f"Plan {plan_id}: the active mapping revision is {current_version}, not the "
                f"{base_version} that was checked"
            )
        else:
            text = (
                f"Plan {plan_id}: the active or approved mapping revision changed "
                f"(active {base_version} -> {current_version}) since it was read"
            )
        super().__init__(text)
        self.plan_id = plan_id
        self.base_version = base_version
        self.current_version = current_version
        self.compared = compared


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


def _seal_endpoint(endpoint: dict[str, Any], encrypt: Callable[[str], str]) -> dict[str, Any]:
    from services.secret_fields import DSN_KEYS, seal_secret_tree

    # A DSN can carry the password, so it is sealed with the secret-named keys.
    return seal_secret_tree(dict(endpoint or {}), encrypt, also=DSN_KEYS)


def _open_endpoint(endpoint: Any, workspace_id: str) -> dict[str, Any]:
    from services.secret_fields import open_secret_tree
    from services.secret_vault import decrypt_for_workspace

    return open_secret_tree(dict(endpoint or {}), lambda v: decrypt_for_workspace(v, workspace_id))


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

    def __post_init__(self) -> None:
        # map_target is Execute's own adoption record: a stored plan never
        # carries one (written or read back; security re-check F1a).
        from services.target_name_legacy import strip_client_map_targets

        self.mappings = strip_client_map_targets(list(self.mappings or []))

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

    def __post_init__(self) -> None:
        # The plan's stream contracts never carry map_target either (written
        # or read back; security re-check F1a, defence in depth).
        from services.target_name_legacy import strip_contract_map_targets

        contracts = (self.policies or {}).get("stream_contracts")
        if isinstance(contracts, list):
            self.policies = {**self.policies,
                             "stream_contracts": strip_contract_map_targets(contracts)}

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "revisions": [r.to_dict() for r in self.revisions],
        }

    def public_dict(self) -> dict[str, Any]:
        """The plan as an API response: endpoint credentials masked (SEC-4)."""
        from services.secret_fields import public_plan_dict

        return public_plan_dict(self)

    def stored_dict(self) -> dict[str, Any]:
        """The plan as written to the store: endpoint credentials encrypted (SEC-4)."""
        from services.secret_vault import encrypt_secret, tenant_id_from_workspace

        tenant = tenant_id_from_workspace(self.workspace_id or "")
        d = self.to_dict()
        for side in ("source", "destination"):
            d[side] = _seal_endpoint(
                d.get(side) or {}, lambda v: encrypt_secret(v, tenant_id=tenant, label="transfer-plan")
            )
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TransferPlanRecord:
        revisions = [PlanRevision.from_dict(r) for r in data.get("revisions") or []]
        workspace = str(data.get("workspace_id") or "")
        return cls(
            id=data["id"],
            name=data.get("name", "Transfer plan"),
            status=data.get("status", "draft"),
            source=_open_endpoint(data.get("source"), workspace),
            destination=_open_endpoint(data.get("destination"), workspace),
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

    def selection(self) -> tuple[int, int]:
        """(active version, approved version or 0): what an approve/revert changes."""
        approved = next((r.version for r in reversed(self.revisions) if r.approved), 0)
        return self.active_version, approved

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
        # utf-8-sig: a store re-saved by an editor that adds a BOM still reads.
        text = path.read_text(encoding="utf-8-sig")
    except FileNotFoundError:
        return []
    except (OSError, UnicodeDecodeError) as exc:
        raise _unreadable(path, exc) from exc
    try:
        plans = _parse_store_text(text)
    except _PARSE_ERRORS as exc:
        raise _unreadable(path, exc) from exc
    _REPORTED.pop(str(path), None)
    return plans


#: What a store text that is not a valid plan store raises while parsing.
_PARSE_ERRORS = (ValueError, TypeError, KeyError, AttributeError)


def _parse_store_text(text: str) -> list[TransferPlanRecord]:
    """Every plan in a store file's text; raises one of ``_PARSE_ERRORS`` otherwise."""
    raw = json.loads(text)
    rows = raw.get("plans", []) if isinstance(raw, dict) else None
    if not isinstance(rows, list):
        raise TypeError("not a transfer plan store document")
    return [TransferPlanRecord.from_dict(p) for p in rows]


def _save_all(plans: list[TransferPlanRecord]) -> None:
    """Replace the store file (inside :func:`_exclusive`), keeping the old one as ``.bak``.

    The file being replaced was just read and parsed by this write, so it is a
    good copy. It is hard-linked (copied where links fail) to a staging name
    and becomes ``<store>.bak`` only once the new file has landed; a failed
    write drops the staging name and leaves the backup as it was. The store is
    always a new file from the atomic replace, so it never shares an inode
    with its backup.
    """
    backup = _backup_path(STORE_PATH)
    staging: Path | None = None
    try:
        if STORE_PATH.exists():
            staging = backup.with_name(f".{backup.name}.{os.getpid()}.staging")
            staging.unlink(missing_ok=True)
            try:
                os.link(STORE_PATH, staging)
            except OSError:
                shutil.copy2(STORE_PATH, staging)
        write_json_atomic(
            STORE_PATH,
            {"plans": [p.stored_dict() for p in plans]},
            indent=2,
            default=json_default,
        )
    except BaseException:
        if staging is not None:
            staging.unlink(missing_ok=True)
        raise
    if staging is None:
        return
    # The new store has landed: the write is committed. Rotate the backups
    # all or nothing: the older generations are first linked to staging names
    # (originals untouched), staging becomes .bak, and only then do the staged
    # older ones replace .bak.1 / .bak.2. If staging cannot become .bak, every
    # generation is still as it was. A failed rotation is logged, never
    # reported as a failed write (a retry would append the same change twice).
    _rotate_backups(staging, backup)


def _link_or_copy(src: Path, dst: Path) -> None:
    dst.unlink(missing_ok=True)
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def _install(staged: Path, dst: Path) -> None:
    """Rename ``staged`` over ``dst``. Store files are never changed in place,
    so hard links are safe copies; when both already name one inode, rename()
    is a no-op that would leave ``staged`` behind, so it is just removed."""
    try:
        if dst.exists() and os.path.samefile(staged, dst):
            staged.unlink(missing_ok=True)
            return
    except OSError:
        pass
    os.replace(staged, dst)


def _rotate_backups(staging: Path, backup: Path) -> None:
    """Shift the generations oldest first, each step a copy, then add staging.

    ``.bak.2 <- .bak.1``, ``.bak.1 <- .bak``, ``.bak <- staging``: each step
    installs a copy (hard link) of a generation that stays where it was, so a
    failure at any step leaves at worst two names for one generation, never
    a dropped one. The first failure stops the rotation (a later step would
    overwrite a generation that was not copied on); only the generation being
    added then does not make it in. The directory is fsynced afterwards.
    """
    generations = _backup_paths(STORE_PATH)
    pid = os.getpid()
    steps: list[tuple[Path | None, Path]] = [
        (generations[i - 1] if i > 0 else None, generations[i]) for i in range(len(generations) - 1, -1, -1)
    ]
    for src, dst in steps:
        try:
            if src is None:
                _install(staging, dst)
                continue
            if not src.exists():
                continue
            staged = dst.with_name(f".{dst.name}.{pid}.staging")
            _link_or_copy(src, staged)
            try:
                _install(staged, dst)
            finally:
                staged.unlink(missing_ok=True)
        except OSError as exc:
            staging.unlink(missing_ok=True)
            logger.warning(
                "Transfer plan store saved, but its backups were not rotated past %s (%s): "
                "no existing backup generation was dropped",
                dst.name,
                type(exc).__name__,
            )
            break
    fsync_directory(STORE_PATH.parent)


def _sweep_staging() -> None:
    """Remove backup staging files a crashed writer left (inside :func:`_exclusive`).

    Holding the cross-process lock, no other writer is mid-rotation.
    """
    try:
        for leftover in STORE_PATH.parent.glob(f".{STORE_PATH.name}.bak*.staging"):
            leftover.unlink(missing_ok=True)
    except OSError:
        logger.debug("Could not sweep plan-store staging files", exc_info=True)


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
    # Owner-only and never written to, but opened read-write: on NFS/EFS Linux
    # emulates flock() with fcntl locks, and an exclusive one needs a writable fd.
    try:
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    except OSError as exc:
        logger.error(
            "Transfer plan store lock file %s cannot be opened for writing (%s)",
            lock_path,
            type(exc).__name__,
        )
        raise PlanStoreLockUnavailable(lock_path, exc) from exc
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise PlanStoreBusy(STORE_PATH) from None
                time.sleep(_LOCK_POLL_SECONDS)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


@contextlib.contextmanager
def _exclusive() -> Iterator[None]:
    """Hold the plan store for one read-modify-write.

    Not re-entrant: a write started inside another on the same thread raises
    :class:`PlanStoreReentry` (the outer write would overwrite it).
    """
    if getattr(_HELD, "held", False):
        raise PlanStoreReentry("a plan-store write cannot run inside another plan-store write")
    deadline = time.monotonic() + LOCK_TIMEOUT_SECONDS
    if not _PROCESS_LOCK.acquire(timeout=LOCK_TIMEOUT_SECONDS):
        raise PlanStoreBusy(STORE_PATH)
    try:
        _HELD.held = True
        try:
            with _store_file_lock(deadline):
                _sweep_staging()
                yield
        finally:
            _HELD.held = False
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


def _check_base_version(
    plan_id: str,
    plan: TransferPlanRecord,
    base_version: int | None,
    base_selection: tuple[int, int] | None = None,
) -> None:
    """Refuse a revision based on plan state that has changed since it was read.

    ``base_version``: the newest revision version (head) the caller read — the
    public ``/mappings`` contract. ``base_selection``: the
    :meth:`TransferPlanRecord.selection` an internal caller read (Map,
    Execute's recovery sync), so an approval or revert made meanwhile is not
    displaced by the new revision becoming active.
    """
    if base_version is not None and int(base_version) != plan.head_version():
        raise PlanVersionConflict(
            plan_id, base_version=int(base_version), current_version=plan.head_version()
        )
    if base_selection is not None and tuple(base_selection) != plan.selection():
        raise PlanVersionConflict(
            plan_id,
            base_version=int(base_selection[0]),
            current_version=plan.active_version,
            compared="selection",
        )


def add_mapping_revision(
    plan_id: str,
    pipeline_result: dict[str, Any],
    *,
    source_columns: list[str] | None = None,
    source_schema: dict[str, str] | None = None,
    target_columns: list[str] | None = None,
    target_schema: dict[str, str] | None = None,
    document_model: dict[str, Any] | None = None,
    base_version: int | None = None,
    base_selection: tuple[int, int] | None = None,
) -> TransferPlanRecord | None:
    """Append a pipeline (Map) result as the new active revision.

    ``base_version`` is the newest revision version the pipeline started from;
    when the plan has gained a revision since (a UI save while Map ran),
    :class:`PlanVersionConflict` is raised and nothing is written, so the save
    stays active. ``None`` appends unconditionally.
    """

    def change(plan: TransferPlanRecord) -> None:
        _check_base_version(plan_id, plan, base_version, base_selection)
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


def add_preflight_run(
    plan_id: str,
    preflight_result: dict[str, Any],
    *,
    mapping_version: int | None = None,
    mapping_hash: str | None = None,
) -> TransferPlanRecord | None:
    """Record a Validate run against the revision it checked.

    ``mapping_version`` / ``mapping_hash`` are the revision Validate read (the
    plan can gain a newer one while it runs). The plan's status becomes
    ``preflight_passed`` / ``preflight_failed`` only while that revision is
    still the active one; a verdict on an older revision is history, not the
    plan's state. Without them (legacy callers) the active revision is used.
    """

    def change(plan: TransferPlanRecord) -> None:
        active = plan.active_revision()
        if mapping_version is None:
            # Legacy callers: the revision Execute runs (active_revision() falls
            # back to the newest when active_version names none), as before.
            version = active.version if active else plan.active_version
        else:
            version = int(mapping_version)
        if mapping_hash is not None:
            digest = mapping_hash
        else:
            checked = next((r for r in plan.revisions if r.version == version), None)
            digest = checked.mapping_hash if checked else (active.mapping_hash if active else "")
        run = {
            # Reuse the caller's run id when it carries one, so the id the
            # operator was handed is the id in plan history.
            "id": str(preflight_result.get("run_id") or uuid.uuid4()),
            "time": _now(),
            "passed": bool(preflight_result.get("passed")),
            "readiness_score": preflight_result.get("readiness_score"),
            "gates": preflight_result.get("gates", []),
            "blockers": preflight_result.get("blockers", []),
            "mapping_version": version,
            "mapping_hash": digest,
        }
        plan.preflight_runs.append(run)
        # The revision Execute would run now: active_revision() falls back to the
        # newest one when active_version names none (legacy / stale data).
        still_active = active is not None and active.version == version and active.mapping_hash == digest
        if still_active:
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
        from services.secret_fields import keep_stored_secrets

        # A masked ``****`` sent back from a GET keeps the stored credential.
        if source is not None:
            plan.source = keep_stored_secrets(source, plan.source)
        if destination is not None:
            plan.destination = keep_stored_secrets(destination, plan.destination)
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
    base_selection: tuple[int, int] | None = None,
) -> TransferPlanRecord | None:
    """Persist user-approved mapping edits as a new revision.

    ``document_model`` is stored with the revision, so each model edit is a
    revision of its own; ``None`` keeps the current model (see
    :func:`_revision_document_model`).

    ``base_version`` is the newest revision version the edit started from
    (:meth:`TransferPlanRecord.head_version`, not ``doc_version``). Unless it
    equals the plan's head now — a newer revision exists, or it names one that
    never did — :class:`PlanVersionConflict` is raised and nothing is written.
    ``None`` (callers that do not track it) appends.
    """

    def change(plan: TransferPlanRecord) -> None:
        _check_base_version(plan_id, plan, base_version, base_selection)
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


def approve_plan_version(
    plan_id: str, version: int | None = None, *, expected_version: int | None = None
) -> TransferPlanRecord | None:
    """Approve ``version`` (default: the active revision) and make it active.

    An explicit version the plan has no revision for raises
    :class:`UnknownPlanVersion` and nothing is written: approving it would
    leave ``active_version`` naming nothing, and Execute would silently run
    another revision.

    ``expected_version`` is the revision the caller checked (Validate's
    ``mapping_version``). When it is no longer the active revision — a save
    landed after Validate — :class:`PlanVersionConflict` (``compared="active"``)
    is raised and nothing is approved.
    """

    def change(plan: TransferPlanRecord) -> None:
        if expected_version is not None:
            current = plan.active_revision()
            current_version = current.version if current else 0
            if int(expected_version) != current_version:
                raise PlanVersionConflict(
                    plan_id,
                    base_version=int(expected_version),
                    current_version=current_version,
                    compared="active",
                )
        if version:
            target_version = int(version)
            if not any(rev.version == target_version for rev in plan.revisions):
                raise UnknownPlanVersion(plan_id, target_version, [r.version for r in plan.revisions])
        else:
            # The revision Execute would run (a stale active_version falls back
            # to the newest); a plan with none keeps 0, as before.
            active = plan.active_revision()
            target_version = active.version if active else plan.active_version
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
