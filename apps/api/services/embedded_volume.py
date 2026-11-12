"""Per-workspace areas of the shared SQLite and DuckDB volumes (security review round 3, H3).

``<data_dir>/sqlite`` and ``<data_dir>/duckdb`` are the only parts of the
service data directory a connector may open. They used to be one namespace
for every workspace: workspace B's editor saved a SQLite connector on
``<data_dir>/sqlite/a_destination.sqlite`` and read workspace A's rows.

Each volume is now partitioned exactly as the exports tree is
(``services.export_paths``): workspace W's files live in
``<volume>/_workspaces/<W>/``, and the binding is the same one, the running
job's workspace, else the stored connector's own (``connector_export_scope``),
else the request's verified tenant workspace:

* a path in the bound workspace's own area opens;
* a path in another workspace's area is refused;
* a path in the flat part of the volume (a file from before the partition):

  - when this workspace provably owns the file, it is moved home first and
    the moved file opens; :func:`migrate_flat_volume_files` does the same for
    every such file at startup (idempotent). The proof is the saved
    SQLite/DuckDB connectors that name the file and were created before the
    volume was partitioned (:func:`mark_partitioned`, written synchronously at
    startup, before any request): all of them must be one workspace's. A
    connector saved afterwards proves nothing, or workspace B could claim A's
    file by naming it;
  - a name that does not exist yet is created in the bound workspace's area;
  - any other flat file has no provable owner and stays out of every
    workspace's reach: only a caller with no workspace in play (system code,
    a platform administrator's all-workspace view, a deployment without
    workspaces) opens it, and such a caller never opens a workspace's area;
* with auth off (the single local operator) every path opens as before; a
  flat path whose file was moved home still resolves to the moved file.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.export_paths import WORKSPACES_DIRNAME, _is_within, _workspace_segment

logger = logging.getLogger(__name__)

#: Written once per volume when it is partitioned: connectors saved after it prove no ownership.
PARTITION_MARKER = ".partitioned_at"
#: SQLite / DuckDB files that belong next to a database file and move with it.
SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal", ".wal")

__all__ = [
    "mark_partitioned",
    "migrate_flat_volume_files",
    "migrate_flat_volume_files_at_startup",
    "volume_open_path",
    "volume_workspace_dir",
]


def volume_workspace_dir(volume_dir: Path, workspace_id: str) -> Path:
    """The area of ``volume_dir`` that one workspace's files live in."""
    return volume_dir / WORKSPACES_DIRNAME / _workspace_segment(workspace_id)


def _bound_workspace() -> str:
    """The job's workspace, else the stored connector's, else the request's own
    (its tenant scope's verified workspace, never a raw header)."""
    from services.export_object_store import bound_job_workspace
    from services.export_paths import _connector_workspace
    from services.workspace_filter import current_workspace_stamp

    return (
        bound_job_workspace()
        or (_connector_workspace.get() or "")
        or current_workspace_stamp(for_write=False)
    )


def _single_operator() -> bool:
    from src.services.auth_service import auth_required

    return not auth_required()


def _volume_dirs() -> list[tuple[str, Path]]:
    from services.platform_config import data_dir

    base = data_dir().resolve(strict=False)
    return [(kind, base / kind) for kind in ("sqlite", "duckdb")]


def _partition_time(volume_dir: Path, *, create: bool = False) -> datetime | None:
    """When ``volume_dir`` was partitioned (``None`` = not yet, and ``create`` is off)."""
    marker = volume_dir / WORKSPACES_DIRNAME / PARTITION_MARKER
    try:
        return datetime.fromisoformat(marker.read_text(encoding="utf-8").strip())
    except FileNotFoundError:
        if not create:
            return None
    except ValueError:
        return None
    marker.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc)
    try:
        fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        return _partition_time(volume_dir)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(stamp.isoformat())
    return stamp


def mark_partitioned() -> None:
    """Record when each volume was partitioned, if not yet recorded (startup, before requests)."""
    for _kind, volume_dir in _volume_dirs():
        _partition_time(volume_dir, create=True)


def _created_before(connector: Any, cutoff: datetime) -> bool:
    try:
        created = datetime.fromisoformat(str(getattr(connector, "created_at", "") or ""))
    except ValueError:
        return False
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return created < cutoff


def _connector_paths(kind: str, connector: Any) -> list[str]:
    """The file a saved SQLite/DuckDB connector names (before any root), or nothing."""
    from connectors.sqlite_common import duckdb_unconfined_path, sqlite_unconfined_path

    database = str(getattr(connector, "database", "") or "")
    connection_string = str(getattr(connector, "connection_string", "") or "")
    try:
        if kind == "sqlite":
            path = sqlite_unconfined_path(database, connection_string)
        else:
            path = duckdb_unconfined_path(database, connection_string)
    except ValueError:
        return []
    if not path or path == ":memory:" or path.lower().startswith(("md:", "motherduck:")):
        return []
    return [path]


def _claims(kind: str, volume_dir: Path) -> dict[str, set[str]]:
    """Flat file (relative to the volume) -> workspaces of the saved connectors naming it
    that were saved before the volume was partitioned."""
    from services.connector_store import list_connectors
    from services.workspace_filter import bound_workspace_filter
    from src.transfer.connector_capabilities import resolve_driver_type

    claims: dict[str, set[str]] = {}
    cutoff = _partition_time(volume_dir)
    if cutoff is None:
        return claims
    container = volume_dir / WORKSPACES_DIRNAME
    with bound_workspace_filter(None):  # every workspace's connectors, for the proof only
        connectors = list_connectors()
    for connector in connectors:
        if resolve_driver_type(str(getattr(connector, "type", "") or "")).lower() != kind:
            continue
        if not _created_before(connector, cutoff):
            continue
        for raw in _connector_paths(kind, connector):
            candidate = Path(raw).expanduser()
            if not candidate.is_absolute():
                candidate = volume_dir / candidate
            resolved = candidate.resolve(strict=False)
            if not _is_within(volume_dir, resolved) or _is_within(container, resolved):
                continue
            owner = str(getattr(connector, "workspace_id", "") or "")
            claims.setdefault(resolved.relative_to(volume_dir).as_posix(), set()).add(owner)
    return claims


def _owner(relative: str, claims: dict[str, set[str]]) -> str:
    """The single workspace every claim names, or ``""`` (no claim, two, or an unscoped one)."""
    owners = claims.get(relative, set())
    return next(iter(owners)) if len(owners) == 1 else ""


def _move(volume_dir: Path, relative: str, workspace_id: str) -> str:
    """Move one flat file (and its sidecars) home: ``moved`` / ``kept_existing`` / ``gone``."""
    from services.export_migration import _link_or_exclusive_copy

    source = volume_dir / relative
    target = volume_workspace_dir(volume_dir, workspace_id).joinpath(*relative.split("/"))
    if target.exists():
        return "kept_existing"
    target.parent.mkdir(parents=True, exist_ok=True)
    moved = "gone"
    for suffix in ("", *SIDECAR_SUFFIXES):
        src = Path(f"{source}{suffix}")
        dst = Path(f"{target}{suffix}")
        if src.is_symlink() or not src.exists():
            continue
        try:
            _link_or_exclusive_copy(src, dst)
        except FileExistsError:
            if not suffix:
                return "kept_existing"
            continue
        except FileNotFoundError:
            continue
        try:
            os.unlink(src)
        except FileNotFoundError:
            pass
        if not suffix:
            moved = "moved"
    if moved == "moved":
        logger.info("Moved %s volume file %s into the area of workspace %s", volume_dir.name, relative, workspace_id)
    return moved


def _flat_files(volume_dir: Path) -> list[Path]:
    found: list[Path] = []
    if not volume_dir.is_dir():
        return found
    for folder, dirs, files in os.walk(volume_dir, followlinks=False):
        here = Path(folder)
        if here == volume_dir:
            dirs[:] = [d for d in dirs if d != WORKSPACES_DIRNAME]
        for name in files:
            path = here / name
            if path.is_symlink() or name.endswith(SIDECAR_SUFFIXES):
                continue
            found.append(path)
    return sorted(found)


def migrate_flat_volume_files() -> dict[str, Any]:
    """Move every flat SQLite/DuckDB volume file with a provable owner into that
    workspace's area. Returns ``{"moved", "left", "kept_existing"}``. Idempotent."""
    summary = {"moved": 0, "left": 0, "kept_existing": 0}
    for kind, volume_dir in _volume_dirs():
        files = _flat_files(volume_dir)
        if files:
            _partition_time(volume_dir, create=True)
        if not files:
            continue
        claims = _claims(kind, volume_dir)
        for path in files:
            relative = path.relative_to(volume_dir).as_posix()
            owner = _owner(relative, claims)
            outcome = _move(volume_dir, relative, owner) if owner else "left"
            if outcome == "kept_existing":
                summary["left"] += 1
            if outcome in summary:
                summary[outcome] += 1
    return summary


def migrate_flat_volume_files_at_startup() -> dict[str, Any]:
    """The startup pass: never raises."""
    try:
        summary = migrate_flat_volume_files()
    except Exception:  # noqa: BLE001 - reported, not fatal
        logger.warning("SQLite/DuckDB volume migration did not run", exc_info=True)
        return {}
    if summary["moved"] or summary["left"]:
        logger.info(
            "SQLite/DuckDB volume migration: %(moved)d moved into workspace areas, %(left)d left "
            "in place (no provable single owner; platform administrators only)", summary,
        )
    return summary


def _refused(label: str, why: str, own: Path | None = None) -> ValueError:
    where = f" ({own})" if own is not None else ""
    return ValueError(
        f"{label} path is {why} of the shared {label} volume and cannot be opened by this "
        f"workspace's connector; use a file in this workspace's own area{where}"
    )


def volume_open_path(resolved: Path, volume_dir: Path, *, kind: str, label: str) -> Path:
    """The file to open for ``resolved`` (inside ``volume_dir``), or a ``ValueError``."""
    container = volume_dir / WORKSPACES_DIRNAME
    workspace = _bound_workspace()
    in_container = _is_within(container, resolved)
    relative = "" if in_container else resolved.relative_to(volume_dir).as_posix()
    if _single_operator():
        if relative and workspace and not resolved.exists():
            moved = volume_workspace_dir(volume_dir, workspace).joinpath(*relative.split("/"))
            if moved.exists():
                return moved
        return resolved
    if not workspace:
        # No workspace in play (system code, a platform administrator's
        # all-workspace view, a deployment that does not use workspaces): the
        # flat, unscoped part opens, no workspace's area does.
        if in_container:
            raise _refused(label, "in a workspace area")
        return resolved
    own = volume_workspace_dir(volume_dir, workspace)
    if _is_within(own, resolved):
        return resolved
    if in_container:
        raise _refused(label, "in another workspace's area", own)
    home = own.joinpath(*relative.split("/")) if relative else own
    if home.exists() or not resolved.exists():
        # Moved home already, or a new file: created in this workspace's area.
        home.parent.mkdir(parents=True, exist_ok=True)
        return home
    if _owner(relative, _claims(kind, volume_dir)) == workspace and _move(volume_dir, relative, workspace) == "moved":
        return home
    raise _refused(label, "a file with no provable owner in the flat part", own)
