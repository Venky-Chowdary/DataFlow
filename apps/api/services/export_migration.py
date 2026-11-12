"""Exports written before workspaces had folders: who owns them, and moving them home.

Per-workspace export folders (audit H-2) put every new export in
``<exports>/_workspaces/<workspace id>/``. A deployment that exported before
that holds files in the flat ``<exports>/`` folder (security review M3). A
flat file belongs to a workspace only when the job records say so:

* a server-generated name that carries its job (``export_<job>.<ext>`` named
  by that job's summary, ``quarantine_<job>_<hex>.csv``): the job's workspace;
* any other name (``orders.csv``, ``nightly/orders.csv``): the workspace of
  the jobs whose export summary recorded exactly this path, when every such
  job is in the same workspace.

Anything else (no record, records of two workspaces, a job with no workspace)
has no provable single owner. It stays where it is and stays readable by
platform administrators only, exactly as before.

:func:`migrate_flat_exports` moves every owned flat file into its workspace's
folder. It runs once at API startup and is idempotent: a second run finds
nothing left to move. :func:`adopt_flat_export` does the same for one file at
the moment a recurring append export first looks for it, so the run continues
its file whether or not the startup pass has happened.
"""

from __future__ import annotations

import logging
import os
import shutil
import threading
from pathlib import Path
from typing import Any

from services.export_paths import (
    EXPORT_EXTENSIONS,
    WORKSPACES_DIRNAME,
    _is_within,
    exports_dir,
    job_named_export_owner,
    workspace_exports_dir,
)

logger = logging.getLogger(__name__)

__all__ = [
    "adopt_flat_export",
    "flat_export_owner",
    "migrate_flat_exports",
    "migrate_flat_exports_at_startup",
    "remove_workspace_exports",
    "start_in_background",
]


def _recorded_owners(root: Path, only: str = "") -> dict[str, set[str]]:
    """Flat export (path relative to ``root``) -> workspaces of the jobs that recorded it.

    ``only`` (a path relative to ``root``) asks about one file with an exact,
    indexed query instead of every claim under the root (round 2, Low).
    """
    try:
        from services.mongodb_service import get_mongodb_service

        store = get_mongodb_service()
        if only:
            claims = store.list_export_file_claims(str(root), path=str(root / only))
        else:
            claims = store.list_export_file_claims(str(root))
    except Exception:  # noqa: BLE001 - no job store, no proof of ownership
        logger.warning("Job records unavailable: flat exports keep their place", exc_info=True)
        return {}
    owners: dict[str, set[str]] = {}
    container = root / WORKSPACES_DIRNAME
    for raw_path, workspace_id in claims:
        path = Path(os.path.abspath(raw_path))
        if not _is_within(root, path) or _is_within(container, path):
            continue  # another root, or already a workspace's own folder
        owners.setdefault(path.relative_to(root).as_posix(), set()).add(workspace_id)
    return owners


def _owner(root: Path, relative: str, recorded: dict[str, set[str]]) -> str:
    """The single workspace that provably owns this flat file, or ``""``."""
    if "/" not in relative:
        named = job_named_export_owner(relative)
        if named is not None:
            return named
    claimed = recorded.get(relative, set())
    return next(iter(claimed)) if len(claimed) == 1 else ""


def flat_export_owner(filename: str) -> str:
    """The workspace that provably owns the flat export ``filename``, or ``""``."""
    root = exports_dir()
    if "/" not in filename and (named := job_named_export_owner(filename)) is not None:
        return named
    return _owner(root, filename, _recorded_owners(root, only=filename))


def _flat_files(root: Path) -> list[Path]:
    """Export files in the flat folder, sub-folders included, workspace folders excluded."""
    found: list[Path] = []
    for folder, dirs, files in os.walk(root, followlinks=False):
        here = Path(folder)
        if here == root:
            dirs[:] = [d for d in dirs if d != WORKSPACES_DIRNAME]
        for name in files:
            path = here / name
            if path.is_symlink() or path.suffix.lower().lstrip(".") not in EXPORT_EXTENSIONS:
                continue
            found.append(path)
    return sorted(found)


def _link_or_exclusive_copy(source: Path, target: Path) -> None:
    """Create ``target`` from ``source`` only if ``target`` does not exist (atomically)."""
    try:
        os.link(source, target, follow_symlinks=False)
        return
    except (FileExistsError, FileNotFoundError):
        raise
    except OSError:
        pass  # a volume without hard links: an O_EXCL copy instead
    with open(source, "rb") as src:
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        with os.fdopen(fd, "wb") as dst:
            shutil.copyfileobj(src, dst)


def _move(root: Path, relative: str, workspace_id: str) -> str:
    """Move one flat file into its workspace's folder: ``moved`` / ``kept_existing`` / ``gone``.

    ``os.link`` then ``unlink``: the link fails when the target exists, so a
    file that appears after the check (an older pod still writing during a
    rolling upgrade) is never overwritten; both files are kept (round 2, Low).
    """
    source = root / relative
    target = workspace_exports_dir(workspace_id).joinpath(*relative.split("/"))
    if target.exists():
        # The workspace already has a file of this name: never overwritten.
        return "kept_existing"
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        _link_or_exclusive_copy(source, target)
    except FileExistsError:
        return "kept_existing"
    except FileNotFoundError:
        return "gone"  # another replica moved it first
    try:
        os.unlink(source)
    except FileNotFoundError:
        pass  # another replica linked and removed it as well: one copy remains
    logger.info("Moved flat export %s into the folder of workspace %s", relative, workspace_id)
    return "moved"


def migrate_flat_exports() -> dict[str, Any]:
    """Move every flat export with a provable owner into that workspace's folder.

    Returns ``{"moved", "left", "kept_existing"}`` counts. Files with no
    provable single owner are left exactly where they are. Idempotent.
    """
    root = exports_dir()
    summary = {"moved": 0, "left": 0, "kept_existing": 0}
    files = _flat_files(root)
    if not files:
        return summary
    recorded = _recorded_owners(root)
    for path in files:
        relative = path.relative_to(root).as_posix()
        owner = _owner(root, relative, recorded)
        outcome = _move(root, relative, owner) if owner else "left"
        if outcome == "kept_existing":
            summary["left"] += 1
        if outcome in summary:
            summary[outcome] += 1
    return summary


def migrate_flat_exports_at_startup() -> dict[str, Any]:
    """The startup pass: never raises (an export volume problem must not fail the boot)."""
    try:
        summary = migrate_flat_exports()
    except Exception:  # noqa: BLE001 - reported, not fatal
        logger.warning("Flat export migration did not run", exc_info=True)
        return {}
    if summary["moved"] or summary["left"]:
        logger.info(
            "Flat export migration: %(moved)d moved into workspace folders, %(left)d left in place "
            "(no provable single owner; platform administrators only)", summary,
        )
    return summary


def start_in_background(task: Any) -> threading.Thread:
    """Run a startup pass off the API lifespan's critical path (round 2, Low).

    The thread handles its own errors: a pass that raises is logged, never
    re-raised into the process.
    """
    def _run() -> None:
        try:
            task()
        except Exception:  # noqa: BLE001 - a startup pass must not take the API down
            logger.warning("Background startup pass %s failed", getattr(task, "__name__", task), exc_info=True)

    thread = threading.Thread(target=_run, name=f"startup-{getattr(task, '__name__', 'pass')}", daemon=True)
    thread.start()
    return thread


def adopt_flat_export(target: Path, workspace_id: str) -> str:
    """Before an append to ``target`` (in a workspace folder) that does not exist yet.

    When a flat file of the same name exists from before workspace folders and
    this workspace provably owns it, it is moved to ``target`` so the append
    continues it. Returns ``""`` then (and when there is no such flat file),
    otherwise a note for the run's summary saying a flat file of this name was
    left alone and why.
    """
    workspace = str(workspace_id or "").strip()
    root = exports_dir()
    base = workspace_exports_dir(workspace, create=False)
    if not workspace or target.exists() or not _is_within(base, target):
        return ""
    relative = target.relative_to(base).as_posix()
    flat = root / relative
    if flat.is_symlink() or not flat.is_file():
        return ""
    if _owner(root, relative, _recorded_owners(root)) == workspace and _move(root, relative, workspace) != "kept_existing":
        return ""
    return (
        f"An export named {relative} from before workspace folders exists and this workspace "
        "cannot be shown to own it, so this run started a new file instead of appending to it. "
        "A platform administrator can move the earlier file into this workspace's export folder."
    )


def remove_workspace_exports(workspace_id: str, *, dry_run: bool = False) -> int:
    """Delete a workspace's export folder (workspace teardown); returns the files it held."""
    folder = workspace_exports_dir(workspace_id, create=False)
    container = exports_dir() / WORKSPACES_DIRNAME
    if not str(workspace_id or "").strip() or folder.is_symlink() or not folder.is_dir():
        return 0
    if folder.parent.resolve() != container.resolve():
        return 0
    count = sum(1 for path in folder.rglob("*") if path.is_file() or path.is_symlink())
    if not dry_run:
        shutil.rmtree(folder, ignore_errors=True)
    return count
