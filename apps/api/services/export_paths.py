"""Single owner of where a file export may land on the API host.

Every export a client can name (``/query/export``, file-export destinations in
Transfer Studio) is confined to one folder: ``apps/api/exports``, or the folder an
administrator names in ``DATAFLOW_EXPORTS_DIR`` (``DATAWRAP_EXPORTS_DIR``). A
client-supplied ``output_path`` is a name under that folder, never a path into
the application tree: it is resolved (``..`` and symlinks followed), compared
structurally with ``os.path.commonpath`` — not a string prefix, which lets
``exports_evil/`` pass — and must carry an export-format extension, so it can
never overwrite a module, config file or store.

**One folder per workspace (audit H-2).** An export written for a workspace
lands in ``<exports>/_workspaces/<workspace id>/``, and a client-supplied name
is confined to that folder, so two workspaces naming ``orders.csv`` hold two
files and neither can name the other's. The flat ``<exports>/`` folder is the
unscoped one: what the single local operator (authentication off) writes, and
whatever a deployment exported before workspaces had folders.
:func:`downloadable_export` is the read side: the caller's own workspace
folders, then a flat file only when the job records prove its owner (or the
caller's tenant scope covers unscoped rows). ``services.export_migration``
moves the provably owned flat files into their workspace folders at startup.
"""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from services.brand_env import getenv_brand

EXPORTS_DIRNAME = "exports"

#: Sub-folder of the exports folder that holds one folder per workspace. No
#: unscoped export may be named into it.
WORKSPACES_DIRNAME = "_workspaces"

_SAFE_WORKSPACE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
#: Server-generated export names that carry the id of the job that wrote them.
_JOB_EXPORT_NAME = re.compile(r"export_(?P<job>[A-Za-z0-9._-]+)\.[A-Za-z0-9]+")
_JOB_QUARANTINE_NAME = re.compile(r"quarantine_(?P<job>[A-Za-z0-9._-]+)_[0-9a-f]{8}\.csv")

#: Extensions an export writer produces. Anything else (``.py``, ``.cfg``,
#: ``.sqlite``, ``.pth`` …) is refused even inside the exports folder.
EXPORT_EXTENSIONS: frozenset[str] = frozenset(
    {
        "csv",
        "tsv",
        "json",
        "jsonl",
        "ndjson",
        "yaml",
        "yml",
        "fwf",
        "txt",
        "xlsx",
        "parquet",
        "avro",
        "orc",
        "xml",
        # A SQLite export is a database file written into the exports folder
        # (``file_export``/``sqlite`` routes to ``database``/``sqlite``, round 11).
        "sqlite",
        "sqlite3",
        "db",
    }
)


class ExportPathError(ValueError):
    """A client-supplied export path that would leave the exports folder."""


def api_root() -> Path:
    return Path(__file__).resolve().parents[1]


def exports_dir(*, create: bool = True) -> Path:
    """The folder exports are confined to: ``DATAFLOW_EXPORTS_DIR``, else ``apps/api/exports``.

    ``create=False`` only names it (a path check must not create the folder).
    """
    configured = (getenv_brand("EXPORTS_DIR") or "").strip()
    path = Path(configured).expanduser() if configured else api_root() / EXPORTS_DIRNAME
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path.resolve()


def _workspace_segment(workspace_id: str) -> str:
    """A folder name for a workspace id (ids are uuid4; anything else is hashed)."""
    ws = str(workspace_id or "").strip()
    if _SAFE_WORKSPACE.fullmatch(ws) and ".." not in ws:
        return ws
    return "ws-" + hashlib.sha256(ws.encode("utf-8")).hexdigest()[:32]


def workspace_exports_dir(workspace_id: str | None, *, create: bool = True) -> Path:
    """The folder one workspace's exports are confined to (``""`` = the flat, unscoped folder)."""
    base = exports_dir()
    ws = str(workspace_id or "").strip()
    if not ws:
        return base
    path = base / WORKSPACES_DIRNAME / _workspace_segment(ws)
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def current_export_workspace() -> str:
    """The workspace an export written right now belongs to.

    The running job's (bound for the whole tracked job, whichever thread or
    worker runs it), else the request's write workspace; ``""`` when neither
    is bound (the single local operator, scripts).
    """
    from services.export_object_store import bound_job_workspace
    from services.workspace_filter import current_workspace_stamp

    return bound_job_workspace() or current_workspace_stamp(for_write=False)


def _is_within(base: Path, candidate: Path) -> bool:
    b = os.path.normcase(str(base))
    c = os.path.normcase(str(candidate))
    try:
        return os.path.commonpath([b, c]) == b
    except ValueError:  # different drives on Windows
        return False


def _relative_parts(raw: str) -> tuple[str, ...]:
    """Path components of ``raw``, splitting on both ``/`` and ``\\``."""
    return tuple(p for p in PureWindowsPath(raw).parts if p not in ("", "."))


def resolve_export_path(
    output_path: str, *, default_ext: str, workspace_id: str | None = None
) -> Path:
    """Absolute, confined path for a client-named export file.

    ``output_path`` may be a bare file name (``orders.csv``), a name under a
    sub-folder (``nightly/orders.csv``, ``exports/nightly/orders.csv``) or an
    absolute path that already lies inside the exports folder. Every one of
    them resolves inside the folder of ``workspace_id`` (default: the workspace
    of the running job or request, :func:`current_export_workspace`); a name
    that would land in another workspace's folder is refused. A name without
    an extension gets ``default_ext``. Raises :class:`ExportPathError` for
    anything else.
    """
    raw = str(output_path or "").strip()
    if not raw:
        raise ExportPathError("Export file name is empty")
    if "\x00" in raw:
        raise ExportPathError("Export path contains a NUL byte")

    root = exports_dir()
    workspace = current_export_workspace() if workspace_id is None else str(workspace_id or "").strip()
    base = workspace_exports_dir(workspace)
    if os.path.isabs(raw) or PureWindowsPath(raw).is_absolute() or PurePosixPath(raw).is_absolute():
        candidate = Path(raw)
        absolute = Path(os.path.abspath(raw))
        if workspace and _is_within(root, absolute) and not _is_within(root / WORKSPACES_DIRNAME, absolute):
            # A path saved before workspaces had folders: the same name, in
            # this workspace's folder.
            candidate = base.joinpath(*absolute.relative_to(root).parts)
    else:
        parts = _relative_parts(raw)
        if parts and parts[0] == EXPORTS_DIRNAME:
            parts = parts[1:]
        if not parts:
            raise ExportPathError("Export path must name a file inside the exports folder")
        candidate = base.joinpath(*parts)

    if not candidate.suffix:
        ext = str(default_ext or "").strip().lower().lstrip(".")
        candidate = candidate.with_name(f"{candidate.name}.{ext}")

    resolved = candidate.resolve()
    if resolved == base or not _is_within(base, resolved):
        raise ExportPathError(
            "Export path must stay inside the server exports folder "
            f"({EXPORTS_DIRNAME}/); use a file name such as 'orders.csv'"
        )
    if not workspace and _is_within(root / WORKSPACES_DIRNAME, resolved):
        raise ExportPathError(
            f"'{WORKSPACES_DIRNAME}' is reserved for workspace exports; use another folder name"
        )
    ext = resolved.suffix.lower().lstrip(".")
    if ext not in EXPORT_EXTENSIONS:
        raise ExportPathError(
            f"Export file extension '.{ext}' is not an export format; "
            f"allowed: {', '.join(sorted(EXPORT_EXTENSIONS))}"
        )
    if resolved.is_dir():
        raise ExportPathError("Export path names a folder, not a file")
    return resolved


def generated_export_path(name: str, *, workspace_id: str | None = None) -> Path:
    """Path for a server-generated export name (still confinement-checked)."""
    if os.path.basename(name) != name or name in ("", ".", ".."):
        raise ExportPathError("Generated export name must be a bare file name")
    return resolve_export_path(name, default_ext="", workspace_id=workspace_id)


# ── download (read side) ─────────────────────────────────────────────────────


def _file_in(folder: Path, filename: str) -> Path | None:
    """``folder/filename`` when it is a real file that resolves inside ``folder``."""
    try:
        resolved = (folder / filename).resolve()
    except (OSError, RuntimeError):
        return None
    if resolved == folder or not _is_within(folder.resolve(), resolved) or not resolved.is_file():
        return None
    return resolved


def _flat_file_owner(filename: str) -> str | None:
    """The workspace a flat (pre-workspace-folder) export provably belongs to.

    A server-generated name carries its job (:func:`job_named_export_owner`);
    any other name belongs to the workspace whose job records name exactly
    this file (``services.export_migration``, security review M3). ``None``
    means the owner cannot be proven.
    """
    owner = job_named_export_owner(filename)
    if owner is not None:
        return owner
    from services.export_migration import flat_export_owner

    return flat_export_owner(filename) or None


def job_named_export_owner(filename: str) -> str | None:
    """The workspace of the job a server-generated flat export name carries.

    ``quarantine_<job>_<hex>.csv``, and ``export_<job>.<ext>`` when that job's
    stored summary names this very file. ``None`` for any other name, and when
    the job is unknown.
    """
    match = _JOB_QUARANTINE_NAME.fullmatch(filename) or _JOB_EXPORT_NAME.fullmatch(filename)
    if match is None:
        return None
    try:
        from services.mongodb_service import get_mongodb_service

        job = get_mongodb_service().get_job(match.group("job"))
    except Exception:  # noqa: BLE001 - no store, no proof of ownership
        return None
    if not job:
        return None
    if filename.startswith("export_"):
        summary = job.get("destination_summary")
        if not isinstance(summary, dict) or str(summary.get("filename") or "") != filename:
            return None
    return str(job.get("workspace_id") or "").strip()


def downloadable_export(filename: str, scope: Any) -> Path | None:
    """The export file ``filename`` names for a caller with tenant ``scope``.

    ``scope`` is the request's :class:`services.tenant_scope.TenantScope`.
    Looked up, in order: the workspace the request acts in, the caller's other
    workspaces, then the flat folder — where a file is served only to its
    job's workspace, or, when no job proves an owner, only to a scope that
    covers unscoped rows (a platform administrator while isolation is
    enforced). ``None`` for anything else, including a name that is not a
    bare file name; the caller answers every ``None`` with the same 404.
    """
    name = str(filename or "")
    if (
        not name
        or name in (".", "..")
        or "\x00" in name
        or os.path.basename(name) != name
        or PureWindowsPath(name).name != name
    ):
        return None
    root = exports_dir()
    container = root / WORKSPACES_DIRNAME
    visible = scope.filter
    if visible.workspace_ids is None:
        # Every workspace: the one the request names first, then each folder.
        named = [scope.workspace_id] if scope.workspace_id else []
        folders = [workspace_exports_dir(ws, create=False) for ws in named]
        if not named and container.is_dir():
            folders += sorted(p for p in container.iterdir() if p.is_dir())
    else:
        ordered = sorted(visible.workspace_ids, key=lambda ws: (ws != scope.workspace_id, ws))
        folders = [workspace_exports_dir(ws, create=False) for ws in ordered if ws]
    for folder in folders:
        found = _file_in(folder, name)
        if found is not None:
            return found

    flat = _file_in(root, name)
    if flat is None:
        return None
    owner = _flat_file_owner(name)
    return flat if scope.allows(owner if owner is not None else "") else None
