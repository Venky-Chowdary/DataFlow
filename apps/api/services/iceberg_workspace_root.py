"""Per-workspace areas of the Iceberg local root (follow-up review 4, H).

``iceberg_catalog.iceberg_local_root`` (``DATAFLOW_ICEBERG_ROOT``, else the
data folder's ``iceberg`` area under confinement) used to be one folder for
every workspace: the jail kept a path under it and nothing more, so workspace
B's local SQLite catalog could name workspace A's catalog file, warehouse or
metadata file there and read A's tables.

The root is now partitioned exactly as the SQLite / DuckDB volumes are
(``services.embedded_volume``): workspace W's catalog files, warehouses and
table files live in ``<root>/_workspaces/<W>/``, and the binding is the same
one (the running job's workspace, else the stored connector's, else the
request's verified workspace). For a workspace-bound caller:

* a relative name is anchored in the own area;
* a path in the own area opens;
* a path in another workspace's area (or the container itself) is refused;
* a path in the flat part of the root (a catalog or warehouse from before the
  partition) opens only when the operator assigned that top-level entry to
  this workspace (:func:`assign_flat_entry`, ``scripts/iceberg_assign_flat_root.py``);
  otherwise it is refused with that remedy. A relative name that is new in the
  own area but exists in the flat part is refused the same way, so a legacy
  table is never silently replaced by an empty one. The files are not moved:
  Iceberg metadata records absolute file locations, which a move would break.

A caller with no workspace bound (system code, a platform administrator's
all-workspace view, a deployment without workspaces) keeps the whole root, as
does the single local operator (auth off), where nothing changes. Messages
name no path and no workspace.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.export_paths import WORKSPACES_DIRNAME, _workspace_segment
from services.local_file_policy import is_within

#: The operator's assignments of flat-root entries, inside the container no workspace opens.
OWNERS_FILE = ".flat_owners.json"
ASSIGN_COMMAND = "python scripts/iceberg_assign_flat_root.py --assign <name> --workspace <workspace id>"


@dataclass(frozen=True)
class Scope:
    """Whose part of the root a caller opens: ``workspace == ""`` is the whole root."""

    workspace: str = ""


def scope_for(workspace_id: Any) -> Scope:
    from services.embedded_volume import _single_operator

    return Scope() if _single_operator() else Scope(str(workspace_id or "").strip())


def current_scope() -> Scope:
    """The caller's scope now (capture it where work moves to another thread)."""
    from services.embedded_volume import _bound_workspace

    return scope_for(_bound_workspace())


def workspace_area(root: Path, workspace_id: str) -> Path:
    return root / WORKSPACES_DIRNAME / _workspace_segment(workspace_id)


def _outside(label: str) -> ValueError:
    return ValueError(  # no host path in the message (security follow-up 1)
        f"Iceberg {label} must be under the Iceberg warehouse folder (DATAFLOW_ICEBERG_ROOT, "
        "else the data folder's iceberg area)"
    )


def _other_area(label: str) -> ValueError:
    return ValueError(
        f"Iceberg {label} is in another workspace's area of the Iceberg warehouse folder, so this "
        "workspace's connector cannot open it. Use a relative name (it is placed in this "
        "workspace's own area) or a path inside this workspace's area."
    )


def _unassigned(label: str) -> ValueError:
    return ValueError(
        f"Iceberg {label} is in the shared part of the Iceberg warehouse folder, from before the "
        "folder was divided per workspace, and is not assigned to this workspace; a workspace's "
        "connector opens only its own area. Use another name (new catalogs and tables are created "
        "in this workspace's area), or ask the operator to assign the existing folder or catalog "
        f"file to this workspace ({ASSIGN_COMMAND}; see the release note)."
    )


def _owners_path(root: Path) -> Path:
    return root / WORKSPACES_DIRNAME / OWNERS_FILE


def flat_owners(root: Path) -> dict[str, str]:
    """``{flat top-level entry: workspace id}`` as the operator assigned them."""
    try:
        data = json.loads(_owners_path(root).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(data, Mapping):
        return {}
    return {str(k): str(v) for k, v in data.items() if isinstance(k, str) and isinstance(v, str)}


def _entry(root: Path, resolved: Path) -> str:
    try:
        parts = resolved.relative_to(root).parts
    except ValueError:
        return ""
    return parts[0] if parts else ""


def _assigned(root: Path, resolved: Path, workspace: str) -> bool:
    entry = _entry(root, resolved)
    return bool(entry) and entry != WORKSPACES_DIRNAME and flat_owners(root).get(entry) == workspace


def _judge(resolved: Path, root: Path, workspace: str, label: str) -> Path:
    if not is_within(resolved, root):
        raise _outside(label)
    if is_within(resolved, workspace_area(root, workspace)):
        return resolved
    if is_within(resolved, root / WORKSPACES_DIRNAME):
        raise _other_area(label)
    if _assigned(root, resolved, workspace):
        return resolved
    raise _unassigned(label)


def place(candidate: Path, root: Path, scope: Scope, label: str) -> Path:
    """The resolved path ``candidate`` names for ``scope``, or a ``ValueError``.

    ``root`` is resolved. A relative ``candidate`` is anchored in the scope's
    area. Creates nothing.
    """
    if not scope.workspace:
        resolved = (candidate if candidate.is_absolute() else root / candidate).resolve()
        if not is_within(resolved, root):
            raise _outside(label)
        return resolved
    if candidate.is_absolute():
        return _judge(candidate.resolve(), root, scope.workspace, label)
    own = workspace_area(root, scope.workspace)
    resolved = (own / candidate).resolve()
    if not is_within(resolved, own):  # climbs out of the own area: judged where it lands
        return _judge(resolved, root, scope.workspace, label)
    flat = (root / candidate).resolve()
    if not resolved.exists() and flat != root and flat.exists() and is_within(flat, root):
        # The same name in the flat part: a legacy catalog or table, never shadowed silently.
        return _judge(flat, root, scope.workspace, label)
    return resolved


def ensure_area(root: Path, scope: Scope) -> None:
    """Create the scope's own area (a SQLite catalog file needs its folder)."""
    if scope.workspace:
        workspace_area(root, scope.workspace).mkdir(parents=True, exist_ok=True)


# --- the operator's assignment of flat entries ---------------------------------------------------


def flat_entries(root: Path) -> list[str]:
    """Top-level entries of the flat part of the root (the container excluded)."""
    try:
        names = os.listdir(root)
    except OSError:
        return []
    return sorted(name for name in names if name != WORKSPACES_DIRNAME)


def _checked_entry(root: Path, name: str) -> str:
    entry = str(name or "").strip()
    if not entry or entry in {".", ".."} or "\x00" in entry or "/" in entry or "\\" in entry:
        raise ValueError("an entry is one top-level name in the flat part of the Iceberg folder")
    if entry == WORKSPACES_DIRNAME:
        raise ValueError(f"{WORKSPACES_DIRNAME} holds the workspace areas and cannot be assigned")
    return entry


def _write_owners(root: Path, owners: Mapping[str, str]) -> None:
    path = _owners_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{OWNERS_FILE}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(dict(sorted(owners.items())), indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def assign_flat_entry(name: str, workspace_id: str, root: Path | None = None) -> None:
    """Let one workspace open an existing flat-root entry (and everything under it)."""
    root = _root(root)
    entry = _checked_entry(root, name)
    workspace = str(workspace_id or "").strip()
    if not workspace:
        raise ValueError("a workspace id is required")
    if not (root / entry).exists() or (root / entry).is_symlink():
        raise ValueError("no such entry in the flat part of the Iceberg folder (symbolic links are not assigned)")
    owners = flat_owners(root)
    if owners.get(entry, workspace) != workspace:
        raise ValueError("the entry is already assigned to another workspace; revoke it first")
    owners[entry] = workspace
    _write_owners(root, owners)


def revoke_flat_entry(name: str, root: Path | None = None) -> bool:
    root = _root(root)
    entry = _checked_entry(root, name)
    owners = flat_owners(root)
    if owners.pop(entry, None) is None:
        return False
    _write_owners(root, owners)
    return True


def _root(root: Path | None) -> Path:
    if root is not None:
        return root.resolve()
    from connectors.iceberg_catalog import iceberg_local_root

    found = iceberg_local_root()
    if found is None:
        raise ValueError("no Iceberg root is configured (DATAFLOW_ICEBERG_ROOT, or confinement in production)")
    return found


# --- save ---------------------------------------------------------------------------------------


def _local_paths(data: Mapping[str, Any]) -> list[tuple[str, str]]:
    """``(label, raw path)`` for each local file a local Iceberg catalog would open."""
    from connectors.iceberg_catalog import _infer_catalog_type, is_remote_location

    raw_extra = data.get("extra")
    extra = dict(raw_extra) if isinstance(raw_extra, Mapping) else {}
    for key in ("catalog_type", "catalog", "catalog_path"):
        if key not in extra and data.get(key):
            extra[key] = data.get(key)
    cs = str(data.get("connection_string") or "").strip()
    warehouse = str(data.get("warehouse") or data.get("database") or "").strip()
    catalog = _infer_catalog_type(cs, str(data.get("region") or ""), warehouse, extra)
    if catalog in {"filesystem", "hadoop"}:
        return [("warehouse", cs or warehouse or ".")]
    if catalog not in {"sql", "sqlite"}:
        return []
    found: list[tuple[str, str]] = []
    if warehouse and not is_remote_location(warehouse):
        found.append(("warehouse", warehouse))
    if cs.lower().startswith("sqlite:///"):
        rest = cs[len("sqlite:///"):].partition("?")[0]
        if rest and rest != ":memory:":
            found.append(("catalog_path", rest))
        return found
    if cs and "://" in cs:
        return found  # a catalog database elsewhere (its local files are refused by the FileIO)
    if not warehouse and cs:
        found.append(("warehouse", cs))
    catalog_path = str(extra.get("catalog_path") or "")
    if catalog_path and Path(catalog_path).is_absolute():
        found.append(("catalog_path", catalog_path))
    return found


def connector_refusal(data: Any) -> str:
    """At save: a local Iceberg path outside the saving workspace's area, else ``""``."""
    if not isinstance(data, Mapping):
        return ""
    from src.transfer.connector_capabilities import resolve_driver_type

    if resolve_driver_type(str(data.get("type") or data.get("format") or "").strip().lower()) != "iceberg":
        return ""
    scope = scope_for(data.get("workspace_id"))
    if not scope.workspace:
        return ""
    from connectors.iceberg_catalog import _jail_local_path, iceberg_local_root

    if iceberg_local_root() is None:
        return ""
    for label, raw in _local_paths(data):
        try:
            _jail_local_path(raw, label, scope=scope)
        except ValueError as exc:
            return str(exc)
    return ""
