"""In-progress transform recipes. A draft is not an approval.

Execute still requires ``approved_shape_recipe_hash``. This store only
remembers the program and a layout the hasher ignores, so a refresh can
restore the canvas.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from services.platform_config import data_dir
from services.shape_models import ShapeError, ShapeRecipe
from services.workspace_filter import WorkspaceFilter, bound_allows, current_workspace_stamp

STORE_PATH = data_dir() / "shape_drafts.json"
MAX_DRAFTS = 50


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load() -> dict[str, Any]:
    if not STORE_PATH.exists():
        return {}
    try:
        raw = json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _save(data: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    # The cap is per workspace: one tenant saving drafts must not evict another's.
    buckets: dict[str, list[tuple[str, Any]]] = {}
    for item in data.items():
        owner = str(item[1].get("workspace_id") or "").strip() if isinstance(item[1], dict) else ""
        buckets.setdefault(owner, []).append(item)
    kept: dict[str, Any] = {}
    for items in buckets.values():
        ranked = sorted(items, key=lambda it: str(it[1].get("saved_at") or "") if isinstance(it[1], dict) else "")
        kept.update(ranked[-MAX_DRAFTS:])
    STORE_PATH.write_text(json.dumps(kept, indent=2), encoding="utf-8")


def route_key(source: str, dest: str) -> str:
    return f"{source.strip()}→{dest.strip()}"[:240]


def _storage_key(key: str, workspace_id: str) -> str:
    """Drafts are per workspace: two tenants editing the same route name must
    neither read nor overwrite each other's canvas (D154). Unscoped legacy
    drafts keep the bare key."""
    return f"{workspace_id}::{key}" if workspace_id else key


def _owner(workspace_id: str | None) -> str:
    """The router resolves the request's workspace; direct callers inherit the
    ambient tenant scope."""
    explicit = str(workspace_id or "").strip()
    return explicit or current_workspace_stamp()


def save_shape_draft(
    *,
    route_key: str,
    recipe: dict[str, Any],
    source_columns: list[str] | None = None,
    canvas_layout: dict[str, Any] | None = None,
    workspace_id: str | None = None,
) -> dict[str, Any]:
    key = route_key.strip()
    if not key:
        raise ShapeError("A draft needs a source and destination.")
    parsed = ShapeRecipe.parse(recipe, source_columns=source_columns or None)
    workspace_id = _owner(workspace_id)
    record = {
        "route_key": key,
        "workspace_id": workspace_id,
        "steps": [step.to_wire() for step in parsed.steps],
        "recipe_hash": parsed.recipe_hash,
        "canvas_layout": canvas_layout if isinstance(canvas_layout, dict) else {},
        "saved_at": _now(),
    }
    data = _load()
    data[_storage_key(key, workspace_id)] = record
    _save(data)
    return record


def load_shape_draft(route_key: str, *, workspace_id: str | None = None) -> dict[str, Any] | None:
    """This workspace's draft for the route, else a legacy unscoped one the
    caller's scope still covers."""
    key = route_key.strip()
    data = _load()
    for candidate in (_storage_key(key, _owner(workspace_id)), key):
        found = data.get(candidate)
        if isinstance(found, dict) and bound_allows(found.get("workspace_id")):
            return found
    return None


def delete_shape_draft(route_key: str, *, workspace_id: str | None = None) -> None:
    key = route_key.strip()
    data = _load()
    data.pop(_storage_key(key, _owner(workspace_id)), None)
    _save(data)


def assign_unscoped_workspaces(workspace_for, *, apply: bool) -> dict[str, int]:
    """Move legacy unscoped drafts into ``workspace_for(draft)``'s workspace (D154 backfill)."""
    unscoped = WorkspaceFilter.only("")
    counts = {"assigned": 0, "skipped": 0}
    data = _load()
    moved: dict[str, Any] = {}
    for storage_key, record in data.items():
        if not isinstance(record, dict) or not unscoped.matches(record.get("workspace_id")):
            moved[storage_key] = record
            continue
        ws = (workspace_for(record) or "").strip()
        counts["assigned" if ws else "skipped"] += 1
        if not ws:
            moved[storage_key] = record
            continue
        record["workspace_id"] = ws
        moved[_storage_key(str(record.get("route_key") or storage_key), ws)] = record
    if apply and counts["assigned"]:
        _save(moved)
    return counts
