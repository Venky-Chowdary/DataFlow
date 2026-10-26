"""In-progress transform recipes. A draft is not an approval.

Execute still requires ``approved_shape_recipe_hash``. This store only
remembers the program and a layout the hasher ignores, so a refresh can
restore the canvas.

Drafts are namespaced by workspace: a route key is only meaningful inside the
workspace that saved it, so the same key in another workspace (or unscoped)
addresses a different draft and can neither read nor replace this one.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from services.platform_config import data_dir
from services.shape_models import ShapeError, ShapeRecipe

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


def _record_workspace(record: Any) -> str:
    if not isinstance(record, dict):
        return ""
    return str(record.get("workspace_id") or "").strip()


def _save(data: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    # The cap is per workspace, so one tenant saving drafts cannot evict another's.
    buckets: dict[str, list[tuple[str, Any]]] = {}
    for item in data.items():
        buckets.setdefault(_record_workspace(item[1]), []).append(item)
    kept: dict[str, Any] = {}
    for items in buckets.values():
        ranked = sorted(items, key=lambda item: str((item[1] or {}).get("saved_at") or ""))
        kept.update(ranked[-MAX_DRAFTS:])
    STORE_PATH.write_text(json.dumps(kept, indent=2), encoding="utf-8")


def _store_key(workspace_id: str, key: str) -> str:
    # JSON-encoding the pair keeps it injective: no route key can be crafted to
    # land on another workspace's slot.
    return json.dumps([workspace_id, key], ensure_ascii=False)


def route_key(source: str, dest: str) -> str:
    return f"{source.strip()}→{dest.strip()}"[:240]


def save_shape_draft(
    *,
    route_key: str,
    recipe: dict[str, Any],
    workspace_id: str = "",
    source_columns: list[str] | None = None,
    canvas_layout: dict[str, Any] | None = None,
) -> dict[str, Any]:
    key = route_key.strip()
    ws = (workspace_id or "").strip()
    if not key:
        raise ShapeError("A draft needs a source and destination.")
    parsed = ShapeRecipe.parse(recipe, source_columns=source_columns or None)
    record = {
        "route_key": key,
        "workspace_id": ws,
        "steps": [step.to_wire() for step in parsed.steps],
        "recipe_hash": parsed.recipe_hash,
        "canvas_layout": canvas_layout if isinstance(canvas_layout, dict) else {},
        "saved_at": _now(),
    }
    data = _load()
    if not ws:
        _drop_legacy(data, key)
    data[_store_key(ws, key)] = record
    _save(data)
    return record


def _drop_legacy(data: dict[str, Any], key: str) -> Any:
    """Remove a pre-namespacing entry (bare route key, no workspace) if present."""
    legacy = data.get(key)
    if isinstance(legacy, dict) and "workspace_id" not in legacy:
        return data.pop(key)
    return None


def load_shape_draft(route_key: str, workspace_id: str = "") -> dict[str, Any] | None:
    key = route_key.strip()
    ws = (workspace_id or "").strip()
    data = _load()
    found = data.get(_store_key(ws, key))
    if found is None and not ws:
        legacy = data.get(key)
        if isinstance(legacy, dict) and "workspace_id" not in legacy:
            found = legacy
    if not isinstance(found, dict) or _record_workspace(found) != ws:
        return None
    return found


def delete_shape_draft(route_key: str, workspace_id: str = "") -> None:
    key = route_key.strip()
    ws = (workspace_id or "").strip()
    data = _load()
    data.pop(_store_key(ws, key), None)
    if not ws:
        _drop_legacy(data, key)
    _save(data)
