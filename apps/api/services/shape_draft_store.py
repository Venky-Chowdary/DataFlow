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
from services.shared_doc_store import tracked
from services.shared_record_store import SharedRecordSet
from services.shape_models import ShapeError, ShapeRecipe
from services.workspace_filter import WorkspaceFilter, bound_allows, current_workspace_stamp

STORE_PATH = data_dir() / "shape_drafts.json"
MAX_DRAFTS = 50
#: The canvas layout is client-supplied: node positions, not data. Capped so
#: one workspace cannot fill the shared store (ops review High 3).
MAX_CANVAS_LAYOUT_BYTES = 256 * 1024
MAX_DRAFT_BYTES = 512 * 1024


class DraftTooLarge(ShapeError):
    """A draft (or its canvas layout) is larger than the store accepts: HTTP 413."""


def _json_size(value: Any) -> int:
    return len(json.dumps(value, default=str).encode("utf-8"))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load(**scope: Any) -> dict[str, Any]:
    """Drafts from the shared control-plane store when one is configured (H-5).

    ``scope`` narrows the database read; a file store returns every draft.
    """
    return _DOC.load(**scope)


def _save(data: dict[str, Any], *, rev: int | None = None) -> None:
    read_at = rev if rev is not None else getattr(data, "rev", None)
    _DOC.save(tracked(_capped(data), read_at))


def _file_load() -> dict[str, Any]:
    if not STORE_PATH.exists():
        return {}
    try:
        raw = json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _capped(data: dict[str, Any]) -> dict[str, Any]:
    # The cap is per workspace: one tenant saving drafts must not evict another's.
    buckets: dict[str, list[tuple[str, Any]]] = {}
    for item in data.items():
        owner = str(item[1].get("workspace_id") or "").strip() if isinstance(item[1], dict) else ""
        buckets.setdefault(owner, []).append(item)
    kept: dict[str, Any] = {}
    for items in buckets.values():
        ranked = sorted(items, key=lambda it: str(it[1].get("saved_at") or "") if isinstance(it[1], dict) else "")
        kept.update(ranked[-MAX_DRAFTS:])
    return kept


def _file_save(data: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STORE_PATH.write_text(json.dumps(dict(data), indent=2), encoding="utf-8")


_DOC = SharedRecordSet(
    "shape_drafts",
    file_load=lambda: _file_load(),
    file_save=lambda data: _file_save(data),
    file_path=lambda: STORE_PATH,
    partition_key="workspace_id",
)


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


@_DOC.retrying
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
    layout = canvas_layout if isinstance(canvas_layout, dict) else {}
    layout_bytes = _json_size(layout)
    if layout_bytes > MAX_CANVAS_LAYOUT_BYTES:
        raise DraftTooLarge(
            f"The canvas layout is {layout_bytes // 1024} KB; a draft's canvas layout may be at most "
            f"{MAX_CANVAS_LAYOUT_BYTES // 1024} KB. Remove unused nodes or annotations and save again."
        )
    record = {
        "route_key": key,
        "workspace_id": workspace_id,
        "steps": [step.to_wire() for step in parsed.steps],
        "recipe_hash": parsed.recipe_hash,
        "canvas_layout": layout,
        "saved_at": _now(),
    }
    draft_bytes = _json_size(record)
    if draft_bytes > MAX_DRAFT_BYTES:
        raise DraftTooLarge(
            f"The draft is {draft_bytes // 1024} KB; a draft may be at most {MAX_DRAFT_BYTES // 1024} KB."
        )
    data = _load(keys=[_storage_key(key, workspace_id)])
    data[_storage_key(key, workspace_id)] = record
    _save(data)
    # The cap is per workspace (in file mode ``_capped`` already applied it to the whole file).
    _DOC.trim(workspace_id, keep=MAX_DRAFTS)
    return record


def load_shape_draft(route_key: str, *, workspace_id: str | None = None) -> dict[str, Any] | None:
    """This workspace's draft for the route, else a legacy unscoped one the
    caller's scope still covers."""
    key = route_key.strip()
    candidates = (_storage_key(key, _owner(workspace_id)), key)
    data = _load(keys=candidates)
    for candidate in candidates:
        found = data.get(candidate)
        if isinstance(found, dict) and bound_allows(found.get("workspace_id")):
            return found
    return None


@_DOC.retrying
def delete_shape_draft(route_key: str, *, workspace_id: str | None = None) -> None:
    key = route_key.strip()
    stored = _storage_key(key, _owner(workspace_id))
    data = _load(keys=[stored])
    data.pop(stored, None)
    _save(data)


@_DOC.retrying
def assign_unscoped_workspaces(workspace_for, *, apply: bool) -> dict[str, int]:
    """Move legacy unscoped drafts into ``workspace_for(draft)``'s workspace (D154 backfill)."""
    unscoped = WorkspaceFilter.only("")
    counts = {"assigned": 0, "skipped": 0}
    data = _load(part="")
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
        _save(moved, rev=getattr(data, "rev", None))
    return counts
