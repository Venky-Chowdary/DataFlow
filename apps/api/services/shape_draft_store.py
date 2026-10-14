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
    if len(data) > MAX_DRAFTS:
        ranked = sorted(data.items(), key=lambda item: str(item[1].get("saved_at") or ""))
        data = dict(ranked[-MAX_DRAFTS:])
    STORE_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def route_key(source: str, dest: str) -> str:
    return f"{source.strip()}→{dest.strip()}"[:240]


def save_shape_draft(
    *,
    route_key: str,
    recipe: dict[str, Any],
    source_columns: list[str] | None = None,
    canvas_layout: dict[str, Any] | None = None,
) -> dict[str, Any]:
    key = route_key.strip()
    if not key:
        raise ShapeError("A draft needs a source and destination.")
    parsed = ShapeRecipe.parse(recipe, source_columns=source_columns or None)
    record = {
        "route_key": key,
        "steps": [step.to_wire() for step in parsed.steps],
        "recipe_hash": parsed.recipe_hash,
        "canvas_layout": canvas_layout if isinstance(canvas_layout, dict) else {},
        "saved_at": _now(),
    }
    data = _load()
    data[key] = record
    _save(data)
    return record


def load_shape_draft(route_key: str) -> dict[str, Any] | None:
    key = route_key.strip()
    found = _load().get(key)
    return found if isinstance(found, dict) else None


def delete_shape_draft(route_key: str) -> None:
    data = _load()
    data.pop(route_key.strip(), None)
    _save(data)
