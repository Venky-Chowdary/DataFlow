"""Persisted workspace preferences — org profile, retention, timezone."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir

STORE_PATH = data_dir() / "workspace.json"

_DEFAULTS: dict[str, Any] = {
    "org_name": "DataFlow",
    "timezone": "UTC",
    "retention_days": 90,
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_raw() -> dict[str, Any]:
    if not STORE_PATH.exists():
        return dict(_DEFAULTS)
    try:
        raw = json.loads(STORE_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return dict(_DEFAULTS)
        merged = dict(_DEFAULTS)
        merged.update({k: v for k, v in raw.items() if k in _DEFAULTS or k.startswith("_")})
        return merged
    except Exception:
        return dict(_DEFAULTS)


def _save(data: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STORE_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def get_workspace_settings() -> dict[str, Any]:
    raw = _load_raw()
    return {
        "org_name": str(raw.get("org_name") or _DEFAULTS["org_name"]),
        "timezone": str(raw.get("timezone") or _DEFAULTS["timezone"]),
        "retention_days": int(raw.get("retention_days") or _DEFAULTS["retention_days"]),
        "updated_at": raw.get("updated_at"),
        "updated_by": raw.get("updated_by"),
    }


def update_workspace_settings(
    *,
    org_name: str | None = None,
    timezone: str | None = None,
    retention_days: int | None = None,
    actor: str | None = None,
) -> dict[str, Any]:
    data = _load_raw()
    if org_name is not None:
        data["org_name"] = org_name.strip()[:128] or _DEFAULTS["org_name"]
    if timezone is not None:
        data["timezone"] = timezone.strip()[:64] or _DEFAULTS["timezone"]
    if retention_days is not None:
        data["retention_days"] = max(1, min(3650, int(retention_days)))
    data["updated_at"] = _now()
    if actor:
        data["updated_by"] = actor
    _save(data)
    return get_workspace_settings()
