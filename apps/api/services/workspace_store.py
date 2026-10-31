"""Persisted workspace preferences — org profile, retention, timezone."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from services.platform_config import data_dir
from services.value_serializer import json_default

STORE_PATH = data_dir() / "workspace.json"

_DEFAULTS: dict[str, Any] = {
    "org_name": "Datawrap",
    "timezone": "UTC",
    "retention_days": 90,
    # Studio surfaces. Absent or false keeps the wizard. Both must be set
    # here and in the operator's browser before a new surface renders.
    "studio_schema_browser": False,
    "studio_canvas": False,
}


_ATTRIBUTION = ("updated_at", "updated_by")


class InvalidTimezone(ValueError):
    pass


def validate_timezone(name: str) -> str:
    """The IANA zone ``name`` names, or ``InvalidTimezone``.

    Checked against the zone database itself (``zoneinfo.available_timezones``),
    so a well-formed but invented name like ``Mars/Olympus`` is refused.
    """
    from zoneinfo import available_timezones

    zone = (name or "").strip()
    if zone in {"UTC", "Etc/UTC"} or zone in available_timezones():
        return zone
    raise InvalidTimezone(
        f"{zone!r} is not an IANA time zone. Use a name such as UTC, "
        "America/New_York or Europe/Berlin."
    )


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
        # Attribution is stored beside the preferences. Filtering it out with the
        # unknown keys made every read report updated_at/updated_by as null.
        merged.update(
            {k: v for k, v in raw.items() if k in _DEFAULTS or k in _ATTRIBUTION or k.startswith("_")}
        )
        return merged
    except Exception:
        return dict(_DEFAULTS)


def _save(data: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STORE_PATH.write_text(json.dumps(data, indent=2, default=json_default), encoding="utf-8")


def get_workspace_settings() -> dict[str, Any]:
    raw = _load_raw()
    return {
        "org_name": str(raw.get("org_name") or _DEFAULTS["org_name"]),
        "timezone": str(raw.get("timezone") or _DEFAULTS["timezone"]),
        "retention_days": int(raw.get("retention_days") or _DEFAULTS["retention_days"]),
        "studio_schema_browser": bool(raw.get("studio_schema_browser")),
        "studio_canvas": bool(raw.get("studio_canvas")),
        "updated_at": raw.get("updated_at"),
        "updated_by": raw.get("updated_by"),
    }


def update_workspace_settings(
    *,
    org_name: str | None = None,
    timezone: str | None = None,
    retention_days: int | None = None,
    studio_schema_browser: bool | None = None,
    studio_canvas: bool | None = None,
    actor: str | None = None,
) -> dict[str, Any]:
    data = _load_raw()
    if org_name is not None:
        data["org_name"] = org_name.strip()[:128] or _DEFAULTS["org_name"]
    if timezone is not None:
        data["timezone"] = validate_timezone(timezone) if timezone.strip() else _DEFAULTS["timezone"]
    if retention_days is not None:
        data["retention_days"] = max(1, min(3650, int(retention_days)))
    if studio_schema_browser is not None:
        data["studio_schema_browser"] = bool(studio_schema_browser)
    if studio_canvas is not None:
        data["studio_canvas"] = bool(studio_canvas)
    data["updated_at"] = _now()
    data["updated_by"] = actor or "system"
    _save(data)
    return get_workspace_settings()
