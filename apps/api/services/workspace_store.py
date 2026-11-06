"""The deployment's general settings — org profile, retention, timezone, studio flags.

One document for the whole deployment (``services.platform_settings``). Where
a control-plane database is configured it lives there
(``workspace_settings`` / ``_id: general``), so every replica — API, worker,
scheduler — reads and writes the same values; the retention purge beat and the
settings audit read it there, uncached. Only a single-node or dev deployment
with no control-plane database keeps it in ``data_dir()/workspace.json``
(``STORE_PATH``), exactly as before.

Migration: the first read on a deployment whose shared store has no document
seeds it from this node's file, if one exists, with an insert-if-absent (the
first replica wins; the others adopt its document), and audits it as
``workspace.settings.migrated``. A file value is never dropped: every known
key is carried over.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from services.platform_config import data_dir
from services.value_serializer import json_default

STORE_PATH = data_dir() / "workspace.json"

_DEFAULTS: dict[str, Any] = {
    "org_name": "Datawrap",
    "timezone": "UTC",
    "retention_days": 90,
    # Off by default: finished jobs older than retention_days are purged on a
    # daily schedule only once a platform admin turns this on (Security 23,
    # services.job_retention_schedule). Off, retention is the admin-run purge.
    "retention_auto_purge": False,
    # Studio surfaces. Absent or false keeps the wizard. Both must be set
    # here and in the operator's browser before a new surface renders.
    "studio_schema_browser": False,
    "studio_canvas": False,
}


_ATTRIBUTION = ("updated_at", "updated_by")


class InvalidTimezone(ValueError):
    pass


@lru_cache(maxsize=1)
def _iana_zones() -> frozenset[str]:
    """The zone database's names, read once (it walks the tzdata tree)."""
    from zoneinfo import available_timezones

    return frozenset(available_timezones())


_ZONE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_+\-]*(/[A-Za-z0-9_+\-]+)*$")
_NOT_ZONES = frozenset({"localtime", "posixrules", "Factory"})


def _is_zone_link(zone: str) -> bool:
    """A backward-compatible tzdata link (``US/Eastern``) some builds leave out
    of ``available_timezones`` but the zone database still resolves. Only a
    plain zone-shaped name is tried — never a path."""
    if not _ZONE_NAME.match(zone) or zone in _NOT_ZONES:
        return False
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    try:
        ZoneInfo(zone)
    except (ZoneInfoNotFoundError, ValueError, KeyError, OSError):
        return False
    return True


def validate_timezone(name: str) -> str:
    """The IANA zone ``name`` names, or ``InvalidTimezone``.

    Checked against the zone database itself (``zoneinfo.available_timezones``),
    so a well-formed but invented name like ``Mars/Olympus`` is refused.
    """
    zone = (name or "").strip()
    if zone in {"UTC", "Etc/UTC"} or zone in _iana_zones() or _is_zone_link(zone):
        return zone
    raise InvalidTimezone(
        f"{zone!r} is not an IANA time zone. Use a name such as UTC, "
        "America/New_York or Europe/Berlin."
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


_COLLECTION = "workspace_settings"
_DOC_ID = "general"


def _shared():
    """The shared control-plane collection, or None on a file-only deployment."""
    from services.control_plane_store import mongo_collection

    return mongo_collection(_COLLECTION)


def _known(raw: dict[str, Any]) -> dict[str, Any]:
    # Attribution is stored beside the preferences. Filtering it out with the
    # unknown keys made every read report updated_at/updated_by as null.
    return {k: v for k, v in raw.items() if k in _DEFAULTS or k in _ATTRIBUTION or k.startswith("_")}


def _read_file() -> dict[str, Any] | None:
    if not STORE_PATH.exists():
        return None
    try:
        raw = json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    return raw if isinstance(raw, dict) else None


def _seed_from_file(coll: Any) -> dict[str, Any] | None:
    """Insert this node's file into the empty shared store; first writer wins."""
    raw = _read_file()
    if raw is None:
        return None
    values = {k: v for k, v in _known(raw).items() if not k.startswith("_")}
    result = coll.update_one(
        {"_id": _DOC_ID},
        {"$setOnInsert": {**values, "_migrated_from": str(STORE_PATH), "_migrated_at": _now()}},
        upsert=True,
    )
    if getattr(result, "upserted_id", None) is not None:
        try:
            from services.audit_coverage import record_security_event

            record_security_event(
                "workspace.settings.migrated",
                outcome="succeeded",
                resource="workspace_settings",
                actor="system",
                details={"source": str(STORE_PATH), "values": values},
            )
        except Exception:  # noqa: BLE001 - the document is in place; the log says so
            import logging

            logging.getLogger(__name__).warning(
                "workspace settings migrated from %s; the audit record failed", STORE_PATH
            )
    return coll.find_one({"_id": _DOC_ID})


def _load_raw() -> dict[str, Any]:
    merged = dict(_DEFAULTS)
    coll = _shared()
    if coll is not None:
        doc = coll.find_one({"_id": _DOC_ID}) or _seed_from_file(coll) or {}
        merged.update(_known({k: v for k, v in doc.items() if k != "_id"}))
        return merged
    raw = _read_file()
    if raw is not None:
        merged.update(_known(raw))
    return merged


def _save(data: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STORE_PATH.write_text(json.dumps(data, indent=2, default=json_default), encoding="utf-8")


def _write(patch: dict[str, Any]) -> dict[str, Any]:
    """Apply ``patch``; returns the document as it was before (for the audit)."""
    coll = _shared()
    if coll is not None:
        # Seed first so a migration never loses the file's other values.
        if coll.find_one({"_id": _DOC_ID}, {"_id": 1}) is None:
            _seed_from_file(coll)
        before = coll.find_one_and_update({"_id": _DOC_ID}, {"$set": patch}, upsert=True) or {}
        merged = dict(_DEFAULTS)
        merged.update(_known({k: v for k, v in before.items() if k != "_id"}))
        return merged
    data = _load_raw()
    before = dict(data)
    data.update(patch)
    _save(data)
    return before


def _cdc_orphan_auto_drop() -> bool:
    from services.cdc_capture_inventory import auto_drop_enabled

    return auto_drop_enabled()


def get_workspace_settings() -> dict[str, Any]:
    """The current settings, read from the store on every call (no cache)."""
    return _present(_load_raw())


def _present(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "org_name": str(raw.get("org_name") or _DEFAULTS["org_name"]),
        "timezone": str(raw.get("timezone") or _DEFAULTS["timezone"]),
        "retention_days": int(raw.get("retention_days") or _DEFAULTS["retention_days"]),
        "retention_auto_purge": bool(raw.get("retention_auto_purge")),
        # C11: off by default, and kept in the shared control-plane store
        # (services.cdc_capture_inventory), not in this per-replica file — every
        # replica's scheduler beat must read the same value.
        "cdc_orphan_auto_drop": _cdc_orphan_auto_drop(),
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
    retention_auto_purge: bool | None = None,
    cdc_orphan_auto_drop: bool | None = None,
) -> dict[str, Any]:
    return update_workspace_settings_with_before(
        org_name=org_name, timezone=timezone, retention_days=retention_days,
        studio_schema_browser=studio_schema_browser, studio_canvas=studio_canvas,
        actor=actor, retention_auto_purge=retention_auto_purge,
        cdc_orphan_auto_drop=cdc_orphan_auto_drop,
    )[1]


def update_workspace_settings_with_before(
    *,
    org_name: str | None = None,
    timezone: str | None = None,
    retention_days: int | None = None,
    studio_schema_browser: bool | None = None,
    studio_canvas: bool | None = None,
    actor: str | None = None,
    retention_auto_purge: bool | None = None,
    cdc_orphan_auto_drop: bool | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Change only the named fields; ``(before, after)`` from the store itself.

    The shared store applies the change with one ``$set`` of those fields and
    hands back the document it replaced, so two replicas writing different
    fields never clobber each other and the audit's from/to is the store's.
    """
    patch: dict[str, Any] = {}
    if org_name is not None:
        patch["org_name"] = org_name.strip()[:128] or _DEFAULTS["org_name"]
    if timezone is not None:
        patch["timezone"] = validate_timezone(timezone) if timezone.strip() else _DEFAULTS["timezone"]
    if retention_days is not None:
        patch["retention_days"] = max(1, min(3650, int(retention_days)))
    if retention_auto_purge is not None:
        patch["retention_auto_purge"] = bool(retention_auto_purge)
    if studio_schema_browser is not None:
        patch["studio_schema_browser"] = bool(studio_schema_browser)
    if studio_canvas is not None:
        patch["studio_canvas"] = bool(studio_canvas)
    if cdc_orphan_auto_drop is not None:
        from services.cdc_capture_inventory import set_auto_drop

        set_auto_drop(bool(cdc_orphan_auto_drop), actor=actor or "system")
    patch["updated_at"] = _now()
    patch["updated_by"] = actor or "system"
    raw_before = _write(patch)
    before = _present(raw_before)
    return before, get_workspace_settings()
