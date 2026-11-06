"""The deployment's general settings — org profile, retention, timezone, studio flags.

One document for the whole deployment (``services.platform_settings``).

* **Control-plane database configured** (``control_plane_store``): the
  settings live there, read on every call (no cache) by the API, the
  retention purge beat and the settings audit. If the database is configured
  but unreachable, reads raise :class:`SettingsUnavailable` — routes answer
  503, safety flags read as off (:func:`safety_flag`), beats skip — and the
  node-local file is never used instead: it may hold another replica's stale
  value (independent review: a down client read ``retention_auto_purge=True``
  from a stale file and the purge beat ran while the shared setting was off).
* **No control-plane database** (single node / dev / in-memory job store):
  ``data_dir()/workspace.json`` (``STORE_PATH``), exactly as before.

Migration (deterministic, no boot-order winner). Each replica that still has
a local file contributes it once to ``workspace_settings_sources`` (keyed by
its content hash; audited ``workspace.settings.migrated``) and renames it to
``workspace.json.migrated`` so it can never re-arm. The effective document is
``defaults`` ← the merge of every contributed file ← every field written
through the shared store. The merge is per field, newest file ``updated_at``
wins, except: a destructive toggle (``retention_auto_purge``) never seeds on
— a platform admin turns it on in the shared store, audited — and
``retention_days`` seeds as the longest window any file held. A write never
discards a contribution: fields it does not name keep their merged value.
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
_SOURCES = "workspace_settings_sources"
_DOC_ID = "general"
#: Flags whose "on" runs something destructive: on only when every source agrees.
SAFETY_FLAGS = ("retention_auto_purge",)


class SettingsUnavailable(RuntimeError):
    """The shared settings store is configured but cannot be read or written."""


def _migrated_path():
    return STORE_PATH.with_name(STORE_PATH.name + ".migrated")


def _shared():
    """The shared collection, None in file mode; raises SettingsUnavailable when down."""
    from services.control_plane_store import ControlPlaneUnavailable, shared_collection

    try:
        return shared_collection(_COLLECTION)
    except ControlPlaneUnavailable as exc:
        raise SettingsUnavailable(str(exc)) from exc


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


def _contribute_local_file(coll: Any) -> None:
    """Add this node's file to the merge sources once, audit it, retire the file."""
    raw = _read_file()
    if raw is None:
        return
    import hashlib

    values = {k: v for k, v in _known(raw).items() if not k.startswith("_")}
    source_id = hashlib.sha256(
        json.dumps(values, sort_keys=True, default=json_default).encode("utf-8")
    ).hexdigest()
    sources = coll.database[_SOURCES]
    result = sources.update_one(
        {"_id": source_id},
        {"$setOnInsert": {"values": values, "file": str(STORE_PATH), "contributed_at": _now()}},
        upsert=True,
    )
    if getattr(result, "upserted_id", None) is not None:
        from services.audit_coverage import record_security_event

        record_security_event(
            "workspace.settings.migrated",
            outcome="succeeded",
            resource="workspace_settings",
            actor="system",
            details={"source": str(STORE_PATH), "source_id": source_id, "values": values},
        )
    try:
        STORE_PATH.replace(_migrated_path())
    except OSError as exc:  # the source is recorded; the file is re-read harmlessly
        import logging

        logging.getLogger(__name__).warning("could not retire %s: %s", STORE_PATH, exc)


#: Fields a file seeds conservatively: destructive toggles never seed on, and
#: the retention window never seeds shorter than the longest any file held.
_SEED_MAX = ("retention_days",)


def _merge_sources(sources: list[dict[str, Any]]) -> dict[str, Any]:
    """The newest whole file wins — except what can delete data.

    Files carry one ``updated_at`` for the whole document, not per field, so
    the files are applied oldest to newest and a field takes the value of the
    newest file that carries it ("newest file", not "newest per field").

    A destructive toggle (``SAFETY_FLAGS``) always seeds off: a replica's
    stale file must never arm a deployment-wide purge (second review); a
    platform admin turns it on through the shared store, audited. The
    retention window seeds as the longest any file held.
    """
    ordered = sorted(
        (dict(s.get("values") or {}) for s in sources),
        key=lambda v: (str(v.get("updated_at") or ""), json.dumps(v, sort_keys=True, default=json_default)),
    )
    merged: dict[str, Any] = {}
    for values in ordered:
        merged.update({k: v for k, v in values.items() if k in _DEFAULTS or k in _ATTRIBUTION})
    for flag in SAFETY_FLAGS:
        merged.pop(flag, None)
    for field in _SEED_MAX:
        carried = [int(v[field]) for v in ordered if isinstance(v.get(field), int)]
        if carried:
            merged[field] = max(carried)
    return merged


def _shared_effective(coll: Any, written: dict[str, Any] | None = None) -> dict[str, Any]:
    merged = dict(_DEFAULTS)
    merged.update(_merge_sources(list(coll.database[_SOURCES].find({}))))
    doc = written if written is not None else (coll.find_one({"_id": _DOC_ID}) or {})
    values = _known({k: v for k, v in doc.items() if k != "_id"})
    if doc.get("_migrated_from"):
        # A document c32b61c4c seeded from the first replica's file: its
        # destructive toggles are that file's, not an admin's — only a flag a
        # platform admin wrote through the store since (``_admin_set``) counts.
        admin_set = set(doc.get("_admin_set") or [])
        for flag in SAFETY_FLAGS:
            if flag not in admin_set:
                values.pop(flag, None)
    merged.update(values)
    return merged


def _load_raw() -> dict[str, Any]:
    coll = _shared()
    if coll is not None:
        try:
            _contribute_local_file(coll)
            return _shared_effective(coll)
        except SettingsUnavailable:
            raise
        except Exception as exc:  # pymongo errors: the store is configured but failing
            import logging

            logging.getLogger(__name__).warning("shared settings store unreadable: %s", exc)
            raise SettingsUnavailable("shared settings store unreadable") from exc
    merged = dict(_DEFAULTS)
    raw = _read_file()
    if raw is not None:
        merged.update(_known(raw))
    return merged


def _save(data: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STORE_PATH.write_text(json.dumps(data, indent=2, default=json_default), encoding="utf-8")


def _write(patch: dict[str, Any]) -> dict[str, Any]:
    """Apply ``patch``; returns the effective document as it was before."""
    coll = _shared()
    if coll is not None:
        try:
            # This node's file joins the merge before anything is written, so a
            # first write never leaves its values out.
            _contribute_local_file(coll)
            update: dict[str, Any] = {"$set": patch}
            flags = [f for f in SAFETY_FLAGS if f in patch]
            if flags:
                update["$addToSet"] = {"_admin_set": {"$each": flags}}
            before_doc = coll.find_one_and_update({"_id": _DOC_ID}, update, upsert=True) or {}
            return _shared_effective(coll, written=before_doc)
        except SettingsUnavailable:
            raise
        except Exception as exc:
            import logging

            logging.getLogger(__name__).warning("shared settings store unwritable: %s", exc)
            raise SettingsUnavailable("shared settings store unwritable") from exc
    data = _load_raw()
    before = dict(data)
    data.update(patch)
    _save(data)
    return before


def safety_flag(name: str) -> bool:
    """A destructive automation's switch: off whenever the store cannot be read."""
    try:
        return bool(get_workspace_settings().get(name))
    except SettingsUnavailable as exc:
        import logging

        logging.getLogger(__name__).warning("%s reads as off: %s", name, exc)
        return False


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
