"""A dropped Postgres logical slot does not keep its last LSN as a resume.

One-shot CDC drops the slot when the run finishes. The job cursor is cleared.
The destination exactly-once watermark is the resume the next Open copies
back, so the following run streams ``slot_missing`` and the change committed
after the drop never lands.

Retirement records that LSN for the cursor (and the slot name). The next Open
ignores it and does not touch the writer fence. A missing slot is the same
fact at run time even when the record was not written: ``initial`` snapshots
current source keys and creates the new slot before that snapshot. ``never``
stays fail-closed. ``wal_status=lost`` on a slot that still exists is not this
path — recreating it at the current WAL would skip the recycled window.

At-least-once upsert of the live source image. Not continuous CDC. Not
migration_proven. Not platform exactly-once.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from services.atomic_file import write_json_atomic
from services.platform_config import data_dir
from services.value_serializer import json_default

_logger = logging.getLogger(__name__)

STORE_PATH = data_dir() / "cdc_retired_slot_resumes.json"
_MAX_RECORDS = 500


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _lsn_of(value: Any) -> str:
    if value is None:
        return ""
    from connectors.lsn_guards import extract_cdc_lsn

    text = str(extract_cdc_lsn(value) or "").strip()
    if text.lower() in {"", "none", "null"}:
        return ""
    return text


def _slot_of(value: Any) -> str:
    text = str(value or "")
    for part in text.split("|"):
        if part.lower().startswith("slot="):
            return part.split("=", 1)[1].strip()[:63]
    return ""


def _mongo_retired():  # type: ignore[no-untyped-def]
    try:
        from services.sync_cursor import _mongo_cursors

        cursors = _mongo_cursors()
        if cursors is None:
            return None
        database = cursors.database
        if database is None:
            return None
        return database["cdc_retired_slot_resumes"]
    except Exception as exc:  # noqa: BLE001
        _logger.debug("retired-slot store unavailable: %s", exc)
        return None


def _load() -> list[dict[str, Any]]:
    coll = _mongo_retired()
    if coll is not None:
        try:
            docs = list(coll.find({}, {"_id": 0}))
            if docs:
                return [doc for doc in docs if isinstance(doc, dict)]
        except Exception as exc:  # noqa: BLE001
            _logger.debug("retired-slot mongo read failed: %s", exc)
    if not STORE_PATH.exists():
        return []
    try:
        import json

        payload = json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return []
    rows = payload.get("resumes") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _save(rows: list[dict[str, Any]]) -> None:
    kept = rows[-_MAX_RECORDS:]
    coll = _mongo_retired()
    if coll is not None:
        try:
            coll.delete_many({})
            if kept:
                coll.insert_many(kept)
        except Exception as exc:  # noqa: BLE001
            _logger.debug("retired-slot mongo write failed: %s", exc)
    write_json_atomic(STORE_PATH, {"resumes": kept}, indent=2, default=json_default)


def retire_slot_lsn(
    lsn: str,
    *,
    cursor_key: str = "",
    slot_name: str = "",
) -> bool:
    """Remember that ``lsn`` belonged to a slot this process dropped."""
    token = _lsn_of(lsn) or str(lsn or "").strip()
    if not token or token.lower() in {"none", "null"}:
        return False
    key = str(cursor_key or "").strip()
    slot = str(slot_name or "").strip()[:63]
    rows = _load()
    for row in rows:
        if str(row.get("lsn") or "") != token:
            continue
        if key and str(row.get("cursor_key") or "") == key:
            return True
        if slot and str(row.get("slot_name") or "") == slot and not key:
            return True
    rows.append(
        {
            "lsn": token,
            "cursor_key": key,
            "slot_name": slot,
            "retired_at": _now(),
        }
    )
    _save(rows)
    return True


def slot_lsn_retired(
    lsn: Any,
    *,
    cursor_key: str = "",
    slot_name: str = "",
) -> bool:
    """True when this cursor or slot dropped ``lsn``.

    The LSN string alone is not enough. Two Postgres clusters can both show
    ``0/16B8A40``. A match needs the cursor key or the slot name.
    """
    token = _lsn_of(lsn)
    if not token:
        return False
    key = str(cursor_key or "").strip()
    slot = str(slot_name or "").strip() or _slot_of(lsn)
    if not key and not slot:
        return False
    for row in _load():
        if str(row.get("lsn") or "") != token:
            continue
        row_key = str(row.get("cursor_key") or "")
        row_slot = str(row.get("slot_name") or "")
        if key and row_key == key:
            return True
        if slot and row_slot == slot:
            return True
    return False


def resume_token_retired(resume: Any, *, cursor_key: str = "") -> bool:
    if not slot_lsn_retired(resume, cursor_key=cursor_key, slot_name=_slot_of(resume)):
        return False
    return True


def without_retired_slot_resume(
    view: Any,
    job_resume: Any,
    *,
    cursor_key: str,
) -> tuple[Any, Any]:
    """Drop a retired dest/job LSN. The fence epoch on ``view`` stays."""
    key = str(cursor_key or "").strip()
    dest_lsn = getattr(view, "committed_lsn", None)
    blob = getattr(view, "resume_blob", None)
    if slot_lsn_retired(dest_lsn, cursor_key=key, slot_name=_slot_of(blob)) or slot_lsn_retired(
        blob, cursor_key=key, slot_name=_slot_of(blob)
    ):
        view = replace(view, committed_lsn=None, resume_blob="")
    if slot_lsn_retired(job_resume, cursor_key=key, slot_name=_slot_of(job_resume)):
        job_resume = None
    return view, job_resume


def retire_after_slot_drop(
    *,
    slot_name: str,
    cursor_keys: list[str],
    prior_by_key: dict[str, Any],
    extra_tokens: list[Any] | None = None,
    dest_type: str = "",
    dest_cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Record every LSN the dropped slot owned, and blank those dest rows."""
    slot = str(slot_name or "").strip()
    keys = [str(key).strip() for key in cursor_keys if str(key).strip()]
    retired: list[str] = []
    cleared_dest: list[str] = []

    def _remember(key: str, raw: Any) -> str:
        token = _lsn_of(raw)
        if not token:
            return ""
        if retire_slot_lsn(token, cursor_key=key, slot_name=slot):
            if token not in retired:
                retired.append(token)
        return token

    for key in keys:
        _remember(key, prior_by_key.get(key))
    for raw in extra_tokens or []:
        token = _lsn_of(raw)
        if not token:
            continue
        for key in keys or [""]:
            _remember(key, token)

    cfg = dict(dest_cfg or {})
    if cfg and keys:
        from connectors.cdc_eos_sql import blank_route_dest_resume, read_route_dest_lsn

        for key in keys:
            try:
                live = read_route_dest_lsn(dest_type, cfg, key)
            except Exception as exc:  # noqa: BLE001
                _logger.warning("Could not read dest CDC resume for %s: %s", key, exc)
                live = None
            token = _remember(key, live)
            if not token:
                continue
            try:
                if blank_route_dest_resume(dest_type, cfg, key, lsn=token):
                    cleared_dest.append(key)
            except Exception as exc:  # noqa: BLE001
                _logger.warning("Could not blank dest CDC resume for %s: %s", key, exc)
    return {
        "retired_lsns": retired,
        "dest_resume_cleared": cleared_dest,
    }


def slot_is_missing(retention: Any) -> bool:
    """True when the probe proved the logical slot is gone.

    ``wal_status=lost`` still has a slot. That window was recycled. This
    helper does not treat it as permission to snapshot under ``initial``.
    """
    if retention is None:
        return False
    from services.cdc_snapshot_mode import _retention_field, _retention_status

    if _retention_status(retention) != "gap":
        return False
    retained = _retention_field(retention, "retained").strip().lower()
    if retained == "slot_missing":
        return True
    details = retention.get("details") if isinstance(retention, dict) else getattr(retention, "details", None)
    return isinstance(details, dict) and details.get("slot_exists") is False


def prepare_resume_for_missing_slot(
    watermark: Any,
    retention: Any,
    reader: Any,
    *,
    mode: Any,
) -> tuple[Any, str]:
    """Void a resume whose slot is gone so the snapshot plan does not stream it.

    ``never`` keeps the token and the plan refuses. Every other mode sees no
    watermark, so ``initial`` snapshots current keys. The reader is disarmed
    before that snapshot: poll must not create a slot at the current WAL to
    catch up the dead LSN. ``snapshot()`` creates the slot first.
    """
    if not slot_is_missing(retention):
        return watermark, ""
    from services.cdc_snapshot_mode import SnapshotMode, parse_snapshot_mode, watermark_present

    try:
        parsed = mode if isinstance(mode, SnapshotMode) else parse_snapshot_mode(mode)
    except ValueError:
        return watermark, ""
    if parsed == SnapshotMode.NEVER or not watermark_present(watermark):
        return watermark, ""
    _disarm_reader(reader)
    return None, (
        "CDC slot is missing — stored LSN is not a resume. "
        "Snapshot current source keys, then create the logical slot before that snapshot. "
        "At-least-once upsert. Not continuous CDC."
    )


def _disarm_reader(reader: Any) -> None:
    if reader is None or not hasattr(reader, "consistent_point_lsn"):
        return
    reader.consistent_point_lsn = None
    reader._resume_expected = False
    if hasattr(reader, "_resume_snapshot"):
        reader._resume_snapshot = False
    if hasattr(reader, "resume_token"):
        reader.resume_token = None
    if hasattr(reader, "snapshot_last_pk"):
        reader.snapshot_last_pk = ""
    if hasattr(reader, "snapshot_table"):
        reader.snapshot_table = ""


def preflight_slot_gate(
    mode: Any,
    watermark: Any,
    retention: Any,
    *,
    cursor_key: str = "",
) -> dict[str, Any] | None:
    """Validate gate for a stored LSN whose slot is missing or whose WAL is gone.

    A missing slot under ``initial`` / ``when_needed`` / ``always`` /
    ``initial_only`` passes as a recovery snapshot. The run reads current
    keys, so the change since the drop is in the snapshot. ``never`` blocks.
    A slot that exists with ``wal_status=lost`` keeps the existing refuse
    rules: ``initial`` blocks, ``when_needed`` snapshots.
    """
    from services.cdc_snapshot_mode import (
        KIND_REFUSE,
        SnapshotMode,
        classify_snapshot_plan,
        parse_snapshot_mode,
        watermark_present,
    )

    key = str(cursor_key or "").strip()
    retired = resume_token_retired(watermark, cursor_key=key)
    missing = slot_is_missing(retention) or retired
    if not missing and retention is None:
        return None
    from services.cdc_snapshot_mode import _retention_status

    gap = _retention_status(retention) == "gap" or retired
    if not gap or not watermark_present(watermark):
        return None
    try:
        parsed = mode if isinstance(mode, SnapshotMode) else parse_snapshot_mode(mode)
    except ValueError:
        return None
    honesty = "at-least-once upsert. not continuous CDC. not migration_proven."
    if missing and parsed == SnapshotMode.NEVER:
        return {
            "id": "g18_cdc_snapshot_mode",
            "status": "block",
            "message": (
                "PostgreSQL replication slot is missing and snapshot_mode=never "
                "will not snapshot current keys. Set snapshot_mode=when_needed "
                "and re-run. Creating a slot at the current WAL would skip the "
                "change committed after the slot was dropped."
            ),
            "duration_ms": 0,
            "details": {
                "snapshot_mode": parsed.value,
                "watermark_present": True,
                "run_snapshot": False,
                "slot_missing": True,
                "resume_retired": retired,
                "primary_action": "open_advanced",
                "honesty": honesty,
            },
        }
    if missing:
        return {
            "id": "g18_cdc_snapshot_mode",
            "status": "pass",
            "message": (
                "PostgreSQL replication slot is missing, so the stored LSN is not "
                "a resume. This run snapshots current source keys — the change "
                "since the slot was dropped is in that snapshot — and creates the "
                "new logical slot before the snapshot. At-least-once upsert. Not "
                "continuous CDC, not migration_proven."
            ),
            "duration_ms": 0,
            "details": {
                "snapshot_mode": parsed.value,
                "watermark_present": True,
                "run_snapshot": True,
                "slot_missing": True,
                "resume_retired": retired,
                "lost_window": True,
                "honesty": honesty,
            },
        }
    plan = classify_snapshot_plan(
        parsed,
        watermark=watermark,
        retention_status="gap",
    )
    if plan.get("kind") == KIND_REFUSE:
        return {
            "id": "g18_cdc_snapshot_mode",
            "status": "block",
            "message": str(plan.get("message") or "CDC resume is before retained log history."),
            "duration_ms": 0,
            "details": {
                "snapshot_mode": parsed.value,
                "watermark_present": True,
                "run_snapshot": False,
                "lost_window": True,
                "primary_action": str(plan.get("next_action") or "open_advanced"),
                "honesty": honesty,
            },
        }
    return {
        "id": "g18_cdc_snapshot_mode",
        "status": "pass",
        "message": str(plan.get("message") or "CDC retention gap — recovery snapshot."),
        "duration_ms": 0,
        "details": {
            "snapshot_mode": parsed.value,
            "watermark_present": True,
            "run_snapshot": bool(plan.get("run_snapshot")),
            "lost_window": True,
            "honesty": honesty,
        },
    }


def probe_postgres_slot_for_preflight(
    source_config: dict[str, Any] | None,
    *,
    source_type: str,
    watermark: Any,
    table: str = "",
    cursor_key: str = "",
) -> Any:
    """Live slot probe when Validate has a Postgres source and a stored LSN.

    A connection failure returns None. That is not proof the slot exists and
    not proof it is missing.
    """
    cfg = dict(source_config or {})
    kind = str(source_type or cfg.get("type") or cfg.get("db_type") or "").strip().lower()
    if kind not in {"postgresql", "postgres"}:
        return None
    if not (cfg.get("host") or cfg.get("connection_string")):
        return None
    from services.cdc_snapshot_mode import watermark_present

    if not watermark_present(watermark):
        return None
    from services.cdc_retention_probe import probe_postgres_retention

    try:
        return probe_postgres_retention(
            cfg,
            table=table,
            cursor_key=cursor_key,
            watermark=str(watermark) if watermark is not None else None,
        )
    except Exception as exc:  # noqa: BLE001
        _logger.warning("CDC slot preflight probe failed: %s", exc)
        return None
