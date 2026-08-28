"""Quarantine Row Contract — Module 9 first-class reject schema.

Charter fields (every rejected row):

* original_value
* expected_type
* actual_type
* failure_reason
* transform_attempted
* recovery_suggestion
* source_pk
* destination_pk
* job_id
* connector
* retry_status

Normalize adds these alongside legacy keys (``reason``, ``values``, …) so
replay / UI keep working. Never invent a primary key value — stamp
``source_pk_proven=false`` when unknown.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

REQUIRED_QUARANTINE_FIELDS = (
    "original_value",
    "expected_type",
    "actual_type",
    "failure_reason",
    "transform_attempted",
    "recovery_suggestion",
    "source_pk",
    "destination_pk",
    "job_id",
    "connector",
    "retry_status",
)

RETRY_STATUSES = frozenset(
    {"open", "pending_replay", "promoted", "replay_failed", "abandoned"}
)
#: Still in the operator replay set — Kafka/Uber DLQ "not yet merged".
RETRY_OPEN_STATUSES = frozenset({"open", "pending_replay", "replay_failed"})
#: Left the replay set. ``promoted`` is Gate-8 child proof, not parent
#: migration_proven. ``abandoned`` is an explicit operator drop of a poison pill.
RETRY_CLOSED_STATUSES = frozenset({"promoted", "abandoned"})


class QuarantineRowContractError(ValueError):
    """Quarantine row cannot be recovered / audited under the Module 9 contract."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _suggest_recovery(reason: str, *, transform: str | None = None) -> str:
    low = (reason or "").lower()
    if "risk contract" in low or "execution policy" in low:
        return "Approve a Migration Risk Contract with an explicit execution policy, then re-Validate."
    if "orphan" in low or "foreign key" in low or "fk_" in low:
        return "Fix parent keys / mapping, run sample orphan probe, or acknowledge FK risk with a signed contract."
    if "confidence" in low:
        return "Remap on Map, Approve override, or accept a Risk Contract — G4 owns the hard block."
    if any(k in low for k in ("cast", "coercion", "lossy", "decimal", "overflow", "truncat")):
        return (
            "Widen destination type, add an explicit transform, or CAST_AND_CONTINUE / "
            "QUARANTINE_ROW via Risk Contract — never silent truncate."
        )
    if any(k in low for k in ("encoding", "control", "unicode", "null")):
        return "Apply strip_controls / normalize_unicode on Map, or quarantine and remediate the source value."
    if transform and transform not in {"none", "identity", ""}:
        return f"Review transform `{transform}` on Map, fix the source value, then replay quarantine."
    return "Inspect the quarantined value, fix Map/transform or source data, then replay via Inspect Quarantine."


def _extract_original_value(row: dict[str, Any]) -> Any:
    if "original_value" in row and row.get("original_value") is not None:
        return row.get("original_value")
    if "value" in row:
        return row.get("value")
    col = str(row.get("column") or row.get("target") or "")
    for bag_key in ("source_values", "values"):
        bag = row.get(bag_key)
        if isinstance(bag, dict) and col and col in bag:
            return bag.get(col)
        if isinstance(bag, dict) and bag:
            # Single-column bags often key by target.
            tgt = str(row.get("target") or "")
            if tgt and tgt in bag:
                return bag.get(tgt)
    return None


def _column_name_list(value: Any) -> list[str] | None:
    """Writer ``primary_key`` is column names, not the row identity."""
    if isinstance(value, (list, tuple)) and value:
        names = [str(x).strip() for x in value]
        if all(names) and all(isinstance(x, str) for x in value):
            return names
    return None


def _pk_from_named_columns(row: dict[str, Any], cols: list[str]) -> Any:
    bags: list[dict[str, Any]] = []
    raw_pk = row.get("pk_value")
    if isinstance(raw_pk, dict):
        bags.append(raw_pk)
    for bag_key in ("source_values", "values"):
        bag = row.get(bag_key)
        if isinstance(bag, dict):
            bags.append(bag)
    for bag in bags:
        if not all(c in bag for c in cols):
            continue
        vals = [bag.get(c) for c in cols]
        if any(v is None or str(v) == "" for v in vals):
            continue
        return vals[0] if len(vals) == 1 else vals
    return None


def _extract_pk(row: dict[str, Any], *, side: str) -> Any:
    """Return the row's PK *value*. Column-name lists are not an identity.

    Writers stamp ``primary_key=["id"]`` plus ``pk_value={"id": 2}``. Treating
    the column list as ``source_pk`` made every finding share ``pk:['id']``,
    so one Promote closed the whole ledger.
    """
    key = "source_pk" if side == "source" else "destination_pk"
    stored = row.get(key)
    cols = _column_name_list(stored)
    if cols:
        resolved = _pk_from_named_columns(row, cols)
        if resolved is not None:
            return resolved
        stored = None
    if stored is not None and str(stored) != "":
        return stored
    aliases = (
        ("pk", "id")
        if side == "source"
        else ("dest_pk", "target_pk")
    )
    for a in aliases:
        raw = row.get(a)
        if raw is None or str(raw) == "":
            continue
        if _column_name_list(raw):
            continue
        return raw
    pk_cols = row.get("primary_key")
    names = _column_name_list(pk_cols)
    if names:
        resolved = _pk_from_named_columns(row, names)
        if resolved is not None:
            return resolved
    if isinstance(pk_cols, str) and pk_cols.strip():
        resolved = _pk_from_named_columns(row, [pk_cols.strip()])
        if resolved is not None:
            return resolved
    for bag_key in ("source_values", "values"):
        bag = row.get(bag_key)
        if not isinstance(bag, dict):
            continue
        for candidate in ("id", "ID", "pk", "uuid", "_id"):
            if candidate in bag and bag.get(candidate) is not None:
                return bag.get(candidate)
    return None


def normalize_quarantine_row(
    row: dict[str, Any] | None,
    *,
    job_id: str = "",
    connector: str = "",
    default_retry_status: str = "open",
) -> dict[str, Any]:
    """Return a row stamped with Module 9 fields. Never invent PK values."""
    raw = dict(row or {})
    reason = str(
        raw.get("failure_reason")
        or raw.get("reason")
        or raw.get("message")
        or "Quarantined — reason not recorded"
    )
    transform = (
        raw.get("transform_attempted")
        if raw.get("transform_attempted") is not None
        else raw.get("transform") or raw.get("suggested_transform") or ""
    )
    transform_s = str(transform or "") or "none"
    expected = str(
        raw.get("expected_type")
        or raw.get("target_type")
        or raw.get("destination_type")
        or ""
    ) or "unknown"
    actual = str(
        raw.get("actual_type")
        or raw.get("source_type")
        or raw.get("inferred_type")
        or ""
    ) or "unknown"
    src_pk = _extract_pk(raw, side="source")
    dst_pk = _extract_pk(raw, side="destination")
    retry = str(raw.get("retry_status") or default_retry_status or "open").lower()
    if retry not in RETRY_STATUSES:
        retry = "open"
    job = str(raw.get("job_id") or job_id or "").strip()
    conn = str(raw.get("connector") or connector or "").strip() or "unknown"

    out = dict(raw)
    out["original_value"] = _extract_original_value(raw)
    out["expected_type"] = expected
    out["actual_type"] = actual
    out["failure_reason"] = reason
    # Keep legacy reason aligned for older UI.
    out.setdefault("reason", reason)
    out["transform_attempted"] = transform_s
    out["recovery_suggestion"] = str(
        raw.get("recovery_suggestion")
        or raw.get("suggested_fix")
        or _suggest_recovery(reason, transform=transform_s)
    )
    out["source_pk"] = src_pk
    out["destination_pk"] = dst_pk
    out["source_pk_proven"] = src_pk is not None
    out["destination_pk_proven"] = dst_pk is not None
    out["job_id"] = job
    out["connector"] = conn
    out["retry_status"] = retry
    out.setdefault("quarantine_contract_version", 1)
    out.setdefault("normalized_at", _now())
    return out


def normalize_quarantine_rows(
    rows: list[dict[str, Any]] | None,
    *,
    job_id: str = "",
    connector: str = "",
) -> list[dict[str, Any]]:
    return [
        normalize_quarantine_row(r, job_id=job_id, connector=connector)
        for r in (rows or [])
        if isinstance(r, dict)
    ]


def quarantine_row_missing_fields(row: dict[str, Any] | None) -> list[str]:
    """Return required contract keys that are absent (empty string job_id counts as missing)."""
    r = row if isinstance(row, dict) else {}
    missing: list[str] = []
    for key in REQUIRED_QUARANTINE_FIELDS:
        if key not in r:
            missing.append(key)
            continue
        if key in {"job_id", "connector", "failure_reason", "recovery_suggestion"}:
            if not str(r.get(key) or "").strip():
                missing.append(key)
    return missing


def assert_quarantine_rows_contract(
    rows: list[dict[str, Any]] | None,
    *,
    require_job_id: bool = True,
) -> None:
    """Fail closed when durable quarantine rows lack job_id (replay would find nothing)."""
    for i, row in enumerate(rows or []):
        if not isinstance(row, dict):
            raise QuarantineRowContractError(f"quarantine row[{i}] is not an object")
        missing = quarantine_row_missing_fields(row)
        if not require_job_id:
            missing = [m for m in missing if m != "job_id"]
        # PK may be null with proven=false — that is allowed; only total absence of key is bad.
        missing = [m for m in missing if m not in {"source_pk", "destination_pk", "original_value"}]
        if missing:
            raise QuarantineRowContractError(
                f"quarantine row[{i}] missing contract fields: {', '.join(missing)}"
            )
