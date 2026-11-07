"""What an absent attribute of a schemaless source means in a relational table (D262).

A batch read of a schemaless source (Mongo, DynamoDB, Redis, Elasticsearch,
Couchbase) returns whole records; an attribute one record lacks — never set, or
``$unset`` since the last run — is known absent. Into a relational destination
that absence is NULL: the destination column has no other spelling of
"not there", and leaving the previous value in place on upsert kept a stale
``note`` after the source dropped it. A CDC page carries partial change images
and keeps the "left alone" rule; a schemaless destination can omit the field
itself.

The same rule is announced at plan / preflight time, because for a
sparse-by-design item store this is a visible change on upsert.
"""

from __future__ import annotations

from typing import Any

WARNING_ID = "schemaless_absent_attribute_null"

ABSENT_ATTRIBUTE_NULL_NOTE = (
    "Absent attributes null the destination column on upsert: a source record "
    "that does not carry a mapped attribute writes NULL there (a $unset / "
    "removed field is NULL after the run, not the previous value). CDC change "
    "streams keep untouched columns as they are."
)


def absent_attribute_nulls_column(src_type: str, dest_type: str, sync_mode: str) -> bool:
    """True when a batch from ``src_type`` into ``dest_type`` writes NULL for absence."""
    from connectors.header_union import SCHEMALESS_SOURCE_TYPES
    from services.db_type_utils import SCHEMALESS_DESTS
    from services.sync_cursor import normalize_sync_mode

    src = str(src_type or "").strip().lower()
    if src not in SCHEMALESS_SOURCE_TYPES:
        return False
    if normalize_sync_mode(str(sync_mode or ""), default="") == "cdc":
        return False
    return str(dest_type or "").strip().lower() not in SCHEMALESS_DESTS


def attach_absent_attribute_notice(
    result: dict[str, Any], src_type: str, dest_type: str, sync_mode: str
) -> None:
    """Add the disclosure to a preflight / plan result when the rule applies."""
    if not absent_attribute_nulls_column(src_type, dest_type, sync_mode):
        return
    warnings = result.setdefault("warnings", [])
    if not isinstance(warnings, list):
        return
    if any(isinstance(w, dict) and w.get("id") == WARNING_ID for w in warnings):
        return
    warnings.append({"id": WARNING_ID, "message": ABSENT_ATTRIBUTE_NULL_NOTE})
