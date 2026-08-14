"""Conservative tombstone polarity — one owner for CDC and keyed upsert.

A false positive here converts a live destination row into a DELETE. Two
rules that used to exist have been removed and must not return:

* **Liveness columns are not tombstones.** ``is_active`` with no polarity
  handling deleted every row where ``is_active = 1`` and kept the inactive
  ones — a complete inversion. An inactive source row still *exists*; an
  operator who wants that behaviour must configure it explicitly.
* **Substring matching is gone.** ``"delete" in name`` captured
  ``deleted_by`` and ``delete_count``. Matching is exact, never substring.

Fail closed on ambiguity: an unrecognised boolean token is *present*, not
deleted. Refusing to delete is recoverable (a later sync corrects a stale
row); deleting on a guess is not.

Debezium envelope rows carry ``__deleted`` / ``__op`` in {d, delete}. Those
are explicit event flags, not business columns. Bare ``op`` is not read —
an orders table with ``op = 'delete'`` as a payload is still a live row.

Soft-delete *mirror* (``_deleted`` flag, COUNT stays) is a different
identity and is not this module. This polarity feeds **hard** DELETE apply
so dest ``COUNT(*)`` can drop, which Airbyte incremental often never does
and Fivetran inferred deletes usually do not (``_fivetran_deleted``).
"""

from __future__ import annotations

import logging
from typing import Any, Mapping, Sequence

logger = logging.getLogger(__name__)

# Exact column names whose truthy value means "this row is deleted".
TOMBSTONE_COLUMNS = frozenset(
    {
        "deleted",
        "deleted_at",
        "deletedat",
        "deleted_on",
        "deleted_ts",
        "deleted_time",
        "date_deleted",
        "is_deleted",
        "isdeleted",
        "deleted_flag",
        "row_deleted",
        "tombstone",
        "is_tombstone",
        "_deleted",
        "__deleted",
    }
)

#: Columns that look deletion-adjacent but are audit metadata, not tombstones.
TOMBSTONE_LOOKALIKES = frozenset(
    {
        "deleted_by",
        "deleted_by_id",
        "deleted_by_user",
        "deleted_reason",
        "delete_count",
        "deletes",
        "deletable",
        "is_deletable",
        "can_delete",
        "soft_delete_enabled",
    }
)

_FALSEY_TOKENS = frozenset({"", "0", "false", "f", "no", "n", "null", "none", "nan"})
_TRUTHY_TOKENS = frozenset({"1", "true", "t", "yes", "y"})
_DELETE_OPS = frozenset({"d", "delete"})

#: Timestamp-style tombstones follow ``deleted_at IS NULL``: any concrete
#: instant means deleted. Boolean-style columns do not guess.
TIMESTAMP_TOMBSTONES = frozenset(
    {
        "deleted_at",
        "deletedat",
        "deleted_on",
        "deleted_ts",
        "deleted_time",
        "date_deleted",
    }
)


def detect_tombstone_column(
    schema: Mapping[str, str] | None,
    columns: Sequence[str],
) -> str | None:
    """Return a soft-delete column name if one is unambiguously present."""
    del schema  # Detection is by name; type is validated at interpretation time.
    for col in columns:
        lowered = (col or "").strip().lower()
        if not lowered or lowered in TOMBSTONE_LOOKALIKES:
            continue
        if lowered in TOMBSTONE_COLUMNS:
            return col
    return None


def looks_like_timestamp(text: str) -> bool:
    """Whether a value is a real instant rather than a zero/sentinel date.

    ``0000-00-00`` and friends are MySQL's "no date" sentinels; reading them
    as a deletion timestamp would delete every row that was never soft-deleted.
    """
    stripped = (text or "").strip()
    if not stripped:
        return False
    if set(stripped) <= {"0", "-", ":", " ", "/", "."}:
        return False
    return True


def is_tombstone_set(record: Mapping[str, Any], tombstone_column: str) -> bool:
    """Whether ``record`` is marked deleted by its soft-delete column."""
    if not tombstone_column:
        return False
    value = record.get(tombstone_column)
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in _FALSEY_TOKENS:
        return False
    if text in _TRUTHY_TOKENS:
        return True
    if tombstone_column.strip().lower() in TIMESTAMP_TOMBSTONES:
        return looks_like_timestamp(text)
    logger.warning(
        "Soft-delete column %r held unrecognised value %r; treating the row as "
        "present rather than deleting it at the destination.",
        tombstone_column,
        text[:64],
    )
    return False


def is_row_tombstone(record: Mapping[str, Any] | None) -> bool:
    """Whether this row is a hard-delete event, not a live upsert.

    ``__deleted`` on the row is the CDC envelope and wins over other columns
    so an explicit ``__deleted=False`` cannot be overridden by a coincidental
    ``deleted_at`` business column on the same payload.
    """
    row = dict(record or {})
    if not row:
        return False
    if "__deleted" in row:
        return is_tombstone_set(row, "__deleted")
    op = row.get("__op")
    if op is not None and str(op).strip().lower() in _DELETE_OPS:
        return True
    column = detect_tombstone_column({}, list(row.keys()))
    if not column:
        return False
    return is_tombstone_set(row, column)
