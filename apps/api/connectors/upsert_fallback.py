"""Generic SQL upsert fallback: UPDATE-then-INSERT, never DELETE, and say so.

When a dialect's native MERGE / ON CONFLICT cannot run (no unique index on
the key, a missing privilege for the stage table, an old server) the generic
SQL writer used to DELETE the matching keys and re-INSERT the rows. That
reset every destination column the mapping does not write, fired ON DELETE
CASCADE / delete triggers, and burned identity values — and the switch was
logged at DEBUG only. The fallback is now ``merge_dialects.update_insert_upsert``
(UPDATE matched keys' mapped columns, INSERT new keys) and the run carries a
warning naming the dialect and why native upsert was not used.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_INFO_KEY = "df_upsert_fallback"


def note_native_upsert_failed(table_obj: Any, dialect_name: str, exc: BaseException) -> None:
    """Record (once per table write) that native upsert failed on this table."""
    info = getattr(table_obj, "info", None)
    name = getattr(table_obj, "name", "table")
    # SQLAlchemy errors carry ``[SQL: …]`` and ``[parameters: …]`` (row values)
    # after the first line: WARNING gets the type and first line only.
    reason = _first_line(exc)
    logger.warning(
        "native %s upsert unavailable on %s; using UPDATE-then-INSERT (%s: %s)",
        dialect_name, name, type(exc).__name__, reason,
    )
    logger.debug("native %s upsert failure on %s: %s", dialect_name, name, exc, exc_info=exc)
    if isinstance(info, dict) and _INFO_KEY not in info:
        info[_INFO_KEY] = (
            f"Native {dialect_name} upsert (MERGE) was not available on {name} "
            f"({reason}); rows were written with UPDATE of matched keys then "
            "INSERT of new keys. Add a unique index on the key for a native, "
            "atomic upsert."
        )


def _first_line(exc: BaseException) -> str:
    orig = getattr(exc, "orig", None)  # the DB-API error, without SQL / parameters
    text = str(orig if orig is not None else exc).strip()
    return text.splitlines()[0][:200] if text else type(exc).__name__


def upsert_fallback_warnings(table_obj: Any) -> list[str]:
    """The job warning for a write whose native upsert fell back, if any."""
    info = getattr(table_obj, "info", None)
    note = info.get(_INFO_KEY) if isinstance(info, dict) else None
    return [note] if note else []
