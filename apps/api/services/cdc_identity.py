"""CDC identity helpers — never invent a default primary key name."""

from __future__ import annotations

from typing import Any


def require_cdc_primary_key(
    primary_key: Any = None,
    *,
    table: str = "",
    primary_keys: dict[str, Any] | None = None,
) -> str | list[str]:
    """Return an explicit CDC primary-key column name or composite list.

    Empty / missing config must not fall back to ``\"id\"`` / ``\"ID\"`` — that
    invents wrong-row upsert/delete identity under at-least-once CDC when the
    real key is composite or differently named.

    Composite keys (``list`` / ``tuple`` / comma-joined string) are preserved —
    never ``str(list)`` mangled into a bogus single column.
    """
    per_table: Any = None
    if primary_keys and table:
        per_table = primary_keys.get(table)
    raw = per_table if per_table not in (None, "") else primary_key
    if isinstance(raw, (list, tuple)):
        cols = [str(c).strip() for c in raw if str(c).strip()]
        if not cols:
            where = f" for table {table!r}" if table else ""
            raise ValueError(
                f"CDC requires an explicit primary_key{where} — refuse inventing "
                "default 'id' (wrong-row upsert under at-least-once)"
            )
        return cols if len(cols) > 1 else cols[0]
    key = str(raw or "").strip()
    if not key:
        where = f" for table {table!r}" if table else ""
        raise ValueError(
            f"CDC requires an explicit primary_key{where} — refuse inventing "
            "default 'id' (wrong-row upsert under at-least-once)"
        )
    if "," in key or ";" in key:
        cols = [p.strip() for p in key.replace(";", ",").split(",") if p.strip()]
        if not cols:
            raise ValueError(
                "CDC primary_key empty after split — refuse inventing default 'id'"
            )
        return cols if len(cols) > 1 else cols[0]
    return key


def require_cdc_primary_keys_map(
    tables: list[str],
    *,
    primary_key: Any = None,
    primary_keys: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Resolve per-table CDC primary keys as connector string names.

    Change-stream constructors store a single string identity (comma-joined
    when composite). Every table must resolve — never invent ``\"id\"``.
    """
    out: dict[str, str] = {}
    for t in tables:
        resolved = require_cdc_primary_key(
            primary_key, table=t, primary_keys=primary_keys
        )
        if isinstance(resolved, list):
            out[t] = ",".join(resolved)
        else:
            out[t] = resolved
    return out
