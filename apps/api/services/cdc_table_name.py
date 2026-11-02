"""One owner for the ``(schema, table)`` a CDC reader attaches to (QA D223).

A user names a SQL Server source ``dbo.qah_cdc0010``; the connector also
carries ``schema="dbo"``. Every CDC reader and the Validate probe joined the
two again, so the capture check looked for ``dbo.dbo.qah_cdc0010`` and blocked
the run as "change data capture is not enabled" although CDC was on. The plain
name worked.

``split_cdc_table`` splits a qualified name exactly once, outside quotes
(``[dbo].[t]``, ``"public"."t"``, ```shop`.`t```): a name's own qualifier wins
over the connector default, a bare name takes the default, and a quoted name
that contains a dot stays one name.
"""

from __future__ import annotations

_QUOTES = {'"': '"', "[": "]", "`": "`"}


def _parts(name: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    closing = ""
    i = 0
    while i < len(name):
        ch = name[i]
        if closing:
            if ch == closing:
                if i + 1 < len(name) and name[i + 1] == closing and closing != "]":
                    buf.append(ch)  # doubled quote inside a quoted name
                    i += 2
                    continue
                closing = ""
            else:
                buf.append(ch)
        elif ch in _QUOTES:
            closing = _QUOTES[ch]
        elif ch == ".":
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    parts.append("".join(buf).strip())
    return parts


def split_cdc_table(table: str, schema: str = "", *, dialect: str = "") -> tuple[str, str]:
    """``(schema, table)`` with the qualifier taken from ``table`` when it has one.

    ``dialect`` is accepted for call-site clarity; the rule is the same on
    every engine (MySQL's "schema" is the database). A three-part SQL Server
    name (``db.schema.table``) keeps its last two parts.
    """
    del dialect
    raw = str(table or "").strip()
    default = str(schema or "").strip()
    if not raw:
        return default, raw
    parts = [p for p in _parts(raw)]
    if len(parts) < 2 or any(not p for p in parts):
        return default, parts[-1] if len(parts) == 1 else raw
    return parts[-2], parts[-1]


def split_cdc_tables(
    tables: list[str], schema: str = "", *, dialect: str = ""
) -> tuple[str, list[str]]:
    """Multi-table form: one schema for the reader, bare table names.

    Tables qualified with two different schemas cannot share one reader's
    schema, so that is refused by name instead of silently attaching to one.
    """
    resolved: list[tuple[str, str]] = [
        split_cdc_table(t, schema, dialect=dialect) for t in tables
    ]
    schemas = {s.lower() for s, _t in resolved if s}
    if len(schemas) > 1:
        raise ValueError(
            "one CDC reader attaches to one schema; these tables name several: "
            + ", ".join(tables)
        )
    chosen = next((s for s, _t in resolved if s), str(schema or "").strip())
    return chosen, [t for _s, t in resolved]


def bare_table_keys(mapping: dict | None, schema: str = "", *, dialect: str = "") -> dict | None:
    """Per-table maps (primary keys) keyed by the bare names too."""
    if not mapping:
        return mapping
    out = dict(mapping)
    for key, value in mapping.items():
        out.setdefault(split_cdc_table(str(key), schema, dialect=dialect)[1], value)
    return out
