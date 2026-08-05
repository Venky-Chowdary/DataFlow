"""MySQL type mapping for canonical schema discovery."""

from __future__ import annotations

import re


def mysql_to_logical(dtype: str) -> str:
    """Map MySQL ``column_type`` to logical carriers, preserving DECIMAL(p,s)."""
    raw = (dtype or "").strip()
    d_early = raw.lower().strip()
    # BIT(n) before text fallback — BIT(1)→boolean via type_system; BIT(n>1)
    # is a bitstring (never opaque TEXT — wave 71).
    if (
        d_early.startswith("bit(")
        or d_early == "bit"
        or d_early.startswith("bit varying")
    ):
        m = re.match(r"^bit\s*\(\s*(\d+)\s*\)$", d_early)
        if m:
            return f"BIT({m.group(1)})"
        return "BIT"
    d = raw.lower()
    if "tinyint(1)" in d:
        return "BOOLEAN"
    # Preserve DECIMAL(p,s) / NUMERIC(p,s) from column_type for ddl_type propagation.
    m = re.match(r"^(decimal|numeric)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)", d)
    if m:
        if m.group(3) is not None:
            return f"DECIMAL({m.group(2)},{m.group(3)})"
        return f"DECIMAL({m.group(2)})"
    if d.startswith("decimal") or d.startswith("numeric"):
        return "DECIMAL"
    if any(tok in d for tok in ("geometry", "point", "polygon", "linestring", "multipoint",
                                  "multipolygon", "multilinestring", "geomcollection")):
        return "GEOGRAPHY"
    # BIGINT UNSIGNED exceeds signed 64-bit — DECIMAL carrier (matches type_system CANONICAL).
    # Must run BEFORE the generic "int" branch ("int" is a substring of "bigint").
    # Preserve ALL unsigned widths so Map/preflight can auto-widen / range-check.
    if "unsigned" in d:
        if "bigint" in d:
            return "BIGINT UNSIGNED"
        if "mediumint" in d:
            return "MEDIUMINT UNSIGNED"
        if "smallint" in d:
            return "SMALLINT UNSIGNED"
        if "tinyint" in d and "tinyint(1)" not in d:
            return "TINYINT UNSIGNED"
        if "int" in d:
            return "INT UNSIGNED"
    if d == "year" or d.startswith("year("):
        # MySQL YEAR — keep carrier so write quarantine enforces 1901–2155 / 0000
        # (non-strict MySQL silently stores invalid years as 0000).
        return "YEAR"
    # Preserve MEDIUMINT range (−8388608..8388607) for bind quarantine.
    if "mediumint" in d:
        return "MEDIUMINT"
    if "int" in d:
        return "INTEGER"
    # IEEE float/double/real — distinct from DECIMAL(p,s).
    if "double" in d or "float" in d or "real" in d:
        return "FLOAT"
    if "bool" in d:
        return "BOOLEAN"
    if d == "date":
        return "DATE"
    # MySQL TIMESTAMP is session-TZ aware; DATETIME is wall-clock NTZ.
    # Preserve fractional-second precision (datetime(6), time(3), timestamp(2)).
    if "timestamp" in d and "datetime" not in d:
        m = re.search(r"timestamp\s*\(\s*(\d+)\s*\)", d)
        return f"TIMESTAMPTZ({m.group(1)})" if m else "TIMESTAMPTZ"
    if "datetime" in d:
        m = re.search(r"datetime\s*\(\s*(\d+)\s*\)", d)
        return f"TIMESTAMP_NTZ({m.group(1)})" if m else "TIMESTAMP_NTZ"
    if d.startswith("time"):
        m = re.search(r"time\s*\(\s*(\d+)\s*\)", d)
        return f"TIME({m.group(1)})" if m else "TIME"
    if "json" in d:
        return "JSON"
    # Preserve BINARY(n)/VARBINARY(n) — G3 width-narrow + write quarantine.
    m = re.match(r"^(varbinary|binary)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        return f"{m.group(1).upper()}({m.group(2)})"
    if "binary" in d or "blob" in d or "varbinary" in d:
        return "BINARY"
    if "uuid" in d:
        return "UUID"
    # MySQL ENUM/SET domains — keep members so G3/write can fail-closed.
    m = re.match(r"^(enum|set)\s*\((.*)\)$", d, re.I | re.DOTALL)
    if m:
        return f"{m.group(1).upper()}({m.group(2).strip()})"
    # Preserve MySQL column_type widths (varchar(255), char(10), …).
    m = re.match(r"^(varchar|char)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        return f"{m.group(1).upper()}({m.group(2)})"
    if d in {"varchar", "char"}:
        return d.upper()
    if "text" in d:
        return "TEXT"
    return "TEXT"
