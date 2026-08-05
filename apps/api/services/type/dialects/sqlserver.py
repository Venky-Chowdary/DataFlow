"""SQL Server type mapping for canonical schema discovery."""

from __future__ import annotations

import re


def sqlserver_to_logical(dtype: str) -> str:
    """Map SQL Server type_name (+ optional (p,s)) to logical carriers."""
    raw = (dtype or "").strip()
    d = raw.lower()
    m = re.match(r"^(decimal|numeric)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$", d)
    if m:
        from services.type_system import zero_scale_numeric_carrier

        if m.group(3) is not None and int(m.group(3)) == 0:
            return zero_scale_numeric_carrier(int(m.group(2)))
        if m.group(3) is not None:
            return f"DECIMAL({m.group(2)},{m.group(3)})"
        return f"DECIMAL({m.group(2)})"
    if d.startswith("decimal") or d.startswith("numeric"):
        return "DECIMAL"
    if d == "money":
        return "MONEY"  # → DECIMAL(19,4) via normalize/ddl; keep token for money quarantine
    if d == "smallmoney":
        return "SMALLMONEY"
    if d in {"float", "real"}:
        return "FLOAT"
    if d in {"int", "bigint", "smallint", "tinyint"}:
        return "INTEGER"
    if d == "bit":
        return "BOOLEAN"
    if d == "date":
        return "DATE"
    if d == "time" or d.startswith("time("):
        m = re.search(r"time\s*\(\s*(\d+)\s*\)", d)
        return f"TIME({m.group(1)})" if m else "TIME"
    if d == "datetimeoffset" or d.startswith("datetimeoffset"):
        # Offset-pinned (SQL Server) — TIMESTAMP_TZ polarity, not session LTZ.
        m = re.search(r"datetimeoffset\s*\(\s*(\d+)\s*\)", d)
        return f"TIMESTAMP_TZ({m.group(1)})" if m else "TIMESTAMP_TZ"
    if d.startswith("datetime2"):
        m = re.search(r"datetime2\s*\(\s*(\d+)\s*\)", d)
        return f"TIMESTAMP_NTZ({m.group(1)})" if m else "TIMESTAMP_NTZ"
    # Minute accuracy + Microsoft rounding — keep carrier (wave 70).
    if d == "smalldatetime":
        return "SMALLDATETIME"
    if d in {"datetime"} or d.startswith("datetime"):
        return "TIMESTAMP_NTZ"
    if d == "uniqueidentifier":
        return "UUID"
    if d == "hierarchyid":
        # Path polarity `/1/2/` — PG create-new maps to LTREE (wave 67).
        return "HIERARCHYID"
    if d == "json":
        return "JSON"
    if d == "xml":
        # Preserve XML specialty (not opaque TEXT) — PG/Oracle XML bind SSOT.
        return "XML"
    if d == "sql_variant":
        # Opaque typed union — JSONB envelope on PG create-new (wave 69).
        return "SQL_VARIANT"
    if d == "geography":
        return "GEOGRAPHY"
    if d == "geometry":
        return "GEOMETRY"
    m = re.match(r"^(varbinary|binary)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        return f"{m.group(1).upper()}({m.group(2)})"
    # SQL Server TIMESTAMP is ROWVERSION (8-byte concurrency token), NOT datetime.
    # HVR/Estuary map it to BYTEA — never invent a clock type (wave 66).
    if d in {"rowversion", "timestamp"}:
        return "ROWVERSION"
    if d in {"binary", "varbinary", "image"}:
        return "BINARY"
    if "varbinary" in d and "(max)" in d:
        return "BINARY"
    if "binary" in d:
        return "BINARY"
    # Prefer parametric carriers when length was folded into dtype (e.g. varchar(50)).
    m = re.match(r"^(n?varchar|n?char)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        base = m.group(1).lower()
        width = m.group(2)
        if base.startswith("n"):
            return f"N{'CHAR' if 'char' in base and 'varchar' not in base else 'VARCHAR'}({width})"
        return f"{'CHAR' if base == 'char' else 'VARCHAR'}({width})"
    if d in {"text", "ntext"} or "(max)" in d:
        return "TEXT"
    if any(tok in d for tok in ("nvarchar", "varchar", "nchar", "char", "sysname")):
        if "char" in d and "varchar" not in d:
            return "CHAR"
        return "VARCHAR"
    return "TEXT"
