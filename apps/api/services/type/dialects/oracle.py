"""Oracle type mapping for canonical schema discovery."""

from __future__ import annotations

import re


def oracle_to_logical(dtype: str) -> str:
    """Map Oracle data_type (+ optional precision/scale) to logical carriers.

    NUMBER(p,0) → INTEGER when p ≤ 18; else DECIMAL(p,0). NUMBER(p,s) → DECIMAL(p,s).
    BINARY_FLOAT/DOUBLE → FLOAT. Oracle DATE includes time-of-day → TIMESTAMP.
    """
    raw = (dtype or "").strip()
    d = raw.upper().replace(" ", "")
    # NUMBER(p,s) / FLOAT(p) carriers
    m = re.match(r"^NUMBER\((\d+)(?:,(\d+))?\)$", d)
    if m:
        from services.type_system import zero_scale_numeric_carrier

        if m.group(2) is not None and int(m.group(2)) == 0:
            return zero_scale_numeric_carrier(int(m.group(1)))
        if m.group(2) is not None:
            return f"DECIMAL({m.group(1)},{m.group(2)})"
        return f"DECIMAL({m.group(1)})"
    if d == "NUMBER" or d.startswith("NUMBER("):
        return "DECIMAL"
    if d in {"BINARY_FLOAT", "BINARY_DOUBLE"} or d.startswith("FLOAT"):
        return "FLOAT"
    if d in {"INTEGER", "INT", "SMALLINT", "BIGINT"}:
        return "INTEGER"
    if d == "BOOLEAN":
        return "BOOLEAN"
    if d == "DATE":
        return "TIMESTAMP"  # Oracle DATE is datetime
    if "TIMESTAMP" in d:
        if "WITHLOCALTIMEZONE" in d:
            return "TIMESTAMP_LTZ"
        if "WITHTIMEZONE" in d:
            return "TIMESTAMP_TZ"
        return "TIMESTAMP_NTZ"
    if "INTERVAL" in raw.upper():
        # Preserve Oracle leading-field / fractional-second precision
        # (INTERVAL DAY(3) TO SECOND(6) — ANSI/Oracle contract).
        raw_u = re.sub(r"\s+", " ", raw.upper()).strip()
        m_ym = re.match(
            r"INTERVAL YEAR(?:\((\d+)\))? TO MONTH(?:\((\d+)\))?",
            raw_u,
        )
        if m_ym:
            if m_ym.group(1):
                return f"INTERVAL YEAR({m_ym.group(1)}) TO MONTH"
            return "INTERVAL YEAR TO MONTH"
        m_ds = re.match(
            r"INTERVAL DAY(?:\((\d+)\))? TO SECOND(?:\((\d+)\))?",
            raw_u,
        )
        if m_ds:
            if m_ds.group(1) or m_ds.group(2):
                day_p = m_ds.group(1) or "2"
                sec_p = m_ds.group(2) or "6"
                return f"INTERVAL DAY({day_p}) TO SECOND({sec_p})"
            return "INTERVAL DAY TO SECOND"
        if "YEAR" in raw_u and "MONTH" in raw_u:
            return "INTERVAL YEAR TO MONTH"
        if any(tok in raw_u for tok in ("DAY", "SECOND", "HOUR", "MINUTE")):
            return "INTERVAL DAY TO SECOND"
        return "INTERVAL"
    # VARCHAR2(n BYTE|CHAR) — BYTE is Oracle default; multi-byte UTF-8 can
    # truncate under BYTE while CHAR semantics still fit (Informatica-class bug).
    m = re.match(r"^(VARCHAR2|NVARCHAR2|CHAR|NCHAR)\((\d+)(?:(BYTE|CHAR))?\)$", d)
    if m:
        fixed = m.group(1) in {"CHAR", "NCHAR"}
        national = m.group(1) in {"NVARCHAR2", "NCHAR"}
        if national:
            prefix = "NCHAR" if fixed else "NVARCHAR"
        else:
            prefix = "CHAR" if fixed else "VARCHAR"
        unit = (m.group(3) or "").upper()
        if unit in {"BYTE", "CHAR"}:
            return f"{prefix}({m.group(2)} {unit})"
        return f"{prefix}({m.group(2)})"
    if d in {"CLOB", "NCLOB", "LONG", "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "VARCHAR"}:
        return "TEXT"
    if d in {"BLOB", "RAW", "LONGRAW", "BFILE"} or d.startswith("RAW("):
        return "BINARY"
    if d == "JSON":
        return "JSON"
    if d in {"XMLTYPE", "XML"}:
        return "XMLTYPE"
    if d in {"ROWID", "UROWID"}:
        return d
    if "SDO_GEOMETRY" in d:
        return "SDO_GEOMETRY"
    if d == "GEOGRAPHY":
        return "GEOGRAPHY"
    if d == "GEOMETRY":
        return "GEOMETRY"
    return "TEXT"
