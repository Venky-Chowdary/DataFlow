"""PostgreSQL type mapping for canonical schema discovery."""

from __future__ import annotations

import re


def _pg_elem_to_logical(elem: str) -> str:
    """Map a PG array element type to a logical carrier (no array recursion)."""
    e = (elem or "").strip().lower()
    if not e:
        return "VARCHAR"
    # Avoid re-entering array branch — strip one level only.
    if e.endswith("[]"):
        e = e[:-2].strip()
    m = re.match(r"^(numeric|decimal)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$", e)
    if m:
        if m.group(3) is not None:
            return f"DECIMAL({m.group(2)},{m.group(3)})"
        return f"DECIMAL({m.group(2)})"
    if e in {"integer", "int", "int4", "smallint", "int2", "bigint", "int8", "serial", "bigserial"}:
        return "INTEGER"
    if e in {"real", "float4", "double precision", "float8"}:
        return "FLOAT"
    if e in {"boolean", "bool"}:
        return "BOOLEAN"
    if e == "date":
        return "DATE"
    if e.startswith("timestamp"):
        return "TIMESTAMPTZ" if "with time zone" in e or e.endswith("tz") else "TIMESTAMP_NTZ"
    if e.startswith("time"):
        if "with time zone" in e or e in {"timetz", "time tz"}:
            return "TIMETZ"
        return "TIME"
    if e in {"uuid"}:
        return "UUID"
    if e in {"json", "jsonb"}:
        return "JSONB" if e == "jsonb" else "JSON"
    if e in {"bytea", "varbyte"}:
        return "BINARY"
    if e in {"text", "varchar", "character varying", "character", "char", "citext", "name"}:
        return "VARCHAR" if e != "citext" else "CITEXT"
    if e.startswith("character varying") or e.startswith("varchar") or e.startswith("character("):
        return "VARCHAR"
    # Specialty scalars (inet, point, pg_lsn, …) — reuse scalar mapper so
    # ARRAY<INET> survives introspect (Airbyte historically emitted untyped arrays).
    scalar = pg_to_logical(e)
    if scalar.startswith("ARRAY<") or scalar.endswith("[]"):
        return "VARCHAR"
    return scalar


def pg_to_logical(dtype: str) -> str:
    """Map PostgreSQL ``format_type`` / data_type strings to Datawrap logical carriers.

    Parametric types keep their dimensions in the type string (DECIMAL(p,s),
    VECTOR(n)) so ``ddl_type`` can propagate them — same contract as DECIMAL.
    INTERVAL / GEOGRAPHY / VECTOR are first-class; they must not collapse to TEXT.
    """
    raw = (dtype or "").strip()
    d = raw.lower()

    # DECIMAL / NUMERIC with typmod — preserve (p,s) for transfer fidelity.
    m = re.match(r"^(numeric|decimal)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$", d)
    if m:
        if m.group(3) is not None:
            return f"DECIMAL({m.group(2)},{m.group(3)})"
        return f"DECIMAL({m.group(2)})"

    # pgvector / halfvec — preserve dimension.
    m = re.match(r"^(vector|halfvec|sparsevec)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        return f"VECTOR({m.group(2)})"
    if d in {"vector", "halfvec", "sparsevec"}:
        return "VECTOR"

    if d == "interval" or d.startswith("interval "):
        # Preserve YM vs DS polarity — bare INTERVAL stays unqualified.
        if "year" in d and "month" in d:
            return "INTERVAL YEAR TO MONTH"
        if any(tok in d for tok in ("day", "second", "hour", "minute")):
            return "INTERVAL DAY TO SECOND"
        return "INTERVAL"
    if d.startswith("geography("):
        # Keep typmod/SRID (geography(Point,4326)) for contract checks.
        return f"GEOGRAPHY{raw[raw.lower().index('('):]}" if "(" in raw else "GEOGRAPHY"
    if d.startswith("geometry("):
        return f"GEOMETRY{raw[raw.lower().index('('):]}" if "(" in raw else "GEOMETRY"
    if d in {"geometry", "geography"}:
        return "GEOGRAPHY" if d == "geography" else "GEOMETRY"

    if d in ("serial", "bigserial", "smallserial"):
        return d.upper()
    # System / WAL identifiers — preserve native carriers (never invent INTEGER/TEXT).
    if d == "oid":
        return "OID"
    if d == "xid8":
        return "XID8"
    if d == "xid":
        return "XID"
    if d == "cid":
        return "CID"
    if d == "tid":
        return "TID"
    if d == "pg_lsn":
        return "PG_LSN"
    if d in ("integer", "smallint", "bigint"):
        return "INTEGER"
    # IEEE floats stay FLOAT — never silently rewrite to fixed-point DECIMAL.
    if d in ("real", "double precision", "double", "float", "float4", "float8"):
        return "FLOAT"
    if d == "money":
        # PostgreSQL money ≈ fixed-scale currency — mirror SQL Server MONEY fidelity.
        return "DECIMAL(19,4)"
    if d in ("numeric", "decimal"):
        return "DECIMAL"
    if d == "boolean":
        return "BOOLEAN"
    if d == "date":
        return "DATE"
    if d == "citext":
        # citext is case-insensitive by type — preserve for uniqueness / Gate-8.
        return "CITEXT"
    # Preserve TZ polarity + fractional-second precision (timestamp(6), time(3)).
    # format_type may emit timestamptz(3) or "timestamp(3) with time zone".
    if "timestamp" in d:
        m = re.search(r"timestamptz\s*\(\s*(\d+)\s*\)", d)
        if m:
            return f"TIMESTAMPTZ({m.group(1)})"
        m = re.search(r"timestamp\s*\(\s*(\d+)\s*\)", d)
        fsp = m.group(1) if m else None
        if (
            "with time zone" in d
            or d == "timestamptz"
            or d.startswith("timestamptz")
        ):
            return f"TIMESTAMPTZ({fsp})" if fsp else "TIMESTAMPTZ"
        return f"TIMESTAMP_NTZ({fsp})" if fsp else "TIMESTAMP_NTZ"
    if (
        d in {"timetz", "time tz"}
        or d.startswith("timetz")
        or (d.startswith("time") and "with time zone" in d and "without" not in d)
    ):
        m = re.search(r"time(?:tz)?\s*\(\s*(\d+)\s*\)", d)
        fsp = m.group(1) if m else None
        return f"TIMETZ({fsp})" if fsp else "TIMETZ"
    if d == "time" or d.startswith("time without") or d.startswith("time("):
        m = re.search(r"time\s*\(\s*(\d+)\s*\)", d)
        fsp = m.group(1) if m else None
        return f"TIME({fsp})" if fsp else "TIME"
    if d == "uuid":
        return "UUID"
    if d == "bytea":
        return "BINARY"
    # Redshift SUPER / VARBYTE (exposed via PG wire format_type).
    if d == "super":
        return "JSON"
    if d == "varbyte" or d.startswith("varbyte("):
        return "BINARY"
    # Typed arrays — preserve element carrier (never invent bare JSON).
    if d.endswith("[]"):
        elem = d[:-2].strip()
        elem_logical = _pg_elem_to_logical(elem)
        return f"ARRAY<{elem_logical}>"
    if " array" in d:
        elem = d.replace(" array", "").strip()
        elem_logical = _pg_elem_to_logical(elem)
        return f"ARRAY<{elem_logical}>"
    if d == "jsonpath":
        # PostgreSQL jsonpath — path expression type (never invent TEXT).
        return "JSONPATH"
    if d in {"json", "jsonb"}:
        # Preserve JSONB polarity — bind SSOT uses native structures on PG.
        return "JSONB" if d == "jsonb" else "JSON"
    if d == "hstore":
        return "HSTORE"
    if d == "ltree":
        return "LTREE"
    if d == "xml":
        return "XML"
    if d == "tsvector":
        return "TSVECTOR"
    if d == "tsquery":
        return "TSQUERY"
    if d == "point":
        return "POINT"
    if d == "line":
        return "LINE"
    if d == "lseg":
        return "LSEG"
    if d == "box":
        return "BOX"
    if d == "path":
        return "PATH"
    if d == "polygon":
        return "POLYGON"
    if d == "circle":
        return "CIRCLE"
    if d == "inet":
        return "INET"
    if d == "cidr":
        return "CIDR"
    if d == "macaddr":
        return "MACADDR"
    if d == "macaddr8":
        return "MACADDR8"
    if d.endswith("multirange"):
        return d.upper()
    if d.endswith("range") and d not in {"range"}:
        return d.upper()  # int4range, tstzrange, …
    if (
        d.startswith("bit(")
        or d == "bit"
        or d.startswith("bit varying")
        or d.startswith("varbit")
    ):
        # BIT(1) → boolean via type_system; BIT(n>1)/VARBIT → bitstring binary.
        m = re.match(r"^bit\s+varying\s*\(\s*(\d+)\s*\)$", d)
        if m:
            return f"BIT VARYING({m.group(1)})"
        m = re.match(r"^varbit\s*\(\s*(\d+)\s*\)$", d)
        if m:
            return f"VARBIT({m.group(1)})"
        m = re.match(r"^bit\s*\(\s*(\d+)\s*\)$", d)
        if m:
            return f"BIT({m.group(1)})"
        if d in {"bit varying", "varbit"}:
            return "BIT VARYING"
        return "BIT"
    if d in {"text", "name", "user-defined"}:
        return "TEXT"
    if d == "txid_snapshot":
        return "TXID_SNAPSHOT"
    if d == "pg_snapshot":
        return "PG_SNAPSHOT"
    # Preserve declared width — VARCHAR(n)/CHAR(n) must not collapse to TEXT
    # (G3 width-narrow + write quarantine depend on parametric carriers).
    m = re.match(r"^(?:character\s+varying|varchar)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        return f"VARCHAR({m.group(1)})"
    m = re.match(r"^(?:character|char)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        return f"CHAR({m.group(1)})"
    if d in {"character varying", "varchar"}:
        return "VARCHAR"
    if d in {"character", "char"}:
        return "CHAR"
    if d.startswith("character varying") or d.startswith("varchar") or d.startswith("character("):
        return "VARCHAR"
    return "TEXT"
