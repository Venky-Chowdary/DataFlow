"""Specialty carrier DDL — vector, spatial and bit-string destinations.

Split out of ``services.type_system`` unchanged. These three families share a
rule the rest of the type ladder does not: the destination either has a native
carrier for the source's *declared* parameters (embedding dimension, spatial
polarity and SRID, bit width) or it does not, and inventing a default there
silently changes what the column means — a 1536-wide embedding written into a
default-width vector, a GEOGRAPHY read back as planar GEOMETRY, a bit mask
packed into BYTEA. Each function returns ``None`` to hand the decision back to
the generic ladder rather than guessing.

``services.type_system`` re-exports every name here, so the historical import
surface is unchanged.
"""

from __future__ import annotations

import re
from typing import Final

from services.type_system import (
    DDL_TYPES,
    DEFAULT_DDL,
    LOGICAL_GEOGRAPHY,
    LOGICAL_STRING,
    LOGICAL_TEXT,
    LOGICAL_VECTOR,
    geometry_kind,
    is_bitstring_carrier,
    is_varying_bitstring_carrier,
    parse_bitstring_width,
    parse_geography_srid,
    spatial_polarity,
    vector_encoding_polarity,
)


# Engines that emit a true vector DDL type only when dimension is known.
_VECTOR_PARAM_TEMPLATES: Final[dict[str, str]] = {
    "postgresql": "vector({n})",
    "snowflake": "VECTOR(FLOAT, {n})",
}

# Platform upper bounds for declared vector dimensions (fail closed → text).
_VECTOR_DIM_CAPS: Final[dict[str, int]] = {
    "postgresql": 16000,  # pgvector practical upper bound
    "snowflake": 4096,
}


def parse_vector_dimension(inferred: str | None) -> int | None:
    """Extract embedding dimension from VECTOR / HALFVEC type strings.

    Accepted carriers (same spirit as DECIMAL(p,s) — params live in the type string):

    * ``VECTOR(1536)`` / ``vector(1536)`` / ``HALFVEC(768)``
    * ``VECTOR(FLOAT, 1536)`` / ``VECTOR(INT, 768)`` (Snowflake-style)
    """
    raw = (inferred or "").strip()
    if not raw:
        return None
    # VECTOR(FLOAT, n) / VECTOR(INT, n)
    m = re.match(
        r"^(?:half)?vec(?:tor)?\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*,\s*(\d+)\s*\)$",
        raw,
        re.IGNORECASE,
    )
    if m:
        dim = int(m.group(1))
        return dim if dim > 0 else None
    # VECTOR(n) / HALFVEC(n) / SPARSEVEC(n)
    m = re.match(
        r"^(?:half|sparse)?vec(?:tor)?\s*\(\s*(\d+)\s*\)$",
        raw,
        re.IGNORECASE,
    )
    if m:
        dim = int(m.group(1))
        return dim if dim > 0 else None
    return None


def _vector_ddl_for_dest(db: str, inferred: str | None) -> str:
    """Emit destination VECTOR DDL with source dimension when the engine needs it.

    Never invents a default dimension (historically Snowflake used 1536). When the
    dimension is unknown or exceeds the platform cap, fall back to the destination
    lossless text sink — CREATE TABLE must not invent a wrong embedding width.
    PostgreSQL preserves HALFVEC/SPARSEVEC encoding (never invent dense vector).
    """
    fallback = DDL_TYPES.get(db, {}).get(LOGICAL_VECTOR, DEFAULT_DDL.get(db, "TEXT"))
    template = _VECTOR_PARAM_TEMPLATES.get(db)
    # Engines without native vector templates keep DDL_TYPES sink (ARRAY/STRING/…).
    if not template and db != "postgresql":
        return fallback

    dim = parse_vector_dimension(inferred)
    if dim is None:
        return DEFAULT_DDL.get(db, "TEXT")

    cap = _VECTOR_DIM_CAPS.get(db, 65535)
    if dim > cap:
        return DEFAULT_DDL.get(db, "TEXT")

    enc = vector_encoding_polarity(inferred)
    if db == "postgresql":
        if enc == "half":
            return f"halfvec({dim})"
        if enc == "sparse":
            return f"sparsevec({dim})"
        return f"vector({dim})"

    if not template:
        return fallback
    return template.format(n=dim)


def _geography_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Preserve GEOMETRY vs GEOGRAPHY polarity (+ SRID typmod when PG-like).

    Bare logical ``geography`` (exact lowercase alias) falls through to
    ``DDL_TYPES`` defaults (PG→GEOMETRY). Explicit ``GEOGRAPHY`` /
    ``GEOMETRY`` / typmod carriers keep polarity — never treat uppercase
    ``GEOGRAPHY`` as the logical alias (SQL Server→PostGIS footgun).
    """
    raw = (inferred or "").strip()
    # Exact logical alias only — ``GEOGRAPHY`` / ``GEOMETRY`` are dual carriers.
    if raw == LOGICAL_GEOGRAPHY:
        return None
    pol = spatial_polarity(inferred)
    srid = parse_geography_srid(inferred)
    if db == "postgresql":
        if pol == "geography":
            kind = geometry_kind(inferred) or "Geometry"
            return f"GEOGRAPHY({kind},{srid})" if srid else "GEOGRAPHY"
        if pol == "geometry":
            kind = geometry_kind(inferred) or "Geometry"
            return f"GEOMETRY({kind},{srid})" if srid else "GEOMETRY"
        return None
    if db == "sqlserver":
        if pol == "geometry":
            return "GEOMETRY"
        if pol == "geography":
            return "GEOGRAPHY"
        return None
    if db == "mysql" and pol in {"geometry", "geography"}:
        return "GEOMETRY"
    if db == "oracle" and (
        pol is not None or "SDO_GEOMETRY" in raw.upper()
    ):
        return "SDO_GEOMETRY"
    if db in {"snowflake", "bigquery"} and pol == "geography":
        return "GEOGRAPHY"
    return None


def _bitstring_ddl_for_dest(db: str, inferred: str | None) -> str | None:
    """Emit native BIT/VARBIT DDL — never invent BYTEA from a bitstring carrier.

    PostgreSQL stores bit masks as bit strings (``B'1010'``), not opaque bytes.
    Mapping BIT(n)→BYTEA invents a byte packing the operator did not declare.
    """
    if not is_bitstring_carrier(inferred):
        return None
    width = parse_bitstring_width(inferred)
    varying = is_varying_bitstring_carrier(inferred)
    if db in {"postgresql", "redshift", "duckdb"}:
        if varying:
            return f"BIT VARYING({width})" if width else "BIT VARYING"
        return f"BIT({width})" if width else "BIT"
    if db in {"mysql", "mariadb"}:
        # MySQL BIT(m) max 64; varying not supported — clamp honestly.
        if width is None:
            return "BIT(64)"
        return f"BIT({min(width, 64)})"
    # Engines without bitstring types — lossless text of 0/1 digits (not BYTEA).
    if width is not None:
        return f"VARCHAR({width})"
    return DDL_TYPES.get(db, {}).get(LOGICAL_TEXT) or DDL_TYPES.get(db, {}).get(
        LOGICAL_STRING, "TEXT"
    )
