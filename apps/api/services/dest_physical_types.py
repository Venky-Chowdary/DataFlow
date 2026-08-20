"""Physical destination column types, read from the destination catalog.

The plan's ``target_type`` is what Map intended; the column the rows actually
landed in is what Gate-8 must fingerprint against. Those two disagree whenever
the destination already existed: a plan that says ``DATETIME(6)`` writing into a
pre-existing ``datetime`` column made reconcile fingerprint the source at
microseconds while the engine stored whole seconds — a strict checksum mismatch
on a correct load, with no column named.

Only what the catalog answers is returned. There is no inference fallback: an
invented type here would silently redefine the comparison basis, which is worse
than not knowing.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_PORTS = {
    "mysql": 3306,
    "sqlserver": 1433,
    "oracle": 1521,
    "redshift": 5439,
    "postgresql": 5432,
}


def physical_column_types(
    db_type: str,
    cfg: dict[str, Any],
    *,
    table: str,
    columns: list[str] | None = None,
) -> dict[str, str]:
    """Catalog column types for ``table``, keyed by the catalog's own spelling.

    Returns ``{}`` when the catalog cannot answer — callers keep the declared
    types rather than comparing against a guess.
    """
    if not table or not db_type:
        return {}
    try:
        from services.dialect_profiles import schema_from_cfg
        from services.schema_introspect import introspect_schema

        info = introspect_schema(
            db_type,
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or _DEFAULT_PORTS.get(db_type, 5432)),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            schema=schema_from_cfg(db_type, cfg),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl", False)),
            warehouse=str(cfg.get("warehouse") or ""),
            table=table,
            catalog_type=str(cfg.get("type") or ""),
            role=str(cfg.get("role") or ""),
            auth_role=str(cfg.get("auth_role") or ""),
            private_key=str(cfg.get("private_key") or ""),
            # A destination probe must never borrow a same-named table from
            # another database on the host.
            strict_namespace=True,
        )
    except Exception as exc:
        logger.warning("physical dest type probe failed: %s", exc, exc_info=exc)
        return {}
    if not info.get("ok"):
        return {}
    wanted = {str(c).lower() for c in (columns or []) if c}
    out: dict[str, str] = {}
    for col in info.get("columns") or []:
        name = str(col.get("name") or "")
        ddl = str(col.get("inferred_type") or "")
        if not name or not ddl:
            continue
        if wanted and name.lower() not in wanted:
            continue
        out[name] = ddl
    return out


def apply_physical_temporal_precision(
    dest_types: dict[str, str],
    db_type: str,
    cfg: dict[str, Any],
    *,
    table: str,
) -> dict[str, str]:
    """Restate declared instant types with the precision the columns keep.

    Both digests — the one taken during the write pass and the destination
    re-read — must agree on what the physical column can hold, or a correct load
    fails its own checksum. Declared types are kept when the catalog cannot
    answer, and only the fractional-second precision is replaced, so timezone
    polarity stays with the contract.
    """
    if not dest_types or not table or not db_type:
        return dest_types
    from services.type_system import with_temporal_fractional_digits

    digits = physical_temporal_digits(
        db_type, cfg, table=table, columns=list(dest_types.keys())
    )
    if not digits:
        return dest_types
    out = dict(dest_types)
    lowered = {name.lower(): name for name in out}
    for catalog_name, kept in digits.items():
        declared_name = lowered.get(catalog_name.lower())
        if not declared_name:
            continue
        declared_ddl = out.get(declared_name) or ""
        if declared_ddl:
            out[declared_name] = with_temporal_fractional_digits(declared_ddl, kept)
    return out


def physical_temporal_digits(
    db_type: str,
    cfg: dict[str, Any],
    *,
    table: str,
    columns: list[str] | None = None,
) -> dict[str, int]:
    """Fractional-second digits each instant column physically keeps.

    Only the precision is taken from the catalog. Timezone polarity stays with
    the declared type: substituting the catalog's spelling wholesale turned a
    ``TIMESTAMPTZ`` contract into a naive carrier and made the digests disagree
    by the offset instead of by a fraction of a second.
    """
    from connectors.sql_temporal import is_temporal_ddl
    from services.type_system import destination_temporal_fractional_digits

    out: dict[str, int] = {}
    for name, ddl in physical_column_types(
        db_type, cfg, table=table, columns=columns
    ).items():
        if not is_temporal_ddl(ddl):
            continue
        digits = destination_temporal_fractional_digits(ddl, dest_db=db_type)
        if digits is not None:
            out[name] = digits
    return out
