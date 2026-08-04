"""Population-scoped FK orphan probe — the only path to RI ``proven``.

Sample Validate never sets ``population_proof``. This module runs full-table
anti-joins (COUNT + examples) for single-column FKs on SQL sources.

Honesty:
- Opt-in only (expensive). Never invent proven when the scan did not run.
- Composite FKs → incomplete (``population_orphan_count=None`` ⇒ not proven).
- Partial / failed scan → fail-closed finding; never silent soft-pass.
"""

from __future__ import annotations

import logging
from typing import Any

from services.sample_orphan_probe import (
    _fk_parts,
    _resolve_source_column,
    _severity,
)

logger = logging.getLogger(__name__)

_MAX_ORPHAN_EXAMPLES = 25

_SQLISH = {
    "postgresql", "postgres", "redshift", "cockroachdb", "timescaledb",
    "supabase", "mysql", "mariadb", "singlestore", "sqlserver", "mssql",
    "synapse", "azure_sql_database", "oracle", "db2", "sqlite", "duckdb",
    "generic_sql", "h2", "snowflake", "clickhouse", "trino", "presto",
    "questdb",
}


def _split_table(name: str, default_schema: str | None) -> tuple[str | None, str]:
    raw = (name or "").strip()
    if not raw:
        return default_schema, ""
    if "." in raw:
        schema, table = raw.split(".", 1)
        return (schema.strip() or None), table.strip()
    return default_schema, raw


def _sql_population_orphan_scan(
    cfg: dict[str, Any],
    *,
    child_table: str,
    child_column: str,
    parent_table: str,
    parent_column: str,
) -> dict[str, Any]:
    """Full-table orphan COUNT + example values via LEFT JOIN anti-join."""
    import sqlalchemy as sa

    from connectors.generic_sql import _engine

    schema = (cfg.get("schema") or "").strip() or None
    child_schema, child_name = _split_table(child_table, schema)
    parent_schema, parent_name = _split_table(parent_table, schema)
    if not child_name or not parent_name or not child_column or not parent_column:
        raise ValueError("incomplete table/column for population orphan scan")

    child = sa.table(child_name, schema=child_schema)
    parent = sa.table(parent_name, schema=parent_schema)
    c_col = sa.column(child_column)
    p_col = sa.column(parent_column)

    orphan_pred = sa.and_(c_col.is_not(None), p_col.is_(None))
    count_stmt = (
        sa.select(sa.func.count())
        .select_from(child.outerjoin(parent, c_col == p_col))
        .where(orphan_pred)
    )
    example_stmt = (
        sa.select(c_col)
        .select_from(child.outerjoin(parent, c_col == p_col))
        .where(orphan_pred)
        .limit(_MAX_ORPHAN_EXAMPLES)
    )

    engine = _engine(cfg)
    with engine.connect() as conn:
        orphan_count = int(conn.execute(count_stmt).scalar() or 0)
        examples = [
            r[0]
            for r in conn.execute(example_stmt).fetchall()
            if r and r[0] is not None
        ]
    return {"orphan_count": orphan_count, "examples": examples}


def _unavailable_finding(
    *,
    note: str,
    validation_mode: str,
    fk_risk_acknowledged: bool,
    foreign_keys: list[dict[str, Any]],
) -> dict[str, Any]:
    cols: list[str] = []
    for fk in foreign_keys:
        c, _, _ = _fk_parts(fk)
        cols.extend(c)
    return {
        "code": "population_orphan_probe_unavailable",
        "severity": _severity(
            validation_mode=validation_mode,
            fk_risk_acknowledged=fk_risk_acknowledged,
        ),
        "columns": cols[:20],
        "coverage": "population_orphan_probe",
        "population_proof": False,
        "message": (
            f"{note} Population orphan scan could not complete — "
            "referential integrity is not proven; fail closed until the scan "
            "succeeds or FK risk is acknowledged via Risk Contract."
        ),
    }


def probe_population_fk_orphans(
    *,
    child_table: str,
    mappings: list[dict[str, Any]] | None,
    foreign_keys: list[dict[str, Any]] | None,
    source_connector_id: str = "",
    source_config: dict[str, Any] | None = None,
    workspace_id: str | None = None,
    validation_mode: str = "strict",
    fk_risk_acknowledged: bool = False,
) -> dict[str, Any]:
    """Full-table FK orphan scan for single-column FKs.

    ``population_proof`` is True only when every FK was fully scanned and
    ``orphan_count == 0``. Incomplete scans set ``complete=False`` and leave
    callers to pass ``population_orphan_count=None`` into RI posture.
    """
    empty = {
        "ran": False,
        "coverage": "none",
        "population_proof": False,
        "complete": False,
        "findings": [],
        "checks": [],
        "orphan_count": 0,
        "child_table": (child_table or "").strip(),
        "note": "Population orphan scan did not run.",
    }
    fks = [fk for fk in (foreign_keys or []) if isinstance(fk, dict)]
    child = (child_table or "").strip()
    if not fks:
        return {**empty, "note": "No FK metadata — population orphan scan skipped."}
    if not child:
        finding = _unavailable_finding(
            note="Child / source table name missing.",
            validation_mode=validation_mode,
            fk_risk_acknowledged=fk_risk_acknowledged,
            foreign_keys=fks,
        )
        return {
            **empty,
            "findings": [finding],
            "note": finding["message"],
        }
    if not source_connector_id and not source_config:
        finding = _unavailable_finding(
            note="Source connector unavailable.",
            validation_mode=validation_mode,
            fk_risk_acknowledged=fk_risk_acknowledged,
            foreign_keys=fks,
        )
        return {
            **empty,
            "findings": [finding],
            "note": finding["message"],
        }

    cfg: dict[str, Any] | None = None
    db_type = ""
    try:
        if source_connector_id:
            from services.connector_probe import probe_cfg_from_saved
            from services.connector_store import get_connector

            conn = get_connector(source_connector_id, workspace_id=workspace_id)
            if conn:
                cfg = probe_cfg_from_saved(conn)
                db_type = (conn.type or "").lower()
        if cfg is None and source_config:
            cfg = dict(source_config)
            db_type = (
                cfg.get("type") or cfg.get("db_type") or cfg.get("format") or ""
            ).lower()
        if not cfg:
            finding = _unavailable_finding(
                note="Could not load source config.",
                validation_mode=validation_mode,
                fk_risk_acknowledged=fk_risk_acknowledged,
                foreign_keys=fks,
            )
            return {
                **empty,
                "findings": [finding],
                "note": finding["message"],
            }
        if db_type:
            cfg = dict(cfg)
            cfg.setdefault("type", db_type)
        if db_type and db_type not in _SQLISH:
            finding = _unavailable_finding(
                note=f"Population orphan scan not implemented for source type {db_type}.",
                validation_mode=validation_mode,
                fk_risk_acknowledged=fk_risk_acknowledged,
                foreign_keys=fks,
            )
            return {
                **empty,
                "findings": [finding],
                "note": finding["message"],
            }
    except Exception as exc:
        logger.warning("Population orphan scan config failed: %s", exc, exc_info=exc)
        finding = _unavailable_finding(
            note=f"Population orphan scan config error ({exc}).",
            validation_mode=validation_mode,
            fk_risk_acknowledged=fk_risk_acknowledged,
            foreign_keys=fks,
        )
        return {
            **empty,
            "findings": [finding],
            "note": finding["message"],
            "error": str(exc),
        }

    maps = list(mappings or [])
    findings: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []
    total_orphans = 0
    complete = True
    sev = _severity(
        validation_mode=validation_mode,
        fk_risk_acknowledged=fk_risk_acknowledged,
    )

    for fk in fks:
        cols, ref_table, ref_cols = _fk_parts(fk)
        if not cols or not ref_table or not ref_cols:
            complete = False
            checks.append({"skipped": True, "reason": "incomplete_fk_metadata", "fk": fk})
            findings.append(
                {
                    "code": "population_orphan_probe_incomplete",
                    "severity": sev,
                    "coverage": "population_orphan_probe",
                    "population_proof": False,
                    "message": (
                        "Incomplete FK metadata — population orphan scan cannot "
                        "prove referential integrity for this constraint."
                    ),
                }
            )
            continue
        if len(cols) != 1 or len(ref_cols) != 1:
            complete = False
            checks.append(
                {
                    "skipped": True,
                    "reason": "composite_fk_not_probed",
                    "columns": cols,
                    "referenced_table": ref_table,
                    "coverage": "population_orphan_probe",
                    "population_proof": False,
                }
            )
            findings.append(
                {
                    "code": "composite_fk_not_probed",
                    "severity": sev,
                    "columns": cols,
                    "referenced_table": ref_table,
                    "referenced_columns": ref_cols,
                    "coverage": "population_orphan_probe",
                    "population_proof": False,
                    "message": (
                        f"Composite FK {cols} → {ref_table}{ref_cols} was not scanned "
                        "(tuple anti-join not implemented). Coverage=population_orphan_probe "
                        "incomplete — RI not proven."
                    ),
                }
            )
            continue

        child_col = _resolve_source_column(maps, cols[0])
        parent_col = ref_cols[0]
        try:
            result = _sql_population_orphan_scan(
                cfg,
                child_table=child,
                child_column=child_col,
                parent_table=ref_table,
                parent_column=parent_col,
            )
        except Exception as exc:
            logger.warning(
                "Population orphan scan failed for %s.%s: %s",
                child,
                child_col,
                exc,
                exc_info=exc,
            )
            complete = False
            findings.append(
                {
                    "code": "population_orphan_probe_unavailable",
                    "severity": sev,
                    "columns": [child_col],
                    "referenced_table": ref_table,
                    "coverage": "population_orphan_probe",
                    "population_proof": False,
                    "message": (
                        f"Population orphan scan failed for {child}.{child_col} → "
                        f"{ref_table}.{parent_col}: {exc}. RI not proven."
                    ),
                }
            )
            checks.append(
                {
                    "column": child_col,
                    "referenced_table": ref_table,
                    "error": str(exc),
                    "coverage": "population_orphan_probe",
                    "population_proof": False,
                }
            )
            continue

        orphan_count = int(result.get("orphan_count") or 0)
        examples = list(result.get("examples") or [])
        total_orphans += orphan_count
        checks.append(
            {
                "column": child_col,
                "referenced_table": ref_table,
                "referenced_column": parent_col,
                "orphan_count": orphan_count,
                "examples": [str(v)[:80] for v in examples[:5]],
                "coverage": "population_orphan_probe",
                "population_proof": orphan_count == 0,
            }
        )
        if orphan_count > 0:
            ex = ", ".join(str(v)[:40] for v in examples[:3])
            findings.append(
                {
                    "code": "fk_orphan_in_population",
                    "severity": sev,
                    "columns": [child_col],
                    "referenced_table": ref_table,
                    "referenced_columns": [parent_col],
                    "orphan_count": orphan_count,
                    "coverage": "population_orphan_probe",
                    "population_proof": False,
                    "message": (
                        f"Population orphan scan: {orphan_count} row(s) in {child}.{child_col} "
                        f"missing from {ref_table}.{parent_col}"
                        + (f" (examples: {ex})" if ex else "")
                        + ". Coverage=population_orphan_probe — RI not proven."
                    ),
                }
            )

    # Proven only when every FK was fully scanned and zero orphans found.
    population_proof = bool(complete and total_orphans == 0 and not findings)

    return {
        "ran": True,
        "coverage": "population_orphan_probe",
        "population_proof": population_proof,
        "complete": complete,
        "findings": findings,
        "checks": checks,
        "orphan_count": total_orphans,
        "child_table": child,
        "note": (
            "Population orphan scan completed — RI proven."
            if population_proof
            else (
                "Population orphan scan ran but is incomplete or found orphans — "
                "referential integrity is not proven."
            )
        ),
    }
