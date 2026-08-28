"""Population-scoped FK orphan probe — the only path to RI ``proven``.

Sample Validate never sets ``population_proof``. This module runs full-table
MATCH SIMPLE anti-joins (COUNT + examples) for single-column and composite
FKs on SQL sources. The join algorithm is ``services.fk_tuple_scan`` — the
same owner as destination post-write RI.

Honesty:
- Opt-in only (expensive). Never invent proven when the scan did not run.
- Column-arity mismatch or failed scan → incomplete (``population_orphan_count=None``).
- Partial / failed scan → fail-closed finding; never silent soft-pass.
"""

from __future__ import annotations

import logging
from typing import Any

from services.sample_orphan_probe import (
    _fk_display,
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


def _sql_population_orphan_scan(
    cfg: dict[str, Any],
    *,
    child_table: str,
    parent_table: str,
    child_column: str = "",
    parent_column: str = "",
    child_columns: list[str] | None = None,
    parent_columns: list[str] | None = None,
) -> dict[str, Any]:
    """Full-table orphan COUNT + examples via MATCH SIMPLE anti-join."""
    from services.fk_tuple_scan import sql_population_orphan_scan

    kids = list(child_columns or ([child_column] if child_column else []))
    parents = list(parent_columns or ([parent_column] if parent_column else []))
    return sql_population_orphan_scan(
        cfg,
        child_table=child_table,
        parent_table=parent_table,
        child_columns=kids,
        parent_columns=parents,
        max_examples=_MAX_ORPHAN_EXAMPLES,
    )


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
    """Full-table FK orphan scan for single-column and composite FKs.

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
    try:
        from services.procedure_source import is_callable_source

        if is_callable_source(source_config):
            return {
                **empty,
                "coverage": "n/a",
                "note": (
                    "Stored-procedure / SQL extract is a result-set snapshot — "
                    "population orphan scan is not run against a procedure name."
                ),
                "skipped": True,
                "reason": "callable_source",
            }
    except Exception:
        pass
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
        if len(cols) != len(ref_cols):
            complete = False
            checks.append(
                {
                    "skipped": True,
                    "reason": "fk_column_pairing_incomplete",
                    "columns": cols,
                    "referenced_table": ref_table,
                    "referenced_columns": ref_cols,
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
                        f"FK {cols} → {ref_table}{ref_cols} has no usable column pairing "
                        f"({len(cols)} child vs {len(ref_cols)} parent). "
                        "Coverage=population_orphan_probe incomplete — RI not proven."
                    ),
                }
            )
            continue

        child_cols = [_resolve_source_column(maps, c) or c for c in cols]
        parent_cols = list(ref_cols)
        label = "+".join(child_cols)
        parent_label = "+".join(parent_cols)
        try:
            result = _sql_population_orphan_scan(
                cfg,
                child_table=child,
                parent_table=ref_table,
                child_columns=child_cols,
                parent_columns=parent_cols,
            )
        except Exception as exc:
            logger.warning(
                "Population orphan scan failed for %s.%s: %s",
                child,
                label,
                exc,
                exc_info=exc,
            )
            complete = False
            findings.append(
                {
                    "code": "population_orphan_probe_unavailable",
                    "severity": sev,
                    "columns": child_cols,
                    "referenced_table": ref_table,
                    "coverage": "population_orphan_probe",
                    "population_proof": False,
                    "message": (
                        f"Population orphan scan failed for {child}.({label}) → "
                        f"{ref_table}.({parent_label}): {exc}. RI not proven."
                    ),
                }
            )
            checks.append(
                {
                    "columns": child_cols,
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
                "column": child_cols[0] if len(child_cols) == 1 else label,
                "columns": child_cols,
                "referenced_table": ref_table,
                "referenced_column": parent_cols[0] if len(parent_cols) == 1 else parent_label,
                "referenced_columns": parent_cols,
                "orphan_count": orphan_count,
                "examples": [_fk_display(v) for v in examples[:5]],
                "coverage": "population_orphan_probe",
                "population_proof": orphan_count == 0,
                "match_simple": True,
            }
        )
        if orphan_count > 0:
            ex = ", ".join(
                text for v in examples[:3] if (text := _fk_display(v, limit=40))
            )
            findings.append(
                {
                    "code": "fk_orphan_in_population",
                    "severity": sev,
                    "columns": child_cols,
                    "referenced_table": ref_table,
                    "referenced_columns": parent_cols,
                    "orphan_count": orphan_count,
                    "coverage": "population_orphan_probe",
                    "population_proof": False,
                    "message": (
                        f"Population orphan scan: {orphan_count} row(s) in {child}.({label}) "
                        f"missing from {ref_table}.({parent_label})"
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
