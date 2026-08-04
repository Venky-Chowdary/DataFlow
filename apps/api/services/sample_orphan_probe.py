"""Sample-scoped FK orphan probe — never invents population RI proof.

Uses Validate sample child FK values and a live parent-key lookup (SQL).
Coverage is always ``sample_orphan_probe`` unless a separate full-table scan
API is wired later. Fail closed on probe errors in strict mode when FKs are
mapped and a parent relation is known.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

logger = logging.getLogger(__name__)

# Cap distinct FK values probed from the Validate sample (O(sample), not O(table)).
_MAX_DISTINCT_FK_VALUES = 500
_PARENT_IN_CHUNK = 200


def distinct_fk_values(
    sample_rows: list[dict[str, Any]] | None,
    column: str,
    *,
    limit: int = _MAX_DISTINCT_FK_VALUES,
) -> list[Any]:
    """Distinct non-null FK values from the Validate sample (stable order)."""
    if not sample_rows or not column:
        return []
    seen: set[str] = set()
    out: list[Any] = []
    for row in sample_rows:
        if not isinstance(row, dict):
            continue
        raw = row.get(column)
        if raw is None:
            continue
        if isinstance(raw, str) and not raw.strip():
            continue
        key = str(raw)
        if key in seen:
            continue
        seen.add(key)
        out.append(raw)
        if len(out) >= limit:
            break
    return out


def orphan_values(
    child_values: Iterable[Any],
    parent_values: Iterable[Any],
) -> list[Any]:
    """Return child values with no match in parent (string-normalized)."""
    parent_keys = {str(v) for v in parent_values if v is not None and str(v) != ""}
    orphans: list[Any] = []
    seen: set[str] = set()
    for v in child_values:
        if v is None:
            continue
        key = str(v)
        if not key or key in seen:
            continue
        if key not in parent_keys:
            seen.add(key)
            orphans.append(v)
    return orphans


def _sql_existing_parent_keys(
    cfg: dict[str, Any],
    *,
    parent_table: str,
    parent_column: str,
    values: list[Any],
) -> list[Any]:
    """Return the subset of ``values`` that exist in the parent table."""
    import sqlalchemy as sa

    from connectors.generic_sql import _engine

    if not values or not parent_table or not parent_column:
        return []

    engine = _engine(cfg)
    schema = (cfg.get("schema") or "").strip() or None
    table_name = parent_table
    if "." in parent_table and schema is None:
        parts = parent_table.split(".", 1)
        schema, table_name = parts[0].strip() or None, parts[1].strip()
    elif "." in parent_table:
        table_name = parent_table.split(".", 1)[-1].strip()

    tbl = sa.table(table_name, schema=schema)
    col = sa.column(parent_column)
    found: list[Any] = []
    with engine.connect() as conn:
        for i in range(0, len(values), _PARENT_IN_CHUNK):
            chunk = values[i : i + _PARENT_IN_CHUNK]
            stmt = sa.select(col).select_from(tbl).where(col.in_(chunk))
            rows = conn.execute(stmt).fetchall()
            found.extend(r[0] for r in rows if r and r[0] is not None)
    return found


def _resolve_source_column(
    mappings: list[dict[str, Any]],
    fk_column: str,
) -> str:
    """Map destination (or same-named) FK column to the source sample field."""
    want = (fk_column or "").strip().lower()
    if not want:
        return ""
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        if bool(m.get("intentional_omit") or m.get("intentionalOmit")):
            continue
        tgt = str(m.get("target") or "").strip()
        src = str(m.get("source") or "").strip()
        if tgt.lower() == want and src:
            return src
        if src.lower() == want:
            return src
    return fk_column


def _fk_parts(fk: dict[str, Any]) -> tuple[list[str], str, list[str]]:
    cols = fk.get("columns") or fk.get("column") or fk.get("fk_columns") or []
    if isinstance(cols, str):
        cols = [cols]
    cols = [str(c).strip() for c in cols if str(c).strip()]
    ref_table = str(
        fk.get("referenced_table") or fk.get("ref_table") or ""
    ).strip()
    ref_schema = str(fk.get("referenced_schema") or fk.get("ref_schema") or "").strip()
    if ref_schema and ref_table and "." not in ref_table:
        ref_table = f"{ref_schema}.{ref_table}"
    ref_cols = fk.get("referenced_columns") or fk.get("ref_columns") or []
    if isinstance(ref_cols, str):
        ref_cols = [ref_cols]
    ref_cols = [str(c).strip() for c in ref_cols if str(c).strip()]
    return cols, ref_table, ref_cols


def _severity(*, validation_mode: str, fk_risk_acknowledged: bool) -> str:
    if fk_risk_acknowledged:
        return "info"
    mode = (validation_mode or "strict").strip().lower()
    if mode in {"strict", "maximum"}:
        return "block"
    return "ack_required"


def probe_sample_fk_orphans(
    *,
    sample_rows: list[dict[str, Any]] | None,
    mappings: list[dict[str, Any]] | None,
    foreign_keys: list[dict[str, Any]] | None,
    source_connector_id: str = "",
    source_config: dict[str, Any] | None = None,
    workspace_id: str | None = None,
    validation_mode: str = "strict",
    fk_risk_acknowledged: bool = False,
) -> dict[str, Any]:
    """Probe Validate-sample FK values against live parent keys.

    Returns an honesty-stamped report. ``population_proof`` is always False.
    Empty foreign_keys / missing connector → ``ran=False`` (not a silent pass).
    """
    empty = {
        "ran": False,
        "coverage": "none",
        "population_proof": False,
        "findings": [],
        "checks": [],
        "orphan_count": 0,
        "checked_values": 0,
        "note": "Sample orphan probe did not run (no FK metadata or source connection).",
    }
    fks = [fk for fk in (foreign_keys or []) if isinstance(fk, dict)]
    if not fks or not sample_rows:
        return empty
    if not source_connector_id and not source_config:
        return {
            **empty,
            "note": (
                "Sample orphan probe skipped — source connector unavailable; "
                "RI not proven."
            ),
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
            return {
                **empty,
                "note": "Sample orphan probe skipped — could not load source config.",
            }
        if db_type:
            cfg = dict(cfg)
            cfg.setdefault("type", db_type)

        sqlish = {
            "postgresql", "postgres", "redshift", "cockroachdb", "timescaledb",
            "supabase", "mysql", "mariadb", "singlestore", "sqlserver", "mssql",
            "synapse", "azure_sql_database", "oracle", "db2", "sqlite", "duckdb",
            "generic_sql", "h2", "snowflake", "bigquery", "databricks",
            "clickhouse", "trino", "presto", "questdb",
        }
        if db_type and db_type not in sqlish:
            return {
                **empty,
                "note": (
                    f"Sample orphan probe not implemented for source type {db_type}; "
                    "RI not proven."
                ),
            }
    except Exception as exc:
        logger.warning("Sample orphan probe config failed: %s", exc, exc_info=exc)
        return {
            **empty,
            "note": f"Sample orphan probe config error — RI not proven ({exc}).",
            "error": str(exc),
        }

    maps = list(mappings or [])
    findings: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []
    total_orphans = 0
    total_checked = 0
    sev = _severity(
        validation_mode=validation_mode,
        fk_risk_acknowledged=fk_risk_acknowledged,
    )

    for fk in fks:
        cols, ref_table, ref_cols = _fk_parts(fk)
        if not cols or not ref_table or not ref_cols:
            checks.append(
                {
                    "skipped": True,
                    "reason": "incomplete_fk_metadata",
                    "fk": fk,
                }
            )
            continue
        # Composite FKs: probe only single-column (composite needs tuple IN).
        if len(cols) != 1 or len(ref_cols) != 1:
            checks.append(
                {
                    "skipped": True,
                    "reason": "composite_fk_not_probed",
                    "columns": cols,
                    "referenced_table": ref_table,
                    "coverage": "sample_orphan_probe",
                    "population_proof": False,
                }
            )
            continue

        child_col = cols[0]
        parent_col = ref_cols[0]
        sample_col = _resolve_source_column(maps, child_col)
        values = distinct_fk_values(sample_rows, sample_col)
        if not values:
            checks.append(
                {
                    "column": child_col,
                    "sample_column": sample_col,
                    "referenced_table": ref_table,
                    "checked_values": 0,
                    "orphan_count": 0,
                    "coverage": "sample_orphan_probe",
                    "population_proof": False,
                    "note": "No non-null FK values in Validate sample.",
                }
            )
            continue

        try:
            present = _sql_existing_parent_keys(
                cfg,
                parent_table=ref_table,
                parent_column=parent_col,
                values=values,
            )
            missing = orphan_values(values, present)
        except Exception as exc:
            logger.warning(
                "Sample orphan parent lookup failed for %s→%s: %s",
                child_col,
                ref_table,
                exc,
                exc_info=exc,
            )
            findings.append(
                {
                    "code": "fk_orphan_probe_failed",
                    "severity": sev if sev == "block" else "ack_required",
                    "columns": [child_col],
                    "referenced_table": ref_table,
                    "coverage": "sample_orphan_probe",
                    "population_proof": False,
                    "message": (
                        f"Could not verify sample orphans for {child_col} → "
                        f"{ref_table}.{parent_col}: {exc}. "
                        "Population RI not proven; fail closed on probe error."
                    ),
                }
            )
            checks.append(
                {
                    "column": child_col,
                    "error": str(exc),
                    "coverage": "sample_orphan_probe",
                    "population_proof": False,
                }
            )
            continue

        total_checked += len(values)
        total_orphans += len(missing)
        check = {
            "column": child_col,
            "sample_column": sample_col,
            "referenced_table": ref_table,
            "referenced_column": parent_col,
            "checked_values": len(values),
            "orphan_count": len(missing),
            "orphan_examples": [str(v)[:80] for v in missing[:5]],
            "coverage": "sample_orphan_probe",
            "population_proof": False,
        }
        checks.append(check)
        if missing:
            examples = ", ".join(str(v)[:40] for v in missing[:3])
            findings.append(
                {
                    "code": "fk_orphan_in_sample",
                    "severity": sev,
                    "columns": [child_col],
                    "referenced_table": ref_table,
                    "referenced_columns": [parent_col],
                    "orphan_count": len(missing),
                    "checked_values": len(values),
                    "coverage": "sample_orphan_probe",
                    "population_proof": False,
                    "message": (
                        f"Sample orphan probe: {len(missing)}/{len(values)} distinct "
                        f"{child_col} value(s) missing from {ref_table}.{parent_col} "
                        f"(examples: {examples}). Coverage=sample_orphan_probe — "
                        "population RI not proven."
                    ),
                }
            )

    return {
        "ran": True,
        "coverage": "sample_orphan_probe",
        "population_proof": False,
        "findings": findings,
        "checks": checks,
        "orphan_count": total_orphans,
        "checked_values": total_checked,
        "note": (
            "Sample orphan probe completed against parent keys for Validate sample "
            "values only — not a full-table / population RI proof."
        ),
    }
