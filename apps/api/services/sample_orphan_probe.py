"""Sample-scoped FK orphan probe — never invents population RI proof.

Uses Validate sample child FK values and a live parent-key lookup (SQL).
Coverage is always ``sample_orphan_probe`` unless a separate full-table scan
API is wired later. Fail closed on probe errors in strict mode when FKs are
mapped and a parent relation is known.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from services.value_serializer import present_cell_text

logger = logging.getLogger(__name__)


def _fk_key(value: Any) -> str | None:
    """Present FK cell. Reader-wired SQL NULL is not a parent lookup key."""
    return present_cell_text(value)


def _fk_display(value: Any, *, limit: int = 80) -> str:
    text = _fk_key(value)
    if text is None:
        return ""
    return text[:limit]


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
        key = _fk_key(raw)
        if key is None or key in seen:
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
    parent_keys = {key for v in parent_values if (key := _fk_key(v)) is not None}
    orphans: list[Any] = []
    seen: set[str] = set()
    for v in child_values:
        key = _fk_key(v)
        if key is None or key in seen:
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
    """Return the subset of single-column ``values`` that exist in the parent."""
    from services.fk_tuple_scan import sql_existing_parent_tuples

    if not values or not parent_table or not parent_column:
        return []
    found = sql_existing_parent_tuples(
        cfg,
        parent_table=parent_table,
        parent_columns=[parent_column],
        values=[(v,) for v in values],
    )
    return [row[0] for row in found if row]


def _sql_existing_parent_tuples(
    cfg: dict[str, Any],
    *,
    parent_table: str,
    parent_columns: list[str],
    values: list[tuple[Any, ...]],
) -> list[tuple[Any, ...]]:
    """Return the subset of composite ``values`` that exist as whole tuples."""
    from services.fk_tuple_scan import sql_existing_parent_tuples

    return sql_existing_parent_tuples(
        cfg,
        parent_table=parent_table,
        parent_columns=parent_columns,
        values=values,
    )


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
    """Normalize source-catalog, dest-inspector, and Studio FK payloads.

    Source probe uses ``columns`` / ``referenced_*``. Destination RI and some
    engine extras use ``constrained_columns`` / ``referred_*``. One parser so
    Validate does not treat a dest-shaped composite as incomplete.
    """
    cols = (
        fk.get("columns")
        or fk.get("column")
        or fk.get("fk_columns")
        or fk.get("constrained_columns")
        or []
    )
    if isinstance(cols, str):
        cols = [cols]
    cols = [str(c).strip() for c in cols if str(c).strip()]
    ref_table = str(
        fk.get("referenced_table")
        or fk.get("ref_table")
        or fk.get("referred_table")
        or ""
    ).strip()
    ref_schema = str(
        fk.get("referenced_schema")
        or fk.get("ref_schema")
        or fk.get("referred_schema")
        or ""
    ).strip()
    if ref_schema and ref_table and "." not in ref_table:
        ref_table = f"{ref_schema}.{ref_table}"
    ref_cols = (
        fk.get("referenced_columns")
        or fk.get("ref_columns")
        or fk.get("referred_columns")
        or []
    )
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


def _unavailable_finding(
    *,
    note: str,
    validation_mode: str,
    fk_risk_acknowledged: bool,
    foreign_keys: list[dict[str, Any]],
) -> dict[str, Any]:
    """Fail-closed when FK metadata exists but orphan probe cannot run."""
    cols: list[str] = []
    for fk in foreign_keys:
        c, _, _ = _fk_parts(fk)
        cols.extend(c)
    return {
        "code": "fk_orphan_probe_unavailable",
        "severity": _severity(
            validation_mode=validation_mode,
            fk_risk_acknowledged=fk_risk_acknowledged,
        ),
        "columns": cols[:20],
        "coverage": "none",
        "population_proof": False,
        "message": (
            f"{note} Known destination/source FK metadata exists but sample orphan "
            "probe could not run — population RI not proven; fail closed until "
            "connector is available or FK risk is acknowledged via risk contract."
        ),
    }


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
    Empty foreign_keys → ``ran=False`` (not a silent pass).
    Known FKs without a runnable probe → fail-closed finding (Module 4).
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
    if not fks:
        return empty
    if not sample_rows:
        finding = _unavailable_finding(
            note="No Validate sample rows for orphan probe.",
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

        sqlish = {
            "postgresql", "postgres", "redshift", "cockroachdb", "timescaledb",
            "supabase", "mysql", "mariadb", "singlestore", "sqlserver", "mssql",
            "synapse", "azure_sql_database", "oracle", "db2", "sqlite", "duckdb",
            "generic_sql", "h2", "snowflake", "bigquery", "databricks",
            "clickhouse", "trino", "presto", "questdb",
        }
        if db_type and db_type not in sqlish:
            finding = _unavailable_finding(
                note=f"Sample orphan probe not implemented for source type {db_type}.",
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
        logger.warning("Sample orphan probe config failed: %s", exc, exc_info=exc)
        finding = _unavailable_finding(
            note=f"Sample orphan probe config error ({exc}).",
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
        if len(cols) != len(ref_cols):
            checks.append(
                {
                    "skipped": True,
                    "reason": "fk_column_pairing_incomplete",
                    "columns": cols,
                    "referenced_table": ref_table,
                    "referenced_columns": ref_cols,
                    "coverage": "sample_orphan_probe",
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
                    "coverage": "sample_orphan_probe",
                    "population_proof": False,
                    "message": (
                        f"FK {cols} → {ref_table}{ref_cols} has no usable column pairing "
                        f"({len(cols)} child vs {len(ref_cols)} parent). "
                        "Coverage=sample_orphan_probe — population RI not proven."
                    ),
                }
            )
            continue

        sample_cols = [_resolve_source_column(maps, c) or c for c in cols]
        parent_cols = list(ref_cols)
        composite = len(cols) > 1
        label = "+".join(cols)
        parent_label = "+".join(parent_cols)

        try:
            if composite:
                from services.fk_tuple_scan import (
                    distinct_fk_tuples,
                    orphan_example_text,
                    orphan_tuples,
                )

                values = distinct_fk_tuples(sample_rows, sample_cols, present_key=_fk_key)
                if not values:
                    checks.append(
                        {
                            "column": label,
                            "columns": cols,
                            "sample_columns": sample_cols,
                            "referenced_table": ref_table,
                            "checked_values": 0,
                            "orphan_count": 0,
                            "coverage": "sample_orphan_probe",
                            "population_proof": False,
                            "note": "No MATCH SIMPLE (all-non-null) FK tuples in Validate sample.",
                        }
                    )
                    continue
                present = _sql_existing_parent_tuples(
                    cfg,
                    parent_table=ref_table,
                    parent_columns=parent_cols,
                    values=values,
                )
                missing = orphan_tuples(values, present, present_key=_fk_key)
                example_fn = orphan_example_text
                checked_n = len(values)
            else:
                child_col = cols[0]
                parent_col = parent_cols[0]
                sample_col = sample_cols[0]
                flat = distinct_fk_values(sample_rows, sample_col)
                if not flat:
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
                present = _sql_existing_parent_keys(
                    cfg,
                    parent_table=ref_table,
                    parent_column=parent_col,
                    values=flat,
                )
                missing = orphan_values(flat, present)
                example_fn = _fk_display
                checked_n = len(flat)
        except Exception as exc:
            logger.warning(
                "Sample orphan parent lookup failed for %s→%s: %s",
                cols,
                ref_table,
                exc,
                exc_info=exc,
            )
            findings.append(
                {
                    "code": "fk_orphan_probe_failed",
                    "severity": sev if sev == "block" else "ack_required",
                    "columns": cols,
                    "referenced_table": ref_table,
                    "coverage": "sample_orphan_probe",
                    "population_proof": False,
                    "message": (
                        f"Could not verify sample orphans for {label} → "
                        f"{ref_table}.({parent_label}): {exc}. "
                        "Population RI not proven; fail closed on probe error."
                    ),
                }
            )
            checks.append(
                {
                    "columns": cols,
                    "error": str(exc),
                    "coverage": "sample_orphan_probe",
                    "population_proof": False,
                }
            )
            continue

        total_checked += checked_n
        total_orphans += len(missing)
        check = {
            "column": cols[0] if not composite else label,
            "columns": cols,
            "sample_columns": sample_cols,
            "referenced_table": ref_table,
            "referenced_column": parent_cols[0] if not composite else parent_label,
            "referenced_columns": parent_cols,
            "checked_values": checked_n,
            "orphan_count": len(missing),
            "orphan_examples": [example_fn(v) for v in missing[:5]],
            "coverage": "sample_orphan_probe",
            "population_proof": False,
            "match_simple": True,
        }
        checks.append(check)
        if missing:
            examples = ", ".join(example_fn(v)[:40] for v in missing[:3] if example_fn(v))
            findings.append(
                {
                    "code": "fk_orphan_in_sample",
                    "severity": sev,
                    "columns": cols,
                    "referenced_table": ref_table,
                    "referenced_columns": parent_cols,
                    "orphan_count": len(missing),
                    "checked_values": checked_n,
                    "coverage": "sample_orphan_probe",
                    "population_proof": False,
                    "message": (
                        f"Sample orphan probe: {len(missing)}/{checked_n} distinct "
                        f"{label} {'tuple' if composite else 'value'}(s) missing from "
                        f"{ref_table}.({parent_label})"
                        + (f" (examples: {examples})" if examples else "")
                        + ". Coverage=sample_orphan_probe — population RI not proven."
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
