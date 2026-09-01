"""Gate G20 — population-level code-system crosswalk coverage.

A 25-row sample can make a status / ICD / GL-account remapping look complete
while a rare code in the population has no target. Passing that code through
unchanged, nulling it, or dropping the row silently corrupts meaning — the
failure mode of every legacy reference-data conversion that only checked a
preview.

This gate runs only when a mapping **declares** a ``code_crosswalk``
(opt-in: auto-detecting "code columns" would either fail every name column
or fail open). Once declared:

* every distinct non-empty source value in the *population* must have a
  target in the map (no implicit identity — ``A→A`` is an explicit entry);
* a sample that happens to be covered is **not** proof — unproven coverage
  blocks (fail closed);
* the write path applies the same map and refuses unmapped codes
  (quarantine / fail — never silent identity).

``is_lossy_coercion`` and mapping confidence floors are untouched.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from services.mapping_constraints import is_intentional_omit

logger = logging.getLogger(__name__)

GATE_ID = "g20_code_crosswalk"
REPORT_SCHEMA = "code_crosswalk_coverage_v1"
_NAMED_LIMIT = 8
#: A code system is low-cardinality. Past this, the column is not a coded
#: field the operator can map by hand — block rather than claim coverage.
MAX_DISTINCT = 100_000
EVIDENCE_EXACT = "exact"
EVIDENCE_SAMPLED = "sampled"
EVIDENCE_UNMEASURED = "unmeasured"


def normalize_code(value: object) -> str | None:
    """Canonical code key. None means "not a code" (NULL / empty / missing)."""
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in {"__df_sql_null__", "__df_ddb_null__", "__df_missing__"}:
        return None
    return text


def declared_crosswalk(mapping: Mapping[str, Any]) -> dict[str, str] | None:
    """Return the operator-declared source→target map, or None if undeclared.

    Empty dict is still a declaration (covers nothing — every code blocks).
    Missing / null is undeclared (this column is not in G20).
    """
    if is_intentional_omit(mapping):
        return None
    raw = mapping.get("code_crosswalk")
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, str] = {}
    for key, dest in raw.items():
        src = normalize_code(key)
        if src is None:
            continue
        out[src] = "" if dest is None else str(dest)
    return out


def coded_mappings(mappings: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Mappings that declared a crosswalk, with the normalized table attached."""
    out: list[dict[str, Any]] = []
    for mapping in mappings or []:
        table = declared_crosswalk(mapping)
        if table is None:
            continue
        source = str(mapping.get("source") or "").strip()
        if not source:
            continue
        out.append(
            {
                "source": source,
                "target": str(mapping.get("target") or "").strip(),
                "system": str(mapping.get("code_crosswalk_system") or "").strip(),
                "crosswalk": table,
                "crosswalk_size": len(table),
            }
        )
    return out


def apply_code_crosswalk(
    value: object,
    mapping: Mapping[str, Any] | None,
) -> tuple[object, str | None]:
    """Rewrite one cell through the mapping's crosswalk.

    No crosswalk → ``(value, None)`` (this column is not coded). NULL/empty
    is not a code. An unmapped non-empty value returns an error — never the
    original code. That is the write-path fail-closed counterpart of G20.
    """
    if mapping is None:
        return value, None
    table = declared_crosswalk(mapping)
    if table is None:
        return value, None
    key = normalize_code(value)
    if key is None:
        return None, None
    if key not in table:
        return None, (
            f"unmapped code {key!r} — code crosswalk does not cover this value"
        )
    return table[key], None


def collect_observed_codes(
    rows: Iterable[Mapping[str, Any]] | None,
    columns: Sequence[str],
    *,
    cap: int = MAX_DISTINCT,
) -> tuple[dict[str, dict[str, int]], bool]:
    """Count distinct non-empty codes per column.

    Returns ``(counts, truncated)``. ``truncated`` is True when any column
    hit ``cap`` — coverage is then unproven.
    """
    wanted = [str(c).strip() for c in columns if str(c).strip()]
    counts: dict[str, dict[str, int]] = {c: {} for c in wanted}
    truncated = False
    if not wanted:
        return counts, False
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        for col in wanted:
            if len(counts[col]) >= cap:
                truncated = True
                continue
            key = normalize_code(row.get(col))
            if key is None:
                continue
            bucket = counts[col]
            bucket[key] = bucket.get(key, 0) + 1
            if len(bucket) >= cap:
                truncated = True
    return counts, truncated


def coverage_for_column(
    *,
    observed: Mapping[str, int],
    crosswalk: Mapping[str, str],
) -> dict[str, Any]:
    """Compare observed population codes to the declared map."""
    mapped = [code for code in observed if code in crosswalk]
    unmapped = [code for code in observed if code not in crosswalk]
    unused = [code for code in crosswalk if code not in observed]
    unmapped_rows = sum(observed[c] for c in unmapped)
    return {
        "observed_distinct": len(observed),
        "mapped_distinct": len(mapped),
        "unmapped_codes": sorted(unmapped),
        "unmapped_rows": unmapped_rows,
        "unused_crosswalk_keys": sorted(unused)[:50],
        "covered": not unmapped,
    }


def build_code_crosswalk_report(
    *,
    mappings: Sequence[Mapping[str, Any]],
    sample_rows: Sequence[Mapping[str, Any]] | None = None,
    population_rows: Sequence[Mapping[str, Any]] | None = None,
    rows_are_population: bool = False,
    observed_codes: Mapping[str, Mapping[str, int]] | None = None,
) -> dict[str, Any]:
    """Auditor-facing coverage report. Never stores a full row."""
    coded = coded_mappings(list(mappings or []))
    if not coded:
        return {
            "schema": REPORT_SCHEMA,
            "declared": False,
            "columns": [],
            "evidence": EVIDENCE_UNMEASURED,
            "honesty": (
                "No mapping declared a code_crosswalk. G20 does not invent "
                "coded fields from names or cardinality."
            ),
        }

    columns = [c["source"] for c in coded]
    truncated = False
    evidence = EVIDENCE_UNMEASURED
    counts: dict[str, dict[str, int]] = {c: {} for c in columns}

    if observed_codes:
        evidence = EVIDENCE_EXACT
        for col in columns:
            raw = observed_codes.get(col) or {}
            counts[col] = {
                key: int(n)
                for key, n in dict(raw).items()
                if normalize_code(key) is not None
            }
            if len(counts[col]) >= MAX_DISTINCT:
                truncated = True
    else:
        rows: Sequence[Mapping[str, Any]] | None = None
        if isinstance(population_rows, Sequence) and not isinstance(
            population_rows, (str, bytes)
        ):
            rows = population_rows
            evidence = EVIDENCE_EXACT if rows_are_population else EVIDENCE_SAMPLED
        elif isinstance(sample_rows, Sequence) and not isinstance(
            sample_rows, (str, bytes)
        ):
            rows = sample_rows
            evidence = EVIDENCE_EXACT if rows_are_population else EVIDENCE_SAMPLED
        if rows is None:
            evidence = EVIDENCE_UNMEASURED
        else:
            counts, truncated = collect_observed_codes(rows, columns)

    if truncated:
        evidence = EVIDENCE_UNMEASURED

    column_reports: list[dict[str, Any]] = []
    any_unmapped = False
    for item in coded:
        src = item["source"]
        cov = coverage_for_column(
            observed=counts.get(src) or {},
            crosswalk=item["crosswalk"],
        )
        if cov["unmapped_codes"]:
            any_unmapped = True
        named = cov["unmapped_codes"][:_NAMED_LIMIT]
        column_reports.append(
            {
                "source": src,
                "target": item["target"],
                "system": item["system"],
                "crosswalk_size": item["crosswalk_size"],
                **cov,
                "unmapped_named": named,
            }
        )

    return {
        "schema": REPORT_SCHEMA,
        "declared": True,
        "columns": column_reports,
        "evidence": evidence,
        "truncated": truncated,
        "any_unmapped": any_unmapped,
        "scan_method": (
            "observed_codes"
            if observed_codes
            else "population_rows"
            if evidence == EVIDENCE_EXACT
            else "sample_rows"
            if evidence == EVIDENCE_SAMPLED
            else "none"
        ),
        "honesty": (
            "Coverage is proven only when evidence=exact and no unmapped "
            "code remains. A covered sample is not a population proof. "
            "Unmapped codes are never passed through as identity."
        ),
    }


def build_code_crosswalk_gate(report: Mapping[str, Any]) -> dict[str, Any]:
    """Return the G20 gate for a coverage report."""
    if not report.get("declared"):
        return {
            "id": GATE_ID,
            "status": "skip",
            "message": (
                "No mapping declared a code crosswalk — population code "
                "coverage was not asked."
            ),
            "duration_ms": 0,
            "details": {
                "schema": REPORT_SCHEMA,
                "declared": False,
                "rule_id": f"{GATE_ID}.undeclared",
            },
        }

    columns = list(report.get("columns") or [])
    evidence = str(report.get("evidence") or EVIDENCE_UNMEASURED)
    unmapped_cols = [
        c for c in columns if isinstance(c, Mapping) and c.get("unmapped_codes")
    ]
    if unmapped_cols:
        named_codes: list[str] = []
        for col in unmapped_cols:
            src = str(col.get("source") or "")
            for code in list(col.get("unmapped_named") or col.get("unmapped_codes") or [])[
                :_NAMED_LIMIT
            ]:
                named_codes.append(f"{src}={code}")
            if len(named_codes) >= _NAMED_LIMIT:
                break
        more = ""
        leftover = sum(len(list(c.get("unmapped_codes") or [])) for c in unmapped_cols) - len(
            named_codes
        )
        if leftover > 0:
            more = f" (+{leftover} more)"
        return {
            "id": GATE_ID,
            "status": "block",
            "message": (
                f"{len(unmapped_cols)} coded column(s) have population values "
                f"with no target in the crosswalk: {', '.join(named_codes)}{more} "
                "— Datawrap will not pass those codes through or drop them silently."
            ),
            "duration_ms": 0,
            "details": {
                "schema": REPORT_SCHEMA,
                "evidence": evidence,
                "columns": columns,
                "rule_id": f"{GATE_ID}.unmapped",
                "remediation_kind": "review_mappings",
                "primary_action": "open_map",
            },
        }

    if report.get("truncated") or evidence != EVIDENCE_EXACT:
        reason = (
            "distinct-code cap reached"
            if report.get("truncated")
            else "only a sample was scanned"
        )
        return {
            "id": GATE_ID,
            "status": "block",
            "message": (
                f"Code crosswalk coverage is unproven ({reason}). A covered "
                "sample is not population proof — add the missing codes after a "
                "full distinct scan, or re-run Validate against the population."
            ),
            "duration_ms": 0,
            "details": {
                "schema": REPORT_SCHEMA,
                "evidence": evidence,
                "truncated": bool(report.get("truncated")),
                "columns": columns,
                "rule_id": f"{GATE_ID}.unproven",
                "remediation_kind": "review_mappings",
                "primary_action": "open_map",
            },
        }

    n = len(columns)
    distinct = sum(int(c.get("observed_distinct") or 0) for c in columns)
    return {
        "id": GATE_ID,
        "status": "pass",
        "message": (
            f"All observed codes on {n} coded column(s) have a target in the "
            f"crosswalk ({distinct} distinct value(s), population scan)."
        ),
        "duration_ms": 0,
        "details": {
            "schema": REPORT_SCHEMA,
            "evidence": evidence,
            "columns": columns,
            "rule_id": f"{GATE_ID}.covered",
        },
    }


def build_code_crosswalk_evidence(
    *,
    mappings: Sequence[Mapping[str, Any]],
    sample_rows: Sequence[Mapping[str, Any]] | None = None,
    population_rows: Sequence[Mapping[str, Any]] | None = None,
    rows_are_population: bool = False,
    observed_codes: Mapping[str, Mapping[str, int]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return ``(report, gate)`` for preflight / proof pack."""
    report = build_code_crosswalk_report(
        mappings=mappings,
        sample_rows=sample_rows,
        population_rows=population_rows,
        rows_are_population=rows_are_population,
        observed_codes=observed_codes,
    )
    return report, build_code_crosswalk_gate(report)


def proof_pack_code_crosswalk(report: Mapping[str, Any] | None) -> dict[str, Any]:
    """Auditor slice. No cell values beyond the unmapped code *keys*."""
    if not isinstance(report, Mapping) or not report.get("declared"):
        return {
            "schema": REPORT_SCHEMA,
            "declared": False,
            "honesty": (
                "No mapping declared a code_crosswalk on this job."
            ),
        }
    columns = []
    for col in list(report.get("columns") or []):
        if not isinstance(col, Mapping):
            continue
        columns.append(
            {
                "source": col.get("source"),
                "target": col.get("target"),
                "system": col.get("system"),
                "crosswalk_size": col.get("crosswalk_size"),
                "observed_distinct": col.get("observed_distinct"),
                "mapped_distinct": col.get("mapped_distinct"),
                "unmapped_codes": list(col.get("unmapped_codes") or [])[:50],
                "unmapped_rows": col.get("unmapped_rows"),
                "covered": col.get("covered"),
            }
        )
    return {
        "schema": REPORT_SCHEMA,
        "declared": True,
        "evidence": report.get("evidence"),
        "truncated": bool(report.get("truncated")),
        "columns": columns,
        "honesty": str(report.get("honesty") or ""),
        "scan_method": report.get("scan_method"),
    }


def _normalize_source_cfg(
    source_connector_id: str,
    source_config: Mapping[str, Any] | None,
) -> tuple[dict[str, Any] | None, str]:
    """Resolve inline or saved connector config the same way uniqueness does."""
    cfg: dict[str, Any] | None = None
    db_type = ""
    if source_connector_id:
        try:
            from services.connector_store import get_connector
            from services.connector_probe import probe_cfg_from_saved

            conn = get_connector(source_connector_id)
            if conn:
                cfg = probe_cfg_from_saved(conn)
                db_type = (conn.type or "").lower()
        except Exception as exc:  # noqa: BLE001 — missing store is a skip, not a crash
            logger.warning("G20 could not load source connector %s: %s", source_connector_id, exc)
    if cfg is None and source_config:
        cfg = dict(source_config)
        db_type = (
            str(
                cfg.get("type")
                or cfg.get("db_type")
                or cfg.get("format")
                or db_type
                or ""
            ).lower()
        )
    if not cfg:
        return None, db_type
    cfg = dict(cfg)
    if db_type:
        cfg.setdefault("type", db_type)
    nested = cfg.get("extra")
    if isinstance(nested, Mapping):
        for key, value in nested.items():
            cfg.setdefault(str(key), value)
    return cfg, db_type


def scan_sql_observed_codes(
    cfg: Mapping[str, Any],
    table: str,
    columns: Sequence[str],
    *,
    cap: int = MAX_DISTINCT,
) -> tuple[dict[str, dict[str, int]], bool]:
    """``GROUP BY`` each coded column. Does not walk row payloads."""
    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table
    from services.sql_object_identity import resolve_object_identity

    engine = _engine(dict(cfg))
    schema, tbl_name = split_qualified_table(
        table, (cfg.get("schema") or "").strip() or None
    )
    ident = resolve_object_identity(engine, tbl_name, schema, columns=list(columns))
    if ident.exists:
        tbl_name = sa.sql.quoted_name(ident.table, True)
        schema = sa.sql.quoted_name(ident.schema, True) if ident.schema else None
        columns = [ident.columns.get(c, c) for c in columns]

    tbl = sa.table(tbl_name, schema=schema)
    counts: dict[str, dict[str, int]] = {str(c): {} for c in columns}
    truncated = False
    with engine.connect() as conn:
        for col_name in columns:
            col = sa.column(col_name)
            cnt = sa.func.count().label("_cnt")
            stmt = (
                sa.select(col, cnt)
                .select_from(tbl)
                .group_by(col)
                .limit(cap + 1)
            )
            rows = conn.execute(stmt).fetchall()
            bucket = counts[str(col_name)]
            for row in rows:
                key = normalize_code(row[0] if row else None)
                if key is None:
                    continue
                if len(bucket) >= cap:
                    truncated = True
                    break
                bucket[key] = int(row[1] if len(row) > 1 else 1)
                if len(bucket) >= cap:
                    truncated = True
    return counts, truncated


def probe_population_codes(
    *,
    columns: Sequence[str],
    source_connector_id: str = "",
    source_config: Mapping[str, Any] | None = None,
    source_table: str = "",
    source_file_id: str = "",
    shape_recipe: Mapping[str, Any] | None = None,
    source_columns: Sequence[str] | None = None,
    cap: int = MAX_DISTINCT,
) -> tuple[dict[str, dict[str, int]] | None, bool, str]:
    """Population distinct scan for G20.

    Returns ``(observed, truncated, method)``. ``observed is None`` means the
    scan did not run — the gate must treat coverage as unproven, never as
    "the sample looked fine".
    """
    wanted = [str(c).strip() for c in columns if str(c).strip()]
    if not wanted:
        return None, False, "none"

    try:
        from services.procedure_source import is_callable_source

        if is_callable_source(source_config):
            return None, False, "skipped_callable"
    except Exception as exc:  # noqa: BLE001 — missing module is a skip
        logger.debug("G20 callable-source check skipped: %s", exc)

    cfg, db_type = _normalize_source_cfg(source_connector_id, source_config)
    table = str(source_table or "").strip()
    if cfg and table:
        from services.source_duplicate_probe import SQLISH_SOURCE_TYPES

        if db_type in SQLISH_SOURCE_TYPES or not db_type:
            try:
                observed, truncated = scan_sql_observed_codes(
                    cfg, table, wanted, cap=cap
                )
                return observed, truncated, "sql_group_by"
            except Exception as exc:  # noqa: BLE001 — engine refusal is unproven
                logger.warning(
                    "G20 SQL GROUP BY on %s failed; coverage stays unproven: %s",
                    table,
                    exc,
                )

    fid = str(source_file_id or "").strip()
    if fid:
        try:
            from services.file_parser import iter_stored_upload_rows

            rows = iter_stored_upload_rows(fid)
            if rows is not None and shape_recipe:
                from services.shape_preflight import shaped_population_rows

                shaped = shaped_population_rows(
                    dict(shape_recipe),
                    rows,
                    source_columns=list(source_columns or []),
                )
                if shaped is not None:
                    rows = shaped
            if rows is not None:
                observed, truncated = collect_observed_codes(rows, wanted, cap=cap)
                return observed, truncated, "file_population"
        except Exception as exc:  # noqa: BLE001 — unreadable upload is unproven
            logger.warning(
                "G20 file population scan failed; coverage stays unproven: %s",
                exc,
            )
    return None, False, "unmeasured"

