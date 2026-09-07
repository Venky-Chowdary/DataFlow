"""Gate G21 — independently recomputed control totals on monetary columns.

A row count (Gate-8 L1) proves two tables have the same cardinality. A bank
examiner asks whether the *ledger* moved: ``SUM(amount)`` on the source equals
``SUM(amount)`` on the destination, on connections this module opened itself.
An in-memory walk of loaded rows, a 25-row sample SUM, or ``ABS(src-tgt) <= 0.01``
is not that proof.

This gate is **opt-in**. Auto-detecting columns named ``amount`` / ``total``
would either SUM identifiers or fail open. A mapping is in G21 when:

* the operator declared ``control_total: true``, or
* the source/dest logical type is a money carrier (``MONEY`` / ``CURRENCY`` /
  ``SMALLMONEY``) and the operator did not set ``control_total: false``.

Identity mappings only. A currency-parse transform is not a source SUM the
destination can be compared to. Quarantined rows without a quarantined-amount
SUM leave the column **unproven** (fail closed) — dest SUM matching the
surviving source slice is not claimed.

Validate states the gate every run and skips: population control totals are a
post-write Gate-8 proof. A signed Migration Risk Contract does not demote G21.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any

from services.decimal_identity import dest_numeric_text_sql
from services.mapping_constraints import is_intentional_omit

logger = logging.getLogger(__name__)

GATE_ID = "g21_control_totals"
REPORT_SCHEMA = "control_totals_v1"
EVIDENCE_EXACT = "exact"
EVIDENCE_SAMPLED = "sampled"
EVIDENCE_UNMEASURED = "unmeasured"

IDENTITY_TRANSFORMS = frozenset(
    {
        "",
        "none",
        "identity",
        "passthrough",
        "copy",
        # Type-preserving numeric binds the write path stamps on DECIMAL/INTEGER
        # identity maps. Not currency/percentage parse.
        "decimal",
        "integer",
    }
)
MONEY_LOGICAL_TYPES = frozenset({"MONEY", "CURRENCY", "SMALLMONEY"})

_TRUTHY = frozenset({"true", "1", "yes", "on"})
_FALSY = frozenset({"false", "0", "no", "off"})


def _norm_type(raw: object) -> str:
    text = str(raw or "").strip().upper()
    if not text:
        return ""
    return text.split("(", 1)[0].strip()


def _flag(value: object) -> bool | None:
    """Tri-state operator declaration. None means unset."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in _TRUTHY:
        return True
    if text in _FALSY:
        return False
    return None


def is_money_logical_type(type_str: object) -> bool:
    return _norm_type(type_str) in MONEY_LOGICAL_TYPES


def _transform_id(mapping: Mapping[str, Any]) -> str:
    """Operator-declared transform. Engine-stamped binds must not hide ``none``."""
    raw = mapping.get("transform")
    if raw is None or str(raw).strip() == "":
        raw = mapping.get("engine_transform") or mapping.get("engineTransform") or ""
    text = str(raw).strip().lower()
    return text


def mapping_asks_control_total(mapping: Mapping[str, Any] | None) -> bool:
    """True when this mapping declared ``control_total: true``.

    MONEY/CURRENCY/SMALLMONEY carriers are *candidates* on Map (checkbox
    defaulted on) but the engine only SUMs an explicit declaration — same
    fail-closed opt-in as G20. Auto-including every money type would SUM
    columns a route never asked to prove.
    """
    if not mapping or is_intentional_omit(mapping):
        return False
    return _flag(mapping.get("control_total")) is True


def is_identity_control_total(mapping: Mapping[str, Any]) -> bool:
    return _transform_id(mapping) in IDENTITY_TRANSFORMS


def control_total_mappings(
    mappings: Sequence[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Mappings G21 will SUM, with source/target names attached."""
    out: list[dict[str, Any]] = []
    for mapping in mappings or []:
        if not mapping_asks_control_total(mapping):
            continue
        source = str(mapping.get("source") or "").strip()
        target = str(mapping.get("target") or "").strip()
        if not source or not target:
            continue
        out.append(
            {
                "source": source,
                "target": target,
                "transform": _transform_id(mapping),
                "identity": is_identity_control_total(mapping),
                "target_type": str(mapping.get("target_type") or ""),
                "source_type": str(mapping.get("source_type") or ""),
            }
        )
    return out


def decimal_from_sql_sum(value: object) -> Decimal | None:
    """Exact Decimal from a SUM payload. Float is unproven — never compared."""
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return None
    text = str(value).strip()
    if not text:
        return Decimal("0")
    # Engine float-as-text (scientific / binary residue) is not a ledger proof.
    lowered = text.lower()
    if "e" in lowered or "inf" in lowered or lowered == "nan":
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _quote_col(db_type: str, name: str) -> str:
    from connectors.sql_identifiers import quote_sql_identifier, require_safe_identifier

    ident = require_safe_identifier(name, preserve_case=True, max_len=128)
    dialect = (db_type or "").strip().lower()
    if dialect in {"mysql", "mariadb", "clickhouse", "databricks"}:
        return quote_sql_identifier(ident, "`")
    if dialect in {"sqlserver", "mssql"}:
        return quote_sql_identifier(ident, "[")
    return quote_sql_identifier(ident, '"')


def _table_ref(db_type: str, schema: str, table: str) -> str:
    from connectors.sql_identifiers import quote_table_ref

    return quote_table_ref(table, schema or None, dialect=db_type or "ansi")


def independent_column_sum(
    db_type: str,
    cfg: Mapping[str, Any] | None,
    *,
    schema: str,
    table: str,
    column: str,
) -> dict[str, Any]:
    """``SELECT <dest text spelling of COALESCE(SUM(col), 0)>`` on a fresh connection.

    Returns ``{available, sum, reason}``. ``sum`` is a Decimal string when
    available. A float driver result is unproven.
    """
    if not table or not column:
        return {
            "available": False,
            "sum": None,
            "reason": "table or column name missing",
        }
    if not cfg:
        return {"available": False, "sum": None, "reason": "no connection config"}

    from connectors.generic_sql import get_sqlalchemy_engine
    import sqlalchemy as sa

    try:
        engine = get_sqlalchemy_engine({**dict(cfg), "type": db_type})
    except Exception as exc:  # noqa: BLE001 — a refused engine is unproven evidence
        return {"available": False, "sum": None, "reason": f"cannot connect: {exc}"}

    quoted_col = _quote_col(db_type, column)
    table_sql = _table_ref(db_type, schema, table)
    # Text keeps NUMERIC/INTEGER exact; a float SUM never becomes Decimal. The
    # spelling is the destination engine's own, not Postgres' — a wrong cast
    # target is a syntax error that would leave a declared ledger unproven.
    sum_text = dest_numeric_text_sql(db_type, f"COALESCE(SUM({quoted_col}), 0)")
    sql = f"SELECT {sum_text} FROM {table_sql}"
    try:
        with engine.connect() as conn:
            row = conn.execute(sa.text(sql)).fetchone()
    except Exception as exc:  # noqa: BLE001 — a failed SUM is unproven, not zero
        logger.warning("G21 independent SUM failed on %s.%s: %s", table, column, exc)
        return {"available": False, "sum": None, "reason": f"sum failed: {exc}"}

    raw = None if row is None else row[0]
    parsed = decimal_from_sql_sum(raw)
    if parsed is None:
        return {
            "available": False,
            "sum": None,
            "reason": (
                "SUM result is not an exact decimal "
                f"(got {type(raw).__name__}) — float sums are not a ledger proof"
            ),
        }
    return {
        "available": True,
        "sum": format(parsed, "f"),
        "reason": "",
        "scan_sql": dest_numeric_text_sql(db_type, "COALESCE(SUM(col), 0)"),
    }


def _column_verdict(
    item: Mapping[str, Any],
    *,
    source_sum: dict[str, Any] | None,
    dest_sum: dict[str, Any] | None,
    rejected_rows: int,
    sample_only: bool,
) -> dict[str, Any]:
    """One column's control-total comparison. Never stores a row payload."""
    base: dict[str, Any] = {
        "source": item["source"],
        "target": item["target"],
        "transform": item["transform"],
        "identity": bool(item["identity"]),
        "matched": False,
        "proven": False,
        "source_sum": None,
        "dest_sum": None,
        "reason": "",
    }
    if sample_only:
        base["reason"] = (
            "A sample SUM is not a population control total — "
            "G21 is a post-write independent SUM of the whole table"
        )
        return base
    if not item["identity"]:
        base["reason"] = (
            f"transform {item['transform']!r} is not identity — "
            "Datawrap will not invent a transformed source SUM"
        )
        return base
    if rejected_rows > 0:
        base["reason"] = (
            f"{rejected_rows} quarantined row(s) and no quarantined-amount SUM "
            "— dest SUM cannot be claimed to match the source ledger"
        )
        return base
    src = source_sum or {}
    dst = dest_sum or {}
    if not src.get("available"):
        base["reason"] = str(src.get("reason") or "source SUM unproven")
        return base
    if not dst.get("available"):
        base["reason"] = str(dst.get("reason") or "destination SUM unproven")
        return base
    src_dec = decimal_from_sql_sum(src.get("sum"))
    dst_dec = decimal_from_sql_sum(dst.get("sum"))
    if src_dec is None or dst_dec is None:
        base["reason"] = "SUM payload was not an exact decimal"
        return base
    base["source_sum"] = format(src_dec, "f")
    base["dest_sum"] = format(dst_dec, "f")
    if src_dec != dst_dec:
        base["reason"] = (
            f"control total mismatch: source SUM={src_dec} dest SUM={dst_dec}"
        )
        return base
    base["matched"] = True
    base["proven"] = True
    base["reason"] = "independent source SUM equals destination SUM"
    return base


def build_control_totals_report(
    *,
    mappings: Sequence[Mapping[str, Any]] | None,
    phase: str = "validate",
    source_sums: Mapping[str, Mapping[str, Any]] | None = None,
    dest_sums: Mapping[str, Mapping[str, Any]] | None = None,
    rejected_rows: int = 0,
    sample_only: bool = False,
) -> dict[str, Any]:
    """Auditor-facing control-total report. Never stores a full row."""
    declared = control_total_mappings(mappings)
    if not declared:
        return {
            "schema": REPORT_SCHEMA,
            "declared": False,
            "columns": [],
            "evidence": EVIDENCE_UNMEASURED,
            "honesty": (
                "No mapping declared control_total and no MONEY/CURRENCY/"
                "SMALLMONEY carrier was in the map. G21 does not invent "
                "monetary columns from names."
            ),
        }

    execute = str(phase or "").lower() in {"execute", "post_write", "reconcile"}
    columns: list[dict[str, Any]] = []
    for item in declared:
        src_key = item["source"]
        tgt_key = item["target"]
        columns.append(
            _column_verdict(
                item,
                source_sum=(source_sums or {}).get(src_key),
                dest_sum=(dest_sums or {}).get(tgt_key),
                rejected_rows=int(rejected_rows or 0),
                sample_only=sample_only or not execute,
            )
        )

    any_unproven = any(not c.get("proven") for c in columns)
    any_mismatch = any(
        c.get("source_sum") is not None
        and c.get("dest_sum") is not None
        and not c.get("matched")
        for c in columns
    )
    evidence = EVIDENCE_UNMEASURED
    if execute and not sample_only and not any_unproven:
        evidence = EVIDENCE_EXACT
    elif sample_only:
        evidence = EVIDENCE_SAMPLED

    return {
        "schema": REPORT_SCHEMA,
        "declared": True,
        "columns": columns,
        "evidence": evidence,
        "any_unproven": any_unproven,
        "any_mismatch": any_mismatch,
        "rejected_rows": int(rejected_rows or 0),
        "honesty": (
            "Control totals are proven only when evidence=exact, every declared "
            "column is an identity mapping, both SUMs are independent exact "
            "Decimals, and they compare equal. A sample SUM is not proof. "
            "A signed risk contract does not waive a ledger mismatch."
        ),
    }


def build_control_totals_gate(report: Mapping[str, Any], *, phase: str = "validate") -> dict[str, Any]:
    """Return the G21 gate for a control-total report."""
    execute = str(phase or "").lower() in {"execute", "post_write", "reconcile"}
    if not report.get("declared"):
        return {
            "id": GATE_ID,
            "status": "skip",
            "message": (
                "No mapping declared a control total — population SUM proof "
                "was not asked."
            ),
            "duration_ms": 0,
            "details": {
                "schema": REPORT_SCHEMA,
                "declared": False,
                "rule_id": f"{GATE_ID}.undeclared",
            },
        }

    if not execute:
        n = len(list(report.get("columns") or []))
        return {
            "id": GATE_ID,
            "status": "skip",
            "message": (
                f"{n} monetary column(s) declared a control total. Population "
                "SUM(source)=SUM(dest) is a post-write Gate-8 proof — a Validate "
                "sample SUM is not that proof."
            ),
            "duration_ms": 0,
            "details": {
                "schema": REPORT_SCHEMA,
                "declared": True,
                "columns": list(report.get("columns") or []),
                "evidence": EVIDENCE_UNMEASURED,
                "rule_id": f"{GATE_ID}.post_write",
                "remediation_kind": "review_mappings",
                "primary_action": "open_map",
            },
        }

    columns = [c for c in list(report.get("columns") or []) if isinstance(c, Mapping)]
    mismatches = [c for c in columns if c.get("any_mismatch") or (
        c.get("source_sum") is not None
        and c.get("dest_sum") is not None
        and not c.get("matched")
    )]
    unproven = [c for c in columns if not c.get("proven")]
    if mismatches:
        named = ", ".join(
            f"{c.get('source')}→{c.get('target')} "
            f"(src {c.get('source_sum')} ≠ dest {c.get('dest_sum')})"
            for c in mismatches[:4]
        )
        return {
            "id": GATE_ID,
            "status": "block",
            "message": (
                f"{len(mismatches)} control total(s) do not match: {named}. "
                "A row count is not a ledger balance."
            ),
            "duration_ms": 0,
            "details": {
                "schema": REPORT_SCHEMA,
                "evidence": report.get("evidence"),
                "columns": columns,
                "rule_id": f"{GATE_ID}.mismatch",
                "remediation_kind": "review_mappings",
                "primary_action": "open_map",
            },
        }
    if unproven:
        named = ", ".join(
            f"{c.get('source')}→{c.get('target')}: {c.get('reason')}"
            for c in unproven[:3]
        )
        return {
            "id": GATE_ID,
            "status": "block",
            "message": (
                f"{len(unproven)} control total(s) are unproven: {named}. "
                "Fail closed — a missing SUM is not a matching ledger."
            ),
            "duration_ms": 0,
            "details": {
                "schema": REPORT_SCHEMA,
                "evidence": report.get("evidence"),
                "columns": columns,
                "rule_id": f"{GATE_ID}.unproven",
                "remediation_kind": "review_mappings",
                "primary_action": "open_map",
            },
        }

    n = len(columns)
    return {
        "id": GATE_ID,
        "status": "pass",
        "message": (
            f"Independent source SUM equals destination SUM on {n} control-total "
            "column(s)."
        ),
        "duration_ms": 0,
        "details": {
            "schema": REPORT_SCHEMA,
            "evidence": EVIDENCE_EXACT,
            "columns": columns,
            "rule_id": f"{GATE_ID}.matched",
        },
    }


def verify_control_totals(
    *,
    mappings: Sequence[Mapping[str, Any]] | None,
    source_db_type: str = "",
    source_cfg: Mapping[str, Any] | None = None,
    source_schema: str = "",
    source_table: str = "",
    dest_db_type: str = "",
    dest_cfg: Mapping[str, Any] | None = None,
    dest_schema: str = "",
    dest_table: str = "",
    rejected_rows: int = 0,
    phase: str = "execute",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Independent source and dest SUMs on separate connections. ``(report, gate)``."""
    declared = control_total_mappings(mappings)
    if not declared:
        report = build_control_totals_report(mappings=mappings, phase=phase)
        return report, build_control_totals_gate(report, phase=phase)

    if str(phase or "").lower() not in {"execute", "post_write", "reconcile"}:
        report = build_control_totals_report(mappings=mappings, phase=phase)
        return report, build_control_totals_gate(report, phase=phase)

    source_sums: dict[str, dict[str, Any]] = {}
    dest_sums: dict[str, dict[str, Any]] = {}
    for item in declared:
        if not item["identity"]:
            continue
        src = item["source"]
        tgt = item["target"]
        if src not in source_sums:
            source_sums[src] = independent_column_sum(
                source_db_type,
                source_cfg,
                schema=source_schema,
                table=source_table,
                column=src,
            )
        if tgt not in dest_sums:
            dest_sums[tgt] = independent_column_sum(
                dest_db_type,
                dest_cfg,
                schema=dest_schema,
                table=dest_table,
                column=tgt,
            )

    report = build_control_totals_report(
        mappings=mappings,
        phase=phase,
        source_sums=source_sums,
        dest_sums=dest_sums,
        rejected_rows=rejected_rows,
    )
    return report, build_control_totals_gate(report, phase=phase)


def proof_pack_control_totals(report: Mapping[str, Any] | None) -> dict[str, Any]:
    """Auditor slice: column names and SUMs, never row payloads."""
    if not isinstance(report, Mapping) or not report.get("declared"):
        return {
            "schema": REPORT_SCHEMA,
            "declared": False,
            "honesty": "No mapping declared a control total on this job.",
        }
    columns = []
    for col in list(report.get("columns") or []):
        if not isinstance(col, Mapping):
            continue
        columns.append(
            {
                "source": col.get("source"),
                "target": col.get("target"),
                "source_sum": col.get("source_sum"),
                "dest_sum": col.get("dest_sum"),
                "matched": bool(col.get("matched")),
                "proven": bool(col.get("proven")),
                "reason": col.get("reason"),
            }
        )
    return {
        "schema": REPORT_SCHEMA,
        "declared": True,
        "evidence": report.get("evidence"),
        "columns": columns,
        "honesty": str(report.get("honesty") or ""),
    }


def apply_control_totals_to_reconcile(
    stamped: dict[str, Any],
    *,
    mappings: Sequence[Mapping[str, Any]] | None,
    source_db_type: str = "",
    source_cfg: Mapping[str, Any] | None = None,
    source_schema: str = "",
    source_table: str = "",
    dest_db_type: str = "",
    dest_cfg: Mapping[str, Any] | None = None,
    dest_schema: str = "",
    dest_table: str = "",
    rejected_rows: int = 0,
) -> dict[str, Any]:
    """Stamp G21 onto a Gate-8 report and fail the job on mismatch/unproven."""
    report, gate = verify_control_totals(
        mappings=mappings,
        source_db_type=source_db_type,
        source_cfg=source_cfg,
        source_schema=source_schema,
        source_table=source_table,
        dest_db_type=dest_db_type,
        dest_cfg=dest_cfg,
        dest_schema=dest_schema,
        dest_table=dest_table,
        rejected_rows=rejected_rows,
        phase="execute",
    )
    out = dict(stamped)
    out["control_totals"] = report
    out["g21_control_totals"] = gate
    if gate.get("status") == "block":
        out["passed"] = False
        prior = str(out.get("message") or "").rstrip()
        extra = str(gate.get("message") or "G21 control totals failed")
        out["message"] = f"{prior} {extra}".strip() if prior else extra
    return out
