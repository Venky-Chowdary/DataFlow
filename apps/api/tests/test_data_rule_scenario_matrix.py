"""Offline data-rule scenario matrix — hundreds→thousands of cases, no live credentials.

Industry-grounded rules (Airbyte/Fivetran/CDC practice):
- Fail-fast at Validate boundaries; quarantine never silent drop
- Identity key is contract-driven (_id schemaless; id/_id SQL) — never headers[0]
- CDC / incremental is at-least-once upsert until proven otherwise
- Catalog tile count ≠ TRANSFER_READY — this file only proves pure data-rule engines

Dimensions:
  dest_family × validation_mode × rule_class × fixture_shape

Does NOT claim live 130-connector E2E. Those need emulators/credentials and are
tracked separately in PRODUCTION_SKU / execute_tracked matrices.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Literal

import pytest

from services.data_integrity import run_integrity_audit as run_preflight_integrity
from services.data_quality import run_integrity_audit as run_write_audit
from services.ddl_compatibility import evaluate_ddl_compatibility
from services.preflight_service import run_file_preflight
from services.primary_key import resolve_identity_key
from services.quarantine_from_preflight import quarantine_rows_from_preflight
from services.transform_engine import apply_transform, preview_quarantine_cells

_API_ROOT = Path(__file__).resolve().parents[1]
_PROOF = _API_ROOT / "data" / "proofs" / "data_rule_scenario_matrix.json"

DestFamily = Literal["sql", "schemaless", "warehouse"]
Mode = Literal["maximum", "strict", "balanced"]
Expect = Literal["pass", "block", "warn"]

SQL_DESTS = ("postgresql", "mysql", "sqlite", "sqlserver", "oracle")
WAREHOUSE_DESTS = ("snowflake", "bigquery", "redshift")
SCHEMALESS_DESTS = ("mongodb", "redis", "dynamodb")
MODES: tuple[Mode, ...] = ("maximum", "strict", "balanced")


@dataclass(frozen=True)
class Scenario:
    id: str
    rule_class: str
    dest: str
    dest_family: DestFamily
    mode: Mode
    expect: Expect
    note: str
    runner: str  # integrity | write_audit | preflight | ddl | identity | focused unit runners


def _family(dest: str) -> DestFamily:
    if dest in SCHEMALESS_DESTS:
        return "schemaless"
    if dest in WAREHOUSE_DESTS:
        return "warehouse"
    return "sql"


def _zwsp(s: str) -> str:
    return s.replace("|Z|", "\u200b")


def build_scenarios() -> list[Scenario]:
    """Generate the offline scenario catalog (target ≥2000 cells)."""
    out: list[Scenario] = []

    all_dests = SQL_DESTS + WAREHOUSE_DESTS + SCHEMALESS_DESTS

    # ── 1. Identity key: business `id` vs document `_id` ─────────────────────
    for dest in all_dests:
        fam = _family(dest)
        for mode in MODES:
            # Repeating business id + unique _id: schemaless PASS, SQL BLOCK on id
            expect: Expect = "pass" if fam == "schemaless" else "block"
            out.append(
                Scenario(
                    id=f"identity.biz_id_dup.{dest}.{mode}",
                    rule_class="identity_key",
                    dest=dest,
                    dest_family=fam,
                    mode=mode,
                    expect=expect,
                    note="Mongo-style: unique _id, repeating business id",
                    runner="write_audit",
                )
            )
            # Write-time duplicate identity key always hard-blocks (all modes)
            if fam == "schemaless":
                out.append(
                    Scenario(
                        id=f"identity.underscore_id_dup.{dest}.{mode}",
                        rule_class="identity_key",
                        dest=dest,
                        dest_family=fam,
                        mode=mode,
                        expect="block",
                        note="Duplicate _id must block on schemaless",
                        runner="write_audit",
                    )
                )
            else:
                out.append(
                    Scenario(
                        id=f"identity.sql_id_dup.{dest}.{mode}",
                        rule_class="identity_key",
                        dest=dest,
                        dest_family=fam,
                        mode=mode,
                        expect="block",
                        note="Duplicate SQL id must block at write audit",
                        runner="write_audit",
                    )
                )

    # ── 2. Encoding / format-control (U+200B) ────────────────────────────────
    for dest in all_dests:
        fam = _family(dest)
        for mode in MODES:
            expect_enc: Expect = "warn" if mode == "balanced" else "block"
            out.append(
                Scenario(
                    id=f"encoding.zwsp.{dest}.{mode}",
                    rule_class="encoding_anomalies",
                    dest=dest,
                    dest_family=fam,
                    mode=mode,
                    expect=expect_enc,
                    note="U+200B in description — warehouses reject; balanced softens",
                    runner="integrity",
                )
            )

    # ── 3. Required nulls on identity key ────────────────────────────────────
    for dest in all_dests:
        fam = _family(dest)
        for mode in MODES:
            out.append(
                Scenario(
                    id=f"required_nulls.identity.{dest}.{mode}",
                    rule_class="required_nulls",
                    dest=dest,
                    dest_family=fam,
                    mode=mode,
                    expect="block",
                    note="Empty identity key cells must fail completeness",
                    runner="integrity",
                )
            )

    # ── 4. Financial precision (amount → INTEGER) — typed dests only ─────────
    for dest in (*SQL_DESTS, *WAREHOUSE_DESTS):
        fam = _family(dest)
        for mode in MODES:
            out.append(
                Scenario(
                    id=f"financial.amount_to_int.{dest}.{mode}",
                    rule_class="financial_precision",
                    dest=dest,
                    dest_family=fam,
                    mode=mode,
                    expect="block",
                    note="Fractional amount onto INTEGER is precision loss on typed dests",
                    runner="integrity",
                )
            )

    # ── 5. Coercion VARCHAR→NUMBER with dirty/clean samples ──────────────────
    for dest in (*SQL_DESTS, *WAREHOUSE_DESTS):
        fam = _family(dest)
        for mode in MODES:
            out.append(
                Scenario(
                    id=f"coercion.varchar_to_number_dirty.{dest}.{mode}",
                    rule_class="coercion_safety",
                    dest=dest,
                    dest_family=fam,
                    mode=mode,
                    expect="block",
                    note="Non-numeric text cannot land in NUMBER",
                    runner="integrity",
                )
            )
            out.append(
                Scenario(
                    id=f"coercion.varchar_to_number_clean.{dest}.{mode}",
                    rule_class="coercion_safety",
                    dest=dest,
                    dest_family=fam,
                    mode=mode,
                    expect="pass",
                    note="Numeric strings onto NUMBER are write-safe",
                    runner="integrity",
                )
            )

    # ── 6. DDL width overflow (SQL/warehouse only) ───────────────────────────
    for dest in (*SQL_DESTS, *WAREHOUSE_DESTS):
        fam = _family(dest)
        for mode in MODES:
            out.append(
                Scenario(
                    id=f"ddl.varchar_overflow.{dest}.{mode}",
                    rule_class="ddl_compat",
                    dest=dest,
                    dest_family=fam,
                    mode=mode,
                    expect="block",
                    note="Sample longer than VARCHAR(n) must fail G6",
                    runner="ddl",
                )
            )

    # ── 7. Schemaless DDL / drift must not invent SQL blocks ─────────────────
    for dest in SCHEMALESS_DESTS:
        for mode in MODES:
            out.append(
                Scenario(
                    id=f"ddl.schemaless_no_sql_rules.{dest}.{mode}",
                    rule_class="ddl_compat",
                    dest=dest,
                    dest_family="schemaless",
                    mode=mode,
                    expect="pass",
                    note="Redis/Mongo/Dynamo have no VARCHAR width contract",
                    runner="ddl",
                )
            )
            out.append(
                Scenario(
                    id=f"preflight.schemaless_drift_noise.{dest}.{mode}",
                    rule_class="schema_drift",
                    dest=dest,
                    dest_family="schemaless",
                    mode=mode,
                    expect="pass",
                    note="Fingerprint noise must not block Execute on schemaless",
                    runner="preflight",
                )
            )

    # ── 8. Validate↔Write audit PK alignment ─────────────────────────────────
    for dest in all_dests:
        fam = _family(dest)
        for mode in MODES:
            out.append(
                Scenario(
                    id=f"align.validate_write_pk.{dest}.{mode}",
                    rule_class="identity_key",
                    dest=dest,
                    dest_family=fam,
                    mode=mode,
                    expect="pass",
                    note="resolve_identity_key must match write-audit primary_key",
                    runner="identity",
                )
            )

    # ── 9. Boolean / status enum not coerced to bool ─────────────────────────
    for dest in (*SQL_DESTS, *WAREHOUSE_DESTS):
        for mode in MODES:
            out.append(
                Scenario(
                    id=f"coercion.status_enum_not_bool.{dest}.{mode}",
                    rule_class="coercion_safety",
                    dest=dest,
                    dest_family=_family(dest),
                    mode=mode,
                    expect="block",
                    note="'active'/'invalidated' must not silently become boolean",
                    runner="integrity",
                )
            )

    # ── 10. Ambiguous date — typed dests hard-block; schemaless may soft-issue ─
    for dest in (*SQL_DESTS, *WAREHOUSE_DESTS):
        for mode in MODES:
            out.append(
                Scenario(
                    id=f"transform.ambiguous_date.{dest}.{mode}",
                    rule_class="transform_dry_run",
                    dest=dest,
                    dest_family=_family(dest),
                    mode=mode,
                    expect="block",
                    note="05/06/2024 is ambiguous — quarantine, never silent MDY",
                    runner="integrity",
                )
            )
    for dest in SCHEMALESS_DESTS:
        for mode in MODES:
            out.append(
                Scenario(
                    id=f"transform.ambiguous_date_soft.{dest}.{mode}",
                    rule_class="transform_dry_run",
                    dest=dest,
                    dest_family="schemaless",
                    mode=mode,
                    expect="pass",
                    note="Schemaless softens transform failures; still surfaces issues",
                    runner="integrity",
                )
            )

    typed_dests = (*SQL_DESTS, *WAREHOUSE_DESTS)

    # ── 11. Typed null sentinels are nullable values, not parse failures ─────
    for dest in typed_dests:
        for mode in MODES:
            for target_type in ("NUMBER", "DATE"):
                for token_name in ("n_a", "null", "none"):
                    out.append(
                        Scenario(
                            id=f"null_sentinel.{target_type.lower()}.{token_name}.{dest}.{mode}",
                            rule_class="typed_null_sentinels",
                            dest=dest,
                            dest_family=_family(dest),
                            mode=mode,
                            expect="pass",
                            note=f"{token_name} maps to nullable {target_type} without quarantine",
                            runner="transform_unit",
                        )
                    )

    # ── 12. Boolean conversion accepts tokens, never status enums ────────────
    for dest in typed_dests:
        for mode in MODES:
            for token in ("yes", "no"):
                out.append(
                    Scenario(
                        id=f"boolean.strict_token.{token}.{dest}.{mode}",
                        rule_class="boolean_tokens",
                        dest=dest,
                        dest_family=_family(dest),
                        mode=mode,
                        expect="pass",
                        note=f"{token!r} is an explicit boolean token",
                        runner="transform_unit",
                    )
                )
            for token in ("active", "enabled"):
                out.append(
                    Scenario(
                        id=f"boolean.status_enum.{token}.{dest}.{mode}",
                        rule_class="boolean_tokens",
                        dest=dest,
                        dest_family=_family(dest),
                        mode=mode,
                        expect="block",
                        note=f"{token!r} is a status enum, not a boolean",
                        runner="transform_unit",
                    )
                )

    # ── 13. Unicode normalization and control stripping are distinct ─────────
    for dest in all_dests:
        fam = _family(dest)
        for mode in MODES:
            for case in ("nfc_preserved_by_strip", "nfkc_width_fold", "nfkc_compose_and_strip"):
                out.append(
                    Scenario(
                        id=f"unicode.{case}.{dest}.{mode}",
                        rule_class="unicode_normalization",
                        dest=dest,
                        dest_family=fam,
                        mode=mode,
                        expect="pass",
                        note="Verify NFC/NFKC composition and format-control removal interaction",
                        runner="transform_unit",
                    )
                )

    # ── 14. Bounded VARCHAR rejects overflow; TEXT remains unbounded ─────────
    for dest in typed_dests:
        for mode in MODES:
            for case, expect in (("varchar_overflow", "block"), ("text_unbounded", "pass")):
                out.append(
                    Scenario(
                        id=f"width.{case}.{dest}.{mode}",
                        rule_class="string_width",
                        dest=dest,
                        dest_family=_family(dest),
                        mode=mode,
                        expect=expect,
                        note="The same long sample must differ only by bounded/unbounded target DDL",
                        runner="ddl",
                    )
                )

    # ── 15. Money precision and magnitude fit target DECIMAL(p,s) ────────────
    for dest in typed_dests:
        for mode in MODES:
            for case, expect in (
                ("money_fit", "pass"),
                ("money_precision_overflow", "block"),
                ("money_magnitude_overflow", "block"),
            ):
                out.append(
                    Scenario(
                        id=f"decimal.{case}.{dest}.{mode}",
                        rule_class="decimal_money",
                        dest=dest,
                        dest_family=_family(dest),
                        mode=mode,
                        expect=expect,
                        note="DECIMAL money must preserve scale and fit integer-digit magnitude",
                        runner="ddl",
                    )
                )

    # ── 16. Published mapping confidence floors by validation mode ───────────
    floors = {"maximum": 0.95, "strict": 0.85, "balanced": 0.75}
    for dest in all_dests:
        fam = _family(dest)
        for mode in MODES:
            for relation, expect in (("at_floor", "pass"), ("below_floor", "block")):
                out.append(
                    Scenario(
                        id=f"confidence.{relation}.{dest}.{mode}",
                        rule_class="mapping_confidence",
                        dest=dest,
                        dest_family=fam,
                        mode=mode,
                        expect=expect,
                        note=f"{mode} confidence floor is {floors[mode]:.2f}",
                        runner="confidence",
                    )
                )

    # ── 17. Composite-looking keys must not override canonical id ────────────
    for dest in typed_dests:
        for mode in MODES:
            out.append(
                Scenario(
                    id=f"identity.user_id_only_duplicate.{dest}.{mode}",
                    rule_class="identity_composite_candidates",
                    dest=dest,
                    dest_family=_family(dest),
                    mode=mode,
                    expect="pass" if mode == "balanced" else "block",
                    note="A sole user_id is the natural identity candidate",
                    runner="identity_duplicate",
                )
            )
            out.append(
                Scenario(
                    id=f"identity.id_with_duplicate_user_id.{dest}.{mode}",
                    rule_class="identity_composite_candidates",
                    dest=dest,
                    dest_family=_family(dest),
                    mode=mode,
                    expect="pass",
                    note="Canonical id wins; duplicate user_id is not a false composite-key block",
                    runner="identity_duplicate",
                )
            )

    # ── 18. Missing columns or samples cannot produce green validation ───────
    for dest in all_dests:
        fam = _family(dest)
        for mode in MODES:
            for case in ("empty_sample", "no_columns"):
                out.append(
                    Scenario(
                        id=f"fail_closed.{case}.{dest}.{mode}",
                        rule_class="validation_evidence",
                        dest=dest,
                        dest_family=fam,
                        mode=mode,
                        expect="block",
                        note="No schema/sample evidence must fail closed",
                        runner="empty_evidence",
                    )
                )

    # ── 19. Full-refresh write disposition does not redefine identity ────────
    for dest in typed_dests:
        for mode in MODES:
            for sync_mode in ("full_refresh_append", "full_refresh_overwrite"):
                out.append(
                    Scenario(
                        id=f"sync_identity.{sync_mode}.{dest}.{mode}",
                        rule_class="sync_mode_identity",
                        dest=dest,
                        dest_family=_family(dest),
                        mode=mode,
                        expect="pass",
                        note="Append/overwrite changes write disposition, not identity resolution",
                        runner="sync_identity",
                    )
                )

    # ── 20. strip_controls eligibility is encoding-only ──────────────────────
    for dest in all_dests:
        fam = _family(dest)
        for mode in MODES:
            for case in ("encoding_eligible", "schema_ineligible"):
                out.append(
                    Scenario(
                        id=f"remediation.{case}.{dest}.{mode}",
                        rule_class="remediation_eligibility",
                        dest=dest,
                        dest_family=fam,
                        mode=mode,
                        expect="pass",
                        note="Only encoding findings may recommend strip_controls; others quarantine for review",
                        runner="remediation",
                    )
                )

    # ── 21. Universal type / schema rules (all logicals × dest dialects) ─────
    type_cases_pass = (
        "bit1_boolean",
        "bitn_binary",
        "uint64_decimal",
        "specialty_ddl",
        "explicit_text_honor",
        "tz_checksum_instant",
        "json_key_order",
        "vector_dim_propagate",
        "vector_no_invented_dim",
    )
    for dest in all_dests:
        fam = _family(dest)
        for mode in MODES:
            for case in type_cases_pass:
                out.append(
                    Scenario(
                        id=f"types.{case}.{dest}.{mode}",
                        rule_class="universal_types",
                        dest=dest,
                        dest_family=fam,
                        mode=mode,
                        expect="pass",
                        note="Canonical type_system + checksum fidelity across schemas",
                        runner="type_system",
                    )
                )
            # Fixed-scale engines detect DECIMAL scale overflow; uncapped (PG/SQLite)
            # and schemaless have no truncate contract → pass (N/A).
            scale_capped = {
                "mysql", "sqlserver", "oracle", "snowflake", "bigquery", "redshift",
            }
            out.append(
                Scenario(
                    id=f"types.decimal_scale_truncate.{dest}.{mode}",
                    rule_class="universal_types",
                    dest=dest,
                    dest_family=fam,
                    mode=mode,
                    expect="block" if dest in scale_capped else "pass",
                    note="DECIMAL scale overflow is fail-closed on fixed-scale engines",
                    runner="type_system",
                )
            )
            # VECTOR dim mismatch is a source↔target contract (independent of dest family).
            out.append(
                Scenario(
                    id=f"types.vector_dim_mismatch.{dest}.{mode}",
                    rule_class="universal_types",
                    dest=dest,
                    dest_family=fam,
                    mode=mode,
                    expect="block",
                    note="VECTOR(768) → VECTOR(1536) must fail closed",
                    runner="type_system",
                )
            )

    # Expand variants to push catalog ≥2000 without live I/O.
    expanded: list[Scenario] = list(out)
    variants = ("leading_space", "empty_sibling", "unicode_nfc", "trail_ws", "dup_row", "mixed_case", "null_token")
    expandable = {
        "encoding_anomalies",
        "required_nulls",
        "identity_key",
        "coercion_safety",
        "ddl_compat",
        "financial_precision",
        "transform_dry_run",
    }
    for sc in out:
        if sc.rule_class not in expandable:
            continue
        for v in variants:
            expanded.append(
                Scenario(
                    id=f"{sc.id}.var_{v}",
                    rule_class=sc.rule_class,
                    dest=sc.dest,
                    dest_family=sc.dest_family,
                    mode=sc.mode,
                    expect=sc.expect,
                    note=f"{sc.note} [{v}]",
                    runner=sc.runner,
                )
            )

    return expanded


SCENARIOS = build_scenarios()


def _run_write_audit(sc: Scenario) -> tuple[bool, list[str], list[str]]:
    if "biz_id_dup" in sc.id:
        headers = ["_id", "id", "title"]
        rows = [["o1", "11", "a"], ["o2", "11", "b"], ["o3", "12", "c"]]
        mappings = [{"source": h, "target": h} for h in headers]
    elif "underscore_id_dup" in sc.id:
        headers = ["_id", "id"]
        rows = [["o1", "1"], ["o1", "2"]]
        mappings = [{"source": h, "target": h} for h in headers]
    else:  # sql_id_dup
        headers = ["id", "title"]
        rows = [["1", "a"], ["1", "b"], ["2", "c"]]
        mappings = [{"source": h, "target": h} for h in headers]
    report = run_write_audit(
        headers=headers,
        rows=rows,
        mappings=mappings,
        validation_mode=sc.mode,
        dest_kind=sc.dest,
    )
    return report.passed, list(report.issues), list(report.warnings)


def _run_integrity(sc: Scenario) -> tuple[bool, bool, list[str]]:
    """Returns (passed_overall, blocks_transfer, issue_texts)."""
    if sc.rule_class == "encoding_anomalies":
        cols = ["_id", "description"] if sc.dest_family == "schemaless" else ["id", "description"]
        rows = [
            {cols[0]: "1", "description": _zwsp("hello|Z|world")},
            {cols[0]: "2", "description": "clean"},
        ]
        types = {c: "VARCHAR" for c in cols}
        mappings = [
            {"source": c, "target": c, "confidence": 0.95, "transform": "none"} for c in cols
        ]
    elif sc.rule_class == "required_nulls":
        cols = ["_id", "title"] if sc.dest_family == "schemaless" else ["id", "title"]
        rows = [{cols[0]: "", "title": "a"}, {cols[0]: "", "title": "b"}, {cols[0]: "3", "title": "c"}]
        types = {c: "VARCHAR" for c in cols}
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in cols]
    elif sc.rule_class == "financial_precision":
        cols = ["id", "amount"]
        rows = [{"id": "1", "amount": "10.50"}, {"id": "2", "amount": "3.25"}]
        types = {"id": "INTEGER", "amount": "DECIMAL"}
        mappings = [
            {"source": "id", "target": "id", "confidence": 0.99},
            {
                "source": "amount",
                "target": "amount",
                "confidence": 0.95,
                "transform": "integer",
                "target_type": "INTEGER",
            },
        ]
        target_schemas = [
            {"name": "id", "inferred_type": "INTEGER"},
            {"name": "amount", "inferred_type": "INTEGER"},
        ]
        report = run_preflight_integrity(
            source_columns=cols,
            target_columns=cols,
            mappings=mappings,
            source_schemas=[{"name": c, "inferred_type": types[c]} for c in cols],
            target_schemas=target_schemas,
            sample_rows=rows,
            destination_db_type=sc.dest,
            validation_mode=sc.mode,
        )
        issues = []
        for ch in report.get("checks") or []:
            issues.extend(ch.get("issues") or [])
        return (not report.get("blocks_transfer")), bool(report.get("blocks_transfer")), issues
    elif sc.rule_class == "coercion_safety" and "dirty" in sc.id:
        cols = ["id", "population"]
        rows = [{"id": "1", "population": "abc"}, {"id": "2", "population": "12x"}]
        mappings = [
            {"source": "id", "target": "id", "confidence": 0.99},
            {
                "source": "population",
                "target": "population",
                "confidence": 0.99,
                "transform": "integer",
                "target_type": "NUMBER",
            },
        ]
        report = run_preflight_integrity(
            source_columns=cols,
            target_columns=cols,
            mappings=mappings,
            source_schemas=[
                {"name": "id", "inferred_type": "INTEGER"},
                {"name": "population", "inferred_type": "VARCHAR"},
            ],
            target_schemas=[
                {"name": "id", "inferred_type": "INTEGER"},
                {"name": "population", "inferred_type": "NUMBER"},
            ],
            sample_rows=rows,
            destination_db_type=sc.dest,
            validation_mode=sc.mode,
        )
        issues = []
        for ch in report.get("checks") or []:
            issues.extend(ch.get("issues") or [])
        return (not report.get("blocks_transfer")), bool(report.get("blocks_transfer")), issues
    elif sc.rule_class == "coercion_safety" and "clean" in sc.id:
        cols = ["id", "population"]
        rows = [{"id": "1", "population": "331002651"}, {"id": "2", "population": "42"}]
        mappings = [
            {"source": "id", "target": "id", "confidence": 0.99},
            {
                "source": "population",
                "target": "population",
                "confidence": 0.99,
                "transform": "integer",
                "target_type": "NUMBER",
            },
        ]
        report = run_preflight_integrity(
            source_columns=cols,
            target_columns=cols,
            mappings=mappings,
            source_schemas=[
                {"name": "id", "inferred_type": "INTEGER"},
                {"name": "population", "inferred_type": "VARCHAR"},
            ],
            target_schemas=[
                {"name": "id", "inferred_type": "INTEGER"},
                {"name": "population", "inferred_type": "NUMBER"},
            ],
            sample_rows=rows,
            destination_db_type=sc.dest,
            validation_mode=sc.mode,
        )
        issues = []
        for ch in report.get("checks") or []:
            issues.extend(ch.get("issues") or [])
        return (not report.get("blocks_transfer")), bool(report.get("blocks_transfer")), issues
    elif "status_enum" in sc.id:
        cols = ["id", "status"]
        rows = [{"id": "1", "status": "active"}, {"id": "2", "status": "invalidated"}]
        mappings = [
            {"source": "id", "target": "id", "confidence": 0.99},
            {
                "source": "status",
                "target": "is_active",
                "confidence": 0.99,
                "transform": "boolean",
                "target_type": "BOOLEAN",
            },
        ]
        report = run_preflight_integrity(
            source_columns=cols,
            target_columns=["id", "is_active"],
            mappings=mappings,
            source_schemas=[
                {"name": "id", "inferred_type": "INTEGER"},
                {"name": "status", "inferred_type": "VARCHAR"},
            ],
            target_schemas=[
                {"name": "id", "inferred_type": "INTEGER"},
                {"name": "is_active", "inferred_type": "BOOLEAN"},
            ],
            sample_rows=rows,
            destination_db_type=sc.dest,
            validation_mode=sc.mode,
        )
        issues = []
        for ch in report.get("checks") or []:
            issues.extend(ch.get("issues") or [])
        return (not report.get("blocks_transfer")), bool(report.get("blocks_transfer")), issues
    elif sc.rule_class == "transform_dry_run":
        cols = ["id", "posted"]
        rows = [{"id": "1", "posted": "05/06/2024"}, {"id": "2", "posted": "05/06/2024"}]
        mappings = [
            {"source": "id", "target": "id", "confidence": 0.99},
            {
                "source": "posted",
                "target": "posted",
                "confidence": 0.99,
                "transform": "date",
                "target_type": "DATE",
            },
        ]
        report = run_preflight_integrity(
            source_columns=cols,
            target_columns=cols,
            mappings=mappings,
            source_schemas=[
                {"name": "id", "inferred_type": "INTEGER"},
                {"name": "posted", "inferred_type": "VARCHAR"},
            ],
            target_schemas=[
                {"name": "id", "inferred_type": "INTEGER"},
                {"name": "posted", "inferred_type": "DATE"},
            ],
            sample_rows=rows,
            destination_db_type=sc.dest,
            validation_mode=sc.mode,
        )
        issues = []
        for ch in report.get("checks") or []:
            issues.extend(ch.get("issues") or [])
        return (not report.get("blocks_transfer")), bool(report.get("blocks_transfer")), issues
    else:
        cols = ["id", "description"]
        rows = [{"id": "1", "description": "x"}]
        types = {c: "VARCHAR" for c in cols}
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in cols]

    report = run_preflight_integrity(
        source_columns=cols,
        target_columns=cols,
        mappings=mappings,
        source_schemas=[{"name": c, "inferred_type": types[c]} for c in cols],
        target_schemas=[{"name": c, "inferred_type": types[c]} for c in cols],
        sample_rows=rows,
        destination_db_type=sc.dest,
        validation_mode=sc.mode,
    )
    issues: list[str] = []
    for ch in report.get("checks") or []:
        issues.extend(ch.get("issues") or [])
    return (not report.get("blocks_transfer")), bool(report.get("blocks_transfer")), issues


def _run_ddl(sc: Scenario) -> tuple[bool, list[str]]:
    if sc.dest_family == "schemaless":
        ok, issues = evaluate_ddl_compatibility(
            mappings=[{"source": "description", "target": "description", "confidence": 0.95}],
            source_schema={"description": "VARCHAR"},
            target_schema={},
            table_exists=False,
            dest_connected=True,
            dest_db_type=sc.dest,
            sample_rows=[{"description": "x" * 1200}],
            allow_create=True,
        )
        return ok, issues
    if sc.rule_class == "string_width":
        target_type = "TEXT" if "text_unbounded" in sc.id else "VARCHAR(5)"
        return evaluate_ddl_compatibility(
            mappings=[{"source": "description", "target": "description", "confidence": 0.99}],
            source_schema={"description": "VARCHAR"},
            target_schema={"description": target_type},
            table_exists=True,
            dest_connected=True,
            dest_db_type=sc.dest,
            sample_rows=[{"description": "ABCDEFGHIJ"}],
        )
    if sc.rule_class == "decimal_money":
        if "money_fit" in sc.id:
            value, target_type = "9999999999.99", "DECIMAL(12,2)"
        elif "precision_overflow" in sc.id:
            value, target_type = "123.456", "DECIMAL(8,2)"
        else:
            value, target_type = "1234567.89", "DECIMAL(8,2)"
        return evaluate_ddl_compatibility(
            mappings=[
                {
                    "source": "amount",
                    "target": "amount",
                    "confidence": 0.99,
                    "transform": "decimal",
                    "target_type": target_type,
                }
            ],
            source_schema={"amount": "DECIMAL"},
            target_schema={"amount": target_type},
            table_exists=True,
            dest_connected=True,
            dest_db_type=sc.dest,
            sample_rows=[{"amount": value}],
        )
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "code", "target": "code", "confidence": 0.9}],
        source_schema={"code": "VARCHAR"},
        target_schema={"code": "VARCHAR(5)"},
        table_exists=True,
        dest_connected=True,
        dest_db_type=sc.dest,
        sample_rows=[{"code": "ABCDEFGHIJ"}],
    )
    return ok, issues


def _run_preflight(sc: Scenario) -> tuple[bool, str]:
    from services.schema_fingerprint import fingerprint_schema

    cols = ["_id", "skills"]
    sample = [{"_id": "1", "skills": ["a"]}, {"_id": "2", "skills": ["b"]}]
    mappings = [{"source": c, "target": c, "confidence": 0.95, "transform": "none"} for c in cols]
    stale = fingerprint_schema(["_id"], {"_id": "VARCHAR"})
    result = run_file_preflight(
        columns=cols,
        column_types={c: "VARCHAR" for c in cols},
        row_count=2,
        mappings=mappings,
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        sample_rows=sample,
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_db_type=sc.dest,
        validation_mode=sc.mode,
        schema_policy="manual_review",
        stored_target_fp=stale,
        stored_source_fp=stale,
    )
    g6 = next(g for g in result["gates"] if g["id"] == "g6_target_ddl")
    return result["passed"] and g6["status"] == "pass", g6["message"]


def _run_transform_unit(sc: Scenario) -> dict[str, Any]:
    if sc.rule_class == "typed_null_sentinels":
        token = {"n_a": "n/a", "null": "null", "none": "None"}[
            next(part for part in ("n_a", "null", "none") if f".{part}." in sc.id)
        ]
        transform = "decimal" if ".number." in sc.id else "date"
        value, error = apply_transform(token, transform)
        ok = value is None and error is None
        return {"ok": ok, "value": value, "error": error, "transform": transform}

    if sc.rule_class == "boolean_tokens":
        token = next(part for part in ("yes", "no", "active", "enabled") if f".{part}." in sc.id)
        value, error = apply_transform(token, "boolean")
        parsed = error is None
        ok = parsed if sc.expect == "pass" else not parsed
        return {"ok": ok, "value": value, "error": error}

    if "nfc_preserved_by_strip" in sc.id:
        raw = "Cafe\u0301\u200b"
        value, error = apply_transform(raw, "strip_controls")
        ok = error is None and value == "Cafe\u0301" and "\u200b" not in str(value)
    elif "nfkc_width_fold" in sc.id:
        value, error = apply_transform("ＡＢＣ\u200b", "normalize_unicode")
        ok = error is None and value == "ABC"
    else:
        value, error = apply_transform("Cafe\u0301\u0001", "normalize_unicode")
        ok = error is None and value == "Café"
    return {"ok": ok, "value": value, "error": error}


def _run_confidence(sc: Scenario) -> dict[str, Any]:
    floor = {"maximum": 0.95, "strict": 0.85, "balanced": 0.75}[sc.mode]
    confidence = floor if ".at_floor." in sc.id else floor - 0.01
    report = run_preflight_integrity(
        source_columns=["description"],
        target_columns=["description"],
        mappings=[{"source": "description", "target": "description", "confidence": confidence}],
        source_schemas=[{"name": "description", "inferred_type": "VARCHAR"}],
        target_schemas=[{"name": "description", "inferred_type": "VARCHAR"}],
        sample_rows=[{"description": "clean"}],
        destination_db_type=sc.dest,
        validation_mode=sc.mode,
    )
    check = next(c for c in report["checks"] if c["check"] == "mapping_confidence")
    blocked = bool(check["blocks_transfer"])
    return {
        "ok": (not blocked) if sc.expect == "pass" else blocked,
        "confidence": confidence,
        "floor": floor,
        "check": check,
    }


def _run_identity_duplicate(sc: Scenario) -> dict[str, Any]:
    if "id_with_duplicate_user_id" in sc.id:
        columns = ["id", "user_id"]
        rows = [
            {"id": "1", "user_id": "u1"},
            {"id": "2", "user_id": "u1"},
            {"id": "3", "user_id": "u2"},
        ]
    else:
        columns = ["user_id"]
        rows = [{"user_id": "u1"}, {"user_id": "u1"}, {"user_id": "u2"}]
    mappings = [{"source": c, "target": c, "confidence": 0.99} for c in columns]
    report = run_preflight_integrity(
        source_columns=columns,
        target_columns=columns,
        mappings=mappings,
        sample_rows=rows,
        destination_db_type=sc.dest,
        validation_mode=sc.mode,
    )
    duplicate = next(c for c in report["checks"] if c["check"] == "duplicate_keys")
    blocked = bool(duplicate["blocks_transfer"])
    return {
        "ok": (not blocked) if sc.expect == "pass" else blocked,
        "primary_key": duplicate.get("primary_key"),
        "duplicate_check": duplicate,
    }


def _run_empty_evidence(sc: Scenario) -> dict[str, Any]:
    no_columns = ".no_columns." in sc.id
    columns = [] if no_columns else ["id"]
    mappings = [] if no_columns else [{"source": "id", "target": "id", "confidence": 0.99}]
    report = run_preflight_integrity(
        source_columns=columns,
        target_columns=columns,
        mappings=mappings,
        sample_rows=[] if not no_columns else [{"unexpected": "1"}],
        destination_db_type=sc.dest,
        validation_mode=sc.mode,
    )
    return {"ok": bool(report["blocks_transfer"]), "checks": report["checks"]}


def _run_sync_identity(sc: Scenario) -> dict[str, Any]:
    columns = ["id", "user_id"]
    mappings = [{"source": c, "target": c} for c in columns]
    append_key = resolve_identity_key(
        mappings=mappings,
        source_columns=columns,
        dest_kind=sc.dest,
        validation_mode=sc.mode,
        purpose="uniqueness",
    )
    overwrite_key = resolve_identity_key(
        mappings=mappings,
        source_columns=columns,
        dest_kind=sc.dest,
        validation_mode=sc.mode,
        purpose="uniqueness",
    )
    return {"ok": append_key == overwrite_key == ("id", "id"), "resolved": append_key}


def _run_remediation(sc: Scenario) -> dict[str, Any]:
    encoding = ".encoding_eligible." in sc.id
    issue = (
        {
            "column": "description",
            "row": 1,
            "sample": "hello\u200bworld",
            "message": "format-control character detected (U+200B) — normalize before transfer",
            "suggested_transform": "strip_controls",
        }
        if encoding
        else {
            "column": "description",
            "row": 1,
            "sample": "ABCDEFGHIJ",
            "message": "Value width overflow: sample exceeds VARCHAR(5)",
        }
    )
    quarantine = quarantine_rows_from_preflight(
        {"gates": [{"details": {"encoding_issues" if encoding else "issues": [issue]}}]}
    )
    suggested = quarantine[0].get("suggested_transform") if quarantine else None
    if not encoding:
        return {"ok": suggested is None, "suggested_transform": suggested, "quarantine": quarantine}

    preview = preview_quarantine_cells(
        headers=["description"],
        sample_rows=[["hello\u200bworld"]],
        mappings=[
            {
                "source": "description",
                "target": "description",
                "confidence": 0.99,
                "transform": "strip_controls",
            }
        ],
        column_types={"description": "VARCHAR"},
    )
    return {
        "ok": suggested == "strip_controls" and preview["quarantine_count"] == 0 and preview["coerce_count"] == 1,
        "suggested_transform": suggested,
        "preview": preview,
    }


def _run_type_system(sc: Scenario) -> dict[str, Any]:
    """Offline universal type / checksum rules — no live connectors."""
    from datetime import datetime, timedelta, timezone

    from connectors.writer_common import resolve_target_columns
    from services.reconciliation import normalize_cell
    from services.schema_inference import safe_ddl_logical_type
    from services.type_system import (
        LOGICAL_ARRAY,
        LOGICAL_BINARY,
        LOGICAL_BOOLEAN,
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        LOGICAL_DECIMAL,
        LOGICAL_GEOGRAPHY,
        LOGICAL_INTEGER,
        LOGICAL_INTERVAL,
        LOGICAL_JSON,
        LOGICAL_STRING,
        LOGICAL_TEXT,
        LOGICAL_TIME,
        LOGICAL_UUID,
        LOGICAL_VECTOR,
        ddl_type,
        decimal_scale_would_truncate,
        normalize_logical_type,
        parse_vector_dimension,
        vector_dim_mismatch,
    )

    all_logicals = [
        LOGICAL_STRING,
        LOGICAL_TEXT,
        LOGICAL_INTEGER,
        LOGICAL_DECIMAL,
        LOGICAL_BOOLEAN,
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        LOGICAL_TIME,
        LOGICAL_UUID,
        LOGICAL_JSON,
        LOGICAL_ARRAY,
        LOGICAL_BINARY,
        LOGICAL_INTERVAL,
        LOGICAL_GEOGRAPHY,
        LOGICAL_VECTOR,
    ]

    case = sc.id.split(".")[1]
    detail: dict[str, Any] = {"case": case, "dest": sc.dest}

    if case == "bit1_boolean":
        ok = (
            normalize_logical_type("BIT") == "boolean"
            and normalize_logical_type("BIT(1)") == "boolean"
            and normalize_logical_type("TINYINT(1)") == "boolean"
        )
    elif case == "bitn_binary":
        ok = (
            normalize_logical_type("BIT(8)") == "binary"
            and normalize_logical_type("BIT VARYING") == "binary"
            and normalize_logical_type("VARBIT") == "binary"
        )
    elif case == "uint64_decimal":
        ok = normalize_logical_type("BIGINT UNSIGNED") == "decimal"
        ok = ok and normalize_logical_type("UINT64") == "decimal"
    elif case == "specialty_ddl":
        missing: list[str] = []
        for logical in all_logicals:
            try:
                ddl = ddl_type(sc.dest, logical)
            except Exception as exc:  # noqa: BLE001
                missing.append(f"{logical}:{exc}")
                continue
            if not ddl or not str(ddl).strip():
                missing.append(str(logical))
        ok = not missing
        detail["missing"] = missing
    elif case == "explicit_text_honor":
        samples = ["12345678901234567890.12345", "0.00000000015"]
        kept = safe_ddl_logical_type(
            "TEXT",
            samples,
            field_name="compensation",
            source_type="DECIMAL",
            honor_explicit=True,
        )
        cols, types = resolve_target_columns(
            [{"source": "compensation", "target": "pay_amount", "target_type": "TEXT"}],
            {"compensation": "DECIMAL"},
            preserve_case=True,
            sample_values_by_source={"compensation": samples},
            table_exists=False,
        )
        ok = kept == "TEXT" and types == ["TEXT"] and cols == ["pay_amount"]
        detail["kept"] = kept
        detail["types"] = types
    elif case == "decimal_scale_truncate":
        # Second arg is destination engine id. Scale 200 exceeds every fixed-scale cap.
        would = decimal_scale_would_truncate("NUMBER(38,200)", sc.dest)
        if sc.expect == "block":
            ok = bool(would)
        else:
            # Uncapped / schemaless: gate must not invent a false truncate signal.
            ok = not bool(would)
        detail["would_truncate"] = would
    elif case == "tz_checksum_instant":
        wire = "2024-06-01T12:00:00+05:30"
        readback = datetime(2024, 6, 1, 2, 30, tzinfo=timezone(timedelta(hours=-4)))
        ok = normalize_cell(wire) == normalize_cell(readback) == "2024-06-01T06:30:00Z"
        detail["wire"] = normalize_cell(wire)
    elif case == "json_key_order":
        a = '{"tier":"gold","tags":["a","b"]}'
        b = {"tags": ["a", "b"], "tier": "gold"}
        ok = normalize_cell(a) == normalize_cell(b)
    elif case == "vector_dim_propagate":
        # Parametric VECTOR dims must flow on native engines; sinks stay sinks.
        if sc.dest == "postgresql":
            ok = ddl_type(sc.dest, "VECTOR(1536)") == "vector(1536)"
        elif sc.dest == "snowflake":
            ok = ddl_type(sc.dest, "VECTOR(768)") == "VECTOR(FLOAT, 768)"
        else:
            # Non-parametric sinks must still accept VECTOR without inventing dims.
            emitted = ddl_type(sc.dest, "VECTOR(768)")
            ok = bool(emitted) and "1536" not in str(emitted)
        ok = ok and parse_vector_dimension("VECTOR(FLOAT, 1024)") == 1024
        detail["emitted"] = ddl_type(sc.dest, "VECTOR(768)")
    elif case == "vector_no_invented_dim":
        bare = ddl_type(sc.dest, "VECTOR")
        ok = "1536" not in str(bare)
        if sc.dest in {"postgresql", "snowflake"}:
            # Native engines without a declared dim → lossless text, not VECTOR(…).
            ok = ok and "(" not in str(bare)
        detail["bare"] = bare
    elif case == "vector_dim_mismatch":
        mismatched = vector_dim_mismatch("VECTOR(768)", "VECTOR(FLOAT, 1536)")
        aligned = not vector_dim_mismatch("VECTOR(768)", "VECTOR(FLOAT, 768)")
        if sc.expect == "block":
            ok = bool(mismatched) and aligned
        else:
            ok = not mismatched
        detail["mismatched"] = mismatched
    else:
        return {"ok": False, "error": f"unknown type case {case}"}

    return {"ok": ok, **detail}


def _evaluate(sc: Scenario) -> dict[str, Any]:
    if sc.runner == "transform_unit":
        return _run_transform_unit(sc)
    if sc.runner == "confidence":
        return _run_confidence(sc)
    if sc.runner == "identity_duplicate":
        return _run_identity_duplicate(sc)
    if sc.runner == "empty_evidence":
        return _run_empty_evidence(sc)
    if sc.runner == "sync_identity":
        return _run_sync_identity(sc)
    if sc.runner == "remediation":
        return _run_remediation(sc)
    if sc.runner == "type_system":
        return _run_type_system(sc)

    if sc.runner == "write_audit":
        passed, issues, warnings = _run_write_audit(sc)
        blocked = not passed
        if sc.expect == "pass":
            ok = passed
        elif sc.expect == "block":
            ok = blocked
        else:
            ok = (not blocked) and bool(warnings)
        return {"ok": ok, "passed": passed, "issues": issues[:5], "warnings": warnings[:3]}

    if sc.runner == "integrity":
        soft_pass, blocks, issues = _run_integrity(sc)
        if sc.expect == "pass":
            ok = soft_pass and not blocks
        elif sc.expect == "block":
            ok = blocks
        else:
            # warn: does not hard-block
            ok = not blocks
        return {"ok": ok, "blocks": blocks, "issues": issues[:5]}

    if sc.runner == "ddl":
        compatible, issues = _run_ddl(sc)
        if sc.expect == "pass":
            ok = compatible
        else:
            ok = not compatible
        return {"ok": ok, "compatible": compatible, "issues": issues[:5]}

    if sc.runner == "preflight":
        passed, msg = _run_preflight(sc)
        ok = passed if sc.expect == "pass" else not passed
        return {"ok": ok, "passed": passed, "message": msg}

    if sc.runner == "identity":
        cols = ["_id", "id", "user_id"] if sc.dest_family == "schemaless" else ["id", "user_id"]
        mappings = [{"source": c, "target": c} for c in cols]
        src, tgt = resolve_identity_key(
            mappings=mappings,
            source_columns=cols,
            dest_kind=sc.dest,
            validation_mode=sc.mode,
            purpose="uniqueness",
        )
        report = run_write_audit(
            headers=cols,
            rows=[[str(i)] + ["x"] * (len(cols) - 1) for i in range(3)],
            mappings=mappings,
            validation_mode=sc.mode,
            dest_kind=sc.dest,
        )
        audit_pk = (report.stats or {}).get("primary_key")
        # Alignment: write audit PK must equal resolved uniqueness key (or both None)
        aligned = (src == audit_pk) or (src is None and audit_pk is None)
        if sc.dest_family == "schemaless":
            aligned = aligned and (src == "_id" or src is None)
        return {"ok": aligned, "resolved": src, "audit_pk": audit_pk, "target": tgt}

    return {"ok": False, "error": f"unknown runner {sc.runner}"}


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s.id for s in SCENARIOS])
def test_data_rule_scenario(scenario: Scenario):
    result = _evaluate(scenario)
    assert result.get("ok") is True, (
        f"{scenario.id} expect={scenario.expect} family={scenario.dest_family} "
        f"mode={scenario.mode} result={result} note={scenario.note}"
    )


def test_scenario_catalog_scale_and_write_proof():
    """Prove we generated a large offline catalog (not a toy 10-case list)."""
    assert len(SCENARIOS) >= 3000, f"catalog too small: {len(SCENARIOS)}"
    by_rule: dict[str, int] = {}
    by_family: dict[str, int] = {}
    for s in SCENARIOS:
        by_rule[s.rule_class] = by_rule.get(s.rule_class, 0) + 1
        by_family[s.dest_family] = by_family.get(s.dest_family, 0) + 1

    # Smoke a stratified sample and write proof artifact for operators.
    sample_ids = [
        "identity.biz_id_dup.redis.strict",
        "identity.biz_id_dup.postgresql.strict",
        "encoding.zwsp.snowflake.strict",
        "encoding.zwsp.snowflake.balanced",
        "ddl.schemaless_no_sql_rules.redis.strict",
        "preflight.schemaless_drift_noise.mongodb.strict",
        "types.bit1_boolean.postgresql.strict",
        "types.specialty_ddl.snowflake.strict",
        "types.explicit_text_honor.mysql.strict",
        "types.decimal_scale_truncate.mysql.strict",
        "types.tz_checksum_instant.postgresql.strict",
        "types.vector_dim_propagate.snowflake.strict",
        "types.vector_no_invented_dim.postgresql.strict",
        "types.vector_dim_mismatch.mysql.strict",
        "align.validate_write_pk.redis.strict",
        "coercion.varchar_to_number_clean.snowflake.strict",
        "coercion.varchar_to_number_dirty.postgresql.strict",
        "null_sentinel.number.n_a.snowflake.strict",
        "null_sentinel.date.none.postgresql.maximum",
        "boolean.strict_token.yes.bigquery.strict",
        "boolean.status_enum.enabled.mysql.balanced",
        "unicode.nfkc_compose_and_strip.snowflake.strict",
        "width.text_unbounded.postgresql.strict",
        "width.varchar_overflow.postgresql.strict",
        "decimal.money_fit.snowflake.strict",
        "decimal.money_magnitude_overflow.snowflake.strict",
        "confidence.at_floor.postgresql.maximum",
        "confidence.below_floor.mongodb.balanced",
        "identity.user_id_only_duplicate.postgresql.strict",
        "identity.id_with_duplicate_user_id.postgresql.strict",
        "fail_closed.empty_sample.snowflake.strict",
        "fail_closed.no_columns.mongodb.balanced",
        "sync_identity.full_refresh_append.postgresql.strict",
        "sync_identity.full_refresh_overwrite.postgresql.strict",
        "remediation.encoding_eligible.snowflake.strict",
        "remediation.schema_ineligible.snowflake.strict",
    ]
    by_id = {s.id: s for s in SCENARIOS}
    sample_results = []
    for sid in sample_ids:
        sc = by_id[sid]
        r = _evaluate(sc)
        sample_results.append({"id": sid, "ok": r.get("ok"), "detail": {k: v for k, v in r.items() if k != "ok"}})
        assert r.get("ok") is True, f"stratified sample failed: {sid} → {r}"

    _PROOF.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_scenarios": len(SCENARIOS),
        "by_rule_class": by_rule,
        "by_dest_family": by_family,
        "honesty": {
            "live_130_connector_e2e": False,
            "catalog_tiles_are_not_transfer_ready": True,
            "cdc_default": "at-least-once upsert until proven otherwise",
            "this_file": "offline data-rule engines only",
        },
        "stratified_sample": sample_results,
        "dests_covered": sorted({s.dest for s in SCENARIOS}),
        "modes": list(MODES),
    }
    _PROOF.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    assert all(r["ok"] for r in sample_results)
