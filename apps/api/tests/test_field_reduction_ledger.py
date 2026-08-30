"""Field Reduction Ledger (G16) — a declared drop must also be an explained one."""

from __future__ import annotations

from typing import Any

from services.field_reduction_ledger import (
    LEDGER_SCHEMA,
    build_field_reduction_evidence,
    build_field_reduction_ledger,
    definition_hash,
    normalize_reason_code,
    observe_field,
    sign_field_reduction_ledger,
    verify_field_reduction_ledger,
)
from services.preflight_service import run_file_preflight

SOURCE_COLUMNS = [
    "acct_id",
    "cust_first",
    "cust_last",
    "branch_code",
    "filler_1",
    "legacy_flag",
    "old_audit_trail",
    "balance_cents",
    "phone",
    "never_used",
]
SOURCE_TYPES = {c: "VARCHAR(64)" for c in SOURCE_COLUMNS}
SOURCE_TYPES["acct_id"] = "BIGINT"
SOURCE_TYPES["balance_cents"] = "BIGINT"
DEST_TYPES = {
    "acct_id": "BIGINT",
    "customer_name": "VARCHAR(128)",
    "branch": "VARCHAR(32)",
    "balance": "NUMERIC(18,2)",
}
SAMPLE_ROWS = [
    {
        "acct_id": i,
        "cust_first": f"first{i}",
        "cust_last": f"last{i}",
        "branch_code": "0042",
        "filler_1": "",
        "legacy_flag": "Y" if i % 2 else "N",
        "old_audit_trail": f"trail-{i}",
        "balance_cents": 1000 + i,
        "phone": f"555-000-{i:04d}",
        "never_used": None,
    }
    for i in range(1, 6)
]


def _carried() -> list[dict[str, Any]]:
    return [
        {"source": "acct_id", "target": "acct_id", "transform": "none", "confidence": 0.99},
        # N:1 fold — two source names into one destination column.
        {
            "source": "cust_first",
            "target": "customer_name",
            "transform": "concat(cust_first,' ',cust_last)",
            "confidence": 0.9,
        },
        {
            "source": "cust_last",
            "target": "customer_name",
            "transform": "concat(cust_first,' ',cust_last)",
            "confidence": 0.9,
        },
        {"source": "branch_code", "target": "branch", "transform": "none", "confidence": 0.95},
        {
            "source": "balance_cents",
            "target": "balance",
            "transform": "divide(balance_cents,100)",
            "confidence": 0.93,
        },
    ]


def _omit(source: str, **extra: Any) -> dict[str, Any]:
    return {"source": source, "target": "", "intentional_omit": True, "confidence": 0.0, **extra}


def _governed_omissions() -> list[dict[str, Any]]:
    return [
        _omit("filler_1", omit_reason="dropped_empty"),
        _omit(
            "legacy_flag",
            omit_reason="dropped_obsolete",
            omit_reason_text="Retired with the 2025 product catalogue",
        ),
        _omit(
            "old_audit_trail",
            omit_reason="archive_only",
            omit_reason_text="Kept for SOX in the mainframe archive",
            archive_reference="s3://bank-archive/acct/audit_trail/2026/",
            retention_until="2033-12-31",
        ),
        _omit(
            "phone",
            omit_reason="dropped_pii_minimization",
            omit_reason_text="Analytics target has no lawful basis for phone numbers",
        ),
        _omit("never_used", omit_reason="dropped_empty"),
    ]


def _ledger(mappings: list[dict[str, Any]], **kwargs: Any) -> dict[str, Any]:
    return build_field_reduction_ledger(
        source_columns=SOURCE_COLUMNS,
        mappings=mappings,
        target_columns=list(DEST_TYPES),
        source_column_types=SOURCE_TYPES,
        target_column_types=DEST_TYPES,
        sample_rows=SAMPLE_ROWS,
        **kwargs,
    )


def _entry(ledger: dict[str, Any], source: str) -> dict[str, Any]:
    return next(e for e in ledger["entries"] if e["source"] == source)


# ── Dispositions ──────────────────────────────────────────────────────────────
def test_every_source_field_gets_exactly_one_disposition():
    ledger = _ledger(_carried() + _governed_omissions())

    assert ledger["schema"] == LEDGER_SCHEMA
    assert [e["source"] for e in ledger["entries"]] == SOURCE_COLUMNS
    assert ledger["source_field_count"] == len(SOURCE_COLUMNS)
    assert ledger["carried_count"] == 5
    assert ledger["reduced_count"] == 5
    assert ledger["unaccounted"] == []
    assert ledger["complete"] is True


def test_many_to_one_fold_and_transform_are_classified_not_flattened():
    ledger = _ledger(_carried() + _governed_omissions())

    assert _entry(ledger, "cust_first")["disposition"] == "merged_into"
    assert _entry(ledger, "cust_last")["targets"] == ["customer_name"]
    assert _entry(ledger, "balance_cents")["disposition"] == "carried_transformed"
    assert _entry(ledger, "acct_id")["disposition"] == "carried"
    # The expression is hashed so an approved fold cannot be edited silently.
    assert _entry(ledger, "balance_cents")["transform_sha256"]


def test_one_to_many_split_is_recorded_as_a_split():
    mappings = [
        {"source": "cust_first", "target": "customer_name"},
        {"source": "cust_first", "target": "branch"},
    ]
    ledger = build_field_reduction_ledger(
        source_columns=["cust_first"], mappings=mappings, sample_rows=SAMPLE_ROWS
    )

    entry = _entry(ledger, "cust_first")
    assert entry["disposition"] == "split_into"
    assert entry["targets"] == ["customer_name", "branch"]


def test_unaccounted_field_is_reported_but_is_left_to_g13_to_block():
    ledger = _ledger(_carried())
    _, gate = build_field_reduction_evidence(
        source_columns=SOURCE_COLUMNS,
        mappings=_carried(),
        sample_rows=SAMPLE_ROWS,
    )

    assert set(ledger["unaccounted"]) == {
        "filler_1",
        "legacy_flag",
        "old_audit_trail",
        "phone",
        "never_used",
    }
    assert ledger["complete"] is False
    # G16 must not duplicate the G13 blocker for the same columns.
    assert gate["status"] == "pass"


# ── Evidence quality ─────────────────────────────────────────────────────────
def test_factual_reason_contradicted_by_the_sample_blocks():
    mappings = _carried() + [
        _omit("legacy_flag", omit_reason="dropped_empty"),
        _omit("old_audit_trail", omit_reason="dropped_empty"),
        _omit("phone", omit_reason="dropped_not_required", omit_reason_text="n/a"),
        _omit("filler_1", omit_reason="dropped_empty"),
        _omit("never_used", omit_reason="dropped_empty"),
    ]
    ledger = _ledger(mappings)
    gate = build_field_reduction_evidence(
        source_columns=SOURCE_COLUMNS, mappings=mappings, sample_rows=SAMPLE_ROWS
    )[1]

    codes = {(i["source"], i["code"]) for i in ledger["blocking_issues"]}
    assert ("legacy_flag", "claim_contradicted_by_sample") in codes
    assert ("old_audit_trail", "claim_contradicted_by_sample") in codes
    # An all-empty sample column keeps its factual claim.
    assert _entry(ledger, "filler_1")["evidence_complete"] is True
    assert gate["status"] == "block"
    assert "legacy_flag" in gate["message"]


def test_constant_claim_needs_a_single_value_in_the_sample():
    mappings = [
        _omit("branch_code", omit_reason="dropped_constant"),
        _omit("legacy_flag", omit_reason="dropped_constant"),
    ]
    ledger = build_field_reduction_ledger(
        source_columns=["branch_code", "legacy_flag"],
        mappings=mappings,
        sample_rows=SAMPLE_ROWS,
    )

    assert _entry(ledger, "branch_code")["evidence_complete"] is True
    assert [i["code"] for i in _entry(ledger, "legacy_flag")["issues"]] == [
        "claim_contradicted_by_sample"
    ]


def test_archive_only_without_an_archive_reference_blocks():
    mappings = [
        _omit(
            "old_audit_trail",
            omit_reason="archive_only",
            omit_reason_text="Kept for SOX",
        )
    ]
    ledger = build_field_reduction_ledger(
        source_columns=["old_audit_trail"], mappings=mappings, sample_rows=SAMPLE_ROWS
    )

    assert [i["code"] for i in ledger["blocking_issues"]] == ["archive_reference_missing"]
    assert build_field_reduction_evidence(
        source_columns=["old_audit_trail"], mappings=mappings, sample_rows=SAMPLE_ROWS
    )[1]["status"] == "block"


def test_judgement_reason_without_a_note_blocks():
    mappings = [_omit("phone", omit_reason="dropped_redundant")]
    ledger = build_field_reduction_ledger(
        source_columns=["phone"], mappings=mappings, sample_rows=SAMPLE_ROWS
    )

    assert [i["code"] for i in ledger["blocking_issues"]] == ["reason_text_missing"]


def test_unknown_reason_code_is_refused_not_guessed():
    mappings = [_omit("phone", omit_reason="dropped_because_reasons")]
    ledger = build_field_reduction_ledger(
        source_columns=["phone"], mappings=mappings, sample_rows=SAMPLE_ROWS
    )

    assert [i["code"] for i in ledger["blocking_issues"]] == ["reason_code_unknown"]
    assert _entry(ledger, "phone")["disposition"] == "dropped_because_reasons"


def test_factual_claim_with_no_sample_is_unverified_not_proven():
    mappings = [_omit("filler_1", omit_reason="dropped_empty")]
    ledger = build_field_reduction_ledger(
        source_columns=["filler_1"], mappings=mappings, sample_rows=[]
    )

    assert [i["code"] for i in ledger["warnings"]] == ["claim_unverified"]
    assert build_field_reduction_gate_status(ledger) == "warn"


def build_field_reduction_gate_status(ledger: dict[str, Any]) -> str:
    from services.field_reduction_ledger import build_field_reduction_gate

    return str(build_field_reduction_gate(ledger)["status"])


# ── Legacy omissions and strict mode ─────────────────────────────────────────
def test_legacy_boolean_omission_warns_but_does_not_break_existing_jobs():
    mappings = _carried() + [_omit(c) for c in ("filler_1", "legacy_flag", "old_audit_trail", "phone", "never_used")]
    ledger = _ledger(mappings)
    gate = build_field_reduction_evidence(
        source_columns=SOURCE_COLUMNS, mappings=mappings, sample_rows=SAMPLE_ROWS
    )[1]

    assert ledger["blocking_issues"] == []
    assert {i["code"] for i in ledger["warnings"]} == {"reason_code_missing"}
    assert gate["status"] == "warn"
    assert all(e["disposition"] == "dropped_unclassified" for e in ledger["entries"] if not e["carried"])


def test_strict_mode_refuses_unexplained_and_unapproved_reductions():
    mappings = _carried() + [_omit("filler_1")]
    strict = build_field_reduction_ledger(
        source_columns=SOURCE_COLUMNS,
        mappings=mappings,
        sample_rows=SAMPLE_ROWS,
        strict=True,
    )

    codes = {i["code"] for i in strict["blocking_issues"]}
    assert "reason_code_missing" in codes
    assert "approval_missing" in codes
    assert strict["strict"] is True


def test_strict_mode_accepts_a_reason_with_a_named_approver():
    mappings = [
        _omit(
            "phone",
            omit_reason="dropped_pii_minimization",
            omit_reason_text="No lawful basis in the analytics target",
            omit_approved_by="dpo@bank.example",
            omit_approved_at="2026-08-30T10:00:00Z",
        )
    ]
    ledger = build_field_reduction_ledger(
        source_columns=["phone"], mappings=mappings, sample_rows=SAMPLE_ROWS, strict=True
    )

    entry = _entry(ledger, "phone")
    assert ledger["blocking_issues"] == []
    assert entry["approved_by"] == "dpo@bank.example"
    assert entry["reason_evidence_kind"] == "declared"


# ── Hashes, signature, honesty ───────────────────────────────────────────────
def test_definition_hash_binds_the_ledger_to_the_field_inventory():
    ten = definition_hash(SOURCE_COLUMNS, SOURCE_TYPES)
    eleven = definition_hash([*SOURCE_COLUMNS, "new_field"], SOURCE_TYPES)
    retyped = definition_hash(SOURCE_COLUMNS, {**SOURCE_TYPES, "phone": "VARCHAR(32)"})

    assert ten != eleven
    assert ten != retyped
    # Order of the inventory is not part of the identity.
    assert definition_hash(list(reversed(SOURCE_COLUMNS)), SOURCE_TYPES) == ten


def test_signed_ledger_detects_a_post_approval_edit():
    signed = sign_field_reduction_ledger(_ledger(_carried() + _governed_omissions()), job_id="job-1")

    assert verify_field_reduction_ledger(signed, job_id="job-1")["ok"] is True
    # A signature is bound to its job, so a ledger cannot be replayed elsewhere.
    assert verify_field_reduction_ledger(signed, job_id="job-2")["ok"] is False

    tampered = dict(signed)
    tampered["entries"] = [
        {**e, "reason_code": "dropped_empty"} if e["source"] == "phone" else e
        for e in signed["entries"]
    ]
    assert verify_field_reduction_ledger(tampered, job_id="job-1")["ok"] is False


def test_ledger_never_claims_population_proof():
    ledger = _ledger(_carried() + _governed_omissions())

    assert ledger["evidence_basis"] == "preflight_sample"
    assert "not population proof" in ledger["honesty"]
    assert _entry(ledger, "filler_1")["observed"]["basis"] == "sample"


def test_observe_field_reports_absence_rather_than_inventing_emptiness():
    absent = observe_field(SAMPLE_ROWS, "column_that_is_not_there")

    assert absent["basis"] == "none"
    assert absent["sampled_rows"] == 0


def test_reason_alias_normalisation_is_conservative():
    assert normalize_reason_code("All-Null") == "dropped_empty"
    assert normalize_reason_code("archive") == "archive_only"
    assert normalize_reason_code("") == ""
    assert normalize_reason_code("dropped_whatever") == "dropped_whatever"


# ── Preflight wiring ─────────────────────────────────────────────────────────
def _preflight(mappings: list[dict[str, Any]]) -> dict[str, Any]:
    return run_file_preflight(
        columns=SOURCE_COLUMNS,
        column_types=SOURCE_TYPES,
        row_count=len(SAMPLE_ROWS),
        mappings=mappings,
        sample_rows=SAMPLE_ROWS,
        source_kind="database",
        destination_db_type="postgresql",
        destination_table_exists=True,
        destination_column_types=DEST_TYPES,
        destination_can_write=True,
        destination_connected=True,
        sync_mode="incremental_append",
    )


def test_preflight_publishes_the_ledger_and_the_g16_gate():
    pf = _preflight(_carried() + _governed_omissions())

    gate = next(g for g in pf["gates"] if g["id"] == "g16_field_reduction")
    assert gate["status"] == "pass"
    ledger = pf["field_reduction_ledger"]
    assert ledger["reduced_count"] == 5
    assert pf["proof_bundle"]["field_reduction_ledger"]["schema"] == LEDGER_SCHEMA


def test_preflight_blocks_when_a_reduction_reason_is_false():
    mappings = _carried() + [
        _omit("filler_1", omit_reason="dropped_empty"),
        _omit("legacy_flag", omit_reason="dropped_empty"),
        _omit("old_audit_trail", omit_reason="dropped_not_required", omit_reason_text="n/a"),
        _omit("phone", omit_reason="dropped_not_required", omit_reason_text="n/a"),
        _omit("never_used", omit_reason="dropped_empty"),
    ]
    pf = _preflight(mappings)

    gate = next(g for g in pf["gates"] if g["id"] == "g16_field_reduction")
    assert gate["status"] == "block"
    assert pf["passed"] is False
    assert any(b["id"] == "g16_field_reduction" for b in pf["blockers"])


def test_exported_proof_pack_carries_the_reduction_ledger():
    from services.signed_proof_pack import export_proof_pack_for_job

    pf = _preflight(_carried() + _governed_omissions())
    pack = export_proof_pack_for_job(
        {
            "_id": "job-42",
            "status": "completed",
            "preflight": {
                "passed": pf["passed"],
                "source_coverage": pf["source_coverage"],
                "field_reduction_ledger": pf["field_reduction_ledger"],
            },
        }
    )

    ledger = pack["preflight_summary"]["field_reduction_ledger"]
    assert ledger["schema"] == LEDGER_SCHEMA
    assert ledger["reduced_count"] == 5
