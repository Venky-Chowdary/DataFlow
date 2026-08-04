from __future__ import annotations

from preflight import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    PreflightContext,
    PreflightEngine,
    SourceConfig,
    TransferPlan,
)
from preflight.models import GateStatus
from preflight.risk_contract import make_clearing_risk_contract


def _happy_plan() -> TransferPlan:
    return TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            parseable=True,
            columns=[
                ColumnSchema(name="AMT", inferred_type="DECIMAL", samples=["1500", "2300"]),
                ColumnSchema(name="PAY_DT", inferred_type="DATE", samples=["20250101"]),
            ],
            row_count_estimate=1000,
        ),
        destination=DestinationConfig(
            kind="snowflake",
            connected=True,
            can_write=True,
            can_create_table=True,
            target_columns=[
                ColumnSchema(name="payment_amount", inferred_type="NUMBER"),
                ColumnSchema(name="payment_date", inferred_type="DATE"),
            ],
        ),
        mappings=[
            ColumnMapping(source="AMT", target="payment_amount", confidence=0.97),
            ColumnMapping(source="PAY_DT", target="payment_date", confidence=0.92),
        ],
        required_targets=["payment_amount", "payment_date"],
        dry_run_passed=True,
        ddl_compatible=True,
        estimated_bytes=1_000_000,
        available_staging_bytes=10_000_000,
    )


def _happy_ctx(plan: TransferPlan | None = None) -> PreflightContext:
    """Happy path must inject a real integrity audit — default stub is fail-closed."""

    class _Ctx(PreflightContext):
        def run_integrity_audit(self, sample_size: int = 1000) -> dict:
            return {
                "blocks_transfer": False,
                "checks_passed": 2,
                "checks_failed": 0,
                "issues": [],
                "warnings": [],
                "summary": "Data integrity checks passed",
                "checks": [],
            }

    return _Ctx(
        plan=plan or _happy_plan(),
        sample_rows=[
            {"AMT": "1500", "PAY_DT": "20250101"},
            {"AMT": "2300", "PAY_DT": "20250102"},
        ],
    )


def test_all_gates_pass():
    engine = PreflightEngine()
    result = engine.run(_happy_ctx())
    assert result.passed
    assert result.blockers == []
    assert result.passed_count >= 7


def test_g8_blocks_without_sample_rows():
    """Execute must not unlock when Gate-8 has zero reconcile evidence."""
    result = PreflightEngine().run(PreflightContext(plan=_happy_plan()))
    assert not result.passed
    assert any(b.gate_id.value == "g8_reconciliation" for b in result.blockers)


def test_g1_blocks_unparseable_file():
    plan = _happy_plan()
    plan.source.parseable = False
    result = PreflightEngine().run(_happy_ctx(plan))
    assert not result.passed
    assert result.blockers[0].gate_id.value == "g1_source"


def test_g4_blocks_low_confidence():
    plan = _happy_plan()
    plan.mappings[0].confidence = 0.5
    result = PreflightEngine().run(_happy_ctx(plan))
    assert not result.passed
    assert any(b.gate_id.value == "g4_mapping_confidence" for b in result.blockers)


def test_g4_allows_override():
    plan = _happy_plan()
    plan.mappings[0].confidence = 0.5
    plan.mappings[0].user_override = True
    result = PreflightEngine().run(_happy_ctx(plan))
    assert result.passed


def test_g4_blocks_ambiguous_mapping():
    plan = _happy_plan()
    plan.mappings[0].requires_review = True
    plan.mappings[0].score_gap = 0.03
    result = PreflightEngine().run(_happy_ctx(plan))
    assert not result.passed
    assert any(b.gate_id.value == "g4_mapping_confidence" for b in result.blockers)


def test_g4_allows_ambiguous_with_override():
    plan = _happy_plan()
    plan.mappings[0].requires_review = True
    plan.mappings[0].user_override = True
    result = PreflightEngine().run(_happy_ctx(plan))
    assert result.passed


def test_g4_blocks_lossy_even_with_user_override():
    """Bare Approve/user_override must not clear lossy_cast without risk_acknowledged."""
    plan = _happy_plan()
    plan.mappings[0].fidelity = "lossy_cast"
    plan.mappings[0].type_narrowing = True
    plan.mappings[0].user_override = True
    plan.mappings[0].requires_review = False
    result = PreflightEngine().run(_happy_ctx(plan))
    assert not result.passed
    assert any(b.gate_id.value == "g4_mapping_confidence" for b in result.blockers)


def test_g4_allows_lossy_with_risk_contract():
    plan = _happy_plan()
    plan.mappings[0].fidelity = "lossy_cast"
    plan.mappings[0].type_narrowing = True
    plan.mappings[0].risk_acknowledged = True
    plan.mappings[0].user_override = True
    plan.mappings[0].risk_contract = make_clearing_risk_contract(
        column=plan.mappings[0].source,
        source_type="DECIMAL",
        destination_type="INTEGER",
    )
    result = PreflightEngine().run(_happy_ctx(plan))
    assert result.passed


def test_g4_boolean_ack_alone_does_not_clear_lossy():
    plan = _happy_plan()
    plan.mappings[0].fidelity = "lossy_cast"
    plan.mappings[0].type_narrowing = True
    plan.mappings[0].risk_acknowledged = True
    plan.mappings[0].user_override = True
    result = PreflightEngine().run(_happy_ctx(plan))
    assert not result.passed
    assert any(b.gate_id.value == "g4_mapping_confidence" for b in result.blockers)


def test_g4_blocks_mutate_without_risk_ack():
    plan = _happy_plan()
    plan.mappings[0].fidelity = "mutate"
    plan.mappings[0].transform = "phone"
    plan.mappings[0].user_override = True
    result = PreflightEngine().run(_happy_ctx(plan))
    assert not result.passed
    assert any(b.gate_id.value == "g4_mapping_confidence" for b in result.blockers)


def test_g4_blocks_struct_flatten_override_without_risk_ack():
    plan = _happy_plan()
    plan.mappings[0].struct_policy = "flatten_top_level_keys"
    plan.mappings[0].requires_review = True
    plan.mappings[0].user_override = True
    result = PreflightEngine().run(_happy_ctx(plan))
    assert not result.passed
    assert any(b.gate_id.value == "g4_mapping_confidence" for b in result.blockers)


def test_g4_skips_intentional_omit_from_confidence():
    plan = _happy_plan()
    plan.mappings.append(
        ColumnMapping(
            source="SSN",
            target="",
            confidence=0.0,
            transform="omit",
            intentional_omit=True,
        )
    )
    result = PreflightEngine().run(_happy_ctx(plan))
    assert result.passed


def test_fail_fast_stops_at_first_blocker():
    plan = _happy_plan()
    plan.source.parseable = False
    plan.destination.connected = False
    result = PreflightEngine(fail_fast=True).run(_happy_ctx(plan))
    assert len(result.blockers) == 1
    assert len(result.gates) == 1


def test_g5_block_message_includes_concrete_issue():
    from preflight.gates import _block_message

    msg = _block_message(
        "Dry-run / integrity failed",
        ["age: cannot cast 'abc' to NUMBER", "score: invalid decimal"],
    )
    assert "age: cannot cast 'abc' to NUMBER" in msg
    assert "+1 more" in msg


def test_g2_blocks_when_can_write_false():
    plan = _happy_plan()
    plan.destination.can_write = False
    plan.destination.table_exists = True
    result = PreflightEngine().run(PreflightContext(plan=plan))
    assert not result.passed
    assert any(b.gate_id.value == "g2_destination" for b in result.blockers)
    g2 = next(g for g in result.gates if g.gate_id.value == "g2_destination")
    assert g2.status == GateStatus.BLOCK
    assert "write" in g2.message.lower() or "insert" in g2.message.lower() or "permission" in g2.message.lower()


def test_g2_blocks_create_denied_for_missing_table():
    plan = _happy_plan()
    plan.destination.can_write = False
    plan.destination.can_create_table = False
    plan.destination.table_exists = False
    result = PreflightEngine().run(PreflightContext(plan=plan))
    assert not result.passed
    g2 = next(g for g in result.gates if g.gate_id.value == "g2_destination")
    assert "create" in g2.message.lower()


def test_g2_blocks_unavailable_probe_on_create_new():
    """Connectivity-only fallback must not green-light create-new."""
    plan = _happy_plan()
    plan.destination.table_exists = False
    plan.destination.can_write = True
    plan.destination.can_create_table = True
    plan.destination.privilege_probe = {
        "status": "unavailable",
        "detail": "ACL describe unavailable",
        "method": "",
    }
    result = PreflightEngine().run(PreflightContext(plan=plan))
    assert not result.passed
    g2 = next(g for g in result.gates if g.gate_id.value == "g2_destination")
    assert g2.status == GateStatus.BLOCK
    assert "unavailable" in g2.message.lower()
    assert "create" in g2.message.lower()


def test_g2_passes_unavailable_probe_when_table_exists():
    plan = _happy_plan()
    plan.destination.table_exists = True
    plan.destination.can_write = True
    plan.destination.privilege_probe = {
        "status": "unavailable",
        "detail": "ACL describe unavailable",
    }
    result = PreflightEngine().run(PreflightContext(plan=plan))
    g2 = next(g for g in result.gates if g.gate_id.value == "g2_destination")
    assert g2.status == GateStatus.PASS
    assert "unavailable" in g2.message.lower()


def test_g3_schemaless_is_skip_not_pass():
    """No DDL type contract must not look like proven type safety."""
    from preflight.gates import gate_g3_schema_contract

    plan = _happy_plan()
    plan.destination.db_type = "mongodb"
    plan.destination.target_columns = []
    result = gate_g3_schema_contract(PreflightContext(plan=plan))
    assert result.status == GateStatus.SKIP
    assert "schemaless" in result.message.lower()


def test_g6_blocks_when_destination_not_connected():
    from preflight.gates import gate_g6_target_ddl

    plan = _happy_plan()
    plan.destination.connected = False
    result = gate_g6_target_ddl(PreflightContext(plan=plan))
    assert result.status == GateStatus.BLOCK
    assert "not connected" in result.message.lower()


def test_g7_blocks_when_bytes_missing_for_nonempty_source():
    from preflight.gates import gate_g7_capacity

    plan = _happy_plan()
    plan.estimated_bytes = 0
    plan.source.row_count_estimate = 5000
    plan.available_staging_bytes = 10_000_000
    result = gate_g7_capacity(PreflightContext(plan=plan))
    assert result.status == GateStatus.BLOCK
    assert "byte estimate" in result.message.lower() or "missing" in result.message.lower()


def test_g9_blocks_unproven_stub_audit():
    """Default context stub must fail-closed — never unlock Execute."""
    from preflight.gates import gate_g9_data_integrity

    result = gate_g9_data_integrity(
        PreflightContext(plan=_happy_plan(), sample_rows=[{"AMT": "1"}])
    )
    assert result.status == GateStatus.BLOCK
    assert "unproven" in result.message.lower() or "not configured" in result.message.lower()


def test_g9_passes_when_audit_proves_checks():
    from preflight.gates import gate_g9_data_integrity

    result = gate_g9_data_integrity(_happy_ctx())
    assert result.status == GateStatus.PASS
