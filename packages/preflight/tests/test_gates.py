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


def test_all_gates_pass():
    engine = PreflightEngine()
    result = engine.run(PreflightContext(plan=_happy_plan()))
    assert result.passed
    assert result.blockers == []
    assert result.passed_count >= 7


def test_g1_blocks_unparseable_file():
    plan = _happy_plan()
    plan.source.parseable = False
    result = PreflightEngine().run(PreflightContext(plan=plan))
    assert not result.passed
    assert result.blockers[0].gate_id.value == "g1_source"


def test_g4_blocks_low_confidence():
    plan = _happy_plan()
    plan.mappings[0].confidence = 0.5
    result = PreflightEngine().run(PreflightContext(plan=plan))
    assert not result.passed
    assert any(b.gate_id.value == "g4_mapping_confidence" for b in result.blockers)


def test_g4_allows_override():
    plan = _happy_plan()
    plan.mappings[0].confidence = 0.5
    plan.mappings[0].user_override = True
    result = PreflightEngine().run(PreflightContext(plan=plan))
    assert result.passed


def test_g4_blocks_ambiguous_mapping():
    plan = _happy_plan()
    plan.mappings[0].requires_review = True
    plan.mappings[0].score_gap = 0.03
    result = PreflightEngine().run(PreflightContext(plan=plan))
    assert not result.passed
    assert any(b.gate_id.value == "g4_mapping_confidence" for b in result.blockers)


def test_g4_allows_ambiguous_with_override():
    plan = _happy_plan()
    plan.mappings[0].requires_review = True
    plan.mappings[0].user_override = True
    result = PreflightEngine().run(PreflightContext(plan=plan))
    assert result.passed


def test_fail_fast_stops_at_first_blocker():
    plan = _happy_plan()
    plan.source.parseable = False
    plan.destination.connected = False
    result = PreflightEngine(fail_fast=True).run(PreflightContext(plan=plan))
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
