"""Enterprise honesty regressions from the connector×type×UX audit."""

from __future__ import annotations

from preflight import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    PreflightContext,
    SourceConfig,
    TransferPlan,
)
from preflight.gates import gate_g3_schema_contract
from preflight.models import GateStatus


def test_pending_dest_schema_never_gets_create_new_risk_stamps():
    """Unknown/incomplete dest schema must not invent create-new risks or DDL."""
    from services.semantic_mapper import _apply_create_new_risk_stamps

    rows = _apply_create_new_risk_stamps(
        [{
            "source": "created_at",
            "target": "created_at",
            "source_type": "TIMESTAMPTZ",
            "target_type": "TIMESTAMPTZ",
            "assignment_strategy": "pending_dest_schema",
            "create_new": False,
            "requires_review": True,
            "reasoning": "Destination schema unavailable — not treating as create-new",
            "confidence": 0.55,
        }],
        "mysql",
    )
    assert rows[0].get("create_new") is False
    assert rows[0].get("assignment_strategy") == "pending_dest_schema"
    assert not rows[0].get("create_new_risks")
    assert rows[0].get("target_type") == "TIMESTAMPTZ"
    assert "create-new type risk" not in str(rows[0].get("reasoning") or "").lower()


def test_unsigned_int_to_bigint_is_value_safe_pass():
    """INT UNSIGNED → BIGINT fits — must not Accept-risk block (enterprise UX)."""
    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="qty", inferred_type="INT UNSIGNED")],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            can_write=True,
            table_exists=True,
            target_columns=[ColumnSchema(name="qty", inferred_type="BIGINT")],
        ),
        mappings=[ColumnMapping(source="qty", target="qty", confidence=0.95)],
        dry_run_passed=True,
        ddl_compatible=True,
        estimated_bytes=1000,
        available_staging_bytes=10_000_000,
    )
    result = gate_g3_schema_contract(PreflightContext(plan=plan))
    assert result.status == GateStatus.PASS, result.message


def test_unsigned_int_to_int_still_blocks():
    from services.type_system import is_lossy_coercion, unsigned_integer_would_overflow

    assert unsigned_integer_would_overflow("INT UNSIGNED", "INTEGER") is True
    assert is_lossy_coercion("INT UNSIGNED", "INTEGER") is True


def test_uuid_create_new_string_sink_stamps_domain_risk():
    from services.type_system import assess_create_new_type_risk, create_new_mapping_target_type

    stamp = create_new_mapping_target_type("UUID", "bigquery")
    # Width-preserving STRING(36) or bare STRING — both are string sinks.
    assert stamp.upper().startswith("STRING")
    kinds = {r["kind"] for r in assess_create_new_type_risk("UUID", stamp, destination_db_type="bigquery")}
    assert "uuid_domain" in kinds or "precision_collapse" in kinds or "lossy_coercion" in kinds


def _mysql_create_new_row(inferred_type: str) -> dict:
    from services.mapping_pipeline import run_mapping_pipeline

    result = run_mapping_pipeline(
        source_columns=["created_at"],
        target_columns=[],
        source_schemas=[{
            "name": "created_at",
            "inferred_type": inferred_type,
            "samples": ["2024-01-01T00:00:00Z"],
        }],
        destination_db_type="mysql",
        destination_table_exists=False,
        use_llm=False,
    )
    row = result["mappings"][0]
    assert row.get("create_new") is True
    return row


def test_create_new_mysql_timestamptz_pipeline_stamps_visible_risks():
    """Declared nanoseconds into MySQL's microsecond carrier drop three digits.

    DATETIME(6) is still the right carrier (no 2038 cap), but a source that
    declares ``(9)`` loses real digits, so the drop needs the Risk Contract.
    """
    row = _mysql_create_new_row("TIMESTAMPTZ(9)")
    assert row["target_type"].upper().startswith("DATETIME(6)"), row["target_type"]
    assert row.get("requires_risk_contract") is True
    kinds = {r.get("kind") for r in row.get("create_new_risks") or []}
    assert "precision_collapse" in kinds
    assert "instant_range_cap" not in kinds


def test_create_new_mysql_timestamptz_keeps_the_instant_and_names_its_ceiling():
    """Cross-engine TIMESTAMPTZ create-new on MySQL is DATETIME(6).

    TIMESTAMP(6) would refuse instants outside 1970..2038. DATETIME(6) stores
    the UTC clock, so Map does not warn about that ceiling and does not
    demand a Risk Contract.
    """
    row = _mysql_create_new_row("TIMESTAMPTZ")
    assert row["target_type"].upper().startswith("DATETIME(6)"), row["target_type"]
    risks = row.get("create_new_risks") or []
    assert "instant_range_cap" not in {r.get("kind") for r in risks}
    assert row.get("requires_risk_contract") is False
    assert str(row.get("fidelity") or "").lower() in {"", "preserve", "lossless"}
