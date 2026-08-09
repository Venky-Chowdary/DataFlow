"""ObjectId → TEXT domain polarity must never be silent green.

Mongo ``_id`` landing in an unbounded PostgreSQL ``TEXT`` column keeps the
24-char hex value, so it is not a value collapse — but the destination stops
enforcing the ObjectId domain. G3 must surface it as a BLOCK until an operator
signs a Migration Risk Contract, and must stay quiet for pinned hex wires
(``CHAR(24)`` / ``VARCHAR(24)`` / ``BINARY(12)``) that keep the domain.
"""

from __future__ import annotations

import hashlib
import json

import pytest
from preflight.gates import gate_g3_schema_contract
from preflight.models import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    GateStatus,
    PreflightContext,
    SourceConfig,
    TransferPlan,
)
from services.specialty_fit import objectid_text_domain_polarity


def _signed_contract() -> dict:
    payload = {
        "approved_by": "migration-lead@example.com",
        "reason": "ObjectId hex is joined downstream as text",
        "execution_policy": "CAST_AND_CONTINUE",
    }
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    return {**payload, "signature": f"mrc-sha256:{digest}"}


@pytest.mark.parametrize(
    "target_type",
    ["TEXT", "CLOB", "STRING", "VARCHAR", "NVARCHAR(MAX)", "LONGTEXT"],
)
def test_unbounded_text_sinks_drop_objectid_domain(target_type: str):
    assert objectid_text_domain_polarity("OBJECTID", target_type) is True


@pytest.mark.parametrize(
    "target_type",
    ["BINARY(12)", "VARBINARY(12)", "CHAR(24)", "VARCHAR(24)", "VARCHAR2(36)", "STRING(24)"],
)
def test_pinned_hex_wires_keep_objectid_domain(target_type: str):
    assert objectid_text_domain_polarity("OBJECTID", target_type) is False


def test_non_objectid_sources_are_untouched():
    assert objectid_text_domain_polarity("VARCHAR(24)", "TEXT") is False
    assert objectid_text_domain_polarity("UUID", "TEXT") is False


def _plan(target_type: str, *, risk_ack: bool = False) -> TransferPlan:
    mapping = ColumnMapping(source="_id", target="user_id", confidence=0.9)
    if risk_ack:
        mapping.risk_contract = _signed_contract()
    return TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="mongodb",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="_id", inferred_type="OBJECTID")],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="postgresql",
            db_type="postgresql",
            connected=True,
            can_write=True,
            table_exists=True,
            target_columns=[ColumnSchema(name="user_id", inferred_type=target_type)],
        ),
        mappings=[mapping],
        dry_run_passed=True,
        ddl_compatible=True,
        estimated_bytes=1000,
        available_staging_bytes=10_000_000,
    )


def _gate(plan: TransferPlan):
    return gate_g3_schema_contract(
        PreflightContext(plan=plan, sample_rows=[{"_id": "507f1f77bcf86cd799439011"}])
    )


def test_g3_blocks_objectid_to_unbounded_text():
    result = _gate(_plan("TEXT"))
    assert result.status == GateStatus.BLOCK
    issues = result.details.get("issues") or []
    assert any("ObjectId" in str(issue) for issue in issues), result.details


def test_g3_passes_pinned_varchar24_wire():
    assert _gate(_plan("VARCHAR(24)")).status == GateStatus.PASS


def test_g3_downgrades_to_warning_with_risk_contract():
    result = _gate(_plan("TEXT", risk_ack=True))
    assert result.status != GateStatus.BLOCK, result.details
    warnings = result.details.get("warnings") or []
    assert any("ObjectId" in str(w) for w in warnings), result.details
