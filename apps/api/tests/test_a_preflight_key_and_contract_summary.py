"""A-preflight — the two refusals an operator hit only after pressing Run.

1. A-1170's MySQL LOB-key refusal fired at write time: Validate passed a
   create-new upsert keyed on ``(region TEXT, id)`` and the job then failed.
   G6 now asks the same shared rule (``mysql_key_compatible_types``).
2. g9 blocked an incremental_upsert whose cursor meaning was undeclared with a
   specific issue, but the collapsed root told the operator "needs an identity
   key and/or a cursor column that was not provided" — both were provided.
"""

from __future__ import annotations

import pytest

from preflight.models import GateId, GateStatus


def _g6(*, dest="mysql", region_type="TEXT", table_exists=False, sync="incremental_upsert",
        target_type=None):
    from preflight.gates import gate_g6_target_ddl
    from preflight.models import ColumnMapping, ColumnSchema, DestinationConfig, SourceConfig, TransferPlan
    from services.preflight_service import FilePreflightContext

    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="postgresql",
            columns=[
                ColumnSchema(name="region", inferred_type=region_type),
                ColumnSchema(name="id", inferred_type="INTEGER"),
                ColumnSchema(name="amount", inferred_type="NUMERIC(10,2)"),
            ],
            connected=True,
            row_count_estimate=4,
        ),
        destination=DestinationConfig(
            kind="database", db_type=dest, connected=True, table_exists=table_exists,
            can_create_table=True, can_write=True,
        ),
        mappings=[
            ColumnMapping(source="region", target="region", confidence=0.99, target_type=target_type),
            ColumnMapping(source="id", target="id", confidence=0.99),
            ColumnMapping(source="amount", target="amount", confidence=0.99),
        ],
        sync_mode=sync,
        validation_mode="strict",
        ddl_compatible=True,
        contract_primary_key="region,id",
    )
    ctx = FilePreflightContext(plan=plan, sample_rows=[{"region": "eu", "id": 1, "amount": "1.5"}])
    return gate_g6_target_ddl(ctx)


@pytest.mark.parametrize("dest", ["mysql", "mariadb"])
def test_text_upsert_key_with_no_width_blocks_at_validate(dest):
    result = _g6(dest=dest)
    assert result.gate_id == GateId.G6_TARGET_DDL
    assert result.status == GateStatus.BLOCK
    assert result.details["rule_id"] == "g6_target_ddl.mysql_lob_key"
    # The same operator message the writer returns (one shared rule, not a copy).
    assert result.message.startswith("MySQL cannot enforce the upsert key (region, id)")
    assert "error 1170" in result.message and "VARCHAR(n)" in result.message


def test_validate_and_writer_refuse_with_the_same_message():
    from services.schema_fidelity import mysql_key_compatible_types

    _types, writer_msg = mysql_key_compatible_types(
        table_name="", conflict_columns=["region", "id"],
        target_cols=["region", "id", "amount"], target_types=["TEXT", "BIGINT", "DECIMAL(10,2)"],
        mappings=[{"source": c, "target": c} for c in ("region", "id", "amount")],
        column_types={"region": "TEXT", "id": "INTEGER", "amount": "NUMERIC(10,2)"},
    )
    assert _g6().message == writer_msg


@pytest.mark.parametrize(
    "kwargs",
    [
        {"region_type": "VARCHAR(8)"},           # declared width → VARCHAR(8) key
        {"table_exists": True},                   # the existing table already holds its key
        {"table_exists": None},                   # unknown is never create-new
        {"sync": "full_refresh_append"},          # no upsert key is created
        {"dest": "postgresql"},                   # PG indexes TEXT
        {"target_type": "VARCHAR(16)"},           # Map chose an indexable carrier
    ],
)
def test_indexable_or_out_of_scope_keys_are_not_blocked(kwargs):
    result = _g6(**kwargs)
    assert result.details.get("rule_id") != "g6_target_ddl.mysql_lob_key"


def _g9_block():
    from services.preflight_cursor_gate import build_sync_contract_gate

    return build_sync_contract_gate(
        [{"name": "orders", "selected": True, "primary_key": ["id"],
          "cursor_field": "updated_at", "sync_mode": "incremental_deduped"}],
        sync="incremental_deduped", validation="strict", dest="mysql", src="postgresql",
        kind="database", source_columns=["id", "amount", "updated_at"],
        pass_status="pass", block_status="block", catalog_primary_key_columns=["id"],
    )


def test_contract_root_names_the_real_g9_issue_and_its_fix():
    from services.root_cause_engine import apply_root_causes_to_preflight

    gate = _g9_block()
    assert gate["status"] == "block"
    real_issue = gate["details"]["issues"][0]
    assert "nothing states that updated_at moves when a row changes" in real_issue
    action = next(v["primary_action"] for v in gate["details"]["cursor_semantics"] if v["status"] == "block")
    assert action

    out = apply_root_causes_to_preflight({"gates": [gate], "blockers": [dict(gate)]})
    root = next(b for b in out["blockers"] if (b.get("details") or {}).get("kind") == "sync_contract_incomplete")
    assert real_issue in root["message"]
    assert "needs an identity key" not in root["message"]
    assert root["details"]["recommended_fix"] == action
    assert root["guidance"]["fix"] == action


def test_contract_root_without_gate_issues_keeps_the_generic_guidance():
    from services.root_cause_engine import apply_root_causes_to_preflight

    gate = {"id": "g9_sync_contract", "status": "block", "message": "Sync mode contract incomplete", "details": {}}
    out = apply_root_causes_to_preflight({"gates": [gate], "blockers": [dict(gate)]})
    root = next(b for b in out["blockers"] if (b.get("details") or {}).get("kind") == "sync_contract_incomplete")
    assert "needs an identity key" in root["message"]
