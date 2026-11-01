"""Round-4 QA regressions.

The deployed retest ran before the round-3 push. These tests pin the
branch: phones stay text, quarantine does not depend on sample position,
replay keeps source cells, and the operator-facing refusals happen at
staging.
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest

from connectors.object_store_common import normalize_object_base_key, object_run_token
from connectors.sql_identifiers import (
    sanitize_identifier,
    unsafe_destination_identifier_reason,
)
from services.quarantine_dlq import restore_replay_source_cells, snapshot_quarantine_cells
from services.schema_inference import classify_value
from services.transform_engine import apply_transform
from services.verification_ladder import run_five_layer_verification
from src.routers.copilot_router import lifecycle_confirm_succeeded
from src.transfer.mapping_write_stamp import prefer_approved_column_types


_EMPTY_SHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def test_bad_row_near_the_top_of_a_file_quarantines_and_loads_the_rest(tmp_path):
    """Balanced file loads quarantine a bad cast and keep the good rows (R4-D02).

    Strict mode still refuses a partial commit. Validate no longer hard-blocks
    the mixed sample, so that balanced load can run.
    """
    from src.transfer.file_stream import stream_file_to_database
    from src.transfer.models import EndpointConfig

    csv_path = tmp_path / "amt.csv"
    csv_path.write_text(
        "id,amt\n1,not-a-number\n2,10\n3,20\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "out.db"
    written, _ddl, summary, _columns = stream_file_to_database(
        str(csv_path),
        "amt.csv",
        EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(db_path),
            table="payments",
        ),
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER", "confidence": 0.99},
            {
                "source": "amt",
                "target": "amt",
                "target_type": "INTEGER",
                "transform": "integer",
                "confidence": 0.99,
            },
        ],
        schema={"id": "INTEGER", "amt": "INTEGER"},
        validation_mode="balanced",
        skip_preflight=True,
    )
    assert written == 2
    assert int(summary.get("rejected_rows") or 0) == 1
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT id, amt FROM payments ORDER BY id").fetchall()
    assert rows == [(2, 10), (3, 20)]


def test_mixed_sample_does_not_block_on_sample_transform():
    """A bad first row plus good rows is a holdout, not a blocking sample transform."""
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["amt"],
        column_types={"amt": "INTEGER"},
        row_count=3,
        mappings=[
            {
                "source": "amt",
                "target": "amt",
                "confidence": 0.99,
                "transform": "integer",
                "target_type": "INTEGER",
            }
        ],
        destination_connected=True,
        sample_rows=[{"amt": "not-a-number"}, {"amt": "10"}, {"amt": "20"}],
        estimated_bytes=64,
        destination_column_types={"amt": "INTEGER"},
        destination_table_exists=True,
        destination_db_type="sqlite",
        skip_population_fit=True,
    )
    decision = (
        (result.get("proof_bundle") or {}).get("transfer_decision") or {}
    )
    reason = str(decision.get("reason") or "")
    assert "Sample transform / cast failures" not in reason
    assert decision.get("decision") != "block"
    kinds = [
        str((b.get("details") or {}).get("kind") or "")
        for b in result.get("blockers") or []
    ]
    assert "sample_transform" not in kinds


def test_database_mixed_sample_still_blocks_on_sample_transform():
    """A database extract stays fail-fast. File quarantine does not apply."""
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["amt"],
        column_types={"amt": "INTEGER"},
        row_count=2,
        mappings=[
            {
                "source": "amt",
                "target": "amt",
                "confidence": 0.99,
                "transform": "integer",
                "target_type": "INTEGER",
            }
        ],
        destination_connected=True,
        sample_rows=[{"amt": "not-a-number"}, {"amt": "10"}],
        estimated_bytes=64,
        destination_column_types={"amt": "INTEGER"},
        destination_table_exists=True,
        destination_db_type="sqlite",
        source_kind="database",
        skip_population_fit=True,
    )
    decision = (result.get("proof_bundle") or {}).get("transfer_decision") or {}
    assert decision.get("decision") == "block"


def test_phone_tokens_stay_text_plain_integer_does_not():
    assert classify_value("+1 555 010 0001") == "VARCHAR"
    assert classify_value("(555) 123-4567") == "VARCHAR"
    assert classify_value("+42") == "VARCHAR"
    assert classify_value("02115") == "VARCHAR"
    assert classify_value("45%") == "VARCHAR"
    assert classify_value("$1,234.50") == "VARCHAR"
    assert classify_value(" 42 ") == "INTEGER"


def test_default_numeric_transforms_refuse_markers():
    pct, err = apply_transform("45%", "integer")
    assert pct is None and err
    trimmed, err = apply_transform(" 42 ", "integer")
    assert err is None and trimmed == 42
    money, err = apply_transform("$1,234.50", "decimal")
    assert err is None and str(money) == "1234.50"
    plain, err = apply_transform("1,234.56", "decimal")
    assert err is None and str(plain) == "1234.56"
    converted, err = apply_transform("$1,234.50", "currency")
    assert err is None and converted == "1234.50"
    percent, err = apply_transform("45%", "percentage")
    assert err is None


def test_json_null_is_a_value_and_broken_objects_are_rejected():
    parsed, err = apply_transform("null", "json")
    assert err is None and parsed == "null"
    broken, err = apply_transform("{bad json", "json")
    assert broken is None and err and "Invalid JSON" in err
    scalar, err = apply_transform("hello", "json")
    assert err is None and scalar == '"hello"'
    missing, err = apply_transform("n/a", "integer")
    assert missing is None and "Null sentinel" in (err or "")


def test_approved_column_types_survive_a_wider_peek():
    request = SimpleNamespace(
        column_types={"phone": "VARCHAR", "qty": "BIGINT"},
        mappings=[
            {"source": "phone", "target": "phone"},
            {"source": "qty", "target": "qty"},
        ],
    )
    schema = prefer_approved_column_types(
        request, {"phone": "BIGINT", "qty": "VARCHAR", "note": "VARCHAR"}
    )
    assert schema["phone"] == "VARCHAR"
    assert schema["qty"] == "BIGINT"
    assert schema["note"] == "VARCHAR"
    untouched = prefer_approved_column_types(
        SimpleNamespace(column_types={}, mappings=[]),
        {"phone": "BIGINT"},
    )
    assert untouched["phone"] == "BIGINT"


def test_failed_replay_restores_original_cells():
    original = [
        {
            "row": 293,
            "column": "price",
            "value": "$1,234.50",
            "values": {"payload": "{bad json", "price": "$1,234.50"},
            "source_values": {"payload": "{bad json", "price": "$1,234.50"},
            "retry_status": "open",
        }
    ]
    snaps = snapshot_quarantine_cells(original)
    original[0]["source_values"]["price"] = "1234.50"
    original[0]["values"]["payload"] = '"{bad json"'
    child = [
        {
            "row": 293,
            "value": "1234.50",
            "values": {"payload": '"{bad json"', "price": "1234.50"},
            "reason": "numeric overflow",
        }
    ]
    restored = restore_replay_source_cells(original, snaps, child)
    assert restored[0]["source_values"]["price"] == "$1,234.50"
    assert restored[0]["source_values"]["payload"] == "{bad json"
    assert restored[0]["values"]["payload"] == "{bad json"
    assert restored[0]["replay_attempt"]["values"]["price"] == "1234.50"


def test_empty_sha256_is_not_an_l3_pass_when_rows_were_rejected():
    report = run_five_layer_verification(
        source_rows=[],
        target_rows=[],
        columns=["id"],
        pk_column="id",
        source_row_count=6,
        target_row_count=0,
        rejected_rows=6,
        source_checksum=_EMPTY_SHA,
        target_checksum=_EMPTY_SHA,
    )
    assert report["layers"]["L3"]["passed"] is False
    assert report["layers"]["L3"]["details"]["reason"] == "empty_digest"
    assert report["passed"] is False


def test_object_key_rejects_parent_segments_run_token_does_not():
    with pytest.raises(ValueError, match="cannot traverse"):
        normalize_object_base_key("QA_R4/../secrets.json")
    with pytest.raises(ValueError, match="cannot traverse"):
        normalize_object_base_key("out.json", schema="../hidden")
    assert ".." not in object_run_token("../../etc/passwd?x=1")
    assert normalize_object_base_key("QA_R4/out/payments.jsonl") == "QA_R4/out/payments.jsonl"


def test_sql_injection_table_name_is_refused_and_names_the_sanitized_spelling():
    raw = 'qa_r4_x"; DROP TABLE qa_r3_customers; --'
    reason = unsafe_destination_identifier_reason("postgresql", raw)
    assert reason
    assert sanitize_identifier(raw.replace(".", "_")) in reason
    assert unsafe_destination_identifier_reason("postgresql", "qa_r4_payments") is None
    assert unsafe_destination_identifier_reason("s3", "QA_R4/../out.json") is None


def test_existing_qty_maps_to_the_integer_column():
    from services.semantic_mapper import map_columns

    columns = ["id", "qty", "pct", "price", "payload", "blob", "label", "code", "updated_at"]
    source = [{"name": name, "inferred_type": "TEXT"} for name in columns]
    source[0] = {"name": "id", "inferred_type": "BIGINT"}
    target_types = {
        "id": "BIGINT",
        "qty": "INTEGER",
        "pct": "NUMERIC(8,4)",
        "price": "NUMERIC(12,2)",
        "payload": "JSONB",
        "blob": "BYTEA",
        "label": "TEXT",
        "code": "VARCHAR(2)",
        "updated_at": "TIMESTAMPTZ",
    }
    target = [{"name": name, "inferred_type": typ} for name, typ in target_types.items()]
    mappings = map_columns(
        columns,
        columns,
        source_schemas=source,
        target_schemas=target,
        destination_db_type="postgresql",
        destination_table_exists=True,
    )
    qty = next(row for row in mappings if row["source"] == "qty")
    assert qty["target"] == "qty"
    assert "INT" in str(qty["target_type"]).upper()
    assert not qty.get("create_new")
    assert not any(row.get("create_new") and row.get("source") == "qty" for row in mappings)


def test_prepare_cdc_applied_false_is_not_ok():
    ok, body = lifecycle_confirm_succeeded(
        {"applied": False, "error": "role is not superuser; wal_level stayed replica"}
    )
    assert ok is False
    assert body["status"] == "not_applied"
    assert "superuser" in body["error"]
    ok, body = lifecycle_confirm_succeeded({"applied": True, "wal_level": "logical"})
    assert ok is True
    ok, body = lifecycle_confirm_succeeded({"schedule_id": "abc"})
    assert ok is True


def test_resume_stages_only_when_the_checkpoint_is_safe(monkeypatch):
    from src.ai.copilot import lifecycle_tools as tools

    job = {
        "id": "6acd54f83e106b159b9e62e0",
        "status": "failed",
        "checkpoint": None,
        "transfer_request": {"source": {}, "destination": {}},
        "rejected_rows": 6,
        "rejected_details": [
            {"row": 1, "retry_status": "open", "value": "a"},
            {"row": 2, "retry_status": "promoted", "value": "b"},
            {"row": 3, "retry_status": "open", "value": "c"},
        ],
    }
    monkeypatch.setattr(tools, "resolve_job", lambda *_a, **_k: (job, ""))
    refused = tools._job_tool("resume_job", job["id"], "")
    assert refused.success is False
    assert "Resume refused — checkpoint not safe" in refused.error
    assert "No durable checkpoint" in refused.error
    brief = tools._job_brief(job)
    assert brief["rejected_rows"] == 2


def test_duplicate_schedule_name_is_refused_at_staging(monkeypatch):
    from src.ai.copilot import schedule_tools as tools
    from src.ai.copilot.query_tools import _tool_result

    plan = _tool_result(
        "plan_transfer",
        success=True,
        output={
            "source": {"connector_id": "s", "table": "payments", "connector_name": "src"},
            "destination": {"connector_id": "d", "table": "payments", "connector_name": "dst"},
            "engine_mappings": [{"source": "id", "target": "id"}],
            "preflight": {"passed": True, "proof_bundle": {"transfer_decision": {"decision": "approve"}}},
            "sync_mode": "full_refresh_append",
            "data_rules": {},
            "mapped_count": 1,
        },
    )
    monkeypatch.setattr(tools, "plan_transfer", lambda **_k: plan)
    monkeypatch.setattr(tools, "_stage_bound_contract", lambda *_a, **_k: {})

    def clash(name, **_k):
        raise ValueError(
            f"A pipeline named “{name}” already exists (QA_R3_sched_up (9bca693f))."
        )

    monkeypatch.setattr("services.schedule_store.assert_unique_schedule_name", clash)
    staged = {"put": False}
    monkeypatch.setattr(
        "src.ai.copilot.ack_ledger.get_ack_ledger",
        lambda: SimpleNamespace(put=lambda **_k: staged.__setitem__("put", True)),
    )
    result = tools.create_schedule(
        source_connector_id="s",
        dest_connector_id="d",
        source_table="payments",
        dest_table="payments",
        cadence="every day",
        name="QA_R3_sched_up",
    )
    assert result.success is False
    assert "already exists" in result.error
    assert staged["put"] is False


def test_run_query_limit_1000_is_not_clamped_to_200(monkeypatch):
    from src.ai.copilot import query_tools as tools

    seen: dict[str, int] = {}

    def fake_run(_saved, body):
        seen["limit"] = body.limit
        rows = [{"id": i} for i in range(536)]
        return rows, ["id"], {"id": "INTEGER"}, False

    monkeypatch.setattr(
        tools,
        "_safe_connector",
        lambda *_a, **_k: ({"id": "c1", "type": "postgresql", "name": "pg"}, None),
    )
    monkeypatch.setattr(tools, "_store_result", lambda **_k: "pr_test")
    monkeypatch.setattr("services.connector_store.get_connector", lambda _cid, **_k: {"id": "c1"})
    monkeypatch.setattr("src.routers.query_router._is_safe_sql", lambda _sql: True)
    monkeypatch.setattr("src.routers.query_router._run_query", fake_run)
    result = tools.run_connector_query(connector_id="c1", query="SHOW TABLES", limit=1000)
    assert result.success, result.error
    assert seen["limit"] == 1000
    assert result.output["row_count"] == 536
    assert result.output["truncated"] is False
    assert result.output["limit"] == 1000


def test_replay_of_zero_rows_is_a_failure():
    from src.routers.connectors_router import replay_job_quarantine

    source = inspect.getsource(replay_job_quarantine)
    assert "int(rows_written or 0) == 0 and records" in source
    assert '"success": status != "failed"' in source


def test_postgresql_overwrite_defers_truncate(monkeypatch):
    from src.transfer.engine_destination import _drop_destination_table
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(kind="database", format="postgresql", table="qa_r4_big", extra={})
    monkeypatch.setattr(
        "src.transfer.engine_destination._raise_if_job_cancelled", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        "src.transfer.engine_destination._pin_overwrite_rows_before", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        "src.transfer.adapters.resolve_connector_config", lambda _d: {"schema": "public"}
    )
    monkeypatch.setattr(
        "src.transfer.adapters.resolve_dest_table", lambda *_a, **_k: "qa_r4_big"
    )
    monkeypatch.setattr(
        "src.transfer.adapters.carry_dest_spelling_across_drop", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        "src.transfer.connector_capabilities.resolve_driver_type", lambda _fmt: "postgresql"
    )

    def fail_empty(*_a, **_k):
        raise AssertionError("autocommit truncate must not run for postgresql")

    monkeypatch.setattr("connectors.table_manager.empty_existing_for_overwrite", fail_empty)
    assert _drop_destination_table(dest) is True
    assert dest.extra["pg_overwrite_in_transaction"] is True


def test_sqlite_source_refuses_a_missing_file(tmp_path):
    from connectors.sqlite_common import require_sqlite_source_file

    missing = tmp_path / "QA_RT5.db"
    with pytest.raises(ValueError):
        require_sqlite_source_file(str(missing))


def test_start_transfer_refuses_an_unsafe_name_even_when_plan_succeeds(monkeypatch):
    from types import SimpleNamespace

    from src.ai.copilot import transfer_tools as tools

    monkeypatch.setattr(
        tools,
        "plan_transfer",
        lambda **_k: SimpleNamespace(
            success=True,
            error="",
            output={
                "destination": {
                    "type": "postgresql",
                    "table": 'qa_r4_x"; DROP TABLE qa_r3_customers; --',
                    "connector_id": "d",
                    "connector_name": "pg",
                },
                "source": {
                    "type": "postgresql",
                    "table": "payments",
                    "connector_id": "s",
                    "connector_name": "pg",
                },
                "preflight": {"passed": True},
                "engine_mappings": [{"source": "id", "target": "id"}],
            },
        ),
    )
    result = tools.start_transfer(
        source_connector_id="s",
        dest_connector_id="d",
        source_table="payments",
        dest_table='qa_r4_x"; DROP TABLE qa_r3_customers; --',
    )
    assert result.success is False
    assert "plain SQL identifier" in (result.error or "")


def test_engine_refuses_an_unsafe_destination_before_the_load(monkeypatch):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    recorded: dict = {}

    class Mongo:
        def update_job_status(self, job_id, status, **kwargs):
            recorded["job_id"] = job_id
            recorded["status"] = status
            recorded.update(kwargs)
            return True

    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service", lambda: Mongo()
    )
    engine = UniversalTransferEngine()
    monkeypatch.setattr(engine, "_resolve_saved_connectors", lambda _request: None)
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table="payments"),
        destination=EndpointConfig(
            kind="database",
            format="postgresql",
            table='qa_r4_x"; DROP TABLE qa_r3_customers; --',
        ),
    )
    result = engine.execute_tracked(request, "job-ident")
    assert result.success is False
    assert "plain SQL identifier" in result.error
    assert recorded["status"] == "failed"
    assert recorded["phase"] == "validate"
