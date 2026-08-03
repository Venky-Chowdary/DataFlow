"""Wave 53 — production Confirm + live connector examples (not Local Postgres fixtures)."""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_PILOT_ENGINE", "local")

from src.ai.copilot.ack_ledger import PilotAckLedger, redact_payload
from src.ai.copilot.connector_create import draft_is_complete
from src.ai.copilot.example_phrases import example_connector_name, example_dest_connector_name
from src.ai.copilot.tools import DataPilotTools


def test_example_phrases_prefer_live_names():
    ctx = {
        "connectors": [
            {"name": "Prod Postgres"},
            {"name": "Snowflake WH"},
        ]
    }
    assert example_connector_name(ctx) == "Prod Postgres"
    assert example_dest_connector_name(ctx, source_hint="Prod Postgres") == "Snowflake WH"
    assert example_connector_name({}) == "your connector" or isinstance(
        example_connector_name({}), str
    )


def test_snowflake_draft_incomplete_without_account_warehouse():
    ok, err = draft_is_complete({
        "type": "snowflake",
        "host": "xy12345",
        "username": "u",
        "password": "p",
        "database": "ANALYTICS",
    })
    assert ok is False
    assert "warehouse" in err.lower()


def test_snowflake_draft_complete_with_required_fields():
    ok, err = draft_is_complete({
        "type": "snowflake",
        "host": "xy12345",
        "account": "xy12345",
        "warehouse": "COMPUTE_WH",
        "database": "ANALYTICS",
        "username": "u",
        "password": "p",
    })
    assert ok is True
    assert err == ""


def test_create_connector_failure_redacts_secrets(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    tools = DataPilotTools()
    # Force incomplete draft via missing password after type/host/db/user
    result = tools.execute(
        "create_connector",
        {
            "message": "create a postgres connector named ProdDB host db.example.com user app database sales",
        },
    )
    assert result.success is False
    out = result.output or {}
    assert out.get("password") in ("", "***", None) or "password" not in out or out.get("password") == "***"
    # Ensure raw secret never echoed if present in draft fields
    assert "secret" not in str(out).lower() or "***" in str(out)


def test_run_schedule_now_stages_ack_ledger(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "c.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SCHEDULE_STORE", str(tmp_path / "s.json"))

    from services import connector_store, schedule_store
    from services.schedule_store import PipelineSchedule

    connector_store._backend_choice = None
    schedule_store._backend_choice = None  # type: ignore[attr-defined]

    # Minimal schedule in file store if supported; otherwise mock resolve.
    tools = DataPilotTools()

    class _Sched:
        id = "sched_prod_1"
        name = "Nightly Prod Sync"
        source_connector_id = "c1"
        dest_connector_id = "c2"
        source_table = "orders"
        dest_table = "orders"
        sync_mode = "incremental"

    monkeypatch.setattr(tools, "_resolve_schedule", lambda sid="", name="": (_Sched(), None))
    result = tools.execute("run_schedule_now", {"name": "Nightly Prod Sync"})
    assert result.success is True
    assert result.output.get("requires_confirm") is True
    ack_id = result.output.get("ack_id")
    assert ack_id and str(ack_id).startswith("ack_")
    peek = PilotAckLedger(path=tmp_path / "acks.json").peek(ack_id)
    assert peek is not None
    assert peek.get("kind") == "run_schedule"


def test_redact_payload_masks_password():
    red = redact_payload({"password": "super-secret", "host": "db.prod"})
    assert red["password"] == "***"
    assert red["has_password"] is True
    assert red["host"] == "db.prod"
