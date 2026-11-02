"""MCP confirm_action spends the ack on the same route the Confirm button uses.

Named fixture: a temporary ack ledger, a temporary connector file, and a
temporary schedule file. The file transfer records the TransferRequest the
engine would run. Nothing here dials a warehouse or writes the workspace stores.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from services.platform_config import data_dir


def _digest(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


class _Http:
    """The request object confirm reads for identity. Role comes from _caller."""

    class state:  # noqa: N801
        pass

    headers: dict[str, str] = {}


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    """Point every store the confirm route writes at this test's directory."""
    real = data_dir()
    before = {
        name: _digest(real / name)
        for name in ("connectors.json", "schedules.json", "pilot_acks.json")
    }
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "")

    import services.connector_store as connector_store
    import services.schedule_store as schedule_store
    import src.ai.copilot.ack_ledger as ack_mod
    from src.routers import copilot_router

    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(ack_mod, "_ledger", None)
    yield SimpleNamespace(tmp=tmp_path, router=copilot_router)
    after = {
        name: _digest(real / name)
        for name in ("connectors.json", "schedules.json", "pilot_acks.json")
    }
    assert after == before


def _role(router, role: str, actor: str):
    return lambda _request: (role, actor)


def _confirm(ack_id: str, reason: str = "qe confirm") -> dict:
    from src.ai.copilot.confirm_ack import confirm_from_tool, reset_mcp_request, set_mcp_request

    token = set_mcp_request(_Http())
    try:
        return confirm_from_tool(ack_id, reason)
    finally:
        reset_mcp_request(token)


def test_editor_confirm_saves_one_connector_and_replay_does_not_save_another(isolated, monkeypatch):
    monkeypatch.setattr(isolated.router, "_caller", _role(isolated.router, "editor", "qe@example.com"))
    from services.connector_store import get_connector, list_connectors
    from src.ai.copilot.tools import DataPilotTools

    staged = DataPilotTools().execute(
        "create_connector",
        {
            "name": "qe-mcp-confirm-connector",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "qe",
            "username": "qe",
            "password": "not-a-live-secret",
            "schema": "public",
            "test_first": False,
        },
    )
    assert staged.success is True, staged.error
    ack_id = staged.output["ack_id"]
    assert staged.output["requires_confirm"] is True
    assert list_connectors() == []

    first = _confirm(ack_id)
    assert first["ok"] is True, first
    assert first["idempotent"] is False
    assert first["kind"] == "create_connector"
    assert first["name"] == "qe-mcp-confirm-connector"
    saved = get_connector(first["connector_id"])
    assert saved is not None
    assert saved.type == "postgresql"
    assert saved.host == "127.0.0.1"
    assert saved.database == "qe"
    assert [c.name for c in list_connectors()].count("qe-mcp-confirm-connector") == 1

    replay = _confirm(ack_id)
    assert replay["ok"] is True, replay
    assert replay["idempotent"] is True
    assert replay["connector_id"] == first["connector_id"]
    assert [c.name for c in list_connectors()].count("qe-mcp-confirm-connector") == 1


def test_viewer_confirm_of_create_connector_does_not_consume_the_ack(isolated, monkeypatch):
    monkeypatch.setattr(isolated.router, "_caller", _role(isolated.router, "viewer", "viewer@example.com"))
    from services.connector_store import list_connectors
    from src.ai.copilot.ack_ledger import get_ack_ledger

    ack_id = get_ack_ledger().put(
        kind="create_connector",
        payload={
            "name": "qe-mcp-viewer-refused",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "qe",
            "username": "qe",
            "password": "not-a-live-secret",
        },
        preview={"name": "qe-mcp-viewer-refused"},
    )
    body = _confirm(ack_id)
    assert body["ok"] is False
    assert "viewer" in body["error"].lower()
    peek = get_ack_ledger().peek(ack_id)
    assert peek is not None
    assert peek["consumed"] is False
    assert list_connectors() == []


def test_editor_confirm_creates_one_enabled_schedule_and_replay_keeps_one(isolated, monkeypatch):
    monkeypatch.setattr(isolated.router, "_caller", _role(isolated.router, "editor", "qe@example.com"))
    from services.mapping_pipeline import run_mapping_pipeline
    from services.schedule_store import get_schedule, list_schedules
    from src.ai.copilot.ack_ledger import get_ack_ledger

    mapped = run_mapping_pipeline(
        ["id", "amount"],
        ["id", "amount"],
        use_llm=False,
        destination_table_exists=True,
        source_types_authoritative=True,
    )
    mappings = list(mapped.get("mappings") or [])
    assert mappings
    assert all(row.get("source") and row.get("target") for row in mappings)

    ack_id = get_ack_ledger().put(
        kind="create_schedule",
        payload={
            "name": "qe-mcp-nightly",
            "source_connector_id": "src-qe",
            "source_table": "payments",
            "dest_connector_id": "dst-qe",
            "dest_table": "payments_wh",
            "interval": "daily",
            "cron": "0 2 * * *",
            "timezone": "UTC",
            "sync_mode": "full_refresh_append",
            "schema_policy": "manual_review",
            "validation_mode": "balanced",
            "mappings": mappings,
            "enabled": True,
        },
        preview={"name": "qe-mcp-nightly"},
    )
    first = _confirm(ack_id)
    assert first["ok"] is True, first
    assert first["idempotent"] is False
    assert first["name"] == "qe-mcp-nightly"
    assert first["enabled"] is True
    assert first["cron"] == "0 2 * * *"
    assert first["next_run_at"]
    saved = get_schedule(first["schedule_id"])
    assert saved is not None
    assert saved.enabled is True
    assert len(saved.mappings) == len(mappings)
    assert len(list_schedules()) == 1

    replay = _confirm(ack_id)
    assert replay["ok"] is True, replay
    assert replay["idempotent"] is True
    assert replay["schedule_id"] == first["schedule_id"]
    assert len(list_schedules()) == 1


def test_file_ack_confirm_hands_the_upload_to_the_engine_once(isolated, monkeypatch):
    monkeypatch.setattr(isolated.router, "_caller", _role(isolated.router, "editor", "qe@example.com"))
    import services.dataset_file as dataset_file
    import src.transfer.background as background
    import src.transfer.engine as engine_mod
    from src.ai.copilot.ack_ledger import get_ack_ledger

    monkeypatch.setattr(dataset_file, "dataset_roots", lambda: [isolated.tmp])
    csv_path = isolated.tmp / "payments.csv"
    csv_path.write_text("id,amount\n1,10\n2,20\n", encoding="utf-8")

    seen: list = []

    class _Engine:
        def _create_pending_job(self, request):
            seen.append(request)
            return "job-qe-file-1"

    def _run(job_id, request):
        seen.append(("run", job_id))

    monkeypatch.setattr(engine_mod, "get_transfer_engine", lambda: _Engine())
    monkeypatch.setattr(background, "run_transfer_async", _run)

    ack_id = get_ack_ledger().put(
        kind="start_transfer",
        payload={
            "source": {"kind": "file", "format": "csv", "table": "payments"},
            "destination": {
                "kind": "database",
                "format": "postgresql",
                "connector_id": "dest-qe",
                "table": "pay",
            },
            "mappings": [{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
            "sync_mode": "full_refresh_append",
            "skip_preflight": True,
            "source_path": str(csv_path),
            "source_filename": "payments.csv",
        },
        preview={"file": "payments.csv"},
    )
    first = _confirm(ack_id)
    assert first["ok"] is True, first
    assert first["idempotent"] is False
    assert first["job_id"] == "job-qe-file-1"
    assert first["source"] == "payments.csv"
    assert first["destination"] == "dest-qe.pay"
    assert seen[0].source.kind == "file"
    assert seen[0].source_path == str(csv_path.resolve())
    assert seen[0].skip_preflight is False
    assert seen[0].triggered_by == "data-pilot"
    assert seen[1] == ("run", "job-qe-file-1")

    replay = _confirm(ack_id)
    assert replay["ok"] is True, replay
    assert replay["idempotent"] is True
    assert replay["job_id"] == "job-qe-file-1"
    assert len(seen) == 2


def test_staged_payments_file_confirms_into_one_engine_request(isolated, monkeypatch):
    """The ack ``start_dataset_transfer`` writes is the ack confirm runs.

    Fixture: ``tests/fixtures/sample_payments.csv`` (10 posted payment rows).
    Destination introspect and preflight are the warehouse boundary. Parse,
    mapping, and confirm are the real ones. The engine records the request.
    """
    monkeypatch.setattr(isolated.router, "_caller", _role(isolated.router, "editor", "qe@example.com"))
    import services.dataset_file as dataset_file
    import src.transfer.background as background
    import src.transfer.engine as engine_mod
    from src.ai.copilot import transfer_tools
    from src.ai.copilot.data_analyst import get_data_analyst
    from src.ai.copilot.dataset_transfer import stage_dataset_transfer

    fixture = Path(__file__).resolve().parent / "fixtures" / "sample_payments.csv"
    assert fixture.is_file()
    monkeypatch.setattr(dataset_file, "dataset_roots", lambda: [fixture.parent])
    schema = SimpleNamespace(name="sample_payments", source="upload", path=str(fixture), file_type="csv")
    monkeypatch.setattr(get_data_analyst(), "resolve_dataset", lambda _hint: schema)
    monkeypatch.setattr(
        transfer_tools,
        "_safe_connector",
        lambda *_a, **_k: (
            {"id": "dest-qe", "name": "Warehouse", "type": "postgresql", "schema": "public"},
            None,
        ),
    )
    columns = [
        "CUST_ID", "AMT", "TXN_DT", "ACCT_NO", "CCY", "REF_NO", "STS", "DESC",
    ]
    monkeypatch.setattr(
        transfer_tools,
        "_introspect",
        lambda *_a, **_k: {
            "ok": True,
            "columns": [
                {"name": name, "inferred_type": "TEXT", "nullable": True} for name in columns
            ],
            "db_type": "postgresql",
            "table_exists": True,
        },
    )
    monkeypatch.setattr(
        transfer_tools,
        "_run_preflight",
        lambda **_k: {
            "passed": True,
            "run_id": "pf_payments_confirm",
            "proof_bundle": {"transfer_decision": {"decision": "approve"}},
        },
    )
    monkeypatch.setattr(transfer_tools, "_stage_bound_contract", lambda *_a, **_k: {})

    seen: list = []

    class _Engine:
        def _create_pending_job(self, request):
            seen.append(request)
            return "job-payments-1"

    monkeypatch.setattr(engine_mod, "get_transfer_engine", lambda: _Engine())
    monkeypatch.setattr(background, "run_transfer_async", lambda job_id, request: seen.append(("run", job_id)))

    staged = stage_dataset_transfer(
        dataset_name="sample_payments",
        dest_connector_name="Warehouse",
        dest_table="payments_wh",
    )
    assert staged.success is True, staged.error
    assert staged.output["preview"]["rows"] == 10
    assert staged.output["preview"]["file"] == "sample_payments.csv"

    first = _confirm(staged.output["ack_id"])
    assert first["ok"] is True, first
    assert first["job_id"] == "job-payments-1"
    assert first["source"] == "sample_payments.csv"
    assert first["destination"] == "dest-qe.payments_wh"
    request = seen[0]
    assert request.source.kind == "file"
    assert request.source_path == str(fixture.resolve())
    assert request.skip_preflight is False
    assert {row.get("source") for row in request.mappings} >= {"CUST_ID", "AMT"}
    assert seen[1] == ("run", "job-payments-1")

    replay = _confirm(staged.output["ack_id"])
    assert replay["idempotent"] is True
    assert replay["job_id"] == "job-payments-1"
    assert len(seen) == 2


def test_confirm_runs_a_schedule_once(isolated, monkeypatch):
    monkeypatch.setattr(isolated.router, "_caller", _role(isolated.router, "editor", "qe@example.com"))
    import services.schedule_runner as runner
    import src.services.schedule_runner as shim
    from services.schedule_store import create_schedule
    from src.ai.copilot.ack_ledger import get_ack_ledger

    sched = create_schedule({
        "name": "qe-mcp-run-now",
        "source_connector_id": "src-qe",
        "source_table": "payments",
        "dest_connector_id": "dst-qe",
        "dest_table": "payments_wh",
        "interval": "daily",
        "cron": "0 2 * * *",
        "timezone": "UTC",
        "sync_mode": "full_refresh_append",
        "mappings": [{"source": "id", "target": "id"}],
        "enabled": True,
    })
    calls: list[tuple[str, bool]] = []

    def _run(schedule_id: str, *, manual: bool = False) -> str:
        calls.append((schedule_id, manual))
        return "job-schedule-1"

    monkeypatch.setattr(runner, "_run_schedule", _run)
    monkeypatch.setattr(shim, "_run_schedule", _run)

    ack_id = get_ack_ledger().put(
        kind="run_schedule",
        payload={"schedule_id": sched.id, "name": sched.name},
        preview={"name": sched.name},
    )
    first = _confirm(ack_id)
    assert first["ok"] is True, first
    assert first["idempotent"] is False
    assert first["job_id"] == "job-schedule-1"
    assert first["schedule_id"] == sched.id
    assert calls == [(sched.id, True)]

    replay = _confirm(ack_id)
    assert replay["idempotent"] is True
    assert replay["job_id"] == "job-schedule-1"
    assert calls == [(sched.id, True)]


def test_file_ack_outside_the_upload_tree_is_refused_and_stays_spendable(isolated, monkeypatch):
    monkeypatch.setattr(isolated.router, "_caller", _role(isolated.router, "editor", "qe@example.com"))
    import services.dataset_file as dataset_file
    import src.transfer.engine as engine_mod
    from src.ai.copilot.ack_ledger import get_ack_ledger

    upload_root = isolated.tmp / "uploads"
    upload_root.mkdir()
    monkeypatch.setattr(dataset_file, "dataset_roots", lambda: [upload_root])
    monkeypatch.setattr(engine_mod, "get_transfer_engine", lambda: pytest.fail("engine must not run"))

    outside = isolated.tmp / "payments.csv"
    outside.write_text("id\n1\n", encoding="utf-8")
    ack_id = get_ack_ledger().put(
        kind="start_transfer",
        payload={
            "source": {"kind": "file", "format": "csv", "table": "payments"},
            "destination": {"kind": "database", "connector_id": "dest-qe", "table": "pay"},
            "mappings": [{"source": "id", "target": "id"}],
            "source_path": str(outside),
            "source_filename": "payments.csv",
        },
        preview={"file": "payments.csv"},
    )
    body = _confirm(ack_id)
    assert body["ok"] is False
    assert "upload" in body["error"].lower()
    peek = get_ack_ledger().peek(ack_id)
    assert peek is not None
    assert peek["consumed"] is False
