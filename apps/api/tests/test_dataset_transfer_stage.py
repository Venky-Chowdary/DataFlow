"""An uploaded file stages through the same ack as a connector transfer.

Named fixture: a two-column CSV under a temporary upload root. Destination
introspect and preflight are the boundaries that would dial a warehouse; the
mapping pipeline and the file parse are the real ones.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from types import SimpleNamespace


def _copilot_without_agent_init():
    """Import copilot modules without running the agent package init."""
    import src.ai as ai_pkg

    name = "src.ai.copilot"
    current = sys.modules.get(name)
    if current is not None and getattr(current, "__file__", None):
        return current
    if current is None:
        mod = types.ModuleType(name)
        mod.__path__ = [str(Path(ai_pkg.__file__).resolve().parent / "copilot")]
        mod.__package__ = name
        sys.modules[name] = mod
        setattr(ai_pkg, "copilot", mod)
    else:
        setattr(ai_pkg, "copilot", current)
    return sys.modules[name]


class _Analyst:
    def __init__(self, schema):
        self.schema = schema

    def resolve_dataset(self, hint: str):
        return self.schema


def _cleared() -> dict:
    return {
        "passed": True,
        "run_id": "pf_dataset_stage",
        "proof_bundle": {"transfer_decision": {"decision": "approve"}},
    }


def test_uploaded_csv_stages_a_file_ack_and_does_not_run(tmp_path, monkeypatch) -> None:
    _copilot_without_agent_init()
    import services.dataset_file as dataset_file
    from src.ai.copilot import transfer_tools
    from src.ai.copilot.dataset_transfer import stage_dataset_transfer

    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setattr(dataset_file, "dataset_roots", lambda: [tmp_path])
    csv_path = tmp_path / "payments.csv"
    csv_path.write_text("id,amount\n1,10\n2,20\n", encoding="utf-8")
    schema = SimpleNamespace(
        name="payments",
        source="upload",
        path=str(csv_path),
        file_type="csv",
    )
    import src.ai.copilot.data_analyst as data_analyst

    monkeypatch.setattr(data_analyst, "get_data_analyst", lambda: _Analyst(schema))
    monkeypatch.setattr(
        transfer_tools,
        "_safe_connector",
        lambda *_a, **_k: ({"id": "dest-1", "name": "Warehouse", "type": "postgresql", "schema": "public"}, None),
    )
    monkeypatch.setattr(
        transfer_tools,
        "_introspect",
        lambda *_a, **_k: {
            "ok": True,
            "columns": [
                {"name": "id", "inferred_type": "INTEGER", "nullable": False},
                {"name": "amount", "inferred_type": "INTEGER", "nullable": True},
            ],
            "db_type": "postgresql",
            "table_exists": True,
        },
    )
    monkeypatch.setattr(transfer_tools, "_run_preflight", lambda **_k: _cleared())
    monkeypatch.setattr(transfer_tools, "_stage_bound_contract", lambda *_a, **_k: {})

    result = stage_dataset_transfer(
        dataset_name="payments",
        dest_connector_name="Warehouse",
        dest_table="pay",
    )
    assert result.success, result.error
    assert result.output["requires_confirm"] is True
    assert result.output["ack_id"]
    assert result.output["preview"]["rows"] == 2
    assert result.output["preview"]["destination"] == "Warehouse.pay"

    from src.ai.copilot.ack_ledger import get_ack_ledger
    from services.confirmed_transfer import transfer_request_from_ack

    payload, err = get_ack_ledger().get_pending_payload(result.output["ack_id"])
    assert not err
    assert payload["skip_preflight"] is False
    assert payload["source"]["kind"] == "file"
    request = transfer_request_from_ack(payload)
    assert request.source.kind == "file"
    assert request.source_filename == "payments.csv"
    assert request.destination.connector_id == "dest-1"
    assert request.destination.table == "pay"
    assert request.mappings


def test_a_template_name_is_not_a_file_transfer(monkeypatch) -> None:
    _copilot_without_agent_init()
    from src.ai.copilot.dataset_transfer import stage_dataset_transfer

    schema = SimpleNamespace(name="Financial Services", source="industry", path="", file_type="")
    import src.ai.copilot.data_analyst as data_analyst

    monkeypatch.setattr(data_analyst, "get_data_analyst", lambda: _Analyst(schema))
    result = stage_dataset_transfer(dataset_name="payment", dest_connector_name="Warehouse")
    assert result.success is False
    assert "template" in result.error
