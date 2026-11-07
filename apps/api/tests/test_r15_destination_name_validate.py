"""QA 2026-11-07: Validate passed a destination name Execute then refused.

Studio: Inventory MySQL -> shop_demo."1M Records". Validate (pf_856bb...) passed;
the run failed 3 seconds in: "Destination name '1M Records' is not a plain SQL
identifier. The writer would store it as 'col_1m_records'." Execute (engine,
connectors_router, schedules, Pilot) ran ``unsafe_destination_identifier_reason``;
Validate never did. The same rule now runs as a hard Validate gate.
"""

from __future__ import annotations

import pytest

from tests import r4_route_harness as h

REFUSED = "1M Records"
SUGGESTED = "col_1m_records"


def _engine_sentence(dest_type: str, name: str) -> str | None:
    from connectors.sql_identifiers import unsafe_destination_identifier_reason

    return unsafe_destination_identifier_reason(dest_type, name)


def test_validate_refuses_the_name_execute_refuses_in_the_same_words():
    from services.preflight_transfer_policy import destination_name_gate

    gate = destination_name_gate("mysql", REFUSED)
    assert gate is not None
    assert gate["id"] == "g23_destination_name"
    assert gate["status"] == "block"
    # Same sentence the run printed, so Validate and Execute agree.
    assert gate["message"] == _engine_sentence("mysql", REFUSED)
    assert gate["details"] == {"destination_table": REFUSED, "suggested_name": SUGGESTED}


@pytest.mark.parametrize("dest_type,name", [
    ("mysql", "records_1m"),           # plain
    ("postgresql", "shop_demo.orders"),  # schema-qualified plain
    ("mysql", ""),                     # no name yet
    ("", REFUSED),                     # no destination type yet
    # Document store: its own naming rules. A neutral id, so the runner's
    # live-Mongo skip does not drop a check that needs no database.
    pytest.param("mongodb", REFUSED, id="document_store"),
    ("s3", REFUSED),                   # object key, not a SQL catalog
])
def test_only_a_sql_catalog_name_the_writer_would_rename_is_refused(dest_type, name):
    from services.preflight_transfer_policy import destination_name_gate

    assert destination_name_gate(dest_type, name) is None
    # Execute lets the same name through: the two never disagree.
    assert _engine_sentence(dest_type, name) is None


@pytest.mark.parametrize("kwargs", [
    {"destination_table": REFUSED},                                   # /preflight/run
    {"destination_endpoint": {"format": "mysql", "table": REFUSED}},  # plan preflight
])
def test_policy_gates_carry_the_refusal_from_either_validate_path(kwargs):
    from services.preflight_transfer_policy import run_transfer_policy_gates

    gates = run_transfer_policy_gates(
        dest_type="mysql", source_type="mysql", source_kind="database",
        source_columns=["id"], **kwargs,
    )
    name = [g for g in gates if g["id"] == "g23_destination_name"]
    assert len(name) == 1 and name[0]["status"] == "block", [g["id"] for g in gates]


def test_a_plain_name_adds_no_gate_row():
    from services.preflight_transfer_policy import run_transfer_policy_gates

    gates = run_transfer_policy_gates(
        dest_type="mysql", source_type="mysql", source_kind="database",
        source_columns=["id"], destination_table="records_1m",
    )
    assert not [g for g in gates if g["id"] == "g23_destination_name"]


def test_the_gate_is_hard_and_explained():
    from services.preflight_rules import HARD_GATE_IDS, PREFLIGHT_GATE_RULES

    assert "g23_destination_name" in HARD_GATE_IDS
    rule = PREFLIGHT_GATE_RULES["g23_destination_name"]
    assert rule["category"] == "hard" and rule["fix"] and rule["why"]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def test_studio_validate_endpoint_blocks_the_name_before_any_run(client):
    columns = ["id", "name"]
    body = client.post("/api/v1/preflight/run", json={
        "dataset_name": "records", "columns": columns,
        "column_types": {"id": "integer", "name": "text"},
        "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in columns],
        "source_kind": "file", "source_type": "csv",
        "dest_kind": "database", "dest_type": "mysql", "dest_table": REFUSED,
        "sync_mode": "full_refresh_overwrite", "validation_mode": "strict",
    }).json()
    gates = {g.get("id"): g for g in body.get("gates") or []}
    assert "g23_destination_name" in gates, sorted(gates)
    gate = gates["g23_destination_name"]
    assert gate["status"] == "block"
    assert SUGGESTED in gate["message"], gate["message"]
    blocking = {b.get("id") or b.get("gate_id") for b in body.get("blockers") or []}
    assert "g23_destination_name" in blocking, body.get("blockers")
