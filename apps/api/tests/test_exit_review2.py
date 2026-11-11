"""Exit rehearsal: regressions from the second adversarial review (review 2) and the web integration.

Each test names what it pins: N9 the definition hash, gap 3 the rehearsals
list and a running summary, N10 masked control totals, N5 / N8 the typed digest
and actionable reasons, N4 the verification lease, gap 1 PII review, and the
Low items.
"""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.test_exit_rehearsal_e2e import _run, _system, world  # noqa: E402,F401


# --------------------------------------------- N9: what the definition hash tracks


def _definition(**over):
    from services.exit_inventory import definition_sha256

    system = {"id": "exs_1", "workspace_id": "ws1", "source": {"connector_id": "c1"},
              "tables": [{"name": "orders", "schema": None, "key_columns": []}], "exit_window_hours": 24,
              "extract_format": "jsonl"}
    system.update(over)
    return definition_sha256(system)


def test_n9_the_extract_format_is_part_of_the_definition(world):  # noqa: F811
    assert _definition() != _definition(extract_format="csv")


def test_n9_an_edit_to_the_connector_behind_the_same_id_changes_the_definition(world, monkeypatch):  # noqa: F811
    from services import schedule_runner

    conn = {"name": "ledger", "type": "postgresql", "host": "db1.internal", "port": 5432, "database": "ledger",
            "username": "svc", "password": "s3cret"}
    monkeypatch.setattr(schedule_runner, "_resolve_connector_for_schedule", lambda _cid, _sched: dict(conn))
    before = _definition()
    conn["password"] = "rotated"  # a secret is never part of the fingerprint
    assert _definition() == before
    conn["host"] = "db2.internal"
    moved = _definition()
    assert moved != before
    conn["database"] = "ledger_v2"
    assert _definition() != moved


def test_n9_changing_the_extract_format_makes_a_proven_system_stale(world):  # noqa: F811
    from services.exit_inventory import get_system, readiness, update

    system = _system(tables=[{"name": "events"}])
    assert _run(system)["verdict"] == "proven"
    assert readiness(get_system(system["id"]))["state"] == "proven"
    update(system["id"], {"extract_format": "csv"}, actor="owner@example.com", expected_version=None)
    ready = readiness(get_system(system["id"]))
    assert ready["stale"] is True and ready["state"] == "not_proven"
    assert "format" in ready["stale_reason"]
