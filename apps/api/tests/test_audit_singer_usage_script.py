"""scripts/audit_singer_usage.py lists Singer connectors and what uses them, read-only."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import audit_singer_usage  # noqa: E402


def _connector(cid, name, ctype, workspace="ws-1", **fields):
    return {"id": cid, "name": name, "type": ctype, "role": "source", "workspace_id": workspace, **fields}


def _schedule(sid, name, source, dest, enabled=True, workspace="ws-1"):
    return {
        "id": sid,
        "name": name,
        "source_connector_id": source,
        "source_table": "t",
        "dest_connector_id": dest,
        "dest_table": "t",
        "interval": "daily",
        "enabled": enabled,
        "workspace_id": workspace,
    }


@pytest.fixture
def stores(tmp_path, monkeypatch):
    for var in ("DATAWRAP_CONNECTOR_STORE", "DATAWRAP_CONNECTOR_STORE_BACKEND", "DATAWRAP_SINGER_TAPS_DIR", "DATAFLOW_SINGER_TAPS_DIR"):
        monkeypatch.delenv(var, raising=False)
    # The scripts export their store/data-dir choice into os.environ; let
    # monkeypatch restore whatever the process had.
    for var in ("DATAFLOW_CONNECTOR_STORE", "DATAFLOW_CONNECTOR_STORE_BACKEND", "DATAFLOW_DATA_DIR", "MONGODB_URI"):
        monkeypatch.setenv(var, "")  # recorded, so teardown restores the original
        monkeypatch.delenv(var)
    import services.connector_store as connector_store
    import services.schedule_store as schedule_store

    monkeypatch.setattr(connector_store, "_backend_choice", None)
    connectors = tmp_path / "connectors.json"
    connectors.write_text(
        json.dumps(
            {
                "connectors": [
                    _connector(
                        "c1",
                        "hubspot-tap",
                        "singer_tap",
                        extra={"tap_command": "tap-hubspot --flag", "tap_config": {"token": "SECRET-TOKEN"}},
                    ),
                    _connector("c2", "bare-tap", "singer_tap", workspace="ws-2"),
                    _connector("c3", "warehouse", "postgresql", host="db.example.com"),
                ]
            }
        ),
        encoding="utf-8",
    )
    schedules = tmp_path / "schedules.json"
    schedules.write_text(
        json.dumps(
            {
                "schedules": [
                    _schedule("s1", "nightly-hubspot", "c1", "c3"),
                    _schedule("s2", "paused-hubspot", "c1", "c3", enabled=False),
                    _schedule("s3", "pg-only", "c3", "c3"),
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(schedule_store, "STORE_PATH", schedules)
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    return connectors, schedules


def test_lists_singer_connectors_with_schedules_and_runnable_state(stores, capsys):
    connectors, schedules = stores
    before = (connectors.read_bytes(), schedules.read_bytes())
    assert audit_singer_usage.main(["--file", str(connectors)]) == 0
    out = capsys.readouterr().out
    assert "hubspot-tap" in out and "bare-tap" in out and "warehouse" not in out
    assert "nightly-hubspot" in out and "paused-hubspot" in out and "pg-only" not in out
    assert "singer_taps_dir_unset" in out
    assert "2 Singer connectors, used by 2 schedules/pipelines" in out
    assert "SECRET-TOKEN" not in out
    assert (connectors.read_bytes(), schedules.read_bytes()) == before


def test_json_reports_tap_state_with_a_taps_dir(stores, capsys, monkeypatch, tmp_path):
    connectors, _ = stores
    taps = tmp_path / "taps"
    taps.mkdir()
    tap = taps / "tap-hubspot"
    tap.write_text("#!/bin/sh\n")
    tap.chmod(0o755)
    monkeypatch.setenv("DATAWRAP_SINGER_TAPS_DIR", str(taps))
    assert audit_singer_usage.main(["--file", str(connectors), "--json"]) == 0
    rows = {r["name"]: r for r in json.loads(capsys.readouterr().out)["rows"]}
    assert rows["hubspot-tap"]["runnable"] == "ok"
    assert rows["hubspot-tap"]["tap"] == "tap-hubspot"
    assert rows["bare-tap"]["runnable"] == "no tap_command recorded"
    assert {u["schedule"] for u in rows["hubspot-tap"]["used_by"]} == {"nightly-hubspot", "paused-hubspot"}


def test_workspace_filter(stores, capsys):
    connectors, _ = stores
    assert audit_singer_usage.main(["--file", str(connectors), "--workspace", "ws-2", "--json"]) == 0
    rows = json.loads(capsys.readouterr().out)["rows"]
    assert [r["name"] for r in rows] == ["bare-tap"]
