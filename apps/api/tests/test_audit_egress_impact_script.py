"""scripts/audit_egress_impact.py lists what the enforcing egress rule would refuse, read-only."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import audit_egress_impact  # noqa: E402


def _connector(name, ctype, workspace="ws-1", **fields):
    return {
        "id": f"id-{name}",
        "name": name,
        "type": ctype,
        "role": "both",
        "workspace_id": workspace,
        **fields,
    }


@pytest.fixture
def store(tmp_path, monkeypatch):
    for var in ("DATAFLOW_EGRESS_ALLOWLIST", "DATAWRAP_CONNECTOR_STORE", "DATAWRAP_CONNECTOR_STORE_BACKEND"):
        monkeypatch.delenv(var, raising=False)
    # The scripts export their store/data-dir choice into os.environ; let
    # monkeypatch restore whatever the process had.
    for var in ("DATAFLOW_CONNECTOR_STORE", "DATAFLOW_CONNECTOR_STORE_BACKEND", "DATAFLOW_DATA_DIR", "MONGODB_URI"):
        monkeypatch.setenv(var, "")  # recorded, so teardown restores the original
        monkeypatch.delenv(var)
    import services.connector_store as connector_store

    monkeypatch.setattr(connector_store, "_backend_choice", None)
    path = tmp_path / "connectors.json"
    path.write_text(
        json.dumps(
            {
                "connectors": [
                    _connector("pg-private", "postgresql", host="10.1.2.3", password="s3cret-pw"),
                    _connector("lab-loopback", "mongodb", connection_string="mongodb://127.0.0.1:27017/x"),
                    _connector("cgnat-api", "rest_api", workspace="ws-2", host="http://100.64.0.9/api"),
                    _connector("meta", "s3", endpoint_url="http://169.254.169.254/latest"),
                    _connector("public-pg", "postgresql", host="8.8.8.8"),
                    _connector("local-file", "sqlite", host="localhost", database="/tmp/x.db"),
                ]
            }
        ),
        encoding="utf-8",
    )
    return path


def test_lists_refused_connectors_with_class_and_count(store, capsys):
    before = store.read_bytes()
    assert audit_egress_impact.main(["--file", str(store)]) == 0
    out = capsys.readouterr().out
    assert "pg-private" in out and "private" in out
    assert "lab-loopback" in out and "loopback" in out
    assert "cgnat-api" in out and "cgnat" in out
    assert "meta" in out and "metadata" in out
    assert "public-pg" not in out and "local-file" not in out
    assert "4 of 6 saved connectors would be refused" in out
    assert "s3cret-pw" not in out and "mongodb://" not in out
    assert store.read_bytes() == before  # read-only


def test_json_output_flags_always_refused_and_filters_workspace(store, capsys):
    assert audit_egress_impact.main(["--file", str(store), "--workspace", "ws-1", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["saved_connectors"] == 5
    assert payload["would_be_refused"] == 3
    by_name = {r["name"]: r for r in payload["rows"]}
    assert by_name["meta"]["refused_regardless_of_flag"] is True
    assert by_name["pg-private"]["refused_regardless_of_flag"] is False
    assert by_name["pg-private"]["field"] == "host"


def test_workspace_allowlist_and_env_allowlist_are_honoured(store, capsys, monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "10.0.0.0/8,127.0.0.0/8")
    assert audit_egress_impact.main(["--file", str(store), "--json"]) == 0
    names = {r["name"] for r in json.loads(capsys.readouterr().out)["rows"]}
    assert names == {"cgnat-api", "meta"}


def test_mongo_url_env_must_be_set(monkeypatch):
    monkeypatch.delenv("NOPE_URL", raising=False)
    with pytest.raises(SystemExit):
        audit_egress_impact.main(["--mongo-url-env", "NOPE_URL"])
