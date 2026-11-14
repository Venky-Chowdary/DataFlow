"""PR 411 review 1, H3: the form routes refuse a filter they cannot read.

``/api/v1/transfer/run`` and ``/api/v1/connectors/transfer`` turned a
``source_filter_json`` that was not valid JSON, a JSON list or a JSON string
into "no filter": every row was copied and the run reported verified, the
audit-4 class (a declared filter silently ignored). Both routes now answer 400
before anything is read.
"""

from __future__ import annotations

import pytest

from tests import r4_route_harness as h

_BAD = {
    "invalid_json": "{column: id, op: eq, value: 1}",
    "json_list": '[{"column": "id", "op": "eq", "value": 1}]',
    "json_string": '"id = 1"',
    "json_number": "1",
    "sql_shape": '{"sql": "id = 1"}',
}


@pytest.mark.parametrize("raw", list(_BAD.values()), ids=list(_BAD))
def test_filter_spec_from_json_refuses(raw):
    from services.row_filter import filter_spec_from_json

    with pytest.raises(ValueError):
        filter_spec_from_json(raw)


def test_filter_spec_from_json_keeps_a_valid_filter_and_no_filter():
    from services.row_filter import filter_spec_from_json

    assert filter_spec_from_json("") == {}
    assert filter_spec_from_json("  ") == {}
    assert filter_spec_from_json('{"column": "id", "op": "eq", "value": 1}') == {
        "column": "id",
        "op": "eq",
        "value": 1,
    }


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    reads: list[str] = []

    import src.transfer.engine as engine_mod

    def _no_job(name):
        def _refuse(self, *a, **k):
            reads.append(name)
            raise AssertionError(f"{name}: a job was started, so the source would be read")

        return _refuse

    for name in ("_create_pending_job", "execute_tracked", "execute"):
        monkeypatch.setattr(engine_mod.UniversalTransferEngine, name, _no_job(name))
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        c.reads = reads  # type: ignore[attr-defined]
        yield c


@pytest.mark.parametrize("raw", list(_BAD.values()), ids=list(_BAD))
def test_transfer_run_form_refuses_unreadable_filter(client, tmp_path, raw):
    data = {
        "source_kind": "file",
        "source_format": "csv",
        "dest_kind": "file",
        "dest_format": "csv",
        "dest_output_path": str(tmp_path / "out.csv"),
        "sync_mode": "full_refresh_overwrite",
        "async_mode": "false",
        "skip_preflight": "true",
        "source_filter_json": raw,
    }
    files = {"file": ("rows.csv", b"id,v\n1,a\n2,b\n3,c\n", "text/csv")}
    resp = client.post("/api/v1/transfer/run", data=data, files=files)
    assert resp.status_code == 400, resp.text
    assert "filter" in resp.text.lower()
    assert client.reads == []


@pytest.mark.parametrize("raw", list(_BAD.values()), ids=list(_BAD))
def test_connectors_transfer_form_refuses_unreadable_filter(client, raw):
    data = {
        "destination_database": "db",
        "destination_collection": "rows",
        "dest_type": "mongodb",
        "async_mode": "false",
        "skip_preflight": "true",
        "source_filter_json": raw,
    }
    files = {"file": ("rows.csv", b"id,v\n1,a\n2,b\n3,c\n", "text/csv")}
    resp = client.post("/api/v1/connectors/transfer", data=data, files=files)
    assert resp.status_code == 400, resp.text
    assert "filter" in resp.text.lower()
    assert client.reads == []
