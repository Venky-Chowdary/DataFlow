"""D273: Pinecone on the control plane names the index host; namespaces list; no source role.

QA: create_connector with host api.pinecone.io and a valid key failed
"Pinecone returned 404. Fix host/port/user/password" (describe_index_stats is
an index-host API). A Pinecone connector listed 0 objects although namespace
r10 held 10 vectors, and planning from it failed "Schema introspection not
implemented for pinecone". The stub plays both the control plane and an index.
"""

from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

KEY = "pcsk_qa_key"


class _Pinecone(BaseHTTPRequestHandler):
    index_host = ""

    def log_message(self, *_a):
        pass

    def _send(self, status, payload):
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):  # noqa: N802
        if self.headers.get("Api-Key") != KEY:
            return self._send(401, {"error": {"code": "UNAUTHENTICATED"}})
        path = urlparse(self.path).path
        if path == "/indexes":
            return self._send(200, {"indexes": [
                {"name": "qa-index", "host": self.index_host},
                {"name": "other", "host": "other-abc.svc.aped.pinecone.io"},
            ]})
        if path == "/describe_index_stats":
            return self._send(200, {"dimension": 3, "totalVectorCount": 12,
                                    "namespaces": {"r10": {"vectorCount": 10}, "": {"vectorCount": 2}}})
        return self._send(404, {"error": "not found"})


@pytest.fixture
def stub():
    srv = HTTPServer(("127.0.0.1", 0), _Pinecone)
    _Pinecone.index_host = f"localhost:{srv.server_address[1]}"
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv.server_address[1]
    srv.shutdown()


def test_control_plane_host_names_the_index_host(stub, monkeypatch):
    import connectors.pinecone_writer as pw

    monkeypatch.setattr(pw, "PINECONE_CONTROL_PLANE_HOST", "127.0.0.1")
    ok, msg = pw.test_pinecone(host=f"http://127.0.0.1:{stub}", password=KEY, database="qa-index")
    assert not ok
    assert "404" not in msg
    assert "control plane" in msg
    assert f"index 'qa-index' is at localhost:{stub}" in msg


def test_control_plane_without_an_index_name_lists_the_indexes(stub, monkeypatch):
    import connectors.pinecone_writer as pw

    monkeypatch.setattr(pw, "PINECONE_CONTROL_PLANE_HOST", "127.0.0.1")
    ok, msg = pw.test_pinecone(host=f"http://127.0.0.1:{stub}", api_key=KEY)
    assert not ok
    assert "qa-index" in msg and "other-abc.svc.aped.pinecone.io" in msg


@pytest.fixture
def connectors(stub, tmp_path):
    from services.connector_store import create_connector, delete_connector

    made = {
        "pinecone": create_connector({"name": "r13-d273-pinecone", "type": "pinecone", "host": f"http://localhost:{stub}", "auth_mode": "api_key", "password": KEY}),
        "dest": create_connector({"name": "r13-d273-dst", "type": "sqlite", "database": str(tmp_path / "dst.db")}),
    }
    yield made
    for conn in made.values():
        delete_connector(conn.id)


def test_namespaces_are_the_listed_objects(connectors):
    from src.ai.copilot.schema_tools import list_connector_objects

    res = list_connector_objects(connector_id=connectors["pinecone"].id)
    assert res.success, res.error
    assert "r10" in res.output["objects"], res.output
    assert res.output["object_kinds"]["r10"] == "namespace"


def test_planning_from_pinecone_says_it_is_destination_only(connectors):
    from src.ai.copilot.tools import get_pilot_tools

    res = get_pilot_tools().execute(
        "plan_transfer",
        {
            "source_connector_id": connectors["pinecone"].id,
            "source_table": "r10",
            "dest_connector_id": connectors["dest"].id,
            "dest_table": "vectors",
            "sync_mode": "full_refresh_overwrite",
        },
    )
    assert not res.success
    assert "destination-only" in res.error
    assert "not implemented" not in res.error
