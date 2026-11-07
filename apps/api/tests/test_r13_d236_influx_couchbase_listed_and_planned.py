"""D236: InfluxDB and Couchbase, advertised as live sources, list and plan.

86f266ce9 / 308e803b5 taught services.schema_introspect to introspect both,
but endpoint_intelligence kept its own dispatch and still answered
"Introspection for `influxdb` not yet implemented": list_connector_objects
showed 0 objects and start_transfer's compatibility read had no source
schema; the plan's sample went through the SQL query console and never
returned. Real HTTP stubs answer exactly what InfluxDB 1.8 and the Couchbase
query service answer.
"""

from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

_INFLUX = {
    "SHOW MEASUREMENTS": {"results": [{"series": [{"name": "measurements", "columns": ["name"], "values": [["qaenv_seed"]]}]}]},
    'SHOW FIELD KEYS FROM "qaenv_seed"': {"results": [{"series": [{"name": "qaenv_seed", "columns": ["fieldKey", "fieldType"], "values": [["amount", "float"], ["id", "integer"], ["name", "string"]]}]}]},
    'SHOW TAG KEYS FROM "qaenv_seed"': {"results": [{"series": [{"name": "qaenv_seed", "columns": ["tagKey"], "values": [["host"]]}]}]},
}


def _points():
    names = ["Zoë", "", "  ", None, "Ω"]
    return {"results": [{"series": [{
        "name": "qaenv_seed", "tags": {"host": "h1"},
        "columns": ["time", "amount", "id", "name"],
        "values": [[f"2024-03-10T02:30:{i:02d}Z", i + 0.25, i, names[i - 1]] for i in range(1, 6)],
    }]}]}


def _docs():
    return {"results": [
        {"qaenv_seed": {"id": 1, "name": "Zoë", "amount": 1.25}},
        {"qaenv_seed": {"id": 2, "name": "", "amount": 2.5}},
        {"qaenv_seed": {"id": 3, "amount": None}},
    ]}


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *_a):
        pass

    def _send(self, payload):
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 — InfluxDB /query
        q = parse_qs(urlparse(self.path).query).get("q", [""])[0]
        self._send(_INFLUX.get(q) or _points())

    def do_POST(self):  # noqa: N802 — Couchbase /query/service
        n = int(self.headers.get("Content-Length") or 0)
        statement = json.loads(self.rfile.read(n) or b"{}").get("statement", "")
        if "system:keyspaces" in statement:
            return self._send({"results": ["qaenv_seed"]})
        return self._send(_docs())


@pytest.fixture
def stub():
    srv = HTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv.server_address[1]
    srv.shutdown()


@pytest.fixture
def connectors(stub, tmp_path):
    from services.connector_store import create_connector, delete_connector

    made = {
        "influxdb": create_connector({"name": "r13-d236-influx", "type": "influxdb", "host": "127.0.0.1", "port": stub, "database": "qaenv", "ssl": False}),
        "couchbase": create_connector({"name": "r13-d236-couchbase", "type": "couchbase", "host": "127.0.0.1", "port": stub, "ssl": False}),
        "dest": create_connector({"name": "r13-d236-dst", "type": "sqlite", "database": str(tmp_path / "dst.db")}),
    }
    yield made
    for conn in made.values():
        delete_connector(conn.id)


@pytest.mark.parametrize("kind", ["influxdb", "couchbase"])
def test_list_connector_objects_lists_the_source(connectors, kind):
    from src.ai.copilot.schema_tools import list_connector_objects

    res = list_connector_objects(connector_id=connectors[kind].id)
    assert res.success, res.error
    assert res.output["objects"] == ["qaenv_seed"], res.output
    assert res.output["connected"] is True
    assert "not yet implemented" not in res.output["message"]


@pytest.mark.parametrize("kind", ["influxdb", "couchbase"])
def test_start_transfer_compatibility_read_has_the_source_schema(connectors, kind):
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    info = introspect_endpoint(
        EndpointConfig(kind="database", format=kind, connector_id=connectors[kind].id, table="qaenv_seed")
    )
    assert info["table_exists"] is True, info
    assert {"id", "name", "amount"} <= set(info["columns"]), info


@pytest.mark.parametrize("kind", ["influxdb", "couchbase"])
def test_plan_transfer_reads_and_samples_the_source(connectors, kind):
    from src.ai.copilot.tools import get_pilot_tools

    res = get_pilot_tools().execute(
        "plan_transfer",
        {
            "source_connector_id": connectors[kind].id,
            "source_table": "qaenv_seed",
            "dest_connector_id": connectors["dest"].id,
            "dest_table": f"seed_{kind}",
            "sync_mode": "full_refresh_overwrite",
        },
    )
    assert "not implemented" not in str(res.error)
    assert "did not answer" not in str(res.error)
    assert res.success, res.error
    mapped = {m.get("source") for m in res.output.get("mappings") or []}
    assert {"id", "name", "amount"} <= mapped
