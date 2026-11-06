"""D272: Stripe metadata values are strings, an absent key is NULL, and the catalog lists.

QA: a customer with no ``metadata.tier`` was quarantined on every destination
("Empty value cannot coerce to integer"): samples like "3"/"12" typed the
column INTEGER and the absent key read as ''. list_connector_objects said
"Introspection for `stripe` not yet implemented" (0 objects) although
plan/transfer of customers worked. The stub answers like api.stripe.com.
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

CUSTOMERS = [
    {"id": "cus_A", "object": "customer", "created": 1700000001, "email": "a@x.test", "name": "Zoë", "livemode": False, "metadata": {"tier": "3", "ref": "007"}},
    {"id": "cus_VYkEk22G8f6xZA", "object": "customer", "created": 1700000002, "email": "b@x.test", "name": "", "livemode": False, "metadata": {}},
    {"id": "cus_C", "object": "customer", "created": 1700000003, "email": "c@x.test", "name": None, "livemode": False, "metadata": {"tier": "12", "ref": "  "}},
]


class _Stripe(BaseHTTPRequestHandler):
    def log_message(self, *_a):
        pass

    def do_GET(self):  # noqa: N802
        path = urlparse(self.path).path
        if self.headers.get("Authorization") != "Bearer sk_test_qa":
            status, body = 401, {"error": {"type": "invalid_request_error"}}
        elif path == "/v1/customers":
            status, body = 200, {"object": "list", "data": CUSTOMERS, "has_more": False}
        elif path == "/v1/account":
            status, body = 200, {"id": "acct_qa"}
        else:
            status, body = 200, {"object": "list", "data": [], "has_more": False}
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


@pytest.fixture
def stripe_host():
    srv = HTTPServer(("127.0.0.1", 0), _Stripe)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def test_an_absent_metadata_key_reads_as_null_not_empty(stripe_host):
    from connectors.stripe import read_object
    from services.value_serializer import SQL_NULL_SENTINEL

    batch = read_object(cfg={"host": stripe_host, "password": "sk_test_qa"}, object="customers", limit=10)
    rows = [dict(zip(batch.headers, r)) for r in batch.rows]
    by_id = {r["id"]: r for r in rows}
    assert by_id["cus_A"]["metadata.tier"] == "3"
    assert by_id["cus_A"]["metadata.ref"] == "007"
    assert by_id["cus_VYkEk22G8f6xZA"]["metadata.tier"] == SQL_NULL_SENTINEL
    assert by_id["cus_C"]["metadata.ref"] == "  "
    assert by_id["cus_C"]["name"] == SQL_NULL_SENTINEL
    assert by_id["cus_VYkEk22G8f6xZA"]["name"] == ""
    assert batch.meta["native_types"]["metadata.tier"] == "string"


def test_a_profile_cannot_retype_a_declared_json_string():
    from services.schema_inference import honor_declared_strings

    out = honor_declared_strings(
        {"metadata.tier": "INTEGER", "created": "INTEGER"},
        {"metadata.tier": "VARCHAR(500)", "created": "INTEGER"},
        {"metadata.tier": ["3", "12"], "created": ["1700000001"]},
        "stripe",
    )
    assert out == {"metadata.tier": "VARCHAR(500)", "created": "INTEGER"}


@pytest.fixture
def connectors(stripe_host, tmp_path):
    from services.connector_store import create_connector, delete_connector

    made = {
        "stripe": create_connector({"name": "r13-d272-stripe", "type": "stripe", "host": stripe_host, "auth_mode": "api_key", "password": "sk_test_qa"}),
        "dest": create_connector({"name": "r13-d272-dst", "type": "sqlite", "database": str(tmp_path / "dst.db")}),
    }
    yield made
    for conn in made.values():
        delete_connector(conn.id)


def test_list_connector_objects_lists_the_stripe_catalog(connectors):
    from src.ai.copilot.schema_tools import list_connector_objects

    res = list_connector_objects(connector_id=connectors["stripe"].id)
    assert res.success, res.error
    assert {"customers", "invoices", "charges", "products"} <= set(res.output["objects"])
    assert res.output["connected"] is True


def test_plan_maps_metadata_as_text(connectors):
    from src.ai.copilot.tools import get_pilot_tools

    res = get_pilot_tools().execute(
        "plan_transfer",
        {
            "source_connector_id": connectors["stripe"].id,
            "source_table": "customers",
            "dest_connector_id": connectors["dest"].id,
            "dest_table": "cust",
            "sync_mode": "full_refresh_overwrite",
        },
    )
    assert res.success, res.error
    engine = {m["source"]: m for m in res.output["engine_mappings"]}
    for key in ("metadata.tier", "metadata.ref"):
        target = str(engine[key].get("target_type") or "").upper()
        assert target.startswith(("VARCHAR", "TEXT")), (key, engine[key])
