"""D270: a HubSpot token without the custom-object schema scope still works.

QA: a private app granted contacts/companies/deals read+write passed
test_connector, but list_connector_objects and plan_transfer died on
``403 ... /crm/v3/schemas`` (the custom-object schema API) with 0 objects, and
sample_connector_object ran past the 45 s deadline ("api.hubapi.com:443 did not
answer") although GET /crm/v3/objects/contacts answered in 0.17 s.

The stub below answers exactly as HubSpot did for that token: 403 on
/crm/v3/schemas, 200 on the standard object and property APIs.
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

TOKEN = "pat-na1-0000-qa"
CONTACTS = [
    {"id": "101", "properties": {"email": "a@x.test", "firstname": "Zoë", "lastname": ""}},
    {"id": "102", "properties": {"email": "b@x.test", "firstname": None, "lastname": "  "}},
    {"id": "103", "properties": {"email": "c@x.test", "firstname": "C", "lastname": "Ω"}},
]


class _Log:
    requests: list[tuple[str, dict]] = []


class _HubSpot(BaseHTTPRequestHandler):
    def log_message(self, *_a):
        pass

    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        url = urlparse(self.path)
        query = parse_qs(url.query)
        _Log.requests.append((url.path, query))
        if self.headers.get("Authorization") not in {f"Bearer {TOKEN}", f"Bearer {TOKEN}-revoked-elsewhere"}:
            return self._send(401, {"status": "error", "category": "INVALID_AUTHENTICATION"})
        if url.path == "/crm/v3/schemas":
            if self.headers.get("Authorization") == f"Bearer {TOKEN}-revoked-elsewhere":
                return self._send(403, {"status": "error", "category": "BANNED", "message": "portal suspended"})
            return self._send(
                403,
                {
                    "status": "error",
                    "category": "MISSING_SCOPES",
                    "message": "This app hasn't been granted all required scopes",
                },
            )
        if url.path == "/crm/v3/properties/contacts":
            return self._send(
                200,
                {
                    "results": [
                        {"name": n, "type": "string", "fieldType": "text", "label": n}
                        for n in ("email", "firstname", "lastname")
                    ]
                },
            )
        if url.path == "/crm/v3/objects/contacts":
            limit = int((query.get("limit") or ["100"])[0])
            return self._send(200, {"results": CONTACTS[:limit]})
        return self._send(404, {"status": "error", "message": "not found"})


@pytest.fixture
def hubspot():
    srv = HTTPServer(("127.0.0.1", 0), _HubSpot)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    _Log.requests.clear()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def test_standard_objects_are_listed_and_the_403_says_custom_objects_unavailable(hubspot):
    from services.schema_introspect import introspect_schema

    out = introspect_schema("hubspot", host=hubspot, port=443, password=TOKEN)
    assert out["ok"], out
    assert {"contacts", "companies", "deals"} <= set(out["tables"])
    assert "custom objects unavailable" in out["warning"]
    assert out["custom_objects_unavailable"] is True


def test_a_403_that_is_not_a_scope_answer_still_fails(hubspot):
    from services.schema_introspect import introspect_schema

    out = introspect_schema("hubspot", host=hubspot, port=443, api_key=f"{TOKEN}-revoked-elsewhere")
    assert not out["ok"]
    assert "403" in out["error"]
    assert "custom_objects_unavailable" not in out


def test_a_standard_object_describes_without_the_custom_object_scope(hubspot):
    from services.schema_introspect import introspect_schema

    out = introspect_schema("hubspot", host=hubspot, port=443, api_key=TOKEN, table="contacts")
    assert out["ok"], out
    assert [c["name"] for c in out["columns"]] == ["id", "email", "firstname", "lastname"]


def test_a_bad_token_is_still_an_auth_failure_not_an_empty_custom_list(hubspot):
    from services.schema_introspect import introspect_schema

    out = introspect_schema("hubspot", host=hubspot, port=443, api_key="wrong")
    assert not out["ok"]
    assert "401" in out["error"]


def test_connector_probe_reads_one_standard_object(hubspot):
    from src.transfer.connector_registry import run_probe

    ok, msg = run_probe("hubspot", {"type": "hubspot", "host": hubspot, "port": 443, "password": TOKEN})
    assert ok, msg
    assert _Log.requests[0][0] == "/crm/v3/objects/contacts"
    assert _Log.requests[0][1].get("limit") == ["1"]


@pytest.fixture
def saved_hubspot(hubspot):
    from services.connector_store import create_connector, delete_connector

    conn = create_connector(
        {
            "name": "r13-d270-hubspot",
            "type": "hubspot",
            "host": hubspot,
            "auth_mode": "api_key",
            "password": TOKEN,
        }
    )
    yield conn
    delete_connector(conn.id)


def test_list_connector_objects_lists_standard_objects(saved_hubspot):
    from src.ai.copilot.schema_tools import list_connector_objects

    res = list_connector_objects(connector_id=saved_hubspot.id)
    assert res.success, res.error
    out = res.output
    assert out["connected"] is True
    assert {"contacts", "companies", "deals"} <= set(out["objects"])
    assert "custom objects unavailable" in out["message"]


def test_plan_carries_the_scope_note_as_data(saved_hubspot, tmp_path):
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tools import get_pilot_tools

    dest = create_connector({"name": "r13-d270-dst", "type": "sqlite", "database": str(tmp_path / "d.db")})
    try:
        res = get_pilot_tools().execute(
            "plan_transfer",
            {
                "source_connector_id": saved_hubspot.id,
                "source_table": "contacts",
                "dest_connector_id": dest.id,
                "dest_table": "contacts_copy",
                "sync_mode": "full_refresh_overwrite",
            },
        )
        assert res.success, res.error
        notes = res.output["source_notes"]
        assert notes and notes[0]["code"] == "custom_objects_unavailable"
        assert "crm.schemas.custom.read" in notes[0]["message"]
    finally:
        delete_connector(dest.id)


def test_sample_reads_one_bounded_page_through_the_reader(saved_hubspot):
    from src.ai.copilot.query_tools import sample_connector_object

    res = sample_connector_object(connector_id=saved_hubspot.id, table="contacts", limit=2, analyze=False)
    assert res.success, res.error
    out = res.output
    assert out["column_type_source"] == "reader"
    rows = {r["id"]: r for r in out["rows"]}
    assert set(rows) == {"101", "102"}
    assert rows["101"]["firstname"] == "Zoë"
    assert rows["102"]["firstname"] is None
    # One object page asked for the sample's limit, not a 50,000-row extract.
    object_reads = [q for p, q in _Log.requests if p == "/crm/v3/objects/contacts"]
    assert object_reads and all(int(q["limit"][0]) <= 2 for q in object_reads)
