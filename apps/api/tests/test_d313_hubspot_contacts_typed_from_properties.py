"""D313: HubSpot contacts are typed from property metadata, and an empty date is NULL.

QA (HubSpot contacts -> new PostgreSQL table, overwrite, balanced): the plan
blocked ``hs_analytics_source_data_2`` / ``hs_object_source_id`` as INTEGER
because most sampled values were digits, then met ``'sample-contact'``; with
CAST_AND_CONTINUE the job quarantined rows whose ``notes_next_activity_date``
was ``''`` ("cannot coerce to datetime").

HubSpot publishes each property's type at ``GET /crm/v3/properties/{object}``
(https://developers.hubspot.com/docs/api/crm/properties): ``string`` /
``enumeration`` are text, ``number`` is a number, ``date`` / ``datetime`` are
dates. The object API returns every value as a JSON string, and a cleared or
unset value as ``""`` or ``null`` (HubSpot clears a property by setting it to
``""``). So the declared type, not the sampled spelling, is the column type, and
``""`` on a typed (non-text) property is "no value": SQL NULL. A text property
keeps ``""`` as the empty string (R2).

The stub answers exactly those two endpoints the way HubSpot does.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

TOKEN = "pat-na1-0000-d313"

PROPERTIES = [
    {"name": "email", "type": "string", "fieldType": "text"},
    # Text properties whose values are mostly digits (HubSpot's own source ids).
    {"name": "hs_object_source_id", "type": "string", "fieldType": "text"},
    {"name": "hs_analytics_source_data_2", "type": "string", "fieldType": "text"},
    {"name": "notes_next_activity_date", "type": "datetime", "fieldType": "date"},
    {"name": "closedate", "type": "date", "fieldType": "date"},
    {"name": "num_notes", "type": "number", "fieldType": "number"},
    {"name": "lead_score", "type": "number", "fieldType": "number"},
    {"name": "hs_is_unworked", "type": "bool", "fieldType": "booleancheckbox"},
]


def _contact(i: int) -> dict:
    props = {
        "email": f"c{i}@example.test",
        "hs_object_source_id": "sample-contact" if i == 7 else str(100000 + i),
        "hs_analytics_source_data_2": "sample-contact" if i == 9 else str(i),
        # HubSpot v3 renders datetimes as ISO-8601 UTC; '' when cleared.
        "notes_next_activity_date": "" if i % 3 == 0 else f"2026-01-{i:02d}T03:04:05.000Z",
        "closedate": "" if i % 4 == 0 else f"2026-02-{(i % 27) + 1:02d}",
        "num_notes": "" if i == 5 else str(i),
        "lead_score": "12.50" if i == 2 else str(i),
        "hs_is_unworked": "" if i == 6 else ("true" if i % 2 else "false"),
    }
    if i == 11:
        props["email"] = ""
    if i == 12:
        props["notes_next_activity_date"] = None
    return {"id": str(500 + i), "properties": props}


CONTACTS = [_contact(i) for i in range(1, 31)]


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
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            return self._send(401, {"status": "error", "category": "INVALID_AUTHENTICATION"})
        if url.path == "/crm/v3/schemas":
            return self._send(200, {"results": []})
        if url.path == "/crm/v3/properties/contacts":
            return self._send(200, {"results": [{**p, "label": p["name"]} for p in PROPERTIES]})
        if url.path == "/crm/v3/objects/contacts":
            limit = int((query.get("limit") or ["100"])[0])
            start = int((query.get("after") or ["0"])[0])
            page = CONTACTS[start:start + limit]
            body: dict = {"results": page}
            if start + limit < len(CONTACTS):
                body["paging"] = {"next": {"after": str(start + limit)}}
            return self._send(200, body)
        return self._send(404, {"status": "error", "message": "not found"})


@pytest.fixture
def hubspot():
    srv = HTTPServer(("127.0.0.1", 0), _HubSpot)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def test_reader_declares_property_types_and_reads_an_empty_typed_value_as_null(hubspot):
    from connectors.hubspot import read_object
    from services.schema_introspect import introspect_schema
    from services.value_serializer import SQL_NULL_SENTINEL as NULL

    batch = read_object(cfg={"host": hubspot, "password": TOKEN, "table": "contacts"}, limit=100)
    declared = dict((batch.meta or {}).get("native_types") or {})
    planned = {
        c["name"]: c["inferred_type"]
        for c in introspect_schema(
            "hubspot", host=hubspot, port=443, api_key=TOKEN, table="contacts"
        )["columns"]
    }
    # Plan (introspection) and run (reader) type every column the same way (R8).
    assert declared == planned
    assert declared["hs_object_source_id"].startswith("VARCHAR")
    assert declared["hs_analytics_source_data_2"].startswith("VARCHAR")
    assert declared["notes_next_activity_date"] == "TIMESTAMPTZ"
    assert declared["closedate"] == "DATE"
    assert declared["hs_is_unworked"] == "BOOLEAN"
    rows = {r[batch.headers.index("id")]: dict(zip(batch.headers, r)) for r in batch.rows}
    assert rows["503"]["notes_next_activity_date"] == NULL
    assert rows["504"]["closedate"] == NULL
    assert rows["505"]["num_notes"] == NULL
    assert rows["506"]["hs_is_unworked"] == NULL
    # Text keeps '' as a value (R2); a null stays NULL (the reader's wire
    # spells SQL NULL as the sentinel).
    assert rows["511"]["email"] == ""
    assert rows["512"]["notes_next_activity_date"] == NULL
    assert rows["507"]["hs_object_source_id"] == "sample-contact"


def test_introspection_types_text_properties_as_text(hubspot):
    from services.schema_introspect import introspect_schema

    out = introspect_schema("hubspot", host=hubspot, port=443, api_key=TOKEN, table="contacts")
    assert out["ok"], out
    types = {c["name"]: c["inferred_type"] for c in out["columns"]}
    assert types["hs_object_source_id"].startswith("VARCHAR")
    assert types["notes_next_activity_date"] == "TIMESTAMPTZ"


@pytest.fixture
def route(hubspot):
    psycopg2 = pytest.importorskip("psycopg2")
    from conftest import pg_test_cfg
    from services.connector_store import create_connector, delete_connector

    cfg = pg_test_cfg()
    try:
        db = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    db.autocommit = True
    sfx = uuid.uuid4().hex[:8]
    src = create_connector({
        "name": f"d313-hs-{sfx}", "type": "hubspot", "host": hubspot,
        "auth_mode": "api_key", "password": TOKEN,
    })
    dst = create_connector({"name": f"d313-pg-{sfx}", "type": "postgresql", "schema": "public", **cfg})
    table = f"d313_contacts_{sfx}"
    try:
        yield {"src": src.id, "dst": dst.id, "table": table, "db": db}
    finally:
        delete_connector(src.id)
        delete_connector(dst.id)
        with db.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{table}")
        db.close()


def _job(ack_id: str) -> dict:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import get_pilot_tools

    job_id = spend_pilot_ack(ack_id, "d313-test")["job_id"]
    deadline = time.time() + 180
    job: dict = {}
    while time.time() < deadline:
        job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    return job


def test_hubspot_contacts_land_typed_with_empty_dates_as_null(route):
    from src.ai.copilot.tools import get_pilot_tools

    args = {
        "source_connector_id": route["src"], "source_table": "contacts",
        "dest_connector_id": route["dst"], "dest_table": route["table"],
        "sync_mode": "full_refresh_overwrite", "validation_mode": "balanced",
    }
    plan = get_pilot_tools().execute("plan_transfer", args)
    assert plan.success, plan.error
    staged = get_pilot_tools().execute("start_transfer", args)
    assert staged.success, staged.error
    job = _job(staged.output["ack_id"])
    assert job.get("status") == "completed", job
    assert int(job.get("records_quarantined") or job.get("rows_quarantined") or 0) == 0, job

    with route["db"].cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (route["table"],),
        )
        types = dict(cur.fetchall())
        assert types["hs_object_source_id"] in {"text", "character varying"}
        assert types["hs_analytics_source_data_2"] in {"text", "character varying"}
        assert types["notes_next_activity_date"] == "timestamp with time zone"
        assert types["closedate"] == "date"
        cur.execute(
            f'SELECT "id", "hs_object_source_id", "notes_next_activity_date", "closedate", '
            f'"email" FROM public.{route["table"]}'
        )
        rows = {str(r[0]): r[1:] for r in cur.fetchall()}
    assert len(rows) == 30
    assert rows["507"][0] == "sample-contact"
    assert rows["503"][1] is None  # '' datetime -> NULL
    assert rows["504"][2] is None  # '' date -> NULL
    assert rows["501"][1] is not None and rows["501"][1].isoformat().startswith("2026-01-01T03:04:05")
    assert rows["511"][3] == ""  # text '' stays the empty string
