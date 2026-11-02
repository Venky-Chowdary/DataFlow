"""QA MXD13 / MXD02 — catalog-live generic_sql / rest_api connectors did not work.

``test_connector`` answered "Unsupported connector type 'genericsql' — no
connectivity probe or transfer driver is registered for it", introspection
said "Introspection for `restapi` not yet implemented", and a rest_api source
failed with "Schema introspection not implemented for restapi". The Pilot
connector-type normaliser collapsed ``generic_sql`` -> ``genericsql`` and
``rest_api`` -> ``restapi``, keys no registry knows; and destination/object
introspection had no rest_api branch at all.
"""

from __future__ import annotations

import http.server
import json
import sqlite3
import threading

import pytest

from src.transfer.models import EndpointConfig


@pytest.fixture()
def sqlite_url(tmp_path):
    path = tmp_path / "g.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'a')")
    return f"sqlite:///{path}"


@pytest.fixture()
def rest_host():
    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = json.dumps([{"id": 1, "title": "a"}, {"id": 2, "title": "b"}]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_a):
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{srv.server_address[1]}"
    finally:
        srv.shutdown()


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("generic_sql", "generic_sql"),
        ("Generic SQL", "generic_sql"),
        ("rest_api", "rest_api"),
        ("REST API", "rest_api"),
        ("sql server", "sqlserver"),
        ("postgres", "postgresql"),
        ("google_sheets", "google_sheets"),
        ("mysql", "mysql"),
    ],
)
def test_pilot_type_normaliser_keeps_registered_driver_ids(raw, expected):
    from src.ai.copilot.connector_create import normalize_connector_type

    assert normalize_connector_type(raw) == expected


def test_create_connector_test_first_probes_generic_sql(sqlite_url):
    from src.ai.copilot.connector_create import build_connector_draft
    from src.transfer.connector_registry import run_probe

    draft = build_connector_draft("", {"type": "generic_sql", "connection_string": sqlite_url})
    assert draft["type"] == "generic_sql"
    ok, msg = run_probe(draft["type"], {"connection_string": sqlite_url})
    assert ok, msg


@pytest.mark.parametrize("legacy, driver", [("genericsql", "generic_sql"), ("restapi", "rest_api")])
def test_rows_saved_with_the_collapsed_type_still_resolve(legacy, driver):
    from src.transfer.connector_capabilities import resolve_driver_type

    assert resolve_driver_type(legacy) == driver


def test_saved_genericsql_row_probes(sqlite_url):
    from src.transfer.connector_registry import run_probe

    ok, msg = run_probe("genericsql", {"connection_string": sqlite_url})
    assert ok, msg


def test_rest_api_object_introspection_describes_the_resource(rest_host):
    from src.transfer.endpoint_intelligence import introspect_endpoint

    out = introspect_endpoint(
        EndpointConfig(kind="database", format="rest_api", host=rest_host, table="posts")
    )
    assert out.get("connected") is True, out.get("message")
    assert "not yet implemented" not in str(out.get("message"))
    assert out.get("columns") == ["id", "title"]


def test_rest_api_source_reads_rows(rest_host):
    from src.transfer.adapters import read_source_database

    records, headers, _schema = read_source_database(
        EndpointConfig(kind="database", format="restapi", host=rest_host, table="posts")
    )
    assert len(records) == 2 and headers == ["id", "title"]


def test_generic_sql_object_list_and_read(sqlite_url):
    from src.transfer.adapters import read_source_database
    from src.transfer.endpoint_intelligence import introspect_endpoint

    listed = introspect_endpoint(
        EndpointConfig(kind="database", format="genericsql", connection_string=sqlite_url)
    )
    assert listed.get("connected") is True, listed.get("message")
    assert "t" in [o["name"] for o in listed.get("objects") or []]
    records, headers, _ = read_source_database(
        EndpointConfig(kind="database", format="generic_sql", connection_string=sqlite_url, table="t")
    )
    assert len(records) == 1 and headers == ["id", "name"]


def test_rest_api_catalog_declares_the_introspection_it_now_has():
    from src.transfer.connector_capabilities import get_capabilities

    caps = get_capabilities("rest_api")
    assert caps["introspect"] is True and caps["read"] is True and caps["write"] is False


def test_saved_genericsql_destination_writes(sqlite_url):
    from src.transfer.adapters import write_destination_database

    dest = EndpointConfig(
        kind="database", format="genericsql", connection_string=sqlite_url, table="g_dest"
    )
    written, _errors, _summary = write_destination_database(
        dest,
        [{"id": "1"}],
        ["id"],
        {"id": "integer"},
        [{"source": "id", "target": "id"}],
    )
    assert written == 1
    path = sqlite_url.removeprefix("sqlite:///")
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM g_dest").fetchone()[0] == 1
