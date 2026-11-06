"""D239 (remaining): every surface shows the port a URL-form host dials.

f589e861b fixed the draft, the store and the brief's effective_port, but
list_connectors still had no port (QA read "port None"), and the brief told
the operator a URL-form host was "from the connection string".
"""

from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    "ctype,host,port",
    [
        ("influxdb", "http://127.0.0.1:18086", 18086),
        ("dynamodb", "http://dynamo.qa.test:8000", 8000),
        ("s3", "http://moto.qa.test:4566", 4566),
        ("gcs", "http://fake-gcs.qa.test:4443", 4443),
    ],
)
def test_list_connectors_shows_the_url_port(ctype, host, port):
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tools import get_pilot_tools

    conn = create_connector({"name": f"r13-d239-{ctype}", "type": ctype, "host": host, "port": 443})
    try:
        res = get_pilot_tools().execute("list_connectors", {})
        assert res.success, res.error
        row = next(c for c in res.output["connectors"] if c["id"] == conn.id)
        assert row["port"] == port
    finally:
        delete_connector(conn.id)


def test_list_connectors_shows_the_driver_port_for_a_bare_host():
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tools import get_pilot_tools

    conn = create_connector({"name": "r13-d239-bare", "type": "influxdb", "host": "influx.qa.test"})
    try:
        row = next(c for c in get_pilot_tools().execute("list_connectors", {}).output["connectors"] if c["id"] == conn.id)
        assert row["port"] == 8086
    finally:
        delete_connector(conn.id)


def test_brief_does_not_call_a_url_host_a_connection_string():
    from src.ai.copilot.lifecycle_tools import _connector_brief

    brief = _connector_brief({"type": "s3", "host": "http://moto.qa.test:4566", "port": 443})
    assert (brief["effective_host"], brief["effective_port"]) == ("moto.qa.test", 4566)
    assert "host_note" not in brief
    via_dsn = _connector_brief(
        {"type": "postgresql", "host": "localhost", "port": 5432, "connection_string": "postgresql://u:p@db.qa.test:6543/x"}
    )
    assert (via_dsn["effective_host"], via_dsn["effective_port"]) == ("db.qa.test", 6543)
    assert "connection string" in via_dsn["host_note"]
