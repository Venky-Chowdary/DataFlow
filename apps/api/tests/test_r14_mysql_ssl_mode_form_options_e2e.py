"""MySQL D10 (third review): a saved "disable" next to the form's CA is refused at save.

Review: ``_refuse_unhonourable_ssl_mode`` never saw the form's TLS options.
``/connectors/test`` checked ``request.connection_string`` only, and a PATCH to
``ssl_mode: "disable"`` on a connector whose saved ``extra`` holds ``ssl_ca``
was checked against the patch alone. Both saved, and every later dial failed,
with a message blaming "ssl-mode=VERIFY_CA in the connection string" when the
connection string had no ssl-mode at all.

Now the check reads the TLS options a dial will read (``extra`` on /test, the
merged saved ``extra`` on an update). When the conflict comes from the
options rather than the DSN, the message names the options. Live MySQL 8
(:3306, read-only: no server variable is changed).
"""

from __future__ import annotations

import os
import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r14_mysql_latin1_cp1252_strict_e2e import _cfg


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def _ca_folder_is_a_connector_area(monkeypatch):
    """The CA file lives where the operator puts it, so list its folder as the operator would.

    Connectors may only open TLS files inside connector areas (connectors/tls_file_options.py),
    and CI writes the MySQL CA under $RUNNER_TEMP. An operator lists such a folder in
    DATAFLOW_SOURCE_PATH_ROOTS; without it these tests saw the (correct) refusal instead of the
    ssl_mode check they pin.
    """
    ca = os.environ.get("DATAFLOW_TEST_MYSQL_CA", "")
    if ca:
        roots = [r for r in os.environ.get("DATAFLOW_SOURCE_PATH_ROOTS", "").split(os.pathsep) if r]
        monkeypatch.setenv("DATAFLOW_SOURCE_PATH_ROOTS", os.pathsep.join([*roots, os.path.dirname(ca)]))


def _ca() -> str:
    ca = os.environ.get("DATAFLOW_TEST_MYSQL_CA", "")
    if not ca or not os.path.exists(ca):
        pytest.skip("needs DATAFLOW_TEST_MYSQL_CA (the :3306 server's CA certificate)")
    return ca


def _body(**over) -> dict:
    cfg = _cfg("mysql")
    return {"name": f"r14-tlsopt-{uuid.uuid4().hex[:6]}", "type": "mysql", "role": "source",
            "host": cfg["host"], "port": cfg["port"], "database": cfg["database"],
            "username": cfg["username"], "password": cfg["password"], "ssl": False, **over}


def test_test_route_refuses_disable_next_to_a_form_ca_naming_the_option(client):
    body = _body(ssl_mode="disable", extra={"ssl_ca": _ca()})
    resp = client.post("/api/v1/connectors/test", json=body)
    assert resp.status_code == 422, (resp.status_code, resp.text[:300])
    detail = str(resp.json().get("detail"))
    assert "ssl_ca" in detail and "connection string" not in detail, detail


def test_save_refuses_disable_next_to_a_form_ca_naming_the_option(client):
    resp = client.post("/api/v1/connectors/saved",
                       json=_body(ssl_mode="disable", extra={"ssl_ca": _ca()}))
    assert resp.status_code == 422, resp.text[:300]
    detail = str(resp.json().get("detail"))
    assert "ssl_ca" in detail and "connection string" not in detail, detail


def test_patch_to_disable_is_checked_against_the_saved_ca(client):
    ca = _ca()
    resp = client.post("/api/v1/connectors/saved", json=_body(ssl_mode="require",
                                                              extra={"ssl_ca": ca}))
    assert resp.status_code == 200, resp.text[:300]
    cid = resp.json()["id"]
    # The saved connector dials with TLS against its CA.
    probe = client.post(f"/api/v1/connectors/saved/{cid}/test").json()
    assert probe.get("success") is True, probe
    resp = client.patch(f"/api/v1/connectors/saved/{cid}", json={"ssl_mode": "disable"})
    assert resp.status_code == 422, (resp.status_code, resp.text[:300])
    assert "ssl_ca" in str(resp.json().get("detail")), resp.text[:300]
    # Nothing was written: it still dials.
    probe = client.post(f"/api/v1/connectors/saved/{cid}/test").json()
    assert probe.get("success") is True, probe


def test_a_dsn_conflict_still_names_the_connection_string():
    from services.connector_store import dial_connection_string

    with pytest.raises(ValueError, match="connection string"):
        dial_connection_string("mysql", "disable", "?ssl-mode=REQUIRED")
    with pytest.raises(ValueError, match="ssl_cert"):
        dial_connection_string("mysql", "disable", "", {"ssl_cert": "/c.pem", "ssl_key": "/k.pem"})
