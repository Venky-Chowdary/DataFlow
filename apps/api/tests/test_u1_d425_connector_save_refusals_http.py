"""U1 / D425 / D276: every route that saves a connector refuses a server-side
file read (libpq ``service=``, ODBC ``Driver=<path>`` / ``DSN=`` / ``FILEDSN=``)
and a hosted SQLite connector with a plain 4xx, and saves nothing.

Build 70cb4669cc05: ``POST /api/v1/connectors/saved`` re-raised the store's
refusal ``ValueError`` as an unhandled 500 ("An unexpected error occurred"),
and the legacy ``POST /api/v1/connectors/`` logged it as "Exception suppressed"
and then saved the connector to MongoDB, skipping the save-time refusal.

These drive the HTTP routes a user drives (TestClient on the real app), with
the connector file store and an in-memory MongoDB isolated to ``tmp_path``,
and assert both the response and that no connector row exists afterwards in
either store. Hosts are reserved example names; no network is opened.
"""

from __future__ import annotations

import pytest

#: (connection_string, extra, the words the refusal must contain)
REFUSED = [
    ("postgresql://app@db.example.com:5432/app?service=prod", {}, "service"),
    ("host=db.example.com dbname=app service=prod", {}, "service"),
    ("Driver=/tmp/evil.so;Server=db.example.com;Database=app", {}, "Driver"),
    ("DSN=prod", {}, "DSN"),
    ("FILEDSN=/tmp/prod.dsn;", {}, "FILEDSN"),
    ("", {"service": "prod"}, "service"),
    ("", {"odbc_connect": "DSN=prod"}, "DSN"),
]

_HOSTED_ENV = ("RAILWAY_DEPLOYMENT_ID", "RAILWAY_ENVIRONMENT_NAME", "RAILWAY_REPLICA_ID")
_SQLITE_FLAGS = (
    "DATAWRAP_SQLITE_DESTINATION",
    "DATAFLOW_SQLITE_DESTINATION",
    "DATAWRAP_LOCAL_FILE_DESTINATIONS",
    "DATAFLOW_LOCAL_FILE_DESTINATIONS",
)


@pytest.fixture()
def client(tmp_path, monkeypatch):
    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    for key in (*_HOSTED_ENV, *_SQLITE_FLAGS):
        monkeypatch.delenv(key, raising=False)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


def _hosted(monkeypatch) -> None:
    monkeypatch.setenv("ENV", "test")
    monkeypatch.setenv("RAILWAY_DEPLOYMENT_ID", "t9-deploy")
    monkeypatch.setenv("RAILWAY_ENVIRONMENT_NAME", "production")


def _saved_rows() -> list:
    """Every connector row in the file store and in the (in-memory) MongoDB."""
    import services.mongodb_service as mongo_mod
    from services.connector_store import list_connectors

    rows = list(list_connectors())
    rows += list(mongo_mod.get_mongodb_service().list_connectors() or [])
    return rows


def _detail(response) -> str:
    body = response.json()
    if isinstance(body, dict):
        err = body.get("error")
        parts = [str(body.get("detail") or "")]
        if isinstance(err, dict):
            parts += [str(err.get("message") or ""), str(err.get("detail") or "")]
        return " ".join(parts)
    return str(body)


def _assert_refused(response, words: str) -> None:
    assert 400 <= response.status_code < 500, (response.status_code, response.text)
    text = _detail(response)
    assert words in text, text
    assert "unexpected error" not in text.lower(), text


def _pg_body(name: str, connection_string: str, extra: dict) -> dict:
    return {
        "name": name,
        "type": "postgresql",
        "role": "source",
        "host": "db.example.com",
        "port": 5432,
        "database": "app",
        "username": "app",
        "password": "dummy-password",
        "connection_string": connection_string,
        "extra": extra,
    }


# ------------------------------------------------------------------ create
#
# Controls: for these forms the egress guard (connector_save_refusal) already
# answered 400 on build 70cb for create, PUT and PATCH, before the store was
# reached, so they pass there too. They pin every save route to the refusal;
# the store-only refusals further down are the ones 70cb got wrong.


@pytest.mark.parametrize("conn_str,extra,words", REFUSED)
def test_saved_create_refuses_server_file_read_with_4xx_and_saves_nothing(client, conn_str, extra, words):
    r = client.post("/api/v1/connectors/saved", json=_pg_body("u1-saved", conn_str, extra))
    _assert_refused(r, words)
    assert _saved_rows() == []


@pytest.mark.parametrize("conn_str,extra,words", REFUSED[:5])
def test_legacy_create_refuses_server_file_read_and_saves_nothing(client, conn_str, extra, words):
    body = _pg_body("u1-legacy", conn_str, extra)
    body.pop("extra")
    r = client.post("/api/v1/connectors/", json=body)
    _assert_refused(r, words)
    assert _saved_rows() == []


def test_legacy_and_saved_create_give_the_same_refusal_text(client):
    conn_str = REFUSED[0][0]
    a = client.post("/api/v1/connectors/saved", json=_pg_body("u1-a", conn_str, {}))
    body = _pg_body("u1-b", conn_str, {})
    body.pop("extra")
    b = client.post("/api/v1/connectors/", json=body)
    assert a.status_code == b.status_code and 400 <= a.status_code < 500, (a.text, b.text)
    assert a.json()["detail"] == b.json()["detail"]


def test_pilot_confirm_create_names_the_refusal_and_saves_nothing(client):
    from src.ai.copilot.ack_ledger import get_ack_ledger

    ack_id = get_ack_ledger().put(
        kind="create_connector",
        payload=_pg_body("u1-pilot", REFUSED[0][0], {}),
    )
    r = client.post("/api/v1/copilot/confirm", json={"ack_id": ack_id})
    _assert_refused(r, "service")
    assert _saved_rows() == []


# ------------------------------------------------------------------ update


def _saved_good(client) -> str:
    r = client.post("/api/v1/connectors/saved", json=_pg_body("u1-good", "", {}))
    assert r.status_code == 200, r.text
    return r.json()["id"]


@pytest.mark.parametrize("conn_str,extra,words", REFUSED)
def test_saved_put_refuses_server_file_read_and_keeps_the_saved_settings(client, conn_str, extra, words):
    from services.connector_store import get_connector

    cid = _saved_good(client)
    r = client.put(f"/api/v1/connectors/saved/{cid}", json=_pg_body("u1-good", conn_str, extra))
    _assert_refused(r, words)
    after = get_connector(cid)
    assert after is not None and not after.connection_string and not after.extra


@pytest.mark.parametrize("conn_str,extra,words", REFUSED)
def test_saved_patch_refuses_server_file_read_and_keeps_the_saved_settings(client, conn_str, extra, words):
    from services.connector_store import get_connector

    cid = _saved_good(client)
    patch = {"connection_string": conn_str} if conn_str else {"extra": extra}
    r = client.patch(f"/api/v1/connectors/saved/{cid}", json=patch)
    _assert_refused(r, words)
    after = get_connector(cid)
    assert after is not None and not after.connection_string and not after.extra


def test_rotate_secrets_refuses_a_server_file_read_connection_string(client):
    from services.connector_store import get_connector

    cid = _saved_good(client)
    r = client.post(
        f"/api/v1/connectors/saved/{cid}/rotate-secrets",
        json={"connection_string": REFUSED[0][0]},
    )
    _assert_refused(r, "service")
    assert not get_connector(cid).connection_string


def test_pilot_confirm_update_names_the_refusal_and_keeps_the_saved_settings(client):
    from services.connector_store import get_connector
    from src.ai.copilot.ack_ledger import get_ack_ledger

    cid = _saved_good(client)
    ack_id = get_ack_ledger().put(
        kind="update_connector",
        payload={"connector_id": cid, "name": "u1-good",
                 "changes": {"connection_string": REFUSED[1][0]}},
    )
    r = client.post("/api/v1/copilot/confirm", json={"ack_id": ack_id})
    _assert_refused(r, "service")
    assert not get_connector(cid).connection_string


# ------------------------------------------------------------- allowed (AC-2)


def test_a_password_containing_service_equals_is_still_saved(client):
    body = _pg_body("u1-pw", "", {})
    body["password"] = "dummy service=prod"
    r = client.post("/api/v1/connectors/saved", json=body)
    assert r.status_code == 200, r.text
    assert len(_saved_rows()) == 1


# ---------------------------------------------------- hosted SQLite (D425/D276)


SQLITE_VARIANTS = [
    {"database": "/tmp/qa_r63.db", "host": "", "port": 0, "username": "", "password": ""},
    {"database": "/home/qa/data/orders.sqlite"},
    {"database": "C:\\Users\\qa\\orders.db", "host": ""},
    {"database": "./relative/orders.db", "port": 0},
    {"database": "/tmp/with space/ünïcode.db"},
]


@pytest.mark.parametrize("variant", SQLITE_VARIANTS)
def test_hosted_sqlite_create_is_a_4xx_with_the_reason_never_500(client, monkeypatch, variant):
    _hosted(monkeypatch)
    body = {"name": "d425-sqlite", "type": "sqlite", "role": "both", **variant}
    r = client.post("/api/v1/connectors/saved", json=body)
    _assert_refused(r, "SQLite isn't available")
    text = _detail(r)
    # AC-2: the user is told the cloud service cannot read their file, and what to do.
    assert "self-hosted" in text and "PostgreSQL" in text, text
    assert "can't read a file on your computer" in text and "upload" in text, text
    assert _saved_rows() == []


def test_hosted_sqlite_legacy_create_is_refused_and_saves_nothing(client, monkeypatch):
    _hosted(monkeypatch)
    body = {"name": "d425-legacy", "type": "sqlite", "role": "both", "database": "/tmp/qa.db",
            "host": "", "port": 0}
    r = client.post("/api/v1/connectors/", json=body)
    _assert_refused(r, "SQLite isn't available")
    assert _saved_rows() == []


def test_hosted_sqlite_update_into_sqlite_is_refused(client, monkeypatch):
    """A saved PostgreSQL connector edited into SQLite on the hosted cloud."""
    from services.connector_store import get_connector

    cid = _saved_good(client)
    _hosted(monkeypatch)
    body = {"name": "u1-good", "type": "sqlite", "role": "source", "database": "/tmp/qa.db"}
    r = client.put(f"/api/v1/connectors/saved/{cid}", json=body)
    _assert_refused(r, "SQLite isn't available")
    assert get_connector(cid).type == "postgresql"


def test_self_hosted_sqlite_create_succeeds(client, tmp_path):
    """Self-hosted keeps SQLite: the same body is created (AC-1 first branch)."""
    import sqlite3

    path = tmp_path / "orders.db"
    sqlite3.connect(str(path)).close()
    body = {"name": "d425-self", "type": "sqlite", "role": "both", "database": str(path),
            "host": "", "port": 0, "username": "", "password": ""}
    r = client.post("/api/v1/connectors/saved", json=body)
    assert r.status_code == 200, r.text
    assert r.json()["type"] == "sqlite"


# ------------------------------------------- the store's own refusal (U1 (a)/(b))
#
# The egress guard judges a connection option by the connector's driver family,
# so it lets these through; the store's save-time check refuses them. On build
# 70cb that refusal was a 500 on /connectors/saved and, on the legacy create,
# was logged as suppressed and the connector was then saved to MongoDB.

STORE_ONLY_HOSTS = [
    ("oracle", "db.example.com service=prod", "service"),
    ("redshift", "db.example.com;DSN=prod", "DSN"),
    ("odbc", "Driver=/tmp/evil.so", "Driver"),
]


@pytest.mark.parametrize("db_type,host,words", STORE_ONLY_HOSTS)
def test_legacy_create_never_falls_through_to_the_document_store_on_a_refusal(client, db_type, host, words):
    body = {"name": "u1-fallthrough", "type": db_type, "host": host, "port": 1521,
            "database": "app", "username": "app", "password": "dummy-password"}
    r = client.post("/api/v1/connectors/", json=body)
    _assert_refused(r, words)
    assert _saved_rows() == []


@pytest.mark.parametrize("db_type,host,words", STORE_ONLY_HOSTS)
def test_saved_create_store_refusal_is_a_422_not_a_500(client, db_type, host, words):
    body = {"name": "u1-store", "type": db_type, "host": host, "port": 1521,
            "database": "app", "username": "app", "password": "dummy-password"}
    r = client.post("/api/v1/connectors/saved", json=body)
    assert r.status_code == 422, r.text
    _assert_refused(r, words)
    assert _saved_rows() == []


def test_saved_create_extra_option_refused_by_the_store_is_a_422(client):
    body = {"name": "u1-extra", "type": "oracle", "host": "db.example.com", "port": 1521,
            "database": "app", "username": "app", "password": "dummy-password",
            "extra": {"Driver": "/tmp/evil.so"}}
    r = client.post("/api/v1/connectors/saved", json=body)
    assert r.status_code == 422, r.text
    _assert_refused(r, "Driver")
    assert _saved_rows() == []


def test_a_description_mentioning_service_equals_is_not_refused(client):
    """U1 edge: prose in extra.description is not a connection option."""
    body = {"name": "u1-desc", "type": "mysql", "host": "db.example.com", "port": 3306,
            "database": "app", "username": "app", "password": "dummy-password",
            "extra": {"description": "replica of service=prod, read only"}}
    r = client.post("/api/v1/connectors/saved", json=body)
    assert r.status_code == 200, r.text
    assert len(_saved_rows()) == 1


# ------------------------------------------------ self-hosted SQLite (D425 AC-1/AC-3)


@pytest.mark.parametrize("database,words", [
    ("", "needs the path of its database file"),
    (":memory:", "in-memory database"),
    ("file:orders.db?mode=ro", "URI filenames are not accepted"),
])
def test_self_hosted_sqlite_unusable_database_is_a_422_naming_the_field(client, database, words):
    body = {"name": "d425-field", "type": "sqlite", "role": "both", "database": database,
            "host": "", "port": 0, "username": "", "password": ""}
    r = client.post("/api/v1/connectors/saved", json=body)
    assert r.status_code == 422, r.text
    text = _detail(r)
    assert text.startswith("Database:") and words in text, text
    assert _saved_rows() == []


def test_self_hosted_sqlite_missing_file_saves_and_its_test_names_the_missing_file(client, tmp_path):
    """AC-1: created, and its test says whether the file is reachable, in plain words."""
    body = {"name": "d425-missing", "type": "sqlite", "role": "both",
            "database": str(tmp_path / "not-here" / "orders.db"),
            "host": "", "port": 0, "username": "", "password": ""}
    r = client.post("/api/v1/connectors/saved", json=body)
    assert r.status_code == 200, r.text
    t = client.post(f"/api/v1/connectors/saved/{r.json()['id']}/test")
    assert t.status_code == 200, t.text
    out = t.json()
    assert out["success"] is False
    assert out["error"]["code"] == "object.file_not_found", out
    assert "couldn't identify the cause" not in out["message"], out
