"""TLS-required is an explicit, user-set fact (SavedConnector.ssl_mode).

Required -> strict, fails closed against a server without TLS, never plaintext.
Unset / legacy -> negotiates and works against the same server.
"""

from __future__ import annotations

import uuid

import psycopg2
import pytest

from services import connector_store
from services.connector_store import SavedConnector, create_connector, delete_connector, update_connector

_PG = {"host": "localhost", "port": 5432, "database": "dataflow", "username": "dataflow", "password": "dataflow"}


def _doc(**over):
    return {"id": "c1", "name": "n", "type": "postgresql", **over}


def test_legacy_document_with_only_ssl_true_is_unset():
    c = SavedConnector.from_dict(_doc(ssl=True))
    assert (c.ssl, c.ssl_mode) == (False, "")


def test_legacy_document_without_ssl_key_is_unset():
    assert SavedConnector.from_dict(_doc()).ssl is False


def test_marked_document_is_required():
    c = SavedConnector.from_dict(_doc(ssl=True, ssl_mode="require"))
    assert (c.ssl, c.ssl_mode) == (True, "require")


def test_non_postgres_connector_keeps_boolean_ssl():
    assert SavedConnector.from_dict(_doc(type="mysql", ssl=True)).ssl is True


@pytest.fixture
def saved():
    made: list[str] = []

    def make(**payload):
        c = create_connector({"name": f"ssl{uuid.uuid4().hex[:6]}", "type": "postgresql", **_PG, **payload})
        made.append(c.id)
        return c

    yield make
    for cid in made:
        delete_connector(cid)


def test_create_without_ssl_is_unset_and_explicit_true_is_required(saved):
    assert saved().ssl_mode == ""
    assert saved(ssl=False).ssl_mode == ""
    req = saved(ssl=True)
    assert (req.ssl, req.ssl_mode) == (True, "require")
    assert connector_store.get_connector(req.id).ssl_mode == "require"


def test_create_keeps_the_boolean_ssl_of_other_engines(saved):
    assert saved(type="mongodb", ssl=False).ssl is False
    assert saved(type="mongodb", ssl=True).ssl is True
    assert saved(type="mongodb").ssl is True


def test_update_follows_the_users_latest_choice_and_ignores_silent_patches(saved):
    c = saved(ssl=True)
    assert update_connector(c.id, {"auth_source": "x"}).ssl_mode == "require"
    off = update_connector(c.id, {"ssl": False})
    assert (off.ssl, off.ssl_mode) == (False, "")
    on = update_connector(c.id, {"ssl": True})
    assert (on.ssl, on.ssl_mode) == (True, "require")


def _pg_without_tls() -> bool:
    try:
        conn = psycopg2.connect(connect_timeout=3, sslmode="require", **{
            "host": "localhost", "port": 5432, "dbname": "dataflow", "user": "dataflow", "password": "dataflow"})
    except psycopg2.OperationalError as exc:
        return "does not support SSL" in str(exc)
    conn.close()
    return False


@pytest.mark.skipif(not _pg_without_tls(), reason="needs a reachable PostgreSQL without TLS")
def test_required_fails_closed_and_unset_connects_on_a_non_tls_server(saved):
    from connectors.postgresql_conn import get_connection

    required, unset = saved(ssl=True), saved()
    kw = dict(connection_string="", host="localhost", port=5432, database="dataflow",
              username="dataflow", password="dataflow")
    with pytest.raises(RuntimeError, match="refusing to fall back to plaintext"):
        get_connection(ssl=required.ssl, **kw)
    get_connection(ssl=unset.ssl, **kw).close()


def test_privilege_probe_dials_with_the_connectors_tls_choice(monkeypatch):
    from services import destination_privilege_probe as probe

    seen: list[bool] = []

    def fake(**kw):
        seen.append(kw["ssl"])
        raise RuntimeError("stop")

    monkeypatch.setattr("connectors.postgresql_conn.get_connection", fake)
    for ssl in (False, True):
        with pytest.raises(RuntimeError):
            probe._probe_postgres_family(
                engine="postgresql", host="h", port=5432, database="d", schema="public",
                table="t", username="u", password="p", connection_string="", ssl=ssl,
                table_exists=None, need_update=False,
            )
    assert seen == [False, True]
