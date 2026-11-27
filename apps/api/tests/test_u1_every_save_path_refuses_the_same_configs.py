"""U1 (review): every path that creates or changes a saved connector refuses the same configs.

One table of configurations the platform must never save — a MySQL option that
reads a host file or enables LOAD DATA LOCAL, the platform's AWS credentials,
MongoDB's AWS auth (the host's identity), a cloud metadata host, a libpq
service, a double-encoded ODBC DSN, an SFTP URL whose port disagrees with the
port field, a store-only server-file read — is sent through each save path a
user or agent can reach:

* ``POST /api/v1/connectors/saved`` (create), ``PUT`` and ``PATCH`` on it,
* ``POST /api/v1/connectors/saved/{id}/rotate-secrets`` (connection-string rows),
* the legacy ``POST /api/v1/connectors/`` (rows it can express),
* Pilot/MCP ``POST /api/v1/copilot/confirm`` for ``create_connector`` and
  ``update_connector`` (the payload as it sits in the approval ledger),
* the MCP ``create_connector`` staging tool.

Each must answer the same status with the same text, and nothing is saved or
changed. Status codes are part of the contract: the egress guard's refusals
stay 400; the store's own refusal is 422.

Build 93d0ee7d4 (review): Confirm-create wrote through ``create_connector``
with only the store's check, so the egress rows were saved there; the legacy
route fell through to MongoDB on any other store error (the SFTP port row).
"""

from __future__ import annotations

import pytest

from tests.test_u1_d425_connector_save_refusals_http import _detail, _saved_rows, client  # noqa: F401

DUMMY = "dummy-password"

#: id -> (base config of that type, the bad change, status, words in the refusal)
ROWS: dict[str, tuple[dict, dict, int, str]] = {
    "mysql-read_default_file": (
        {"type": "mysql", "host": "db.example.com", "port": 3306, "database": "app"},
        {"connection_string": "mysql+pymysql://app@db.example.com:3306/app?read_default_file=/root/.my.cnf"},
        400, "read_default_file"),
    "mysql-local_infile-url": (
        {"type": "mysql", "host": "db.example.com", "port": 3306, "database": "app"},
        {"connection_string": "mysql+pymysql://app@db.example.com:3306/app?local_infile=1"},
        400, "local_infile"),
    "mysql-local_infile-extra": (
        {"type": "mysql", "host": "db.example.com", "port": 3306, "database": "app"},
        {"extra": {"local_infile": 1}}, 400, "local_infile"),
    "mysql-init_command-extra": (
        {"type": "mysql", "host": "db.example.com", "port": 3306, "database": "app"},
        {"extra": {"init_command": "SELECT 1"}}, 400, "init_command"),
    "s3-platform-aws-credentials": (
        {"type": "s3", "host": "", "port": 0, "database": "qa-bucket", "username": "AKIADUMMY"},
        {"extra": {"use_default_aws_credentials": True}}, 400, "use_default_aws_credentials"),
    "mongo-aws-auth": (  # (a "mongodb" test id is skipped without a live MongoDB)
        {"type": "mongodb", "host": "db.example.com", "port": 27017, "database": "app"},
        {"connection_string": "mongodb://db.example.com:27017/app?authMechanism=MONGODB-AWS"},
        400, "authMechanism"),
    "metadata-host": (
        {"type": "postgresql", "host": "db.example.com", "port": 5432, "database": "app"},
        {"host": "169.254.169.254"}, 400, "169.254.169.254"),
    "libpq-service": (
        {"type": "postgresql", "host": "db.example.com", "port": 5432, "database": "app"},
        {"connection_string": "postgresql://app@db.example.com:5432/app?service=prod"}, 400, "service"),
    "odbc-double-encoded-dsn": (
        {"type": "sqlserver", "host": "db.example.com", "port": 1433, "database": "app"},
        {"connection_string": "mssql+pyodbc://app@db.example.com:1433/app?odbc_connect=DSN%253Dprod"},
        400, "DSN"),
    "sftp-url-port-disagrees": (
        {"type": "sftp", "host": "files.example.com", "port": 22, "database": "/"},
        # 22 is a form default (the URL's port wins, D208); 2222 is an explicit, different port.
        {"host": "sftp://files.example.com:2200", "port": 2222}, 400, "2200"),
    "store-only-file-read": (
        {"type": "oracle", "host": "db.example.com", "port": 1521, "database": "app"},
        {"host": "db.example.com service=prod"}, 422, "service"),
}
CS_ROWS = [k for k, (_b, bad, _s, _w) in ROWS.items() if set(bad) == {"connection_string"}]
LEGACY_ROWS = [k for k, (_b, bad, _s, _w) in ROWS.items() if "extra" not in bad]


def _body(name: str, base: dict, change: dict | None = None) -> dict:
    return {"name": name, "role": "both", "username": "app", "password": DUMMY, **base, **(change or {})}


def _assert_refused(r, status: int, words: str) -> str:
    assert r.status_code == status, (r.status_code, r.text)
    text = _detail(r)
    assert words in text and "unexpected error" not in text.lower(), text
    return r.json()["detail"]


def _good(client, row: str) -> tuple[str, dict]:
    base, _bad, _s, _w = ROWS[row]
    r = client.post("/api/v1/connectors/saved", json=_body(f"u1-{row}", base))
    assert r.status_code == 200, r.text
    from services.connector_store import get_connector

    return r.json()["id"], get_connector(r.json()["id"]).to_dict()


def _unchanged(cid: str, before: dict) -> None:
    from services.connector_store import get_connector

    after = get_connector(cid).to_dict()
    for key in ("host", "port", "connection_string", "extra", "type"):
        assert after.get(key) == before.get(key), (key, before.get(key), after.get(key))


# ------------------------------------------------------------------ creates


@pytest.mark.parametrize("row", sorted(ROWS))
def test_rest_create(client, row):  # noqa: F811
    base, bad, status, words = ROWS[row]
    _assert_refused(client.post("/api/v1/connectors/saved", json=_body("u1-c", base, bad)), status, words)
    assert _saved_rows() == []


@pytest.mark.parametrize("row", LEGACY_ROWS)
def test_legacy_create(client, row):  # noqa: F811
    base, bad, status, words = ROWS[row]
    _assert_refused(client.post("/api/v1/connectors/", json=_body("u1-l", base, bad)), status, words)
    assert _saved_rows() == []


@pytest.mark.parametrize("row", sorted(ROWS))
def test_confirm_create(client, row):  # noqa: F811
    from src.ai.copilot.ack_ledger import get_ack_ledger

    base, bad, status, words = ROWS[row]
    ack_id = get_ack_ledger().put(kind="create_connector", payload=_body("u1-p", base, bad))
    _assert_refused(client.post("/api/v1/copilot/confirm", json={"ack_id": ack_id}), status, words)
    assert _saved_rows() == []


@pytest.mark.parametrize("row", CS_ROWS)
def test_mcp_create_tool_refuses_at_staging(client, row):  # noqa: F811
    base, bad, _status, words = ROWS[row]
    r = client.post(
        "/api/v1/mcp",
        headers={"Accept": "application/json, text/event-stream"},
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
            "name": "create_connector",
            "arguments": {**_body("u1-mcp", base, bad), "test_first": False},
        }},
    )
    result = r.json()["result"]
    assert result["isError"], result
    assert words in result["content"][0]["text"], result
    assert _saved_rows() == []


# ------------------------------------------------------------------ updates


@pytest.mark.parametrize("row", sorted(ROWS))
def test_rest_put(client, row):  # noqa: F811
    base, bad, status, words = ROWS[row]
    cid, before = _good(client, row)
    _assert_refused(client.put(f"/api/v1/connectors/saved/{cid}", json=_body(f"u1-{row}", base, bad)),
                    status, words)
    _unchanged(cid, before)


@pytest.mark.parametrize("row", sorted(ROWS))
def test_rest_patch(client, row):  # noqa: F811
    _base, bad, status, words = ROWS[row]
    cid, before = _good(client, row)
    _assert_refused(client.patch(f"/api/v1/connectors/saved/{cid}", json=bad), status, words)
    _unchanged(cid, before)


@pytest.mark.parametrize("row", CS_ROWS)
def test_rotate_secrets(client, row):  # noqa: F811
    _base, bad, status, words = ROWS[row]
    cid, before = _good(client, row)
    r = client.post(f"/api/v1/connectors/saved/{cid}/rotate-secrets", json=bad)
    _assert_refused(r, status, words)
    _unchanged(cid, before)


@pytest.mark.parametrize("row", sorted(ROWS))
def test_confirm_update(client, row):  # noqa: F811
    from src.ai.copilot.ack_ledger import get_ack_ledger

    _base, bad, status, words = ROWS[row]
    cid, before = _good(client, row)
    ack_id = get_ack_ledger().put(kind="update_connector",
                                  payload={"connector_id": cid, "name": f"u1-{row}", "changes": bad})
    _assert_refused(client.post("/api/v1/copilot/confirm", json={"ack_id": ack_id}), status, words)
    _unchanged(cid, before)


def test_every_path_gives_the_same_text_for_the_same_config(client):  # noqa: F811
    from src.ai.copilot.ack_ledger import get_ack_ledger

    base, bad, status, words = ROWS["mysql-read_default_file"]
    texts = {
        "create": _assert_refused(client.post("/api/v1/connectors/saved", json=_body("u1-s", base, bad)),
                                  status, words),
        "legacy": _assert_refused(client.post("/api/v1/connectors/", json=_body("u1-s", base, bad)),
                                  status, words),
        "confirm": _assert_refused(client.post("/api/v1/copilot/confirm", json={
            "ack_id": get_ack_ledger().put(kind="create_connector", payload=_body("u1-s", base, bad))}),
            status, words),
    }
    assert len(set(texts.values())) == 1, texts


# ----------------------------------------------------- the legacy route's store


def test_legacy_create_answers_503_on_a_store_failure_and_saves_nowhere(client, monkeypatch):  # noqa: F811
    """No fall-through to another collection when the configured store fails."""
    import services.connector_store as store

    def _down(_data):
        raise store.ConnectorStoreError("Connector create failed in MongoDB (the configured connector store)")

    monkeypatch.setattr(store, "create_connector", _down)
    base = ROWS["libpq-service"][0]
    r = client.post("/api/v1/connectors/", json=_body("u1-down", base))
    assert r.status_code == 503, r.text
    assert _saved_rows() == []


def test_legacy_create_answers_a_store_value_error_as_a_4xx_and_saves_nowhere(client, monkeypatch):  # noqa: F811
    import services.connector_store as store

    def _refuse(_data):
        raise ValueError("The store will not take this connector: the port is out of range.")

    monkeypatch.setattr(store, "create_connector", _refuse)
    base = ROWS["libpq-service"][0]
    r = client.post("/api/v1/connectors/", json=_body("u1-val", base))
    assert r.status_code == 422, r.text
    assert "port is out of range" in r.json()["detail"]
    assert _saved_rows() == []
