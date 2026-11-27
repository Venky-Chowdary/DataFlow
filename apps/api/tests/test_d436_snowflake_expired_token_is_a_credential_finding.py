"""D436: an expired Snowflake programmatic access token is a credential finding everywhere.

QA r73-r75 (build 70cb): testing a Snowflake connection whose PAT had expired
returned error.code "unclassified", "Datawrap couldn't identify the cause of
this failure. The database said: 394401 (08001): Programmatic access token is
expired ... Contact support". plan_transfer with it as source answered with the
driver text and "list tables on that connector"; as destination the plan went on
with an unread schema and a mapping review per column.

No Snowflake account is reachable from this machine, so the driver is faked at
its own boundary: ``snowflake.connector.connect`` raises the connector's real
``DatabaseError`` with errno 394401 / sqlstate 08001 and the message Snowflake
sends, exactly what the driver raises for an expired PAT. Everything above it
(the saved connector, the test route, the probe, the MCP plan) is the product.
"""

from __future__ import annotations

import sqlite3

import pytest

from tests.test_u1_d425_connector_save_refusals_http import client  # noqa: F401  (fixture)

EXPIRED = "Snowflake rejected the access token because it has expired"


@pytest.fixture
def expired_pat(monkeypatch):
    snowflake_connector = pytest.importorskip("snowflake.connector")
    from snowflake.connector.errors import DatabaseError

    calls: list[dict] = []

    def _connect(**kwargs):
        calls.append(kwargs)
        raise DatabaseError(msg="Programmatic access token is expired", errno=394401, sqlstate="08001")

    monkeypatch.setattr(snowflake_connector, "connect", _connect)
    return calls


def _snowflake():
    from services.connector_store import create_connector

    return create_connector({
        "name": "QAR73-SNOW", "type": "snowflake", "role": "both", "host": "exampleorg-qaacct",
        "database": "QA", "schema": "PUBLIC", "warehouse": "QA_WH", "username": "QA_USER",
        "password": "dummy-pat-value", "auth_mode": "pat", "workspace_id": "",
    })


def _sqlite(tmp_path):
    from services.connector_store import create_connector

    path = tmp_path / "orders.db"
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany("INSERT INTO orders VALUES (?, ?)", [(i, f"n{i}") for i in range(1, 14)])
    conn.close()
    return create_connector({"name": "orders-lite", "type": "sqlite", "role": "both",
                             "database": str(path), "workspace_id": ""})


def _mcp(api, name: str, arguments: dict) -> tuple[bool, str]:
    r = api.post(
        "/api/v1/mcp",
        headers={"Accept": "application/json, text/event-stream"},
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/call",
              "params": {"name": name, "arguments": arguments}},
    )
    assert r.status_code == 200, r.text
    result = r.json()["result"]
    return (not result["isError"]), result["content"][0]["text"]


def test_connection_test_reports_an_expired_token_with_the_edit_action(client, expired_pat):  # noqa: F811
    snow = _snowflake()
    r = client.post(f"/api/v1/connectors/saved/{snow.id}/test")
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["success"] is False and expired_pat, out
    assert out["error"]["code"] == "auth.token_expired", out
    assert out["message"].startswith(EXPIRED) and "replace the token" in out["message"], out
    assert out["error"]["action"]["ui"] == "open_connection", out
    assert "couldn't identify the cause" not in out["message"]


def test_plan_with_snowflake_as_source_shows_the_same_credential_finding(client, expired_pat, tmp_path):  # noqa: F811
    snow, lite = _snowflake(), _sqlite(tmp_path)
    ok, text = _mcp(client, "plan_transfer", {
        "source_connector_id": snow.id, "source_table": "ORDERS",
        "dest_connector_id": lite.id, "dest_table": "orders_copy", "sync_mode": "full_refresh_overwrite",
    })
    assert not ok
    assert EXPIRED in text and "QAR73-SNOW" in text, text
    assert "list tables" not in text and "394401" not in text, text


@pytest.mark.parametrize("tool", ["plan_transfer", "start_transfer"])
def test_plan_and_one_shot_into_snowflake_show_the_finding_not_mapping_reviews(
    client, expired_pat, tmp_path, tool,  # noqa: F811
):
    snow, lite = _snowflake(), _sqlite(tmp_path)
    ok, text = _mcp(client, tool, {
        "source_connector_id": lite.id, "source_table": "orders",
        "dest_connector_id": snow.id, "dest_table": "ORDERS_COPY", "sync_mode": "full_refresh_overwrite",
    })
    assert not ok, text[:2000]
    assert EXPIRED in text and "QAR73-SNOW" in text, text
    assert "review" not in text.lower(), text


@pytest.mark.parametrize("raw,code", [
    ("394401 (08001): Programmatic access token is expired", "auth.token_expired"),
    ("390318 (08001): OAuth access token expired. [1172527951366]", "auth.token_expired"),
    ("390144 (08004): JWT token is invalid.", "auth.token_invalid"),
])
def test_a_run_failure_with_the_vendor_code_is_classified_as_credentials(raw, code):
    """Expiry between plan and run: the job failure reads the same class."""
    from snowflake.connector.errors import DatabaseError

    from services.error_taxonomy import classify

    errno = int(raw.split(" ", 1)[0])
    exc = DatabaseError(msg=raw.split(": ", 1)[1], errno=errno, sqlstate="08001")
    found = classify(exc, side="destination", phase="connect")
    assert found.code == code, (found.code, found.message)
    assert found.action["ui"] == "open_connection"
