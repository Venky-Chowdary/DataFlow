"""Query console extract fallback: non-SQL connectors get queried via
DuckDB materialization instead of failing with 'SQLAlchemy dialect/driver
is not available'.

Covers the S3-class gap the user reported: every non-SQL connector must be
queryable, not only dialect-backed ones.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import services.query_extract as qe
from tests.connector_certification.fixture_server import (
    FixtureResponse,
    FixtureServer,
)


def _connector(ctype: str, **extra) -> SimpleNamespace:
    return SimpleNamespace(
        id="conn-test",
        type=ctype,
        host="",
        port=0,
        database="",
        username="",
        password="",
        connection_string="",
        schema="",
        warehouse="",
        ssl=False,
        extra=extra,
        auth_mode="",
    )


def _body(query: str, limit: int = 100) -> SimpleNamespace:
    return SimpleNamespace(query=query, limit=limit, database="", params={})


@pytest.fixture
def fake_endpoint(monkeypatch):
    monkeypatch.setattr(
        "services.connector_probe.endpoint_from_saved_connector",
        lambda *a, **kw: SimpleNamespace(
            table="", collection="", database="", schema="", connection_string=""
        ),
    )


def test_extract_routing_split() -> None:
    # The reported failure class routes to extract…
    assert qe.connector_queryable_via_extract("s3")
    assert qe.connector_queryable_via_extract("zendesk")
    assert qe.connector_queryable_via_extract("hubspot")
    assert qe.connector_queryable_via_extract("sendgrid")
    # …SQL dialects keep the native path.
    assert not qe.connector_queryable_via_extract("postgresql")
    assert not qe.connector_queryable_via_extract("mysql")
    assert not qe.connector_queryable_via_extract("kafka")


def test_unsupported_type_is_honest_refusal(fake_endpoint) -> None:
    with pytest.raises(HTTPException) as excinfo:
        qe.run_extract_query(_connector("kafka"), _body("SELECT 1"))
    assert excinfo.value.status_code == 400
    assert "isn't supported" in excinfo.value.detail


def test_sdk_connector_queryable_via_duckdb(fake_endpoint) -> None:
    body = {
        "lists": [{"id": 5001, "name": "Fixture List", "recipient_count": 12}]
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/v3/contactdb/lists",
            responses=[FixtureResponse(body=body)],
        )
        conn = _connector(
            "sendgrid",
            base_url=fixture.base_url,
            api_key="SG.synthetic",
        )
        rows, columns, _schema, truncated = qe.run_extract_query(
            conn, _body('SELECT * FROM "lists"')
        )

    assert columns == ["id", "name", "recipient_count"]
    assert rows == [
        {"id": "5001", "name": "Fixture List", "recipient_count": "12"}
    ]
    assert truncated is False


def test_sdk_query_sql_guard_still_applies(fake_endpoint) -> None:
    """The extract path delegates to the same safe-SQL pipeline — writes refuse."""
    conn = _connector("sendgrid", base_url="http://127.0.0.1:1", api_key="x")
    with pytest.raises(HTTPException) as excinfo:
        qe.run_extract_query(conn, _body("DROP TABLE lists"))
    assert excinfo.value.status_code == 400


def test_extract_cap_sets_truncated(fake_endpoint, monkeypatch) -> None:
    monkeypatch.setattr(qe, "_EXTRACT_MAX_ROWS", 3)
    rows_page = [
        {"id": i, "name": f"n{i}"} for i in range(10)
    ]
    with FixtureServer() as fixture:
        fixture.add_route(
            "/v3/contactdb/lists",
            responses=[FixtureResponse(body={"lists": rows_page})],
        )
        conn = _connector(
            "sendgrid",
            base_url=fixture.base_url,
            api_key="SG.synthetic",
        )
        rows, _columns, _schema, truncated = qe.run_extract_query(
            conn, _body('SELECT COUNT(*) AS c FROM "lists"')
        )
    assert truncated is True
    assert rows == [{"c": 3}]
