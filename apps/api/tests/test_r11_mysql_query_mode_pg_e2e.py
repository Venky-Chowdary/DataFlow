"""QA round 11 (build 9c75ada4), Addendum A — MySQL query mode → PostgreSQL.

"Source table name required" with the table blank. The query reached every
entry point (MCP ``source_query``, REST ``/transfer/execute`` source extras,
Studio ``/transfer/run`` ``source_extra_json``); the streaming source peek
(``source_peek.peek_stream_source``) then demanded a table *before* asking
whether the source was a query, for every dialect. The callable check is now
the first branch of the peek, the streaming setup and the batch reader.

Real MySQL 8 and MariaDB 11 into a real PostgreSQL, strict, NULL / '' /
whitespace / unicode, run 2 into the table DataFlow created.
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests.test_r11_mysql_pg_fk_ddl_e2e import (  # noqa: F401 — fixture re-export
    KINDS,
    TERMINAL,
    _connector,
    _my,
    _run,
    _sql,
    _wait,
    client,
)

EXPECTED = [(1, "x"), (2, None), (3, ""), (4, " 日本 ")]


@pytest.fixture
def query_source(request):
    kind = request.param
    tag = uuid.uuid4().hex[:6]
    src, dst = f"r11qs_{tag}", f"r11qd_{tag}"
    _my(
        kind,
        f"CREATE TABLE {src} (id INT UNSIGNED PRIMARY KEY, v VARCHAR(20), hidden INT) "
        "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        f"INSERT INTO {src} VALUES (1,'x',0),(2,NULL,0),(3,'',0),(4,' 日本 ',0),(5,'gone',1)",
    )
    names = {
        "kind": kind,
        "src": src,
        "dst": dst,
        "query": f"SELECT id, v FROM {src} WHERE hidden = 0",
    }
    try:
        yield names
    finally:
        _my(kind, f"DROP TABLE IF EXISTS {src}")
        _sql(f"DROP TABLE IF EXISTS {dst}")


def _rows(table: str) -> list[tuple]:
    return _sql(f"SELECT id, v FROM {table} ORDER BY id")


@pytest.mark.parametrize("query_source", KINDS, indirect=True)
def test_query_mode_through_mcp_run1_run2(client, query_source):
    src = _connector(query_source["kind"], "source")
    dst = _connector("postgresql", "destination")
    for _run_no in (1, 2):
        _run(
            client, source_connector_id=src, source_table="",
            source_read_mode="query", source_query=query_source["query"],
            dest_connector_id=dst, dest_table=query_source["dst"], primary_key="id",
        )
        assert _rows(query_source["dst"]) == EXPECTED


def _mappings() -> list[dict]:
    return [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "v")]


@pytest.mark.parametrize("query_source", KINDS, indirect=True)
def test_query_mode_through_rest_execute_run1_run2(client, query_source):
    src = _connector(query_source["kind"], "source")
    dst = _connector("postgresql", "destination")
    for _run_no in (1, 2):
        resp = client.post(
            "/api/v1/transfer/execute",
            json={
                "source": {
                    "kind": "database", "format": query_source["kind"],
                    "connector_id": src, "table": "",
                    "source_read_mode": "query", "source_query": query_source["query"],
                },
                "destination": {
                    "kind": "database", "format": "postgresql", "connector_id": dst,
                    "table": query_source["dst"], "schema": "public",
                },
                "mappings": _mappings(),
                "sync_mode": "full_refresh_overwrite",
                "validation_mode": "strict",
                "async_mode": False,
            },
        )
        assert resp.status_code == 200, resp.text
        job = _wait(str(resp.json().get("job_id") or ""))
        assert job.get("status") == "completed", {
            k: job.get(k) for k in ("status", "error", "message")
        }
        assert _rows(query_source["dst"]) == EXPECTED


@pytest.mark.parametrize("query_source", KINDS, indirect=True)
def test_query_mode_through_studio_transfer_run_run1_run2(client, query_source):
    src = _connector(query_source["kind"], "source")
    dst = _connector("postgresql", "destination")
    for _run_no in (1, 2):
        resp = client.post(
            "/api/v1/transfer/run",
            data={
                "source_kind": "database",
                "source_format": query_source["kind"],
                "source_connector_id": src,
                "source_table": "",
                "source_extra_json": json.dumps(
                    {"source_read_mode": "query", "source_query": query_source["query"]}
                ),
                "dest_kind": "database",
                "dest_format": "postgresql",
                "dest_connector_id": dst,
                "dest_schema": "public",
                "dest_table": query_source["dst"],
                "mappings_json": json.dumps(_mappings()),
                "sync_mode": "full_refresh_overwrite",
                "validation_mode": "strict",
                "async_mode": "false",
            },
        )
        assert resp.status_code == 200, resp.text
        job = _wait(str(resp.json().get("job_id") or ""))
        assert job.get("status") == "completed", {
            k: job.get(k) for k in ("status", "error", "message")
        }
        assert _rows(query_source["dst"]) == EXPECTED
