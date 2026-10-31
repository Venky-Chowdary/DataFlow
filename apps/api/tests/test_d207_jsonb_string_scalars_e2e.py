"""D207 (provisional) - a PG jsonb column whose documents are top-level JSON
string scalars (``"hello"``, ``""``, ``"   "``) must load on run 1 with no
overrides, and run 2 into the table DataFlow created must load too.

QA: ``''`` and ``'   '`` inferred VARCHAR and blocked as "JSONB -> TEXT
collapse"; ``"hello"`` blocked as "JSONB -> JSONB collapse"; ``""`` alone gave
EMPTY_VALUE_NOT_NULLABLE. psycopg2 hands a jsonb string scalar over already
parsed, as the Python str ``hello`` / ``''`` / ``'   '``, and the samplers
rendered it as bare text instead of as the JSON document it is.

Route: ``start_transfer`` (strict) -> ``/api/v1/copilot/confirm`` -> job,
PG -> new PG / MySQL / SQL Server tables, twice. Every value is compared
exactly as stored text; SQL NULL and JSON ``null`` stay distinct (all three
engines can hold both).
"""

from __future__ import annotations

import uuid

import pytest

from tests import value_fidelity_route as r

# id -> jsonb literal (None = SQL NULL)
_ROWS = {
    1: '"hello"',
    2: '""',
    3: '"   "',
    4: "1",
    5: "true",
    6: "null",
    7: "{}",
    8: None,
    9: '"Łukasz 山田 🎉"',
}


@pytest.fixture
def client(tmp_path, monkeypatch):
    with r.make_client(tmp_path, monkeypatch) as c:
        yield c


def _read(kind: str, table: str) -> dict[int, object]:
    doc = "doc::text" if kind == "postgresql" else "doc"
    rows = r.query(kind, f"SELECT id, {doc} FROM {r.quote(kind, table)}")  # nosec B608
    return {int(a): b for a, b in rows}


def _source(token: str, rows: dict[int, str | None]) -> str:
    table = f"d207_{token}"
    r.execute("postgresql", f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, doc JSONB)")
    for i, doc in rows.items():
        r.execute("postgresql", f"INSERT INTO {table} VALUES (%s, %s::jsonb)", (i, doc))  # nosec B608
    return table


def _drive(client, kind: str, rows: dict[int, str | None]) -> None:
    token = uuid.uuid4().hex[:8]
    r.dest_config(kind)  # skips when the engine is down
    r.pg_conn().close()
    table = _source(token, rows)
    dest_table = f"{table}_{kind[:5]}_out"
    try:
        src_id, dst_id = r.connectors(token, kind)
        for run in (1, 2):  # run 2 lands in the table run 1 created
            r.run(
                client,
                source_connector_id=src_id, source_table=table,
                dest_connector_id=dst_id, dest_table=dest_table,
                sync_mode="full_refresh_overwrite", primary_key="id",
            )
            got = _read(kind, dest_table)
            assert set(got) == set(rows), (run, got)
            for i, want in rows.items():
                if want is None:
                    assert got[i] is None, (run, i, got[i])  # SQL NULL stays SQL NULL
                else:
                    # PG jsonb::text, MySQL JSON and SQL Server NVARCHAR all
                    # print these documents in exactly their source spelling.
                    assert got[i] == want, (run, i, got[i], want)
    finally:
        r.execute("postgresql", f"DROP TABLE IF EXISTS {table}")  # nosec B608
        r.drop(kind, dest_table)


@pytest.mark.parametrize("kind", ["postgresql", "mysql", "sqlserver"])
def test_jsonb_string_scalars_load_twice_with_no_overrides(client, kind):
    _drive(client, kind, _ROWS)


@pytest.mark.parametrize("kind", ["postgresql", "mysql"])
def test_jsonb_column_of_only_empty_string_documents(client, kind):
    """QA: a column whose only documents are ``""`` gave EMPTY_VALUE_NOT_NULLABLE."""
    _drive(client, kind, {1: '""', 2: '""', 3: None})


def test_jsonb_column_of_only_text_scalars(client):
    """QA: ``"hello"`` alone blocked as "JSONB -> JSONB collapse"."""
    _drive(client, "postgresql", {1: '"hello"', 2: '"   "', 3: '"x"'})


def test_mcp_sample_carries_each_document_as_engine_text(client):
    """The sampler behind Map/preflight shows what the Execute reader carries:
    ``"hello"`` with its quotes, JSON ``null`` distinct from SQL NULL. A plain
    run_query (the query playground) still decodes as before."""
    from services.connector_store import create_connector
    from src.ai.copilot.query_tools import sample_connector_object

    token = uuid.uuid4().hex[:8]
    table = _source(token, _ROWS)
    try:
        conn = create_connector({"name": f"d207-s-{token}", "role": "source", "workspace_id": "",
                                 "ssl": False, **r.dest_config("postgresql")})
        res = sample_connector_object(connector_id=conn.id, table=table, limit=50,
                                      analyze=False, full_rows=True)
        assert res.success, res.error
        got = {int(row["id"]): row["doc"] for row in res.output["rows"]}
        assert got == _ROWS
        # The pooled connection decodes json again for everyone else.
        from src.routers.query_router import QueryExecuteRequest, _run_query
        from services.connector_store import get_connector

        rows, *_ = _run_query(get_connector(conn.id), QueryExecuteRequest(
            connector_id=conn.id, query=f"SELECT id, doc FROM {table} ORDER BY id", limit=50))
        assert {int(x["id"]): x["doc"] for x in rows}[7] == {}
    finally:
        r.execute("postgresql", f"DROP TABLE IF EXISTS {table}")  # nosec B608
