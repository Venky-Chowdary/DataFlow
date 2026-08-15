"""generic_sql / BigQuery snapshot extract is one SELECT, not OFFSET pages."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from connectors.generic_sql import read_table_scan_batch
from connectors.sql_snapshot_scan import SNAPSHOT_SCAN_SOURCES, fetch_scan_page


def test_snapshot_scan_sources_cover_warehouse_matrix():
    for src in (
        "snowflake",
        "mysql",
        "postgresql",
        "redshift",
        "bigquery",
        "generic_sql",
        "sqlserver",
        "oracle",
        "databricks",
        "sqlite",
        "mongodb",
    ):
        assert src in SNAPSHOT_SCAN_SOURCES


def test_fetch_scan_page_falls_back_to_fetchall_for_test_doubles():
    cur = MagicMock()
    cur.fetchmany.return_value = MagicMock()
    cur.fetchall.return_value = [(1,), (2,)]
    assert fetch_scan_page(cur, 10) == [(1,), (2,)]
    cur.fetchmany.return_value = [(3,)]
    assert fetch_scan_page(cur, 10) == [(3,)]


def test_generic_sql_scan_never_offsets():
    pytest.importorskip("sqlalchemy")
    import sqlalchemy as sa

    result = MagicMock()
    result.fetchmany.side_effect = [
        [("1", "A"), ("2", "B")],
        [],
    ]
    streamed = MagicMock()
    streamed.execute.return_value = result
    conn = MagicMock()
    conn.execution_options.return_value = streamed
    engine = MagicMock()
    engine.connect.return_value = conn
    col = sa.column("id")
    table_obj = MagicMock()
    table_obj.c = {"id": col}
    table_obj.primary_key = None

    state: dict = {}
    with (
        patch("connectors.generic_sql.SQLALCHEMY_AVAILABLE", True),
        patch("connectors.generic_sql._engine", return_value=engine),
        patch("connectors.generic_sql._schema_name", return_value="dbo"),
        patch("connectors.generic_sql._dialect_key", return_value="mssql"),
        patch("connectors.generic_sql._reflect_table", return_value=table_obj),
        patch("connectors.generic_sql._tz_safe_projection", return_value=[col]),
        patch(
            "connectors.generic_sql._serialize_source_row",
            side_effect=lambda row, cols, dialect: list(row),
        ),
        patch("connectors.generic_sql._count_table_raw", return_value=2),
    ):
        first = read_table_scan_batch(
            host="h",
            port=1433,
            database="db",
            username="u",
            password="p",
            schema="dbo",
            connection_string="",
            ssl=False,
            table="T",
            type="sqlserver",
            columns=["id"],
            offset=0,
            limit=2,
            scan_state=state,
        )
        second = read_table_scan_batch(
            host="h",
            port=1433,
            database="db",
            username="u",
            password="p",
            schema="dbo",
            connection_string="",
            ssl=False,
            table="T",
            type="sqlserver",
            columns=["id"],
            offset=2,
            limit=2,
            scan_state=state,
        )

    assert first.rows == [["1", "A"], ["2", "B"]]
    assert second.rows == []
    streamed.execute.assert_called_once()
    stmt = streamed.execute.call_args[0][0]
    compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
    assert "OFFSET" not in compiled.upper()


def test_bigquery_scan_sql_has_no_offset():
    pytest.importorskip("google.cloud.bigquery")
    from connectors.bigquery_reader import read_table_scan_batch

    job = MagicMock()
    job.schema = [MagicMock(name="id")]
    job.schema[0].name = "id"
    row = MagicMock()
    row.values.return_value = (1,)
    job.result.return_value = [row]
    client = MagicMock()
    client.query.return_value = job
    client.get_table.return_value.schema = [MagicMock(name="id")]
    client.get_table.return_value.schema[0].name = "id"
    count_row = {"cnt": 1}
    client.query.return_value.result.return_value = [count_row]

    # First query is COUNT, second is the scan. Side-effect the two jobs.
    count_job = MagicMock()
    count_job.result.return_value = [{"cnt": 1}]
    scan_job = MagicMock()
    scan_job.schema = [MagicMock()]
    scan_job.schema[0].name = "id"
    scan_row = MagicMock()
    scan_row.values.return_value = (1,)
    scan_job.result.return_value = [scan_row]
    client.query.side_effect = [count_job, scan_job]

    state: dict = {}
    with (
        patch("connectors.bigquery_conn.get_client", return_value=client),
        patch("connectors.bigquery_conn._is_local_endpoint", return_value=(False, "")),
    ):
        batch = read_table_scan_batch(
            host="proj",
            port=443,
            database="proj",
            username="",
            password="",
            schema="ds",
            connection_string="",
            ssl=False,
            table="T",
            columns=["id"],
            offset=0,
            limit=10,
            scan_state=state,
        )

    assert batch.rows == [["1"]]
    scan_sql = client.query.call_args_list[-1].args[0]
    assert "OFFSET" not in scan_sql.upper()
    assert "LIMIT" not in scan_sql.upper()
