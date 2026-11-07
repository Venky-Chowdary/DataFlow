"""Snowflake snapshot extract is one SELECT + fetchmany, not OFFSET pages."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from connectors.snowflake_reader import close_table_scan, read_table_scan_batch


def test_scan_reuses_one_cursor_and_never_offsets():
    cur = MagicMock()
    cur.description = [("C_CUSTKEY",), ("C_NAME",)]
    cur.fetchone.return_value = (150_000,)
    cur.fetchmany.side_effect = [
        [(1, "A"), (2, "B")],
        [(3, "C")],
        [],
    ]
    conn = MagicMock()
    conn.cursor.return_value = cur
    state: dict = {}
    executed: list[str] = []

    def execute(sql, *args):
        executed.append(sql)
        return None

    cur.execute.side_effect = execute

    with (
        patch("connectors.snowflake_reader.get_connection", return_value=conn),
        patch("connectors.snowflake_reader.normalize_account", return_value="acct"),
        patch(
            "connectors.snowflake_reader.resolve_or_fold_snowflake_table",
            return_value="CUSTOMER",
        ),
    ):
        first = read_table_scan_batch(
            host="acct",
            port=443,
            database="SNOWFLAKE_SAMPLE_DATA",
            username="u",
            password="p",
            schema="TPCH_SF1",
            connection_string="",
            warehouse="COMPUTE_WH",
            table="CUSTOMER",
            columns=["C_CUSTKEY", "C_NAME"],
            offset=0,
            limit=2,
            scan_state=state,
        )
        second = read_table_scan_batch(
            host="acct",
            port=443,
            database="SNOWFLAKE_SAMPLE_DATA",
            username="u",
            password="p",
            schema="TPCH_SF1",
            connection_string="",
            warehouse="COMPUTE_WH",
            table="CUSTOMER",
            columns=["C_CUSTKEY", "C_NAME"],
            offset=2,
            limit=2,
            scan_state=state,
        )
        third = read_table_scan_batch(
            host="acct",
            port=443,
            database="SNOWFLAKE_SAMPLE_DATA",
            username="u",
            password="p",
            schema="TPCH_SF1",
            connection_string="",
            warehouse="COMPUTE_WH",
            table="CUSTOMER",
            columns=["C_CUSTKEY", "C_NAME"],
            offset=4,
            limit=2,
            scan_state=state,
        )

    select_sql = [s for s in executed if "SELECT" in s.upper() and "C_CUSTKEY" in s.upper()]
    assert len(select_sql) == 1
    assert "OFFSET" not in select_sql[0].upper()
    assert "AT (TIMESTAMP" not in select_sql[0].upper()
    assert first.rows == [["1", "A"], ["2", "B"]]
    assert second.rows == [["3", "C"]]
    assert third.rows == []
    assert conn.close.called
    close_table_scan(state)


def test_batch_reader_uses_scan_when_state_provided():
    from src.transfer.batch_readers import _read_batch_impl

    state: dict = {"marker": True}
    with patch(
        "connectors.snowflake_reader.read_table_scan_batch",
        return_value=MagicMock(headers=["ID"], rows=[["1"]], total_rows=1),
    ) as scan:
        _read_batch_impl(
            "snowflake",
            {
                "host": "acct",
                "port": 443,
                "database": "DB",
                "username": "u",
                "password": "p",
                "schema": "PUBLIC",
                "warehouse": "WH",
            },
            "T",
            ["ID"],
            0,
            5000,
            scan_state=state,
        )
    scan.assert_called_once()
    assert scan.call_args.kwargs["scan_state"] is state


def test_mysql_and_postgres_batch_readers_use_scan_when_state_provided():
    from src.transfer.batch_readers import _read_batch_impl

    for src, module in (
        ("mysql", "connectors.mysql_reader.read_table_scan_batch"),
        ("postgresql", "connectors.postgresql_reader.read_table_scan_batch"),
        ("redshift", "connectors.postgresql_reader.read_table_scan_batch"),
    ):
        state: dict = {"marker": src}
        with patch(
            module,
            return_value=MagicMock(headers=["ID"], rows=[["1"]], total_rows=1),
        ) as scan:
            _read_batch_impl(
                src,
                {
                    "host": "h",
                    "port": 3306 if src == "mysql" else 5432,
                    "database": "DB",
                    "username": "u",
                    "password": "p",
                    "schema": "public",
                },
                "T",
                ["ID"],
                0,
                5000,
                scan_state=state,
            )
        scan.assert_called_once()
        assert scan.call_args.kwargs["scan_state"] is state


def test_warehouse_batch_readers_use_scan_when_state_provided():
    from src.transfer.batch_readers import _read_batch_impl

    for src, module in (
        ("bigquery", "connectors.bigquery_reader.read_table_scan_batch"),
        ("generic_sql", "connectors.generic_sql.read_table_scan_batch"),
        ("sqlserver", "connectors.sqlserver_reader.read_table_scan_batch"),
        ("oracle", "connectors.oracle_reader.read_table_scan_batch"),
        ("databricks", "connectors.generic_sql.read_table_scan_batch"),
        ("sqlite", "connectors.sqlite_reader.read_table_scan_batch"),
    ):
        state: dict = {"marker": src}
        with patch(
            module,
            return_value=MagicMock(headers=["ID"], rows=[["1"]], total_rows=1),
        ) as scan:
            _read_batch_impl(
                src,
                {
                    "host": "h",
                    "port": 443,
                    "database": "DB",
                    "username": "u",
                    "password": "p",
                    "schema": "public",
                    "type": src,
                },
                "T",
                ["ID"],
                0,
                5000,
                scan_state=state,
            )
        scan.assert_called_once()
        assert scan.call_args.kwargs["scan_state"] is state
