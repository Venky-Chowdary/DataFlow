"""Named-fixture proofs: CDC mid-snapshot resume seeks by PK, not OFFSET.

Debezium-class invariant: a crash after page N must continue with
``WHERE pk > last_pk`` (lexicographic successor). OFFSET / ROW_NUMBER is
legacy-token fallback only. Streaming watermarks (binlog file/pos/GTID,
LSN, SCN, CT version) must survive the seek and must not be recaptured.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from connectors.mysql_change_stream import MySqlChangeStreamCdc
from connectors.oracle_change_stream import (
    OracleFlashbackCdc,
    decode_oracle_resume_token,
    encode_oracle_resume_token,
)
from connectors.postgresql_change_stream import (
    PostgreSqlChangeStreamCdc,
    decode_pg_resume_token,
    decode_pg_snapshot_progress,
    encode_pg_resume_token,
)
from connectors.sqlserver_cdc_native import (
    SqlServerNativeCdc,
    decode_mssql_cdc_token,
    encode_mssql_cdc_token,
)
from connectors.sqlserver_change_stream import (
    SqlServerChangeTrackingCdc,
    decode_sqlserver_resume_token,
    encode_sqlserver_resume_token,
)
from services.cdc_snapshot_resume import (
    classify_snapshot_resume,
    last_pk_from_records,
    snapshot_keyset_sql,
)


def test_classify_snapshot_resume_prefers_last_pk() -> None:
    assert classify_snapshot_resume(last_pk="2", offset=50) == "keyset"
    assert classify_snapshot_resume(last_pk="", offset=50) == "offset"
    assert classify_snapshot_resume(last_pk="", offset=0) == "scan"


def test_last_pk_from_records_uses_column_order_not_text_max() -> None:
    records = [{"id": "99", "n": "a"}, {"id": "200", "n": "b"}]
    assert last_pk_from_records(records, ["id"]) == "200"
    oracle_case = [{"ID": "2", "AMOUNT": "20"}]
    assert last_pk_from_records(oracle_case, ["id"]) == "2"


def test_snapshot_keyset_sql_dialects_bind_last_pk() -> None:
    mysql_sql, mysql_params = snapshot_keyset_sql(
        table_ref="`orders`",
        quoted_pk_columns=["`id`"],
        last_pk="2",
        limit=10,
        dialect="mysql",
    )
    assert "OFFSET" not in mysql_sql.upper()
    assert "ROW_NUMBER" not in mysql_sql.upper()
    assert "`id` > %s" in mysql_sql
    assert mysql_params == ["2", 10]

    mssql_sql, mssql_params = snapshot_keyset_sql(
        table_ref="[dbo].[orders]",
        quoted_pk_columns=["[id]"],
        last_pk="2",
        limit=10,
        dialect="sqlserver",
    )
    assert "TOP (10)" in mssql_sql
    assert "OFFSET" not in mssql_sql.upper()
    assert mssql_params == ["2"]

    ora_sql, ora_params = snapshot_keyset_sql(
        table_ref='"APP"."ORDERS"',
        quoted_pk_columns=['"ID"'],
        last_pk="2",
        limit=10,
        dialect="oracle",
    )
    assert "ROW_NUMBER" not in ora_sql.upper()
    assert "ROWNUM" in ora_sql.upper()
    assert ora_params["k0"] == "2"
    assert ora_params["lim"] == 10


def test_mysql_keyset_resume_keeps_binlog_file_pos_gtid() -> None:
    """PK seek must not replace or recapture the binlog handoff tip."""
    cdc = MySqlChangeStreamCdc(
        {
            "host": "localhost",
            "port": 3306,
            "database": "dataflow",
            "username": "u",
            "password": "p",
        },
        table="orders",
        primary_key="id",
        batch_size=2,
        resume_token={
            "phase": "snapshot",
            "offset": 2,
            "last_pk": "2",
            "file": "mysql-bin.000001",
            "pos": 4,
            "gtid": "uuid:1-10",
            "table": "orders",
        },
    )

    class _Cur:
        def __init__(self) -> None:
            self.sqls: list[str] = []
            self._sql = ""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            self._sql = str(sql)
            self.sqls.append(self._sql)
            self.params = params

        def fetchone(self):
            if "gtid_executed" in self._sql.lower():
                return ("recaptured-gtid",)
            if "MASTER" in self._sql.upper() or "BINARY LOG" in self._sql.upper():
                return ("mysql-bin.999999", 9999)
            return None

        def fetchall(self):
            if ">" in self._sql and "OFFSET" not in self._sql.upper():
                return [("3", "30")]
            return []

        @property
        def description(self):
            return [("id",), ("amount",)]

    class _Conn:
        def __init__(self) -> None:
            self.cur = _Cur()

        def autocommit(self, _v):
            return None

        def cursor(self):
            return self.cur

        def close(self):
            return None

    conn = _Conn()
    with (
        patch.object(cdc, "_acquire_cdc_lease"),
        patch.object(cdc, "_ensure_decode_schema"),
        patch.object(cdc, "heartbeat"),
        patch("connectors.mysql_change_stream.get_connection", return_value=conn),
    ):
        batches = list(cdc.snapshot())

    keyset_sql = [s for s in conn.cur.sqls if "WHERE" in s.upper() and ">" in s]
    assert keyset_sql, conn.cur.sqls
    assert all("OFFSET" not in s.upper() for s in keyset_sql)
    assert batches[0].resume_token["file"] == "mysql-bin.000001"
    assert batches[0].resume_token["pos"] == 4
    assert batches[0].resume_token["gtid"] == "uuid:1-10"
    assert batches[0].resume_token["last_pk"] == "3"
    assert batches[-1].resume_token["phase"] == "streaming"
    assert batches[-1].resume_token["file"] == "mysql-bin.000001"
    assert "last_pk" not in batches[-1].resume_token


def test_mysql_legacy_offset_resume_still_uses_offset() -> None:
    cdc = MySqlChangeStreamCdc(
        {
            "host": "localhost",
            "port": 3306,
            "database": "dataflow",
            "username": "u",
            "password": "p",
        },
        table="orders",
        primary_key="id",
        batch_size=10,
        resume_token={
            "phase": "snapshot",
            "offset": 2,
            "file": "mysql-bin.000001",
            "pos": 4,
            "table": "orders",
        },
    )
    page = MagicMock()
    page.headers = ["id", "amount"]
    page.rows = [["3", "30"]]
    empty = MagicMock()
    empty.rows = []
    with (
        patch.object(cdc, "_acquire_cdc_lease"),
        patch.object(cdc, "_ensure_decode_schema"),
        patch.object(cdc, "heartbeat"),
        patch.object(cdc, "_current_binlog_position", return_value={"file": "mysql-bin.000001", "pos": 4}),
        patch(
            "connectors.mysql_change_stream.get_connection",
            side_effect=RuntimeError("no live mysql"),
        ),
        patch("connectors.mysql_change_stream.read_table_batch", return_value=page) as batched,
        patch("connectors.mysql_change_stream.read_table_scan_batch", return_value=empty) as scanned,
    ):
        batches = list(cdc.snapshot())
    assert batched.called
    assert not scanned.called
    assert batches[0].resume_token["offset"] == 3
    assert batches[0].resume_token["file"] == "mysql-bin.000001"


def test_sqlserver_ct_keyset_resume_keeps_version() -> None:
    token = encode_sqlserver_resume_token(
        10, table="orders", phase="snapshot", offset=2, last_pk="2"
    )
    state = decode_sqlserver_resume_token(token)
    assert state["last_pk"] == "2"
    cdc = SqlServerChangeTrackingCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
        schema="dbo",
        batch_size=2,
        resume_token=token,
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("id",), ("amount",)]
    cur.fetchall.side_effect = [[("3", "30")], []]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn), patch.object(cdc, "_acquire_cdc_lease"):
        batches = list(cdc.snapshot())

    sqls = [str(c.args[0]) for c in cur.execute.call_args_list if c.args]
    assert any("WHERE" in s.upper() and ">" in s for s in sqls)
    assert all("OFFSET" not in s.upper() for s in sqls)
    assert decode_sqlserver_resume_token(batches[0].resume_token)["last_pk"] == "3"
    assert decode_sqlserver_resume_token(batches[-1].resume_token)["version"] == 10
    assert decode_sqlserver_resume_token(batches[-1].resume_token)["phase"] == "streaming"


def test_sqlserver_ct_legacy_offset_resume_uses_offset() -> None:
    token = encode_sqlserver_resume_token(10, table="orders", phase="snapshot", offset=2)
    cdc = SqlServerChangeTrackingCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
        resume_token=token,
        batch_size=2,
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("id",), ("amount",)]
    cur.fetchall.side_effect = [[("3", "30")], []]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn), patch.object(cdc, "_acquire_cdc_lease"):
        list(cdc.snapshot())

    sqls = [str(c.args[0]) for c in cur.execute.call_args_list if c.args]
    assert any("OFFSET" in s.upper() for s in sqls)


def test_sqlserver_native_keyset_resume_keeps_lsn() -> None:
    token = encode_mssql_cdc_token(
        "0abc",
        table="orders",
        phase="snapshot",
        offset=2,
        last_pk="2",
        capture_instance="dbo_orders",
    )
    assert decode_mssql_cdc_token(token)["last_pk"] == "2"
    cdc = SqlServerNativeCdc(
        {"host": "localhost", "database": "app"},
        table="orders",
        primary_key="id",
        schema="dbo",
        batch_size=2,
        resume_token=token,
        capture_instance="dbo_orders",
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("id",), ("amount",)]
    cur.fetchone.return_value = ("dbo_orders",)
    cur.fetchall.side_effect = [
        [("3", "30")],
        [],
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with (
        patch.object(cdc, "_conn", return_value=conn),
        patch.object(cdc, "_acquire_cdc_lease"),
        patch.object(cdc, "_maybe_record_capture_schema"),
    ):
        batches = list(cdc.snapshot())

    sqls = [str(c.args[0]) for c in cur.execute.call_args_list if c.args]
    dump = [s for s in sqls if "FROM" in s.upper() and "orders" in s.lower()]
    assert dump
    assert any("WHERE" in s.upper() and ">" in s for s in dump)
    assert all("OFFSET" not in s.upper() for s in dump)
    handoff = decode_mssql_cdc_token(batches[-1].resume_token)
    assert handoff["phase"] == "streaming"
    assert handoff["lsn"] == "0abc"
    assert decode_mssql_cdc_token(batches[0].resume_token)["last_pk"] == "3"


def test_oracle_keyset_resume_keeps_scn_and_skips_row_number() -> None:
    token = encode_oracle_resume_token(
        1000, table="ORDERS", phase="snapshot", offset=2, last_pk="2"
    )
    assert decode_oracle_resume_token(token)["last_pk"] == "2"
    cdc = OracleFlashbackCdc(
        {"host": "localhost", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        batch_size=2,
        resume_token=token,
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("ID",), ("AMOUNT",)]
    cur.fetchall.side_effect = [[("3", "30")], []]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn), patch.object(cdc, "_acquire_cdc_lease"):
        batches = list(cdc.snapshot())

    sqls = [str(c.args[0]) for c in cur.execute.call_args_list if c.args]
    dump = [s for s in sqls if "FROM" in s.upper()]
    assert dump
    assert all("ROW_NUMBER" not in s.upper() for s in dump)
    assert any("ROWNUM" in s.upper() and ">" in s for s in dump)
    assert decode_oracle_resume_token(batches[-1].resume_token)["scn"] == 1000
    assert decode_oracle_resume_token(batches[0].resume_token)["last_pk"] == "3"


def test_oracle_legacy_offset_resume_uses_row_number() -> None:
    token = encode_oracle_resume_token(1000, table="ORDERS", phase="snapshot", offset=2)
    cdc = OracleFlashbackCdc(
        {"host": "localhost", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        batch_size=2,
        resume_token=token,
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("ID",), ("AMOUNT",), ("DF_RN",)]
    cur.fetchall.side_effect = [[("3", "30", 3)], []]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn), patch.object(cdc, "_acquire_cdc_lease"):
        list(cdc.snapshot())

    sqls = [str(c.args[0]) for c in cur.execute.call_args_list if c.args]
    assert any("ROW_NUMBER" in s.upper() for s in sqls)


def test_pg_token_roundtrip_url_encodes_last_pk() -> None:
    token = encode_pg_resume_token(
        "df_slot",
        lsn="0/16B3600",
        phase="snapshot",
        last_pk="a=b|c",
        table="orders",
    )
    assert "last_pk=" in token
    assert "a=b|c" not in token
    table, last_pk = decode_pg_snapshot_progress(token)
    assert table == "orders"
    assert last_pk == "a=b|c"
    slot, lsn, phase = decode_pg_resume_token(
        token, database="test", table="orders", cursor_key="k"
    )
    assert slot == "df_slot"
    assert lsn == "0/16B3600"
    assert phase == "snapshot"
    streaming = encode_pg_resume_token("df_slot", lsn="0/16B3600", phase="streaming")
    assert "last_pk=" not in streaming
    assert decode_pg_snapshot_progress(streaming) == ("", "")


def test_pg_poll_continues_snapshot_instead_of_wal() -> None:
    """phase=snapshot must finish the dump. WAL-first skips undumped rows."""
    cursor_key = "pg:test:orders→sql:test:dst:stream"
    token = encode_pg_resume_token(
        "df_test_orders_abcd1234",
        lsn="0/16B3600",
        phase="snapshot",
        last_pk="2",
        table="orders",
    )
    cdc = PostgreSqlChangeStreamCdc(
        {
            "host": "localhost",
            "port": 5432,
            "database": "test",
            "username": "",
            "password": "",
            "connection_string": "",
            "ssl": False,
            "schema": "public",
        },
        table="orders",
        primary_key="id",
        cursor_key=cursor_key,
        columns=["id", "amount"],
        resume_token=token,
        output_plugin="test_decoding",
        batch_size=2,
    )
    assert cdc.phase == "snapshot"
    assert cdc.snapshot_last_pk == "2"

    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("id",), ("amount",)]
    cur.fetchall.side_effect = [[("3", "30.00")], []]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    conn.autocommit = True

    with (
        patch("connectors.postgresql_change_stream.get_connection", return_value=conn),
        patch.object(cdc, "_ensure_slot", return_value="0/16B3500"),
        patch.object(cdc, "_ensure_decode_schema", return_value={}),
        patch("connectors.postgresql_reader._order_by_clause", return_value='"id"'),
    ):
        batches = list(cdc.poll())

    sqls = [str(c.args[0]) for c in cur.execute.call_args_list if c.args]
    assert any("WHERE" in s.upper() and ">" in s for s in sqls)
    assert all("OFFSET" not in s.upper() for s in sqls)
    assert all("peek_changes" not in s for s in sqls)
    assert "phase=snapshot" in str(batches[0].resume_token)
    assert "last_pk=" in str(batches[0].resume_token)
    slot, lsn, phase = decode_pg_resume_token(
        str(batches[-1].resume_token),
        database="test",
        table="orders",
        cursor_key=cursor_key,
    )
    assert phase == "streaming"
    assert lsn == "0/16B3600"
    assert slot == cdc.slot_name
    assert cdc.phase == "streaming"


def test_logminer_fresh_snapshot_is_held_scan_not_row_number() -> None:
    from connectors.oracle_logminer import OracleLogMinerCdc, decode_logminer_token

    cdc = OracleLogMinerCdc(
        {"host": "localhost", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        batch_size=2,
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("ID",), ("AMOUNT",)]
    cur.fetchone.side_effect = [(9000,)]
    cur.fetchall.side_effect = [[("1", "10"), ("2", "20")], []]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn), patch.object(cdc, "_acquire_cdc_lease"):
        batches = list(cdc.snapshot())

    sqls = [str(c.args[0]) for c in cur.execute.call_args_list if c.args]
    dump = [s for s in sqls if "FROM" in s.upper() and "V$DATABASE" not in s.upper()]
    assert dump
    assert all("ROW_NUMBER" not in s.upper() for s in dump)
    assert all("OFFSET" not in s.upper() for s in dump)
    assert decode_logminer_token(batches[0].resume_token)["last_pk"] == "2"
    assert decode_logminer_token(batches[-1].resume_token)["phase"] == "streaming"
    assert decode_logminer_token(batches[-1].resume_token)["scn"] == 9000
    assert "last_pk" not in decode_logminer_token(batches[-1].resume_token) or not decode_logminer_token(
        batches[-1].resume_token
    ).get("last_pk")


def test_logminer_keyset_resume_keeps_scn() -> None:
    from connectors.oracle_logminer import (
        OracleLogMinerCdc,
        decode_logminer_token,
        encode_logminer_token,
    )

    token = encode_logminer_token(
        1000, table="ORDERS", phase="snapshot", offset=2, last_pk="2"
    )
    assert decode_logminer_token(token)["last_pk"] == "2"
    cdc = OracleLogMinerCdc(
        {"host": "localhost", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        batch_size=2,
        resume_token=token,
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("ID",), ("AMOUNT",)]
    cur.fetchall.side_effect = [[("3", "30")], []]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn), patch.object(cdc, "_acquire_cdc_lease"):
        batches = list(cdc.snapshot())

    sqls = [str(c.args[0]) for c in cur.execute.call_args_list if c.args]
    dump = [s for s in sqls if "FROM" in s.upper()]
    assert dump
    assert all("ROW_NUMBER" not in s.upper() for s in dump)
    assert any("ROWNUM" in s.upper() and ">" in s for s in dump)
    assert decode_logminer_token(batches[-1].resume_token)["scn"] == 1000
    assert decode_logminer_token(batches[0].resume_token)["last_pk"] == "3"


def test_logminer_legacy_offset_resume_uses_row_number() -> None:
    from connectors.oracle_logminer import OracleLogMinerCdc, encode_logminer_token

    token = encode_logminer_token(1000, table="ORDERS", phase="snapshot", offset=2)
    cdc = OracleLogMinerCdc(
        {"host": "localhost", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        batch_size=2,
        resume_token=token,
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("ID",), ("AMOUNT",), ("DF_RN",)]
    cur.fetchall.side_effect = [[("3", "30", 3)], []]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn), patch.object(cdc, "_acquire_cdc_lease"):
        list(cdc.snapshot())

    sqls = [str(c.args[0]) for c in cur.execute.call_args_list if c.args]
    assert any("ROW_NUMBER" in s.upper() for s in sqls)


def test_logminer_incremental_chunk_uses_keyset_not_row_number() -> None:
    from connectors.oracle_logminer import OracleLogMinerCdc

    cdc = OracleLogMinerCdc(
        {"host": "localhost", "username": "APP"},
        table="orders",
        primary_key="id",
        schema="APP",
        batch_size=2,
    )

    class _Sig:
        primary_key = "id"
        chunk_size = 2
        last_pk = "2"
        table = "orders"

    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("ID",), ("AMOUNT",)]
    cur.fetchall.return_value = [("3", "30")]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch.object(cdc, "_conn", return_value=conn):
        records, new_last, done = cdc._fetch_incremental_chunk(_Sig())

    sqls = [str(c.args[0]) for c in cur.execute.call_args_list if c.args]
    assert any(">" in s and "ROWNUM" in s.upper() for s in sqls)
    assert all("ROW_NUMBER" not in s.upper() for s in sqls)
    assert records[0]["ID"] == "3"
    assert new_last == "3"
    assert done is True
