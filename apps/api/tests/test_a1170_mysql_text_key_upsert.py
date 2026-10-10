"""A-1170 — PG→MySQL composite incremental_upsert hung for minutes.

The upsert key (region TEXT, id INT) was emitted as ``UNIQUE KEY (region, id)``
on a TEXT column. MySQL refuses that with error 1170 ("BLOB/TEXT column used in
key specification without a key length"); pymysql wraps it in
``OperationalError``, and ``is_connection_lost`` matched the wrapper class name,
so the writer reconnect-retried a deterministic DDL refusal for the whole
reconnect budget (12 attempts / 600 s).

Fixed at both halves: MySQL errors are classified by their error code, and a
LOB key column gets a ``VARCHAR(n)`` carrier from the source's declared width,
or the run is refused before any DDL when no width is declared.
"""

from __future__ import annotations

import threading
import time

import pymysql
import pytest

from connectors.write_resilience import is_connection_lost
from tests.helpers.live_env import mysql_creds, mysql_up


@pytest.mark.parametrize(
    "code,message,lost",
    [
        (1170, "BLOB/TEXT column 'region' used in key specification without a key length", False),
        (1071, "Specified key was too long; max key length is 3072 bytes", False),
        (1064, "You have an error in your SQL syntax", False),
        (1205, "Lock wait timeout exceeded; try restarting transaction", False),
        (2013, "Lost connection to MySQL server during query", True),
        (2006, "MySQL server has gone away", True),
        (2003, "Can't connect to MySQL server on '127.0.0.1'", True),
        (4031, "The client was disconnected by the server because of inactivity", True),
    ],
)
def test_mysql_errors_are_classified_by_code_not_by_wrapper(code, message, lost):
    assert is_connection_lost(pymysql.err.OperationalError(code, message)) is lost


@pytest.mark.parametrize("code,lost", [(1170, False), (1071, False), (2013, True), (2006, True)])
def test_sqlalchemy_wrapped_mysql_errors_are_classified_by_the_driver_code(code, lost):
    from sqlalchemy import exc as sa_exc

    orig = pymysql.err.OperationalError(code, "driver message")
    wrapped = sa_exc.OperationalError("CREATE TABLE t (...)", {}, orig)
    assert type(wrapped).__module__ == "sqlalchemy.exc"
    assert is_connection_lost(wrapped) is lost
    assert is_connection_lost(sa_exc.DBAPIError("stmt", {}, orig)) is lost


@pytest.mark.parametrize("code,lost", [(1170, False), (2013, True)])
def test_chained_mysql_errors_are_classified_by_the_driver_code(code, lost):
    class WriterSetupError(RuntimeError):
        pass

    try:
        try:
            raise pymysql.err.OperationalError(code, "driver message")
        except pymysql.err.OperationalError as driver_exc:
            raise WriterSetupError("connection setup failed") from driver_exc
    except WriterSetupError as chained:
        assert is_connection_lost(chained) is lost


def test_cyclic_error_chain_is_bounded():
    a, b = RuntimeError("operational a"), RuntimeError("operational b")
    a.__cause__, b.__cause__ = b, a
    assert is_connection_lost(a) is False


def test_closed_socket_without_a_code_stays_a_lost_connection():
    assert is_connection_lost(pymysql.err.InterfaceError(0, "")) is True


def test_lob_key_column_is_sized_from_the_declared_source_width():
    from services.schema_fidelity import mysql_key_compatible_types

    types, refusal = mysql_key_compatible_types(
        table_name="t",
        conflict_columns=["region", "id"],
        target_cols=["region", "id", "note"],
        target_types=["TEXT", "BIGINT", "TEXT"],
        mappings=[{"source": "src_region", "target": "region"}, {"source": "id", "target": "id"}],
        column_types={"src_region": "character varying(16)", "id": "integer"},
    )
    assert refusal is None
    assert types == ["VARCHAR(16)", "BIGINT", "TEXT"]  # non-key LOB untouched


def test_key_wider_than_the_mysql_index_limit_is_refused():
    from services.schema_fidelity import mysql_key_compatible_types

    _types, refusal = mysql_key_compatible_types(
        table_name="t",
        conflict_columns=["a", "b"],
        target_cols=["a", "b"],
        target_types=["TEXT", "TEXT"],
        mappings=[],
        column_types={"a": "varchar(600)", "b": "varchar(600)"},
    )
    assert refusal and "1071" in refusal and "3072" in refusal


mysql_live = pytest.mark.skipif(not mysql_up(), reason="MySQL (MYSQL_* env) not reachable")


def _write(table: str, region_type: str) -> tuple[object, float]:
    from connectors.mysql_writer import write_mapped_rows

    creds = mysql_creds()
    box: dict[str, object] = {}

    def _go() -> None:
        box["result"] = write_mapped_rows(
            host=creds["host"], port=creds["port"], database=creds["database"],
            username=creds["username"], password=creds["password"],
            schema="", connection_string="", ssl=False, table_name=table,
            headers=["region", "id", "amount"],
            data_rows=[["eu", "1", "1.50"], ["us", "1", "2.50"]],
            mappings=[{"source": h, "target": h} for h in ("region", "id", "amount")],
            column_types={"region": region_type, "id": "integer", "amount": "numeric(10,2)"},
            write_mode="upsert",
            conflict_columns=["region", "id"],
        )

    started = time.monotonic()
    worker = threading.Thread(target=_go, daemon=True)
    worker.start()
    worker.join(timeout=60)
    assert not worker.is_alive(), "MySQL writer still retrying after 60 s (1170 treated as a dropped socket)"
    return box["result"], time.monotonic() - started


def _mysql():
    creds = mysql_creds()
    return pymysql.connect(
        host=creds["host"], port=creds["port"], user=creds["username"],
        password=creds["password"], database=creds["database"], autocommit=True,
    )


@mysql_live
def test_live_text_key_without_a_width_is_refused_fast_before_ddl():
    table = "a1170_text_key"
    with _mysql() as conn:
        conn.cursor().execute(f"DROP TABLE IF EXISTS `{table}`")
    result, seconds = _write(table, "text")
    assert result.ok is False
    assert seconds < 30
    assert "region" in result.error and "1170" in result.error and "VARCHAR(n)" in result.error
    with _mysql() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = DATABASE() AND table_name = %s", (table,))
        assert cur.fetchone()[0] == 0, "refusal must come before any DDL"


@mysql_live
def test_live_declared_width_key_upserts_twice_with_a_unique_key():
    table = "a1170_varchar_key"
    with _mysql() as conn:
        conn.cursor().execute(f"DROP TABLE IF EXISTS `{table}`")
    first, _ = _write(table, "character varying(8)")
    assert first.ok, first.error
    second, _ = _write(table, "character varying(8)")
    assert second.ok, second.error
    with _mysql() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM `{table}`")
        assert cur.fetchone()[0] == 2
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() "
            "AND table_name = %s AND non_unique = 0", (table,),
        )
        assert cur.fetchone()[0] >= 2  # (region, id) unique index
