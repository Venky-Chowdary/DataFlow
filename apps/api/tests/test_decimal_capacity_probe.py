"""An unconstrained numeric column is measured, not guessed.

PostgreSQL ``numeric`` with no typmod declares no bound, so comparing it against
any destination carrier concludes the destination is narrower. That refuses
routes which would have moved every row intact — the column almost always holds
ordinary money or quantity values. Measuring the column answers the question
with proof instead, and the answer has to cut both ways: data that genuinely
does not fit must still be refused.
"""

from __future__ import annotations

import socket
import uuid

import pytest

from services.decimal_capacity_probe import (
    DecimalCapacity,
    unconstrained_decimal_columns,
)


def _pg_reachable() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_reachable(), reason="PostgreSQL not reachable on 127.0.0.1:5432"
)


@pytest.fixture()
def pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )
    try:
        yield conn
    finally:
        conn.close()


def test_capacity_renders_a_carrier_that_holds_the_value():
    assert DecimalCapacity(int_digits=4, scale=2).as_type() == "DECIMAL(6,2)"
    assert DecimalCapacity(int_digits=1, scale=0).as_type() == "DECIMAL(1,0)"
    assert DecimalCapacity(int_digits=30, scale=9).as_type() == "DECIMAL(39,9)"


def test_only_undeclared_decimals_are_probed():
    columns = [
        {"name": "bare", "inferred_type": "DECIMAL"},
        {"name": "declared", "inferred_type": "DECIMAL(12,2)"},
        {"name": "scale_only", "inferred_type": "DECIMAL(10)"},
        {"name": "text", "inferred_type": "VARCHAR(10)"},
        {"name": "int", "inferred_type": "INT4"},
    ]
    assert unconstrained_decimal_columns(columns) == ["bare"]


def _make_table(conn, rows: list[str]) -> str:
    name = "cap_probe_" + uuid.uuid4().hex[:10]
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE "{name}" (id int, amount numeric)')
        for i, value in enumerate(rows, start=1):
            cur.execute(f'INSERT INTO "{name}" VALUES (%s, {value})', (i,))
    conn.commit()
    return name


def _measured_type(conn, table: str) -> tuple[str, bool]:
    from services.schema_introspect import introspect_schema

    info = introspect_schema(
        "postgresql",
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        table=table,
    )
    for col in info.get("columns", []):
        if col.get("name") == "amount":
            return (
                str(col.get("inferred_type")),
                bool(col.get("decimal_capacity_measured")),
            )
    raise AssertionError(f"amount column missing from introspect: {info}")


def test_ordinary_money_column_measures_to_a_narrow_carrier(pg_conn):
    table = _make_table(pg_conn, ["1000.00", "2000.50", "-30.25"])
    try:
        declared, measured = _measured_type(pg_conn, table)
        assert measured is True
        assert declared == "DECIMAL(6,2)"
    finally:
        with pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE "{table}"')
        pg_conn.commit()


def test_a_column_that_really_is_wide_measures_wide(pg_conn):
    """The probe must be able to refuse, or it is just a different guess."""
    table = _make_table(
        pg_conn, ["1.5", "123456789012345678901234567890.123456789"]
    )
    try:
        declared, measured = _measured_type(pg_conn, table)
        assert measured is True
        assert declared == "DECIMAL(39,9)"

        from services.type_system import decimal_params_would_narrow

        # 39 digits does not fit Snowflake's 38, and the measurement is what
        # proves it — the bare type could only ever have said "unknown".
        assert (
            decimal_params_would_narrow(
                declared, "NUMBER(38,9)", dest_db="snowflake"
            )
            is True
        )
    finally:
        with pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE "{table}"')
        pg_conn.commit()


def test_empty_column_proves_nothing_and_stays_bare(pg_conn):
    table = _make_table(pg_conn, ["NULL", "NULL"])
    try:
        declared, measured = _measured_type(pg_conn, table)
        assert measured is False
        assert declared == "DECIMAL"
    finally:
        with pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE "{table}"')
        pg_conn.commit()


def test_sub_unit_values_keep_room_for_the_leading_zero(pg_conn):
    table = _make_table(pg_conn, ["0.5", "0.25"])
    try:
        declared, _ = _measured_type(pg_conn, table)
        assert declared == "DECIMAL(3,2)"
    finally:
        with pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE "{table}"')
        pg_conn.commit()
