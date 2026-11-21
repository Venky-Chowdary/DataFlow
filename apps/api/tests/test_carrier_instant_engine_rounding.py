"""Gate-8's instant model equals what each engine stores on a narrowing.

``carrier_instant.quantize_instant_for_carrier`` predicts the instant a
destination keeps when a value with more fractional digits lands in a narrower
carrier. The destination is the truth of what landed, so this module writes
boundary instants through each engine's real writer, reads them back and
requires the model to equal the stored value — then Gate-8 reports neither a
false difference nor hides a real one.

Measured rules: PostgreSQL rounds half away from zero around 2000-01-01 (its
timestamp epoch), so ``1999-12-31 23:59:59.9995`` keeps ``.999``; MySQL rounds
half up; MariaDB truncates unless the session has ``TIME_ROUND_FRACTIONAL``,
which the MySQL-family writer now sets; SQL Server ``datetime2`` rounds half up.
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parity_engines  # noqa: E402
from services.carrier_instant import quantize_instant_for_carrier  # noqa: E402

BOUNDARY = [
    datetime(1969, 12, 31, 23, 59, 59, 999500),
    datetime(1985, 7, 1, 12, 0, 0, 499500),
    datetime(1999, 12, 31, 23, 59, 59, 999500),
    datetime(1999, 12, 31, 23, 59, 59, 998500),
    datetime(2000, 1, 1, 0, 0, 0, 500),
    datetime(2024, 1, 1, 0, 0, 0, 123500),
    datetime(2024, 6, 30, 23, 59, 59, 999600),
    datetime(1950, 3, 3, 3, 3, 3, 500000),
    datetime(2030, 3, 3, 3, 3, 3, 500000),
]


def q(value, ddl, engine):
    return quantize_instant_for_carrier(value, ddl_type=ddl, engine=engine)


def test_postgresql_rounds_half_away_from_zero_around_2000() -> None:
    assert q(datetime(1999, 12, 31, 23, 59, 59, 999500), "timestamp(3)", "postgresql") == datetime(
        1999, 12, 31, 23, 59, 59, 999000)
    assert q(datetime(1969, 12, 31, 23, 59, 59, 999500), "timestamp(3)", "postgresql") == datetime(
        1969, 12, 31, 23, 59, 59, 999000)
    assert q(datetime(2000, 1, 1, 0, 0, 0, 500), "timestamp(3)", "postgresql") == datetime(2000, 1, 1, 0, 0, 0, 1000)
    assert q(datetime(2024, 1, 1, 0, 0, 0, 123500), "timestamp(3)", "postgresql") == datetime(
        2024, 1, 1, 0, 0, 0, 124000)
    aware = datetime(2000, 1, 1, 1, 59, 59, 999500, tzinfo=timezone(timedelta(hours=2)))  # 23:59:59.9995 UTC
    got = q(aware, "timestamp(3) with time zone", "postgresql")
    assert got.astimezone(timezone.utc) == datetime(1999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc)


def test_mysql_and_sqlserver_round_half_up_on_the_timeline() -> None:
    for engine, ddl in (("mysql", "DATETIME(3)"), ("sqlserver", "DATETIME2(3)")):
        assert q(datetime(1999, 12, 31, 23, 59, 59, 999500), ddl, engine) == datetime(2000, 1, 1)
        assert q(datetime(1969, 12, 31, 23, 59, 59, 999500), ddl, engine) == datetime(1970, 1, 1)


CARRIERS = {
    "postgresql": [("timestamp(3)", 3), ("timestamp(0)", 0), ("timestamptz(3)", 3)],
    "mysql": [("DATETIME(3)", 3), ("DATETIME(0)", 0)],
    "mariadb": [("DATETIME(3)", 3), ("DATETIME(0)", 0)],
    "sqlserver": [("DATETIME2(3)", 3), ("DATETIME2(0)", 0)],
}
#: The product names a MariaDB destination ``mysql`` (connector_capabilities).
PRODUCT_ENGINE = {"mariadb": "mysql"}
LIVE = [e for e in CARRIERS if parity_engines.available(e)]


def _session(engine: str):
    """A connection set up the way the product's write path sets it up."""
    conn = parity_engines.connect(engine)
    if engine in {"mysql", "mariadb"}:
        from connectors.write_resilience import apply_mysql_session_guards

        apply_mysql_session_guards(conn)
    return conn


def _literal(value: datetime, engine: str) -> str:
    text = value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f") if value.tzinfo else value.strftime(
        "%Y-%m-%d %H:%M:%S.%f")
    if engine == "postgresql":
        return f"'{text}{'+00' if value.tzinfo else ''}'"
    if engine == "sqlserver":
        return f"CAST('{text}' AS datetime2(7))"
    return f"'{text}'"


@pytest.mark.parametrize("engine", LIVE)
def test_the_model_equals_what_the_engine_stores(engine) -> None:
    """The engine narrows (COPY, LOAD DATA and engine-copy routes hand it full
    precision text); the Python writer instead holds such a row out
    ("refuse silent truncate"), so engine narrowing is where the model matters."""
    from services.reconciliation import fingerprint_for_reconcile

    product = PRODUCT_ENGINE.get(engine, engine)
    for ddl, _digits in CARRIERS[engine]:
        aware = "tz" in ddl
        values = [v.replace(tzinfo=timezone.utc) if aware else v for v in BOUNDARY]
        table = f"psd1_inst_{uuid.uuid4().hex[:8]}"
        conn = _session(engine)
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, ts {ddl})")
                for i, value in enumerate(values):
                    cur.execute(f"INSERT INTO {table} VALUES ({i}, {_literal(value, engine)})")
                cur.execute(f"SELECT id, ts FROM {table} ORDER BY id")
                stored = dict(cur.fetchall())
                cur.execute(f"DROP TABLE {table}")
            conn.commit()
        finally:
            conn.close()
        for i, value in enumerate(values):
            model = q(value, ddl, product)
            landed = stored[i]
            if aware:
                assert model.astimezone(timezone.utc) == landed.astimezone(timezone.utc), (engine, ddl, value, landed)
            else:
                assert model == landed.replace(tzinfo=None), (engine, ddl, value, model, landed)
            # Gate-8: the source instant and what landed fingerprint alike.
            assert fingerprint_for_reconcile(value, ddl_type=ddl, engine=product) == fingerprint_for_reconcile(
                landed, ddl_type=ddl, engine=product), (engine, ddl, value, landed)


@pytest.mark.skipif("mariadb" not in LIVE, reason="MariaDB not configured (DATAFLOW_TEST_MARIADB_HOST)")
def test_mariadb_truncates_without_the_write_session_mode() -> None:
    """Why the writer sets TIME_ROUND_FRACTIONAL: a bare MariaDB session truncates."""
    conn = parity_engines.connect("mariadb")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            if "mariadb" not in str(cur.fetchone()[0]).lower():
                pytest.skip("the MariaDB leg is not a MariaDB server")
            cur.execute("SET SESSION sql_mode = REPLACE(@@sql_mode, 'TIME_ROUND_FRACTIONAL', '')")
            cur.execute("SELECT CAST('2024-01-01 00:00:00.1236' AS DATETIME(3))")
            assert cur.fetchone()[0] == datetime(2024, 1, 1, 0, 0, 0, 123000)
    finally:
        conn.close()
