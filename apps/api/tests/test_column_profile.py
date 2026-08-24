"""Engine-side column profiles must catch the silent corruptions at any scale.

The in-memory verification ladder refuses above VERIFICATION_LADDER_MAX_ROWS, so
on the largest tables the column-level checks that catch a silently nulled field
or a truncated numeric stop running. These tests hold the engine-side profile to
the same job the in-memory L2 does — detect a divergence and name the column —
and to the discipline that keeps it from crying wolf: a scale-only numeric
difference is not a divergence, and statistics that depend on collation or
summation order are not compared.
"""

from __future__ import annotations

import socket
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from services.column_profile import (
    build_profile_sql,
    classify_column,
    engine_profile_ladder,
    profile_engine_family,
    profile_supported,
    same_profile_family,
)

# --------------------------------------------------------------------------- #
# Pure logic — no database required
# --------------------------------------------------------------------------- #


def test_engine_families_are_recognised():
    assert profile_engine_family("PostgreSQL") == "postgresql"
    assert profile_engine_family("mariadb") == "mysql"
    assert profile_engine_family("oracle") == ""
    assert profile_supported("timescaledb")
    assert not profile_supported("snowflake")
    assert same_profile_family("postgres", "postgresql")
    assert same_profile_family("mysql", "mariadb")
    assert not same_profile_family("postgresql", "mysql")


@pytest.mark.parametrize(
    "declared,expected",
    [
        ("bigint", "exact_numeric"),
        ("integer", "exact_numeric"),
        ("numeric(12,2)", "exact_numeric"),
        ("decimal(10,4)", "exact_numeric"),
        ("money", "exact_numeric"),
        ("double precision", "float"),
        ("real", "float"),
        ("float", "float"),
        ("timestamp without time zone", "temporal_ts"),
        ("datetime", "temporal_ts"),
        ("date", "temporal_ts"),
        ("timestamp with time zone", "temporal_instant"),
        ("time without time zone", "temporal_time"),
        ("time", "temporal_time"),
        ("interval", "other"),
        ("text", "other"),
        ("varchar(255)", "other"),
        ("jsonb", "other"),
        ("boolean", "other"),
        ("bytea", "other"),
    ],
)
def test_column_classification(declared, expected):
    assert classify_column(declared) == expected


def test_timestamp_is_disambiguated_per_engine():
    """The trap: bare ``timestamp`` is a wall clock in PG, an instant in MySQL."""
    from services.column_profile import normalize_catalog_type

    assert classify_column(normalize_catalog_type("mysql", "timestamp")) == "temporal_instant"
    assert classify_column(normalize_catalog_type("mysql", "datetime")) == "temporal_ts"
    assert classify_column(
        normalize_catalog_type("postgresql", "timestamp without time zone")
    ) == "temporal_ts"
    assert classify_column(
        normalize_catalog_type("postgresql", "timestamp with time zone")
    ) == "temporal_instant"


def test_sql_takes_sum_only_for_exact_numeric():
    sql, plans = build_profile_sql(
        "postgresql",
        '"t"',
        ["id", "note", "amount", "ratio", "ts"],
        {
            "id": "bigint",
            "note": "text",
            "amount": "numeric(12,2)",
            "ratio": "double precision",
            "ts": "timestamp",
        },
    )
    # Exact numerics get sum; float/temporal get min/max but no sum; text neither.
    assert sql.count("sum(") == 2  # id, amount
    assert sql.count("min(") == 4  # id, amount, ratio, ts
    assert 'count("note")' in sql
    assert 'sum("note")' not in sql
    assert 'sum("ratio")' not in sql  # float sum is order-dependent


def test_mysql_dialect_quotes_and_casts():
    sql, _ = build_profile_sql("mysql", "`t`", ["id"], {"id": "int"})
    assert "`id`" in sql
    assert "CAST(sum(`id`) AS CHAR)" in sql


def test_cross_engine_decisions_keep_only_engine_independent_stats():
    """The heart of cross-engine safety, provable without a database."""
    from services.column_profile import _apply_decisions, _comparison_decisions
    from services.verification_ladder import ColumnAggregate

    profile = {
        "n": ColumnAggregate("n", null_count=1, non_null_count=9, distinct_count=None,
                             min_value="10.50", max_value="99.90", sum_value="123.40"),
        "f": ColumnAggregate("f", null_count=0, non_null_count=10, distinct_count=None,
                             min_value="1.5000", max_value="2.5000", sum_value=None),
        "ts": ColumnAggregate("ts", null_count=0, non_null_count=10, distinct_count=None,
                              min_value="2024-01-01T00:00:00.000000",
                              max_value="2024-12-31T23:59:59.000000", sum_value=None),
        "inst": ColumnAggregate("inst", null_count=0, non_null_count=10, distinct_count=None,
                                min_value="2024-01-01 00:00:00+00", max_value="x", sum_value=None),
        "s": ColumnAggregate("s", null_count=2, non_null_count=8, distinct_count=None,
                             min_value=None, max_value=None, sum_value=None),
    }
    kinds = {"n": "exact_numeric", "f": "float", "ts": "temporal_ts",
             "inst": "temporal_instant", "s": "other"}
    dec = _comparison_decisions(kinds, kinds, cross_engine=True)
    out = _apply_decisions(profile, dec)

    # NULL rate survives for every kind — the silent-null detector.
    assert [out[c].null_count for c in ("n", "f", "ts", "inst", "s")] == [1, 0, 0, 0, 2]
    # Numeric canonicalized to values.
    assert out["n"].sum_value == "123.4"
    assert (out["f"].min_value, out["f"].max_value) == ("1.5", "2.5")
    # Wall-clock temporal kept (ISO canonical compares across engines).
    assert out["ts"].min_value == "2024-01-01T00:00:00.000000"
    # Zone-aware instant dropped cross-engine (offset renders differently).
    assert out["inst"].min_value is None
    # Same-engine keeps the instant.
    same = _apply_decisions(profile, _comparison_decisions(kinds, kinds, cross_engine=False))
    assert same["inst"].min_value == "2024-01-01 00:00:00+00"


def test_mismatched_kinds_are_not_compared():
    """A source timestamp mapped onto a destination instant is not compared."""
    from services.column_profile import _comparison_decisions

    dec = _comparison_decisions(
        {"c": "temporal_ts"}, {"c": "temporal_instant"}, cross_engine=False
    )
    assert dec["c"] == "drop"


def test_cross_engine_scale_only_numeric_difference_is_not_a_divergence():
    from services.column_profile import _apply_decisions, _comparison_decisions
    from services.verification_ladder import ColumnAggregate, compare_column_aggregates

    src = {"amt": ColumnAggregate("amt", 0, 3, None, "0.10", "9.90", "10.00")}
    dst = {"amt": ColumnAggregate("amt", 0, 3, None, "0.1000", "9.9000", "10.0000")}
    kinds = {"amt": "exact_numeric"}
    dec = _comparison_decisions(kinds, kinds, cross_engine=True)
    l2 = compare_column_aggregates(
        _apply_decisions(src, dec), _apply_decisions(dst, dec)
    )
    assert l2.passed, l2.details


# --------------------------------------------------------------------------- #
# Live PostgreSQL
# --------------------------------------------------------------------------- #

_PG = {"host": "127.0.0.1", "port": 5432, "database": "dataflow",
       "username": "dataflow", "password": "dataflow"}
_TYPES = {"id": "bigint", "email": "text", "amount": "numeric(12,2)", "created_at": "timestamp"}


def _reachable(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


pg = pytest.mark.skipif(not _reachable(5432), reason="PostgreSQL not reachable")
my = pytest.mark.skipif(not _reachable(3306), reason="MySQL/MariaDB not reachable")


@pytest.fixture()
def pg_tables():
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(host="127.0.0.1", port=5432, dbname="dataflow",
                            user="dataflow", password="dataflow")
    conn.autocommit = True
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"cp_src_{sfx}", f"cp_dst_{sfx}"
    with conn.cursor() as cur:
        for t in (src, dst):
            cur.execute(
                f'CREATE TABLE "{t}" '
                "(id bigint, email text, amount numeric(12,2), created_at timestamp)"
            )
        cur.execute(
            f"INSERT INTO \"{src}\" SELECT g, 'e'||g, (g%1000)::numeric/100, "
            f"timestamp '2024-01-01' + (g||' seconds')::interval "
            "FROM generate_series(1, 3000) g"
        )
        cur.execute(f'INSERT INTO "{dst}" SELECT * FROM "{src}"')
    try:
        yield conn, src, dst
    finally:
        with conn.cursor() as cur:
            for t in (src, dst):
                cur.execute(f'DROP TABLE IF EXISTS "{t}"')
        conn.close()


def _pg_ladder(src, dst, *, source_rows=3000, target_rows=3000, pairs=None):
    return engine_profile_ladder(
        source_engine="postgresql", source_cfg=_PG, source_schema="public", source_table=src,
        dest_engine="postgresql", dest_cfg=_PG, dest_schema="public", dest_table=dst,
        pairs=pairs or [(c, c) for c in _TYPES], types=_TYPES,
        source_rows=source_rows, target_rows=target_rows,
    )


@pg
def test_identical_populations_pass_at_scale(pg_tables):
    _conn, src, dst = pg_tables
    ladder = _pg_ladder(src, dst)
    assert ladder is not None
    assert ladder["passed"]
    assert ladder["localization"]["columns"] == []
    assert ladder["assurance_level"] == "engine_column_profile"
    # It never claims the stronger proofs it did not run.
    assert ladder["population_checksum_proof"] is False


@pg
def test_a_silently_nulled_column_is_localized(pg_tables):
    """The Airbyte DESTINATION_TYPECAST_ERROR class: field quietly becomes NULL."""
    conn, src, dst = pg_tables
    with conn.cursor() as cur:
        cur.execute(f'UPDATE "{dst}" SET email = NULL WHERE id <= 300')
    ladder = _pg_ladder(src, dst)
    assert not ladder["passed"]
    assert ladder["localization"]["columns"] == ["email"]
    assert "email" in ladder["localization_summary"]


@pg
def test_a_numeric_drift_is_caught_by_sum_and_extremes(pg_tables):
    conn, src, dst = pg_tables
    with conn.cursor() as cur:
        cur.execute(f'UPDATE "{dst}" SET amount = amount + 0.01 WHERE id = 7')
    ladder = _pg_ladder(src, dst)
    assert not ladder["passed"]
    assert "amount" in ladder["localization"]["columns"]


@pg
def test_a_scale_only_difference_is_not_a_divergence(pg_tables):
    """numeric(12,2) 10.50 and a widened copy 10.5000 carry the same value."""
    conn, src, dst = pg_tables
    with conn.cursor() as cur:
        cur.execute(f'ALTER TABLE "{dst}" ALTER COLUMN amount TYPE numeric(14, 4)')
    ladder = _pg_ladder(src, dst)
    assert ladder["passed"], ladder["localization"]


@pg
def test_a_dropped_row_fails_l1(pg_tables):
    conn, src, dst = pg_tables
    with conn.cursor() as cur:
        cur.execute(f'DELETE FROM "{dst}" WHERE id = 7')
    ladder = _pg_ladder(src, dst, target_rows=2999)
    assert not ladder["layers"]["L1"]["passed"]


@pg
def test_a_rename_lines_the_profiles_up(pg_tables):
    conn, src, dst = pg_tables
    with conn.cursor() as cur:
        cur.execute(f'ALTER TABLE "{dst}" RENAME COLUMN email TO contact')
    pairs = [("id", "id"), ("email", "contact"),
             ("amount", "amount"), ("created_at", "created_at")]
    ladder = _pg_ladder(src, dst, pairs=pairs)
    assert ladder["passed"], ladder["localization"]


# --------------------------------------------------------------------------- #
# Live MySQL / MariaDB
# --------------------------------------------------------------------------- #

_MY = {"host": "127.0.0.1", "port": 3306, "database": "dataflow",
       "username": "dataflow", "password": "dataflow"}
_MY_TYPES = {"id": "bigint", "email": "varchar(255)",
             "amount": "decimal(12,2)", "created_at": "datetime"}


@pytest.fixture()
def my_tables():
    pymysql = pytest.importorskip("pymysql")
    conn = pymysql.connect(host="127.0.0.1", port=3306, user="dataflow",
                           password="dataflow", database="dataflow", autocommit=True)
    sfx = uuid.uuid4().hex[:6]
    src, dst = f"cp_src_{sfx}", f"cp_dst_{sfx}"
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows = [(i, f"e{i}", Decimal(i % 1000) / 100, base + timedelta(seconds=i))
            for i in range(1, 3001)]
    with conn.cursor() as cur:
        for t in (src, dst):
            cur.execute(
                f"CREATE TABLE `{t}` "
                "(id bigint, email varchar(255), amount decimal(12,2), created_at datetime)"
            )
        cur.executemany(
            f"INSERT INTO `{src}` (id,email,amount,created_at) VALUES (%s,%s,%s,%s)", rows
        )
        cur.execute(f"INSERT INTO `{dst}` SELECT * FROM `{src}`")
    try:
        yield conn, src, dst
    finally:
        with conn.cursor() as cur:
            for t in (src, dst):
                cur.execute(f"DROP TABLE IF EXISTS `{t}`")
        conn.close()


def _my_ladder(src, dst, *, source_rows=3000, target_rows=3000):
    return engine_profile_ladder(
        source_engine="mysql", source_cfg=_MY, source_schema="dataflow", source_table=src,
        dest_engine="mariadb", dest_cfg=_MY, dest_schema="dataflow", dest_table=dst,
        pairs=[(c, c) for c in _MY_TYPES], types=_MY_TYPES,
        source_rows=source_rows, target_rows=target_rows,
    )


@my
def test_mysql_identical_populations_pass(my_tables):
    _conn, src, dst = my_tables
    ladder = _my_ladder(src, dst)
    assert ladder is not None
    assert ladder["passed"]


@my
def test_mysql_silently_nulled_column_is_localized(my_tables):
    conn, src, dst = my_tables
    with conn.cursor() as cur:
        cur.execute(f"UPDATE `{dst}` SET email = NULL WHERE id <= 200")
    ladder = _my_ladder(src, dst)
    assert not ladder["passed"]
    assert ladder["localization"]["columns"] == ["email"]


@my
def test_mysql_numeric_drift_is_caught(my_tables):
    conn, src, dst = my_tables
    with conn.cursor() as cur:
        cur.execute(f"UPDATE `{dst}` SET amount = amount + 0.01 WHERE id = 9")
    ladder = _my_ladder(src, dst)
    assert not ladder["passed"]
    assert "amount" in ladder["localization"]["columns"]


# --------------------------------------------------------------------------- #
# Cross-engine: PostgreSQL ↔ MySQL/MariaDB (the supervisor parity primitive)
# --------------------------------------------------------------------------- #

_X_TYPES = {"id": "bigint", "email": "varchar(255)",
            "amount": "decimal(12,2)", "created_at": "datetime"}


@pytest.fixture()
def cross_tables():
    """A PostgreSQL source and a MySQL/MariaDB destination holding one dataset."""
    psycopg2 = pytest.importorskip("psycopg2")
    pymysql = pytest.importorskip("pymysql")
    pg = psycopg2.connect(host="127.0.0.1", port=5432, dbname="dataflow",
                          user="dataflow", password="dataflow")
    pg.autocommit = True
    my = pymysql.connect(host="127.0.0.1", port=3306, user="dataflow",
                         password="dataflow", database="dataflow", autocommit=True)
    sfx = uuid.uuid4().hex[:6]
    src, dst = f"xe_src_{sfx}", f"xe_dst_{sfx}"
    # Naive wall clock: a PG ``timestamp`` and a MySQL ``datetime`` store these
    # identical components, which is exactly the cross-engine case under test.
    base = datetime(2024, 1, 1)  # noqa: DTZ001 — wall-clock, no zone, by design
    rows = [(i, f"e{i}", Decimal(i % 1000) / 100, base + timedelta(seconds=i))
            for i in range(1, 2001)]
    with pg.cursor() as cur:
        cur.execute(
            f'CREATE TABLE "{src}" '
            "(id bigint, email text, amount numeric(12,2), created_at timestamp)"
        )
        cur.executemany(f'INSERT INTO "{src}" VALUES (%s,%s,%s,%s)', rows)
    with my.cursor() as cur:
        cur.execute(
            f"CREATE TABLE `{dst}` "
            "(id bigint, email varchar(255), amount decimal(12,2), created_at datetime)"
        )
        cur.executemany(
            f"INSERT INTO `{dst}` (id,email,amount,created_at) VALUES (%s,%s,%s,%s)", rows
        )
    try:
        yield pg, my, src, dst
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst}`")
        pg.close()
        my.close()


def _cross_ladder(src, dst, *, source_rows=2000, target_rows=2000):
    return engine_profile_ladder(
        source_engine="postgresql", source_cfg=_PG, source_schema="public", source_table=src,
        dest_engine="mysql", dest_cfg=_MY, dest_schema="dataflow", dest_table=dst,
        pairs=[(c, c) for c in _X_TYPES], types=_X_TYPES,
        source_rows=source_rows, target_rows=target_rows,
    )


@pg
@my
def test_cross_engine_identical_populations_pass(cross_tables):
    _pg, _my, src, dst = cross_tables
    ladder = _cross_ladder(src, dst)
    assert ladder is not None
    assert ladder["cross_engine"] is True
    assert ladder["passed"], ladder["localization"]
    # Wall-clock temporals ARE compared across engines (ISO-canonical), so a
    # matching datetime column is not a divergence and not in the declined list.
    compared = " ".join(ladder["layers"]["L2"]["details"]["compared_statistics"])
    assert "wall-clock" in compared
    assert "created_at" not in ladder["localization"]["columns"]


@pg
@my
def test_cross_engine_silently_nulled_column_is_localized(cross_tables):
    _pg, my, src, dst = cross_tables
    with my.cursor() as cur:
        cur.execute(f"UPDATE `{dst}` SET email = NULL WHERE id <= 150")
    ladder = _cross_ladder(src, dst)
    assert not ladder["passed"]
    assert ladder["localization"]["columns"] == ["email"]


@pg
@my
def test_cross_engine_numeric_drift_is_caught(cross_tables):
    _pg, my, src, dst = cross_tables
    with my.cursor() as cur:
        cur.execute(f"UPDATE `{dst}` SET amount = amount + 0.01 WHERE id = 11")
    ladder = _cross_ladder(src, dst)
    assert not ladder["passed"]
    assert "amount" in ladder["localization"]["columns"]


@pg
@my
def test_cross_engine_dropped_row_fails_l1(cross_tables):
    _pg, my, src, dst = cross_tables
    with my.cursor() as cur:
        cur.execute(f"DELETE FROM `{dst}` WHERE id = 11")
    ladder = _cross_ladder(src, dst, target_rows=1999)
    assert not ladder["layers"]["L1"]["passed"]


@pg
@my
def test_cross_engine_zone_aware_instant_is_declined_not_false_flagged():
    """A PG ``timestamptz`` and a MySQL ``timestamp`` render their offset
    differently, so their min/max is declined cross-engine — never a false flag.
    NULL and row parity still hold."""
    psycopg2 = pytest.importorskip("psycopg2")
    pymysql = pytest.importorskip("pymysql")
    pgc = psycopg2.connect(host="127.0.0.1", port=5432, dbname="dataflow",
                           user="dataflow", password="dataflow")
    pgc.autocommit = True
    myc = pymysql.connect(host="127.0.0.1", port=3306, user="dataflow",
                          password="dataflow", database="dataflow", autocommit=True)
    sfx = uuid.uuid4().hex[:6]
    src, dst = f"xi_src_{sfx}", f"xi_dst_{sfx}"
    try:
        with pgc.cursor() as cur:
            cur.execute(f'CREATE TABLE "{src}" (id bigint, seen timestamptz)')
            cur.execute(
                f"INSERT INTO \"{src}\" SELECT g, timestamptz '2024-01-01 00:00:00+00' "
                f"+ (g||' seconds')::interval FROM generate_series(1, 500) g"
            )
        with myc.cursor() as cur:
            cur.execute(f"CREATE TABLE `{dst}` (id bigint, seen timestamp NULL)")
            cur.executemany(
                f"INSERT INTO `{dst}` (id, seen) VALUES (%s, %s)",
                [(i, datetime(2024, 1, 1) + timedelta(seconds=i))  # noqa: DTZ001
                 for i in range(1, 501)],
            )
        ladder = engine_profile_ladder(
            source_engine="postgresql", source_cfg=_PG, source_schema="public",
            source_table=src, dest_engine="mysql", dest_cfg=_MY, dest_schema="dataflow",
            dest_table=dst, pairs=[("id", "id"), ("seen", "seen")],
            types={"id": "bigint", "seen": "timestamp"},
        )
        assert ladder is not None and ladder["cross_engine"] is True
        # Row + NULL parity hold; the instant column is declined, not flagged.
        assert ladder["layers"]["L1"]["passed"]
        assert "seen" not in ladder["localization"]["columns"]
        declined = " ".join(ladder["layers"]["L2"]["details"]["not_compared"])
        assert "zone-aware" in declined
    finally:
        with pgc.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
        with myc.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dst}`")
        pgc.close()
        myc.close()


@pg
@my
def test_cross_engine_wall_clock_temporal_drift_is_caught(cross_tables):
    """A timestamp that lost its clock is caught even across engines.

    PostgreSQL ``timestamp`` and MySQL ``datetime`` are both wall-clock; each
    engine renders min/max to the same ISO shape in SQL, so a datetime moved to a
    different instant on the destination is a real, localized divergence — the
    corruption class this increment exists to close.
    """
    _pg, my, src, dst = cross_tables
    with my.cursor() as cur:
        cur.execute(f"UPDATE `{dst}` SET created_at = '1999-01-01 00:00:00' WHERE id = 5")
    ladder = _cross_ladder(src, dst)
    assert not ladder["passed"]
    assert "created_at" in ladder["localization"]["columns"]
