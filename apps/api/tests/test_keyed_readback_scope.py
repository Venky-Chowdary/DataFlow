"""Keyed (batch-scoped) destination read-back for append/upsert Gate-8.

Appending into a populated destination makes whole-table digests
incomparable — the target legitimately holds rows this run never wrote.
Re-reading only the written keys restores per-cell proof of the batch while
the row count stays whole-table. These tests pin the two ways that silently
degrades: an uncast key type that aborts the query (``integer = text``), and
a key scope that quietly widens to the whole table.
"""

from __future__ import annotations

import os
import sqlite3
import uuid

import pytest

from services.reconciliation import (
    KEYED_READBACK_ID_CAP,
    keyed_readback_sa_clause,
    keyed_readback_scope,
    keyed_readback_where,
    verify_target,
)


def test_scope_requires_both_ids_and_pk():
    assert keyed_readback_scope(["1", "2"], "id") == (["1", "2"], "id")
    assert keyed_readback_scope(["1"], "") == ([], "")
    assert keyed_readback_scope([], "id") == ([], "")
    assert keyed_readback_scope(None, "id") == ([], "")


def test_scope_drops_unusable_ids_and_caps():
    ids, pk = keyed_readback_scope([None, "", "7", 8], " id ")
    assert (ids, pk) == (["7", "8"], "id")

    many, _ = keyed_readback_scope([str(i) for i in range(KEYED_READBACK_ID_CAP + 50)], "id")
    assert len(many) == KEYED_READBACK_ID_CAP


@pytest.mark.parametrize(
    ("dialect", "expected_quote", "expected_cast"),
    [
        ("postgresql", '"id"', "VARCHAR(4000)"),
        ("mysql", "`id`", "CHAR"),
        ("sqlserver", "[id]", "NVARCHAR(4000)"),
        ("oracle", '"id"', "VARCHAR2(4000)"),
        ("clickhouse", "`id`", "String"),
        ("databricks", '"id"', "STRING"),
    ],
)
def test_where_casts_and_quotes_per_dialect(dialect, expected_quote, expected_cast):
    where = keyed_readback_where("id", ["1", "2"], dialect=dialect, placeholders=["%s", "%s"])
    assert where == f"WHERE CAST({expected_quote} AS {expected_cast}) IN (%s,%s)"


def test_where_never_inlines_key_values():
    where = keyed_readback_where(
        "id", ["1; DROP TABLE t", "2"], dialect="postgresql", placeholders=["%s", "%s"]
    )
    assert "DROP TABLE" not in where


def test_where_trims_surplus_placeholders():
    where = keyed_readback_where(
        "id", ["1"], dialect="postgresql", placeholders=["%s", "%s", "%s"]
    )
    assert where.endswith("IN (%s)")


def test_sa_clause_binds_every_key():
    where, params = keyed_readback_sa_clause("id", ["a", "b"], dialect="oracle")
    assert where == 'WHERE CAST("id" AS VARCHAR2(4000)) IN (:k0,:k1)'
    assert params == {"k0": "a", "k1": "b"}


def _sqlite_dest(tmp_path, rows):
    db = tmp_path / "dest.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", rows)
    conn.commit()
    conn.close()
    return {"connection_string": str(db), "database": str(db)}


def test_sqlite_append_fingerprints_only_written_keys(tmp_path):
    """Pre-existing rows must not enter the batch digest, but must be counted."""
    dest = _sqlite_dest(tmp_path, [("1", "old"), ("2", "new")])

    batch_count, batch_chk = verify_target(
        "sqlite",
        dest,
        schema="",
        table_name="t",
        fallback_rows=0,
        fallback_checksum="",
        written_ids=["2"],
        pk_column="id",
    )
    whole_count, whole_chk = verify_target(
        "sqlite",
        dest,
        schema="",
        table_name="t",
        fallback_rows=0,
        fallback_checksum="",
    )

    assert batch_count == whole_count == 2  # cardinality stays whole-table
    assert batch_chk and batch_chk != whole_chk


def test_sqlite_unkeyed_batch_falls_back_to_whole_table(tmp_path):
    dest = _sqlite_dest(tmp_path, [("1", "old"), ("2", "new")])
    kwargs = dict(
        schema="", table_name="t", fallback_rows=0, fallback_checksum=""
    )
    no_pk = verify_target("sqlite", dest, written_ids=["2"], pk_column=None, **kwargs)
    whole = verify_target("sqlite", dest, **kwargs)
    assert no_pk == whole


@pytest.mark.skipif(
    os.environ.get("SKIP_LIVE_PG") == "1", reason="live postgres disabled"
)
@pytest.mark.parametrize(
    ("pk_ddl", "keys"),
    [
        ("BIGINT", ["1", "2"]),
        ("INTEGER", ["1", "2"]),
        ("UUID", [str(uuid.uuid4()), str(uuid.uuid4())]),
        ("TEXT", ["a", "b"]),
    ],
)
def test_postgres_keyed_readback_survives_non_text_pk(pk_ddl, keys):
    """``CAST(pk AS text)`` — an uncast bigint/uuid aborts with 42883 and the
    read-back returns no proof at all, silently downgrading the verdict."""
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            dbname="dataflow",
            user="postgres",
            password="postgres",  # nosec B106 - local test container
            connect_timeout=3,
        )
    except psycopg2.Error:  # pragma: no cover - env without local postgres
        pytest.skip("local postgres unavailable")

    table = f"keyed_rb_{uuid.uuid4().hex[:8]}"
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id {pk_ddl} PRIMARY KEY, name TEXT)")
        for i, k in enumerate(keys):
            cur.execute(f"INSERT INTO {table} VALUES (%s, %s)", (k, f"row{i}"))
    dest = {
        "host": "localhost",
        "port": 5433,
        "database": "dataflow",
        "username": "postgres",
        "password": "postgres",  # nosec B106 - local test container
    }
    try:
        count, chk = verify_target(
            "postgresql",
            dest,
            schema="public",
            table_name=table,
            fallback_rows=0,
            fallback_checksum="",
            written_ids=[keys[0]],
            pk_column="id",
        )
        _, whole = verify_target(
            "postgresql",
            dest,
            schema="public",
            table_name=table,
            fallback_rows=0,
            fallback_checksum="",
        )
        assert count == 2
        assert chk, f"{pk_ddl} keyed read-back returned no digest"
        assert chk != whole
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


@pytest.mark.skipif(
    os.environ.get("SKIP_LIVE_MYSQL") == "1", reason="live mysql disabled"
)
def test_mysql_keyed_readback_survives_integer_pk():
    pymysql = pytest.importorskip("pymysql")
    try:
        conn = pymysql.connect(
            host="127.0.0.1",
            port=3307,
            user="root",
            password=os.environ.get("DF_TEST_MYSQL_PASSWORD", "mysql"),
            database="dataflow",
            connect_timeout=3,
        )
    except pymysql.Error:  # pragma: no cover - env without local mysql
        pytest.skip("local mysql unavailable")

    table = f"keyed_rb_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name VARCHAR(32))")
        cur.execute(f"INSERT INTO {table} VALUES (1, 'old'), (2, 'new')")
    conn.commit()
    dest = {
        "host": "127.0.0.1",
        "port": 3307,
        "database": "dataflow",
        "username": "root",
        "password": os.environ.get("DF_TEST_MYSQL_PASSWORD", "mysql"),
    }
    try:
        count, chk = verify_target(
            "mysql",
            dest,
            schema="",
            table_name=table,
            fallback_rows=0,
            fallback_checksum="",
            written_ids=["2"],
            pk_column="id",
        )
        _, whole = verify_target(
            "mysql",
            dest,
            schema="",
            table_name=table,
            fallback_rows=0,
            fallback_checksum="",
        )
        assert count == 2
        assert chk and chk != whole
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()
        conn.close()


def test_sqlserver_keyed_readback_survives_integer_pk():
    pymssql = pytest.importorskip("pymssql")
    password = os.environ.get("DF_TEST_MSSQL_PASSWORD", "Dataflow!2345")
    try:
        conn = pymssql.connect(
            server="127.0.0.1",
            port=1433,
            user="sa",
            password=password,
            database="master",
            login_timeout=5,
        )
    except pymssql.Error:  # pragma: no cover - env without local sql server
        pytest.skip("local sql server unavailable")

    table = f"keyed_rb_{uuid.uuid4().hex[:8]}"
    conn.autocommit(True)
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE dbo.{table} (id BIGINT PRIMARY KEY, name NVARCHAR(32))")
    cur.execute(f"INSERT INTO dbo.{table} VALUES (1, 'old'), (2, 'new')")
    dest = {
        "host": "127.0.0.1",
        "port": 1433,
        "database": "master",
        "username": "sa",
        "password": password,
        "schema": "dbo",
    }
    try:
        count, chk = verify_target(
            "sqlserver",
            dest,
            schema="dbo",
            table_name=table,
            fallback_rows=0,
            fallback_checksum="",
            written_ids=["2"],
            pk_column="id",
        )
        _, whole = verify_target(
            "sqlserver",
            dest,
            schema="dbo",
            table_name=table,
            fallback_rows=0,
            fallback_checksum="",
        )
        assert count == 2
        assert chk and chk != whole
    finally:
        cur.execute(f"DROP TABLE IF EXISTS dbo.{table}")
        conn.close()


def test_oracle_keyed_readback_survives_number_pk():
    oracledb = pytest.importorskip("oracledb")
    password = os.environ.get("DF_TEST_ORACLE_PASSWORD", "dataflow")
    try:
        conn = oracledb.connect(
            user="system",
            password=password,
            dsn="localhost:1521/FREEPDB1",
        )
    except oracledb.Error:  # pragma: no cover - env without local oracle
        pytest.skip("local oracle unavailable")

    table = f"KEYED_RB_{uuid.uuid4().hex[:8].upper()}"
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {table} (ID NUMBER PRIMARY KEY, NAME VARCHAR2(32))")
    cur.execute(f"INSERT INTO {table} VALUES (1, 'old')")
    cur.execute(f"INSERT INTO {table} VALUES (2, 'new')")
    conn.commit()
    dest = {
        "host": "localhost",
        "port": 1521,
        "database": "FREEPDB1",
        "username": "system",
        "password": password,
        "schema": "SYSTEM",
    }
    try:
        count, chk = verify_target(
            "oracle",
            dest,
            schema="SYSTEM",
            table_name=table,
            fallback_rows=0,
            fallback_checksum="",
            written_ids=["2"],
            pk_column="ID",
        )
        _, whole = verify_target(
            "oracle",
            dest,
            schema="SYSTEM",
            table_name=table,
            fallback_rows=0,
            fallback_checksum="",
        )
        assert count == 2
        assert chk and chk != whole
    finally:
        cur.execute(f"DROP TABLE {table}")
        conn.close()


def test_batch_scoped_pass_never_claims_the_whole_population():
    """A keyed digest must be reported as batch proof, not whole-table proof.

    ``target_rows`` stays whole-table for the operator, but the verdict text
    and ``checksum_scope`` have to say which rows the digest actually covered —
    otherwise a 20-row proof reads as "40 rows verified".
    """
    from services.reconcile_coverage import WRITTEN_BATCH_KEYS
    from services.reconciliation import reconcile

    report = reconcile(
        source_rows=20,
        target_rows=40,
        source_checksum="abc",
        target_checksum="abc",
        allow_extra_rows=True,
        checksum_scope=WRITTEN_BATCH_KEYS,
    ).to_dict()

    assert report["passed"] is True
    assert report["checksum_match"] is True
    assert report["assurance_level"] == "full_checksum"
    assert report["checksum_scope"] == WRITTEN_BATCH_KEYS
    assert "20 row(s) this run wrote" in report["message"]
    assert "40 row(s) in total" in report["message"]
    # Never a whole-population claim.
    assert "population_proof" in report and report["population_proof"] is False
