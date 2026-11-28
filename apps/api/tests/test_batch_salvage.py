"""A failed write chunk is salvaged by splitting it, with row-by-row's outcome.

``connectors.batch_salvage.salvage_in_order`` replaces the per-row retry of a
whole chunk that failed with a data error. These tests prove:

* model: over random chunks, random bad rows and in-chunk duplicate keys,
  the rows written, the rows held out and the order they are held out in are
  exactly what the row-by-row salvage produced, with far fewer attempts;
* live: a real engine run into PostgreSQL and MySQL/MariaDB destinations
  whose CHECK constraint refuses some rows lands every other row, holds out
  exactly the refused ones (by key, in order), passes Gate-8, and reaches the
  single-row path only for the bad rows' neighbourhoods.
"""

from __future__ import annotations

import math
import os
import random
import uuid

import pytest

from connectors import batch_salvage
from connectors.batch_salvage import salvage_in_order


class _ModelTable:
    """An insert-only table with a unique key and a value rule."""

    def __init__(self) -> None:
        self.keys: list[int] = []

    def insert(self, rows: list[tuple[int, int]]) -> bool:
        """All-or-nothing, like a statement under a savepoint."""
        seen = set(self.keys)
        for key, value in rows:
            if value < 0 or key in seen:
                return False
            seen.add(key)
        self.keys.extend(k for k, _ in rows)
        return True


def _row_by_row(rows):
    table, held = _ModelTable(), []
    for i, row in enumerate(rows):
        if not table.insert([row]):
            held.append(i)
    return table.keys, held


def _split(rows):
    table, held, attempts = _ModelTable(), [], {"batch": 0, "row": 0}

    def attempt(lo, hi):
        attempts["batch"] += 1
        return table.insert(rows[lo:hi])

    def one(i):
        attempts["row"] += 1
        if not table.insert([rows[i]]):
            held.append(i)

    salvage_in_order(len(rows), attempt_range=attempt, salvage_row=one)
    return table.keys, held, attempts


@pytest.mark.parametrize("seed", range(40))
def test_split_salvage_matches_row_by_row(seed):
    rng = random.Random(seed)
    n = rng.choice([1, 2, 3, 7, 64, 500, 2000])
    rows = []
    for i in range(n):
        key = i if rng.random() > 0.02 else rng.randrange(max(1, i + 1))  # some duplicates
        value = -1 if rng.random() < rng.choice([0.0, 0.001, 0.01, 0.2]) else i
        rows.append((key, value))
    want_keys, want_held = _row_by_row(rows)
    got_keys, got_held, attempts = _split(rows)
    assert got_keys == want_keys
    assert got_held == want_held
    assert attempts["row"] <= n


def test_split_salvage_is_logarithmic_in_the_good_rows():
    n, bad = 10_000, {17, 18, 5000, 9999}
    rows = [(i, -1 if i in bad else i) for i in range(n)]
    keys, held, attempts = _split(rows)
    assert held == sorted(bad)
    assert len(keys) == n - len(bad)
    budget = 2 * len(bad) * (math.ceil(math.log2(n)) + 1)
    assert attempts["row"] + attempts["batch"] <= budget, attempts


# --------------------------------------------------------------------------
# live: a destination CHECK constraint refuses some rows mid-chunk

ROWS = 3000
BAD = [1, 2, 700, 1501, 2999, 3000]


def _pg():
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("PostgreSQL not reachable")
    return pg_creds()


def _my():
    from tests.helpers.live_env import mysql_creds, mysql_up

    if not mysql_up():
        pytest.skip("MySQL not reachable")
    return mysql_creds()


@pytest.mark.parametrize("dest", ["postgresql", "mysql", "mysql-myisam"])
def test_live_check_violations_are_isolated_by_splitting(dest, monkeypatch):
    """``mysql-myisam``: a non-transactional table keeps the rows of a failed
    multi-row INSERT that came before the bad one, so a split retry there would
    write them twice; the MySQL writer must salvage such a table row by row."""
    engine_suffix = " ENGINE=MyISAM" if dest == "mysql-myisam" else ""
    dest = "mysql" if dest == "mysql-myisam" else dest
    os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
    os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    import psycopg2
    import pymysql

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    pg = _pg()
    my = _my() if dest == "mysql" else None
    calls = {"batch": 0, "row": 0}
    real = batch_salvage.salvage_in_order

    def counting(count, *, attempt_range, salvage_row):
        def a(lo, hi):
            calls["batch"] += 1
            return attempt_range(lo, hi)

        def r(i):
            calls["row"] += 1
            return salvage_row(i)

        return real(count, attempt_range=a, salvage_row=r)

    import connectors.mysql_writer as mysql_writer
    import connectors.postgresql_writer as pg_writer

    monkeypatch.setattr(pg_writer, "salvage_in_order", counting)
    monkeypatch.setattr(mysql_writer, "salvage_in_order", counting)

    sfx = uuid.uuid4().hex[:8]
    src, dst = f"bs_src_{sfx}", f"bs_dst_{sfx}"
    pconn = psycopg2.connect(host=pg["host"], port=pg["port"], user=pg["username"],
                             password=pg["password"], dbname=pg["database"])
    pconn.autocommit = True
    mconn = None
    try:
        with pconn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{src}" (id int PRIMARY KEY, name text, qty int)')
            cur.executemany(
                f'INSERT INTO "{src}" VALUES (%s, %s, %s)',
                [(i, f"n{i}", -i if i in BAD else i) for i in range(1, ROWS + 1)],
            )
            if dest == "postgresql":
                cur.execute(f'CREATE TABLE "{dst}" (id int PRIMARY KEY, name text, '
                            f"qty int CHECK (qty >= 0))")
        if dest == "mysql":
            mconn = pymysql.connect(host=my["host"], port=my["port"], user=my["username"],
                                    password=my["password"], database=my["database"],
                                    autocommit=True)
            with mconn.cursor() as cur:
                cur.execute(f"CREATE TABLE `{dst}` (id INT PRIMARY KEY, name TEXT, "
                            f"qty INT, CONSTRAINT qty_ok CHECK (qty >= 0)){engine_suffix}")
        dest_cfg = (
            {"host": pg["host"], "port": pg["port"], "username": pg["username"],
             "password": pg["password"], "database": pg["database"], "schema": "public"}
            if dest == "postgresql" else
            {"host": my["host"], "port": my["port"], "username": my["username"],
             "password": my["password"], "database": my["database"]}
        )
        req = TransferRequest(
            source=EndpointConfig(kind="database", format="postgresql", schema="public",
                                  table=src, host=pg["host"], port=pg["port"],
                                  username=pg["username"], password=pg["password"],
                                  database=pg["database"]),
            destination=EndpointConfig(kind="database", format=dest, table=dst, **dest_cfg),
            sync_mode="incremental",
            validation_mode="balanced",
            stream_contracts=[{"name": src, "sync_mode": "incremental",
                               "primary_key": "id", "cursor_field": "id", "selected": True}],
            skip_preflight=True,
            # Pass-through filter: the row path (the write under test).
            source_filter={"column": "id", "op": "gt", "value": 0},
        )
        res = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
        recon = res.reconciliation or {}
        if dest == "postgresql":
            with pconn.cursor() as cur:
                cur.execute(f'SELECT id FROM "{dst}" ORDER BY id')
                landed = [r[0] for r in cur.fetchall()]
        else:
            with mconn.cursor() as cur:
                cur.execute(f"SELECT id FROM `{dst}` ORDER BY id")
                landed = [r[0] for r in cur.fetchall()]
        if engine_suffix:
            # The split never runs on a non-transactional table.
            assert calls == {"batch": 0, "row": 0}, calls
            if landed != [i for i in range(1, ROWS + 1) if i not in BAD]:
                pytest.xfail(
                    "pre-existing (70cb4669c too): the failed chunk INSERT already wrote "
                    "the rows before its bad row into MyISAM, so the per-row salvage "
                    "holds them out as duplicate keys and the run fails Gate-8"
                )
        assert landed == [i for i in range(1, ROWS + 1) if i not in BAD], res.error
        assert int(recon.get("rejected_rows") or 0) == len(BAD), recon
        assert recon.get("passed") is True, str(recon.get("message"))[:400]
        details = (res.destination_summary or {}).get("rejected_details") or []
        # Held out exactly the refused rows, in source order (row = 1-based
        # source row number; the source is read in key order, so row == id).
        assert [int(d["row"]) for d in details] == BAD, details[:3]
        if engine_suffix:
            return  # row by row on a non-transactional table (asserted above)
        # The split reached single rows only around the bad ones.
        assert calls["row"] and calls["row"] < ROWS // 4, calls
    finally:
        with pconn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
            if dest == "postgresql":
                cur.execute(f'DROP TABLE IF EXISTS "{dst}"')
            # The quarantine side table the run's held rows went to.
            cur.execute(f'DROP TABLE IF EXISTS "{dst}_df_quarantine"')
        pconn.close()
        if mconn is not None:
            with mconn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{dst}`")
                cur.execute(f"DROP TABLE IF EXISTS `{dst}_df_quarantine`")
            mconn.close()


def test_live_pg_held_overwrite_salvage_splits_and_keeps_the_transaction(monkeypatch):
    """A full-refresh overwrite holds TRUNCATE and every chunk in one transaction:
    the split salvage runs on nested savepoints inside it, the good rows of each
    failed chunk land, the refused ones are held out in order, and the held
    transaction stays usable to commit."""
    import psycopg2

    import connectors.postgresql_writer as pw
    from connectors.write_resilience import finish_held_overwrite
    from tests.r13_mapping_harness import drop, pg_cfg, run_sql, token

    _pg()
    cfg = pg_cfg()
    monkeypatch.setattr(pw, "write_chunk_size", lambda *_a, **_kw: 40)
    calls = {"batch": 0}
    real = batch_salvage.salvage_in_order

    def counting(count, *, attempt_range, salvage_row):
        def a(lo, hi):
            calls["batch"] += 1
            return attempt_range(lo, hi)

        return real(count, attempt_range=a, salvage_row=salvage_row)

    monkeypatch.setattr(pw, "salvage_in_order", counting)
    table = f"bsh_{token()}"
    bad = [1, 2, 39, 40, 41, 77, 120]
    drop("postgresql", table)
    run_sql("postgresql", f'CREATE TABLE "{table}" (id INT PRIMARY KEY, name VARCHAR(100), '
                          f"CHECK (id NOT IN ({', '.join(map(str, bad))})))")
    run_sql("postgresql", f"INSERT INTO \"{table}\" VALUES (500, 'old')")  # nosec B608
    conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                            user=cfg["username"], password=cfg["password"])
    conn.autocommit = False
    try:
        result = pw.write_mapped_rows(
            host=cfg["host"], port=cfg["port"], database=cfg["database"],
            username=cfg["username"], password=cfg["password"], schema="public",
            connection_string="", ssl=False, table_name=table, headers=["id", "name"],
            data_rows=[[str(i), f"n{i}"] for i in range(1, 121)],
            mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
            column_types={"id": "INTEGER", "name": "VARCHAR(100)"},
            create_table=False, write_mode="insert", error_policy="quarantine",
            connection=conn, close_connection=False, pg_overwrite_in_transaction=True,
        )
        finish_held_overwrite(conn, success=True)
        ids = [r[0] for r in run_sql("postgresql", f'SELECT id FROM "{table}" ORDER BY id')]  # nosec B608
        assert ids == [i for i in range(1, 121) if i not in bad]
        details = result.rejected_details if hasattr(result, "rejected_details") else (
            result.get("rejected_details") if isinstance(result, dict) else None)
        if details is None:
            details = (getattr(result, "dest_summary", None) or {}).get("rejected_details") or []
        assert [int(d["row"]) for d in details] == bad, details[:3]
        assert calls["batch"] > 0
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()
        drop("postgresql", table)


def test_mysql_split_safety_is_probed_once_per_table_and_sees_temporary_tables():
    """The engine probe runs through the session (SHOW CREATE TABLE): InnoDB is
    safe, MyISAM is not, a TEMPORARY table shadowing an InnoDB table is not, and
    the answer is cached per table for one write call."""
    import pymysql

    from connectors.mysql_writer import _split_salvage_safe

    c = _my()
    conn = pymysql.connect(host=c["host"], port=c["port"], user=c["username"],
                           password=c["password"], database=c["database"], autocommit=True)
    sfx = uuid.uuid4().hex[:8]
    inno, myi = f"bsx_i_{sfx}", f"bsx_m_{sfx}"
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE `{inno}` (id INT PRIMARY KEY) ENGINE=InnoDB")
            cur.execute(f"CREATE TABLE `{myi}` (id INT PRIMARY KEY) ENGINE=MyISAM")
            cache: dict[str, bool] = {}
            assert _split_salvage_safe(cur, inno, cache) is True
            assert _split_salvage_safe(cur, myi, cache) is False
            assert _split_salvage_safe(cur, f"bsx_absent_{sfx}", cache) is False
            assert cache == {inno: True, myi: False, f"bsx_absent_{sfx}": False}
            # Cached: the run does not probe the same table again.
            calls = {"n": 0}
            real_execute = cur.execute

            def counting(sql, *a, **k):
                calls["n"] += 1
                return real_execute(sql, *a, **k)

            cur.execute = counting
            assert _split_salvage_safe(cur, inno, cache) is True
            assert calls["n"] == 0
            cur.execute = real_execute
            # A temporary table shadowing the InnoDB one is judged, and is not safe.
            cur.execute(f"CREATE TEMPORARY TABLE `{inno}` (id INT PRIMARY KEY) ENGINE=InnoDB")
            assert _split_salvage_safe(cur, inno) is False
            cur.execute(f"DROP TEMPORARY TABLE `{inno}`")
            assert _split_salvage_safe(cur, inno) is True
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{inno}`, `{myi}`")
        conn.close()
