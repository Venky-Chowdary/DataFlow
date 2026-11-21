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


@pytest.mark.parametrize("dest", ["postgresql", "mysql"])
def test_live_check_violations_are_isolated_by_splitting(dest, monkeypatch):
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
                            f"qty INT, CONSTRAINT qty_ok CHECK (qty >= 0))")
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
        assert landed == [i for i in range(1, ROWS + 1) if i not in BAD], res.error
        assert int(recon.get("rejected_rows") or 0) == len(BAD), recon
        assert recon.get("passed") is True, str(recon.get("message"))[:400]
        details = (res.destination_summary or {}).get("rejected_details") or []
        held_keys = []
        for d in details:
            row = d.get("row_data") or d.get("mapped_row") or {}
            key = row.get("id") if isinstance(row, dict) else None
            if key is not None:
                held_keys.append(int(key))
        if held_keys:
            assert sorted(set(held_keys)) == BAD, held_keys
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
