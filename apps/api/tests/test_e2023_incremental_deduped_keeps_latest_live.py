"""E2-023 — incremental_deduped into a NEW MySQL table keeps the latest version per key.

QA (Oct 8) saw the mode silently become full_refresh_append: 30 rows for 10
keys. On 3e258e90f the same route is refused at preflight instead
("Duplicate identity keys ... uniqueness checks failed on the Validate
sample"): the duplicate gate kept a SQL source probe's repeated keys blocking
even though a declared cursor defines the winner. Once past it, two more
seams failed the correct 10-row result: the MySQL writer never reported the
images it collapsed (Gate-8 expected 30), and the bounded source re-read
fingerprinted every version (checksum over 30 vs 10).

Live PostgreSQL -> MySQL through ``execute_tracked``.
"""

from __future__ import annotations

import os
import uuid

import pytest

from tests.test_d196_sqlserver_change_tracking_cdc_live import isolated  # noqa: F401 — fixture

PG = {
    "host": os.environ.get("PGHOST", "127.0.0.1"), "port": int(os.environ.get("PGPORT", "5432")),
    "database": os.environ.get("PGDATABASE", "dataflow"), "username": os.environ.get("PGUSER", "dataflow"),
    "password": os.environ.get("PGPASSWORD", "dataflow"),
}
MY = {
    "host": os.environ.get("DATAFLOW_TEST_MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT", "3306")),
    "database": os.environ.get("DATAFLOW_TEST_MYSQL_DB", "dataflow"),
    "username": os.environ.get("DATAFLOW_TEST_MYSQL_USER", "root"),
    "password": os.environ.get("DATAFLOW_TEST_MYSQL_PASSWORD", ""),
}
_LATEST_NAMES = {0: None, 1: "", 2: "  ", 3: "東京 ✓ 🚀"}


def _pg():
    import psycopg2

    try:
        conn = psycopg2.connect(host=PG["host"], port=PG["port"], user=PG["username"],
                                password=PG["password"], dbname=PG["database"], connect_timeout=3)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"live PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    return conn


def _my():
    import pymysql

    try:
        return pymysql.connect(host=MY["host"], port=MY["port"], user=MY["username"], password=MY["password"],
                               database=MY["database"], charset="utf8mb4", autocommit=True, connect_timeout=3)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"live MySQL not reachable: {exc}")


def _run(fake, table):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    contract = {"name": table, "selected": True, "sync_mode": "incremental_deduped",
                "primary_key": ["cust_code"], "cursor_field": "updated_at",
                "cursor_semantics": "modification_timestamp"}
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema="public", table=table, **PG),
        destination=EndpointConfig(kind="database", format="mysql", table=table, **MY),
        sync_mode="incremental_deduped", stream_contracts=[contract],
        skip_preflight=False, validation_mode="strict")
    jid = "e2023" + uuid.uuid4().hex[:12]
    fake.update_job_status(jid, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(req, jid)


def test_three_versions_per_key_land_as_the_latest_one(isolated):
    pg, my = _pg(), _my()
    table = "e2023_" + uuid.uuid4().hex[:8]
    cur = pg.cursor()
    cur.execute(f'CREATE TABLE public."{table}" (row_id INT PRIMARY KEY, cust_code TEXT NOT NULL, '
                "name TEXT, updated_at TIMESTAMP NOT NULL)")
    rows, n = [], 0
    for k in range(10):
        for v in range(3):
            n += 1
            name = _LATEST_NAMES.get(k, f"c{k} v{v}") if v == 2 else f"c{k} v{v}"
            rows.append((n, f"C{k:02d}", name, f"2026-01-0{v + 1} 10:00:00"))
    cur.executemany(f'INSERT INTO public."{table}" VALUES (%s,%s,%s,%s)', rows)
    mc = my.cursor()
    try:
        first = _run(isolated, table)
        assert first.success, first.error
        assert first.reconciliation.get("passed") is True
        mc.execute(f"SELECT cust_code, name, updated_at FROM `{table}` ORDER BY cust_code")
        got = mc.fetchall()
        assert len(got) == 10, got
        by_key = {r[0]: r for r in got}
        assert all(str(r[2]).startswith("2026-01-03") for r in got), "latest version per key"
        assert by_key["C00"][1] is None and by_key["C01"][1] == "" and by_key["C02"][1] == "  "
        assert by_key["C03"][1] == "東京 ✓ 🚀" and by_key["C09"][1] == "c9 v2"
        assert int(first.destination_summary.get("collapsed_duplicate_rows") or 0) == 20

        # Second run into the table DataFlow created: one newer version, one new key.
        cur.executemany(f'INSERT INTO public."{table}" VALUES (%s,%s,%s,%s)',
                        [(31, "C04", "c4 v3", "2026-01-04 10:00:00"), (32, "C10", "new", "2026-01-04 10:00:00")])
        second = _run(isolated, table)
        assert second.success, second.error
        mc.execute(f"SELECT cust_code, name FROM `{table}` ORDER BY cust_code")
        got2 = dict(mc.fetchall())
        assert len(got2) == 11 and got2["C04"] == "c4 v3" and got2["C10"] == "new"
    finally:
        cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        mc.execute(f"DROP TABLE IF EXISTS `{table}`")
        pg.close()
        my.close()


def test_latest_wins_only_where_order_defines_the_winner():
    from services.primary_key import latest_wins_dedupe

    cursor = [{"selected": True, "cursor_field": "updated_at"}]
    assert latest_wins_dedupe("", "incremental_deduped", stream_contracts=cursor) == "keep_last"
    assert latest_wins_dedupe("", "incremental_upsert", source_type="kafka") == "keep_last"
    # No cursor, no ordered log: a repeated source key still blocks.
    assert latest_wins_dedupe("", "incremental_deduped", stream_contracts=[{"selected": True}]) == ""
    # Append never collapses; an explicit operator policy always wins.
    assert latest_wins_dedupe("", "incremental_append", stream_contracts=cursor) == ""
    assert latest_wins_dedupe("block", "incremental_deduped", stream_contracts=cursor) == "block"
