"""Accuracy A7 live: the ledger classifies real engine job documents.

A real PostgreSQL -> MySQL run (Validate on) that writes is ``agreed_pass``;
a run Execute's own preflight refuses (MySQL zero dates) is ``agreed_block``
with ``failed_at == "preflight"``. Unit truth table: test_validate_execute_ledger.py.
"""

from __future__ import annotations

import pytest

from services import validate_execute_ledger as ledger
from tests.c3_live_helpers import mysql_endpoint, mysql_sql, pg_endpoint, pg_sql, run_transfer, token
from tests.test_commit_safe_review_round2 import isolated  # noqa: F401 — fixture


@pytest.fixture(autouse=True)
def _live(tmp_path, monkeypatch):
    from tests.helpers.live_env import mysql_up, pg_up

    if not (mysql_up() and pg_up()):
        pytest.skip("live MySQL and PostgreSQL not reachable")
    monkeypatch.setattr(ledger, "_path", lambda: tmp_path / "validate_execute_runs.jsonl")
    monkeypatch.setattr(ledger._LOG, "backend", lambda: None)


def test_live_run_that_writes_is_agreed_pass(isolated):  # noqa: F811
    table = f"acc7_ok_{token()}"
    try:
        pg_sql(f'CREATE TABLE public."{table}" (id INT PRIMARY KEY, v TEXT)')
        pg_sql(f"INSERT INTO public.\"{table}\" VALUES (1,'a'),(2,'b')")
        result = run_transfer(isolated, pg_endpoint(table), mysql_endpoint(table))
        assert result.success, result.error
        job = isolated.get_job(result.job_id)
        rec = ledger.record_run_outcome(result.job_id, job["status"], job)
        assert (rec["execute_preflight"], rec["outcome"], rec["failed_at"]) == ("passed", "agreed_pass", None)
    finally:
        pg_sql(f'DROP TABLE IF EXISTS public."{table}"')
        mysql_sql(f"DROP TABLE IF EXISTS {table}")


def test_live_run_refused_by_preflight_is_agreed_block(isolated):  # noqa: F811
    import pymysql

    from tests.helpers.live_env import mysql_creds

    table = f"acc7_blk_{token()}"
    c = mysql_creds()
    try:
        mysql_sql(f"CREATE TABLE {table} (id INT PRIMARY KEY, d DATE NULL)")
        conn = pymysql.connect(host=c["host"], port=int(c["port"]), user=c["username"],
                               password=c["password"], database=c["database"], autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute("SET SESSION sql_mode=''")
                cur.execute(f"INSERT INTO {table} VALUES (1,'2024-01-01'),(2,'0000-00-00')")
        finally:
            conn.close()
        result = run_transfer(isolated, mysql_endpoint(table), pg_endpoint(table))
        assert not result.success
        job = isolated.get_job(result.job_id)
        rec = ledger.record_run_outcome(result.job_id, job["status"], job)
        assert (rec["execute_preflight"], rec["outcome"], rec["failed_at"]) == (
            "blocked", "agreed_block", "preflight")
    finally:
        mysql_sql(f"DROP TABLE IF EXISTS {table}")
        pg_sql(f'DROP TABLE IF EXISTS public."{table}"')
        pg_sql(f'DROP TABLE IF EXISTS public."{table}_df_quarantine"')
