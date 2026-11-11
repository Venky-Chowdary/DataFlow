"""Exit rehearsal against a live PostgreSQL source (skipped when none is reachable).

Real PG types (NUMERIC with scale, TIMESTAMPTZ, BOOLEAN, TEXT with NULL and
unicode, BIGINT) extracted to JSONL by the engine, then re-read on both sides
with Gate-8's fingerprints: a clean extract is proven, and a row changed in
the source after the extract (the source moved during the rehearsal) is
reported, never hidden.
"""

from __future__ import annotations

import uuid

import pytest

from tests.test_exit_rehearsal_e2e import _run, world  # noqa: F401

psycopg2 = pytest.importorskip("psycopg2")


@pytest.fixture()
def pg(world, monkeypatch):  # noqa: F811
    from services import schedule_runner
    from tests.test_parity_live_pg import _pg_cfg

    cfg = _pg_cfg()
    try:
        conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
                                user=cfg["username"], password=cfg["password"], connect_timeout=3)
    except psycopg2.Error as exc:
        pytest.skip(f"PostgreSQL not reachable for the live rehearsal test: {type(exc).__name__}")
    conn.autocommit = True
    table = f"exit_src_{uuid.uuid4().hex[:10]}"
    saved = {"name": "pg", "type": "postgresql", "host": cfg["host"], "port": cfg["port"],
             "database": cfg["database"], "username": cfg["username"], "password": cfg["password"],
             "schema": "public"}
    monkeypatch.setattr(schedule_runner, "_resolve_connector_for_schedule", lambda _c, _s: dict(saved))
    with conn.cursor() as cur:
        cur.execute(f"""CREATE TABLE {table} (id BIGINT PRIMARY KEY, amount NUMERIC(12,4), paid BOOLEAN,
                        at TIMESTAMPTZ, note TEXT)""")  # nosec B608 - generated test name
        cur.execute(f"""INSERT INTO {table}
                        SELECT g, g + 0.1249, g % 2 = 0, TIMESTAMPTZ '2026-01-01 00:00:00+00' + g * interval '1 hour',
                               CASE WHEN g % 5 = 0 THEN NULL ELSE 'naïve ✓ ' || g END
                        FROM generate_series(1, 40) g""")  # nosec B608
    try:
        yield conn, table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        conn.close()


def _pg_system(table):
    from services.exit_inventory import declare

    return declare({"name": "PG ledger", "source": {"connector_id": "pg"}, "tables": [{"name": table}],
                    "exit_window_hours": 24}, workspace_id="ws1", actor="owner@example.com")


def test_a_postgres_rehearsal_is_proven_over_real_types(pg):
    _conn, table = pg
    out = _run(_pg_system(table))
    t = out["tables"][0]
    assert out["verdict"] == "proven", (t["reason"], t["differences"])
    assert t["rows_source"] == t["rows_extracted"] == 40 and t["key_columns"] == ["id"]
    totals = {c["column"]: c for c in t["control_totals"]}
    assert totals["amount"]["source"]["sum"] == "824.9960" and totals["amount"]["match"] is True
    assert out["duration"]["measured_seconds"] is not None


def test_a_source_change_during_the_rehearsal_is_reported(pg):
    conn, table = pg

    def change_source(_rehearsal):
        with conn.cursor() as cur:
            cur.execute(f"UPDATE {table} SET amount = amount + 1 WHERE id = 7")  # nosec B608

    out = _run(_pg_system(table), tamper=change_source)
    t = out["tables"][0]
    assert out["verdict"] == "incomplete"
    kinds = {d["kind"] for d in t["differences"]}
    assert "changed_rows" in kinds and "control_total" in kinds
