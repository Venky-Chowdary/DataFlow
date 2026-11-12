"""Review 4, M-2 on live PostgreSQL: a CSV column inferred as DATE is a cast from text, enforced while writing.

``2024-1-5`` past the sample landed as DATE ``2024-01-05`` with the run
reading success and nothing enforced. Skipped when no PostgreSQL is reachable.
"""

from __future__ import annotations

import uuid

import pytest

from tests.test_reversibility_review3 import _run, fake  # noqa: F401

psycopg2 = pytest.importorskip("psycopg2")


@pytest.fixture()
def pg_table():
    from tests.test_parity_live_pg import _pg_cfg

    cfg = _pg_cfg()
    try:
        conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["database"], user=cfg["username"],
                                password=cfg["password"], connect_timeout=3)
    except psycopg2.Error as exc:
        pytest.skip(f"PostgreSQL not reachable: {type(exc).__name__}")
    conn.autocommit = True
    table = f"r4m2_{uuid.uuid4().hex[:8]}"
    try:
        yield cfg, conn, table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608 - generated test name
        conn.close()


def _request(cfg, table, last, good="2024-01-{d:02d}"):
    from src.transfer.models import EndpointConfig, TransferRequest

    lines = ["id,v"] + [f"{i}," + good.format(d=i % 28 + 1) for i in range(1, 400)] + [f"400,{last}"]
    dest = EndpointConfig(kind="database", format="postgresql", host=cfg["host"], port=cfg["port"],
                          database=cfg["database"], username=cfg["username"], password=cfg["password"],
                          schema="public", table=table)
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"), source_filename="t.csv",
        source_content=("\n".join(lines) + "\n").encode(), destination=dest,
        sync_mode="full_refresh_overwrite", validation_mode="strict", skip_preflight=False,
        stream_contracts=[{"name": table, "primary_key": ["id"], "selected": True}],
        require_reversibility=True, compliance_acknowledged=True,
        acknowledgment_actor="owner@example.com", acknowledgment_reason="reviewed run of the reversibility gate")


def _rows(conn, table):
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
        if cur.fetchone()[0] is None:
            return None
        cur.execute(f"SELECT v::text FROM {table} WHERE id = 400")  # nosec B608 - generated test name
        return cur.fetchall()


def test_m2_a_csv_date_past_the_sample_that_postgres_would_respell_fails_the_run(pg_table, fake):  # noqa: F811
    cfg, conn, table = pg_table
    res = _run(fake, _request(cfg, table, "2024-1-5"))
    assert res.success is False, (res.destination_summary or {}).get("reversibility_enforcement")
    assert f"{table}.v would change" in str(res.error), res.error
    assert "2024-1-5" not in str(res.error)
    assert _rows(conn, table) in (None, []), "the respelled date was written"


def test_m2_an_honest_csv_date_column_is_enforced_on_every_row(pg_table, fake):  # noqa: F811
    cfg, conn, table = pg_table
    res = _run(fake, _request(cfg, table, "2024-01-05"))
    assert res.success is True, res.error
    columns = {c["column"]: c["rows_checked"] for c in res.destination_summary["reversibility_enforcement"]["columns"]}
    assert columns.get("v") == 400, columns
    assert _rows(conn, table) == [("2024-01-05",)]


def test_m2_an_honest_csv_timestamp_column_is_not_blocked_by_its_own_rendering(pg_table, fake):  # noqa: F811
    cfg, conn, table = pg_table
    res = _run(fake, _request(cfg, table, "2024-01-05 10:00:00.5", good="2024-01-{d:02d} 10:00:00"))
    assert res.success is True, res.error
    assert _rows(conn, table) == [("2024-01-05 10:00:00.5",)]


def test_m2_a_csv_timestamp_postgres_would_cut_fails_the_run(pg_table, fake):  # noqa: F811
    cfg, conn, table = pg_table
    res = _run(fake, _request(cfg, table, "2024-01-05 10:00:00.123456789", good="2024-01-{d:02d} 10:00:00"))
    assert res.success is False and f"{table}.v" in str(res.error), res.error
    assert ".123456789" not in str(res.error)
    assert _rows(conn, table) in (None, [])
