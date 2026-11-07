"""D233: a live PostgreSQL timestamp column is read back with its own precision.

The writer read ``timestamp with time zone`` bare. With a document source
bound, a bare timestamp is a BSON millisecond date, so the live TIMESTAMPTZ(6)
column re-materialized as TIMESTAMPTZ(3) and every microsecond row was
quarantined (Mongo ISO strings -> new PostgreSQL table).

Live: PostgreSQL via the r4 harness; skipped otherwise.
"""
from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h


@pytest.fixture
def table():
    try:
        conn = h.pg_conn()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    name = "d233_live_" + uuid.uuid4().hex[:8]
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            f'CREATE TABLE "{name}" (a TIMESTAMPTZ(6), b TIMESTAMPTZ(3), '
            "c TIMESTAMP, d TIME(4), e DATE)"
        )
    try:
        yield conn, name
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{name}"')
        conn.close()


@pytest.mark.parametrize("source_engine", ["mongodb", "elasticsearch", "postgresql"])
def test_live_temporal_carriers_keep_their_precision(table, source_engine):
    from connectors.postgresql_writer import _fetch_pg_column_types, _pg_resolve_carriers
    from services.source_engine_scope import bind_source_engine

    conn, name = table
    with conn.cursor() as cur:
        live = _fetch_pg_column_types(cur, "public", name)
    cols = ["a", "b", "c", "d", "e"]
    with bind_source_engine(source_engine):
        physical, _bind = _pg_resolve_carriers(
            target_cols=cols, dest_types=live, logical_types=[""] * 5, engine="postgresql"
        )
    got = dict(zip(cols, (t.upper() for t in physical)))
    assert got["a"] == "TIMESTAMPTZ(6)", got
    assert got["b"] == "TIMESTAMPTZ(3)", got
    assert got["c"] == "TIMESTAMP(6)", got
    assert got["d"] == "TIME(4)", got
    assert got["e"] == "DATE", got
