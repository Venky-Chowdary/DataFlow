"""D389 — SCD2 into TimescaleDB works like PostgreSQL.

QA: ``create_schedule`` (PostgreSQL -> TimescaleDB, scd2, key ``id``) was
accepted, and every run failed with 0 rows: "SCD2 requires a SQL table
destination (not timescaledb)". TimescaleDB is PostgreSQL with an extension;
the connector resolves to the PostgreSQL driver and the history writer
(``scd2_engine``) issues only CREATE TABLE (no key: a history table holds
several versions of a key), ALTER TABLE ADD COLUMN, INSERT and UPDATE, all of
which a TimescaleDB database accepts on a plain table or a hypertable.

Live through the QA route (MCP create_schedule -> three scheduler ticks), with
the destination connector typed ``timescaledb`` on the local PostgreSQL. The
TimescaleDB extension is not installed here, so this proves the product path
(gate, driver, writer, proof) and not hypertable storage; see the report.
"""

from __future__ import annotations

import uuid

import pytest

import services.config  # noqa: F401,E402 - settings parse before the harness blanks env

from tests import r4_route_harness as h
from tests import r11_pg_writer_route as r
from tests.test_d347_propagate_columns_mirror_scd2 import _completed, client  # noqa: F401
from tests.test_r10_fidelity_run2_e2e import _schedule
from tests.test_r6_d207_oracle_watermark_typed_bind_e2e import _beat

pytestmark = pytest.mark.timeout(900)


def _history(dst: str) -> list[tuple]:
    return [
        (int(i), str(s), bool(c))
        for i, s, c in r.pg(
            f'SELECT id, status, is_current FROM public."{dst}" '  # nosec B608
            "ORDER BY id, valid_from"
        )
    ]


@pytest.mark.parametrize("dest_type", ["postgresql", "timescaledb"])
def test_scd2_schedule_into_timescale_keeps_history_like_postgres(
    client, dest_type  # noqa: F811
):
    token = uuid.uuid4().hex[:8]
    src, dst = f"d389_s_{token}", f"d389_d_{token}"
    r.pg(
        f'CREATE TABLE public."{src}" (id integer PRIMARY KEY, status varchar(12), '
        "updated_at timestamp NOT NULL)"
    )
    r.pg(
        f'INSERT INTO public."{src}" VALUES (1, %s, %s), (2, %s, %s), (3, %s, %s)',
        ("a", "2026-11-01 00:00:01", "b", "2026-11-01 00:00:02", "c", "2026-11-01 00:00:03"),
    )
    src_id = r.connector("source", r.pg_cfg())
    dst_id = r.connector("destination", r.pg_cfg(type=dest_type))
    try:
        sched = _schedule(
            client, name=f"d389-{dest_type}-{token}",
            source_connector_id=src_id, source_table=src,
            dest_connector_id=dst_id, dest_table=dst,
            sync_mode="scd2", primary_key="id",
            cursor_column="updated_at", cursor_semantics="modification_timestamp",
        )
        _completed(_beat(sched), "run 1")
        r.pg(f"UPDATE public.\"{src}\" SET status = 'b2', updated_at = %s WHERE id = 2",
             ("2026-11-02 00:00:00",))
        _completed(_beat(sched), "run 2")
        r.pg(f"UPDATE public.\"{src}\" SET status = 'c2', updated_at = %s WHERE id = 3",
             ("2026-11-03 00:00:00",))
        r.pg(f"INSERT INTO public.\"{src}\" VALUES (4, 'd', %s)", ("2026-11-03 00:00:00",))
        _completed(_beat(sched), "run 3")

        assert _history(dst) == [
            (1, "a", True),
            (2, "b", False), (2, "b2", True),
            (3, "c", False), (3, "c2", True),
            (4, "d", True),
        ]
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)


def test_timescale_scd2_is_accepted_at_plan_by_the_same_gate_as_postgres() -> None:
    """R8: plan, create_schedule and run share destination_sync_mode_refusal."""
    from services.destination_sync_modes import destination_sync_mode_refusal

    for mode in ("scd2", "full_refresh_mirror"):
        assert destination_sync_mode_refusal("timescaledb", mode) == (
            destination_sync_mode_refusal("postgresql", mode)
        )
