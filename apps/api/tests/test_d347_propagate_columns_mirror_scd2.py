"""D347: schema_policy=propagate_columns must evolve mirror and scd2 destinations.

QA MX8: PG -> MySQL schedules (mirror soft delete, scd2 keyed on ``id``), runs
1 and 2 green, then ``ALTER TABLE ... ADD COLUMN tier varchar(10)`` on the
source plus row changes. Run 3 failed for both with "MySQL live schema is
missing mapped field(s) 'tier' - refuse Map VARCHAR invent" and the
destination untouched, although overwrite/upsert on the same policy add the
column. Run 3 must add ``tier`` (nullable) and land the changes.

Real engines through the user's route: MCP ``create_schedule`` -> confirm ->
scheduler tick (``_run_due_schedules``) -> job. Skips when PG/MySQL are absent.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from tests import r4_route_harness as h
from tests import r11_pg_writer_route as r
from tests.test_r10_fidelity_run2_e2e import _schedule
from tests.test_r6_d207_oracle_watermark_typed_bind_e2e import _beat

pytestmark = pytest.mark.timeout(900)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    import services.transfer_plan_store as plan_store

    monkeypatch.setattr(plan_store, "STORE_PATH", tmp_path / "transfer_plans.json")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _completed(job: dict, run: str) -> None:
    assert str(job.get("status")) == "completed", {
        "run": run, "status": job.get("status"),
        "error": str(job.get("error") or job.get("message") or "")[:900],
    }


def _my(sql: str) -> list[tuple]:
    return r.mysql_execute("mysql", sql)


def _seed(src: str) -> None:
    r.pg(
        f'CREATE TABLE public."{src}" (id integer PRIMARY KEY, status varchar(12), '
        "amount numeric(10,2), updated_at timestamp NOT NULL)"
    )
    r.pg(
        f'INSERT INTO public."{src}" SELECT g, \'s\' || g, g * 1.5, '
        "timestamp '2026-11-01 00:00:00' + g * interval '1 minute' "
        "FROM generate_series(1, 6) g"
    )


def _route(src_id: str, dst_id: str, src: str, dst: str, mode: str) -> dict[str, Any]:
    route: dict[str, Any] = {
        "source_connector_id": src_id, "source_table": src,
        "dest_connector_id": dst_id, "dest_table": dst,
        "sync_mode": mode, "primary_key": "id",
        "schema_policy": "propagate_columns",
    }
    if mode == "scd2":
        route["cursor_column"] = "updated_at"
        route["cursor_semantics"] = "modification_timestamp"
    return route


def _dest_columns(dst: str) -> set[str]:
    rows = _my(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = DATABASE() AND table_name = '{dst}'"
    )
    return {str(c[0]).lower() for c in rows}


@pytest.mark.parametrize("mode", ["full_refresh_mirror", "scd2"])
def test_d347_added_source_column_propagates_for_mirror_and_scd2(client, mode):
    token = uuid.uuid4().hex[:8]
    src, dst = f"d347_s_{token}", f"d347_d_{token}"
    _seed(src)
    src_id = r.connector("source", r.pg_cfg())
    dst_id = r.connector("destination", r.mysql_family("mysql"))
    try:
        sched = _schedule(client, name=f"d347-{mode}-{token}",
                          **_route(src_id, dst_id, src, dst, mode))
        _completed(_beat(sched), "run 1")
        r.pg(f"UPDATE public.\"{src}\" SET status = 'two', "
             f"updated_at = timestamp '2026-11-02 00:00:00' WHERE id = 2")
        _completed(_beat(sched), "run 2")

        r.pg(f'ALTER TABLE public."{src}" ADD COLUMN tier varchar(10)')
        r.pg(f"UPDATE public.\"{src}\" SET tier = 'gold', status = 'one', "
             f"updated_at = timestamp '2026-11-03 00:00:00' WHERE id = 1")
        r.pg(f'DELETE FROM public."{src}" WHERE id = 3')
        r.pg(f"INSERT INTO public.\"{src}\" VALUES (7, 's7', 10.5, "
             f"timestamp '2026-11-03 00:00:00', 'silver')")
        _completed(_beat(sched), "run 3")

        assert "tier" in _dest_columns(dst)
        if mode == "scd2":
            current = {
                int(i): t for i, t in _my(
                    f"SELECT id, tier FROM `{dst}` WHERE is_current = 1"
                )
            }
            assert current == {1: "gold", 2: None, 4: None, 5: None, 6: None, 7: "silver"}
            # Old (closed) versions keep NULL in the added column.
            closed = _my(f"SELECT tier FROM `{dst}` WHERE is_current = 0")
            assert closed and all(t is None for (t,) in closed), closed
        else:
            live = {
                int(i): (t, int(d or 0)) for i, t, d in _my(
                    f"SELECT id, tier, _deleted FROM `{dst}`"
                )
            }
            assert live[1] == ("gold", 0)
            assert live[7] == ("silver", 0)
            assert live[3][1] == 1  # soft-deleted
            assert all(live[i] == (None, 0) for i in (2, 4, 5, 6)), live
    finally:
        h.drop_table("postgresql", src)
        _my(f"DROP TABLE IF EXISTS `{dst}`")


# --------------------------------------------------------------------------- #
# Unit level: the two halves of the fix, no live engine.
# --------------------------------------------------------------------------- #


def test_d347_spool_is_typed_with_the_planned_add_column_only():
    from src.transfer.models import EndpointConfig
    from src.transfer.stream_scd2 import _staging_endpoint

    dest = EndpointConfig(
        kind="database", format="mysql", table="t",
        extra={"schema_types": {"id": "BIGINT", "status": "VARCHAR(12)"}},
    )
    mappings = [
        {"source": "id", "target": "id", "target_type": "BIGINT"},
        {"source": "status", "target": "status", "target_type": "VARCHAR(12)"},
        # The propagate plan's ADD COLUMN: create_new + its stamped type.
        {"source": "tier", "target": "tier", "target_type": "VARCHAR(10)",
         "create_new": True},
        # Not a planned ADD: stays a gap, so the writer still fails closed.
        {"source": "loose", "target": "loose", "target_type": "TEXT"},
    ]
    staged = _staging_endpoint(dest, "job-1", mappings)
    assert staged.extra["schema_types"] == {
        "id": "BIGINT", "status": "VARCHAR(12)", "tier": "VARCHAR(10)",
    }
    # The destination's own probe is untouched.
    assert "tier" not in dest.extra["schema_types"]


def _scd2_sqlite(path, rows, columns, *, add_new_columns):
    from services.scd2_engine import apply_scd2
    from src.transfer.models import EndpointConfig

    endpoint = EndpointConfig(
        kind="database", format="sqlite", connection_string=f"sqlite:///{path}",
        database=str(path), table="dim",
    )
    return apply_scd2(
        endpoint, rows, columns=columns,
        schema={c: "string" for c in columns},
        mappings=[{"source": c, "target": c, "target_type": "TEXT"} for c in columns],
        conflict_columns=["id"], add_new_columns=add_new_columns,
    )


def test_d347_scd2_adds_an_approved_new_column_to_the_history_table(tmp_path):
    import sqlite3

    db = tmp_path / "d347.sqlite"
    first = _scd2_sqlite(db, [{"id": "1", "name": "a"}], ["id", "name"],
                         add_new_columns=False)
    assert first.get("ok") is not False, first
    second = _scd2_sqlite(
        db, [{"id": "1", "name": "a", "tier": None}, {"id": "2", "name": "b", "tier": "gold"}],
        ["id", "name", "tier"], add_new_columns=True,
    )
    assert second.get("ok") is not False, second
    conn = sqlite3.connect(str(db))
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(dim)")}
        rows = conn.execute("SELECT id, tier FROM dim WHERE is_current = 1 ORDER BY id").fetchall()
    finally:
        conn.close()
    assert "tier" in cols
    assert rows == [("1", None), ("2", "gold")]


def test_d347_scd2_without_approval_never_alters_the_history_table(tmp_path):
    import sqlite3

    db = tmp_path / "d347n.sqlite"
    _scd2_sqlite(db, [{"id": "1", "name": "a"}], ["id", "name"], add_new_columns=False)
    with pytest.raises(Exception):
        _scd2_sqlite(db, [{"id": "2", "name": "b", "tier": "gold"}],
                     ["id", "name", "tier"], add_new_columns=False)
    conn = sqlite3.connect(str(db))
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(dim)")}
    finally:
        conn.close()
    assert "tier" not in cols
