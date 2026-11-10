"""D334: a source column widening must not wedge every later scheduled run.

QA MX5: PG -> MySQL schedules with schema_policy=propagate_columns (overwrite,
incremental_upsert, append) into tables DataFlow created (varchar(12) /
varchar(20)). After ``ALTER COLUMN status TYPE varchar(30)`` on the source
(plus an additive column and row changes) every run failed in seconds with
"1 lossy mapping(s) lack a verified Migration Risk Contract (execution
policy): status", and a schedule has no way to accept that contract.

Root cause: the widen itself was planned, but the widened pair was then
re-graded together with its single-step transform (``default:NA`` on
``status``, as in MX5). The approved plan carried that row as ``preserve``;
the regrade called it ``mutate``, which Execute's review treats as needing a
signed Migration Risk Contract. Runs without the widen never regraded it.

A widening is the safe direction: under propagate_columns the destination
column is widened and the run lands every row. Real engines through MCP
``create_schedule`` -> confirm -> scheduler tick, plus unit cases of the
regrade and the refusal text that remains for a real lossy pair.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from tests import r4_route_harness as h
from tests import r11_pg_writer_route as r
from tests.test_d347_propagate_columns_mirror_scd2 import (  # noqa: F401 - fixture
    _completed,
    _my,
    client,
)
from tests.test_r10_fidelity_run2_e2e import _schedule
from tests.test_r6_d207_oracle_watermark_typed_bind_e2e import _beat

pytestmark = pytest.mark.timeout(900)

_MODES: dict[str, dict[str, Any]] = {
    "full_refresh_overwrite": {},
    "incremental_upsert": {
        "cursor_column": "updated_at", "cursor_semantics": "modification_timestamp",
    },
    "incremental_append": {
        "cursor_column": "updated_at", "cursor_semantics": "insert_only",
    },
}


def _dest_type(dst: str, col: str) -> str:
    rows = _my(
        "SELECT column_type FROM information_schema.columns WHERE "
        f"table_schema = DATABASE() AND table_name = '{dst}' AND column_name = '{col}'"
    )
    return str(rows[0][0]).lower() if rows else ""


@pytest.mark.parametrize("mode", list(_MODES))
def test_d334_source_varchar_widen_is_applied_on_the_next_scheduled_run(client, mode):
    token = uuid.uuid4().hex[:8]
    src, dst = f"d334_s_{token}", f"d334_d_{token}"
    r.pg(
        f'CREATE TABLE public."{src}" (id integer PRIMARY KEY, status varchar(12), '
        "amount_s varchar(20), updated_at timestamp NOT NULL)"
    )
    r.pg(
        f'INSERT INTO public."{src}" SELECT g, \'s\' || g, (g * 1.5)::text, '
        "timestamp '2026-11-01 00:00:00' + g * interval '1 minute' "
        "FROM generate_series(1, 6) g"
    )
    src_id = r.connector("source", r.pg_cfg())
    dst_id = r.connector("destination", r.mysql_family("mysql"))
    route = {
        "source_connector_id": src_id, "source_table": src,
        "dest_connector_id": dst_id, "dest_table": dst,
        "sync_mode": mode, "primary_key": "id",
        "schema_policy": "propagate_columns", **_MODES[mode],
        "column_transforms": {"status": "default:NA"}, "validation_mode": "strict",
    }
    try:
        # QA's order: run 1 creates the table, the schedule is saved against it.
        r.run(client, {k: v for k, v in route.items() if k != "validation_mode"})
        assert _dest_type(dst, "status") == "varchar(12)"
        sched = _schedule(client, name=f"d334-{mode}-{token}", **route)
        _completed(_beat(sched), "run 2 (scheduled, unchanged source)")

        r.pg(f'ALTER TABLE public."{src}" ALTER COLUMN status TYPE varchar(30)')
        r.pg(f'ALTER TABLE public."{src}" ALTER COLUMN amount_s TYPE varchar(40)')
        r.pg(f'ALTER TABLE public."{src}" ADD COLUMN tier varchar(10)')
        long_status = "a status thirty chars long....."[:30]
        if mode == "incremental_append":
            r.pg(f"INSERT INTO public.\"{src}\" VALUES (7, %s, %s, "
                 "timestamp '2026-11-03 00:00:00', 'gold')", (long_status, "9" * 40))
        else:
            r.pg(f"UPDATE public.\"{src}\" SET status = %s, amount_s = %s, tier = 'gold', "
                 "updated_at = timestamp '2026-11-03 00:00:00' WHERE id = 1",
                 (long_status, "9" * 40))
        _completed(_beat(sched), "run 2 (after the widen)")

        assert _dest_type(dst, "status") == "varchar(30)"
        assert _dest_type(dst, "amount_s") == "varchar(40)"
        changed = 7 if mode == "incremental_append" else 1
        row = _my(f"SELECT status, amount_s, tier FROM `{dst}` WHERE id = {changed}")
        assert row == [(long_status, "9" * 40, "gold")], row
        assert _my(f"SELECT COUNT(*) FROM `{dst}`")[0][0] == (7 if changed == 7 else 6)

        # The schedule keeps running on the widened table.
        _completed(_beat(sched), "run 3 (quiet)")
    finally:
        h.drop_table("postgresql", src)
        _my(f"DROP TABLE IF EXISTS `{dst}`")


# --------------------------------------------------------------------------- #
# Unit level
# --------------------------------------------------------------------------- #

_ROW = {
    "source": "status", "target": "status", "source_type": "VARCHAR(12)",
    "target_type": "VARCHAR(12) COLLATE utf8mb4_bin", "transform": "default:NA",
    "fidelity": "preserve", "target_type_origin": "destination_catalog",
    "assignment_strategy": "bind_existing",
}


def _plan(row: dict) -> dict:
    from services.schema_drift import plan_destination_evolution

    planned, types, _ = plan_destination_evolution(
        [dict(row)], source_columns=["status"], source_schema={"status": "VARCHAR(30)"},
        dest_types={"status": "VARCHAR(12) COLLATE utf8mb4_bin"}, dest_db="mysql",
    )
    assert types["status"].upper().startswith("VARCHAR(30)"), types
    return planned[0]


def test_d334_widen_keeps_the_plans_transform_verdict():
    from services.mapping_pipeline import assert_mappings_executable

    planned = _plan(_ROW)
    assert planned["fidelity"] == "preserve", planned
    assert planned["transform"] == "default:NA"
    assert_mappings_executable([planned])  # no Risk Contract demanded


def test_d334_widen_does_not_launder_a_transform_the_plan_already_graded():
    planned = _plan({**_ROW, "fidelity": "mutate"})
    assert planned["fidelity"] == "mutate"


def test_d334_a_real_lossy_pair_names_both_types_and_a_reachable_way_out():
    from services.mapping_pipeline import assert_mappings_executable

    lossy = {**_ROW, "transform": "none", "source_type": "VARCHAR(30)",
             "fidelity": "lossy_cast", "type_narrowing": True}
    with pytest.raises(ValueError) as exc:
        assert_mappings_executable([dict(lossy)])
    text = str(exc.value)
    assert "status (VARCHAR(30) → VARCHAR(12) COLLATE utf8mb4_bin)" in text
    assert "risk_acceptance" in text and "PATCH /api/v1/schedules" in text
    # The schedule-row path the message names does clear it.
    signed = {**lossy, "risk_contract": {
        "approved_by": "ops", "reason": "accept truncation",
        "execution_policy": "QUARANTINE_ROW",
    }}
    assert_mappings_executable([signed])
