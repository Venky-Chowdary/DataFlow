"""D348: an added source column must not invent SCD2 history for unchanged rows.

QA MX8-scd2 (PG -> MySQL, key ``id``, cursor ``updated_at``): after
``ADD COLUMN tier varchar(10)`` (NULL for the old rows) plus one update, one
delete and one insert, run 3 closed and re-opened a value-identical version
for EVERY unchanged current row. The stored ``row_hash`` was taken over the
old column set; the new hash covers ``tier`` too, so every key compared as
changed although ``tier`` is NULL on both sides.

Expected: only the really changed id gains a version, the deleted id is
closed, the inserted id is one current row, unchanged ids keep their version.
Real engines through the schedule route (skips when PG/MySQL are absent),
plus a SQLite unit case of the merge itself.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections import Counter

import pytest

from tests import r4_route_harness as h
from tests import r11_pg_writer_route as r
from tests.test_d347_propagate_columns_mirror_scd2 import (  # noqa: F401 - fixture
    _completed,
    _my,
    _route,
    _seed,
    client,
)
from tests.test_r10_fidelity_run2_e2e import _schedule
from tests.test_r6_d207_oracle_watermark_typed_bind_e2e import _beat

pytestmark = pytest.mark.timeout(900)


def _versions(dst: str) -> Counter:
    return Counter(int(i) for (i,) in _my(f"SELECT id FROM `{dst}`"))


@pytest.mark.parametrize("dest_column_first", [False, True])
def test_d348_added_null_column_opens_no_version_for_unchanged_rows(client, dest_column_first):
    token = uuid.uuid4().hex[:8]
    src, dst = f"d348_s_{token}", f"d348_d_{token}"
    _seed(src)
    src_id = r.connector("source", r.pg_cfg())
    dst_id = r.connector("destination", r.mysql_family("mysql"))
    try:
        sched = _schedule(client, name=f"d348-{token}",
                          **_route(src_id, dst_id, src, dst, "scd2"))
        _completed(_beat(sched), "run 1")
        r.pg(f"UPDATE public.\"{src}\" SET status = 'two', "
             f"updated_at = timestamp '2026-11-02 00:00:00' WHERE id = 2")
        _completed(_beat(sched), "run 2")
        before = _versions(dst)
        assert before == Counter({1: 1, 2: 2, 3: 1, 4: 1, 5: 1, 6: 1})

        r.pg(f'ALTER TABLE public."{src}" ADD COLUMN tier varchar(10)')
        if dest_column_first:
            # QA's D347 workaround: the column already exists at the destination.
            _my(f"ALTER TABLE `{dst}` ADD COLUMN tier varchar(10) NULL")
        r.pg(f"UPDATE public.\"{src}\" SET tier = 'gold', status = 'one', "
             f"updated_at = timestamp '2026-11-03 00:00:00' WHERE id = 1")
        r.pg(f'DELETE FROM public."{src}" WHERE id = 3')
        r.pg(f"INSERT INTO public.\"{src}\" VALUES (7, 's7', 10.5, "
             f"timestamp '2026-11-03 00:00:00', NULL)")
        _completed(_beat(sched), "run 3")

        after = _versions(dst)
        # Only id 1 (real update) and id 7 (insert) gain a row; id 3 is closed.
        assert after == Counter({1: 2, 2: 2, 3: 1, 4: 1, 5: 1, 6: 1, 7: 1}), after
        current = {int(i) for (i,) in _my(f"SELECT id FROM `{dst}` WHERE is_current = 1")}
        assert current == {1, 2, 4, 5, 6, 7}

        # A quiet run after the add changes nothing either.
        _completed(_beat(sched), "run 4")
        assert _versions(dst) == after
    finally:
        h.drop_table("postgresql", src)
        _my(f"DROP TABLE IF EXISTS `{dst}`")


def _apply(db, rows, columns):
    from services.scd2_engine import apply_scd2
    from src.transfer.models import EndpointConfig

    endpoint = EndpointConfig(
        kind="database", format="sqlite", connection_string=f"sqlite:///{db}",
        database=str(db), table="dim",
    )
    out = apply_scd2(
        endpoint, rows, columns=columns, schema={c: "string" for c in columns},
        mappings=[{"source": c, "target": c, "target_type": "TEXT"} for c in columns],
        conflict_columns=["id"], add_new_columns=True,
    )
    assert out.get("ok") is not False, out
    return out


def test_d348_merge_treats_an_added_null_column_as_unchanged(tmp_path):
    db = tmp_path / "d348.sqlite"
    _apply(db, [{"id": "1", "name": "a"}, {"id": "2", "name": None}], ["id", "name"])
    cols = ["id", "name", "tier"]
    out = _apply(db, [{"id": "1", "name": "a", "tier": None},
                      {"id": "2", "name": None, "tier": None}], cols)
    assert (out["rows_written"], out["updated_rows"]) == (0, 0), out
    # Real changes still version: a value in the new column, an edited value,
    # and a value set to NULL.
    out = _apply(db, [{"id": "1", "name": "a", "tier": "gold"},
                      {"id": "2", "name": "b", "tier": None}], cols)
    assert (out["rows_written"], out["updated_rows"]) == (2, 2), out
    out = _apply(db, [{"id": "1", "name": None, "tier": "gold"},
                      {"id": "2", "name": "b", "tier": None}], cols)
    assert (out["rows_written"], out["updated_rows"]) == (1, 1), out
    conn = sqlite3.connect(str(db))
    try:
        n = conn.execute("SELECT COUNT(*) FROM dim").fetchone()[0]
    finally:
        conn.close()
    assert n == 5
