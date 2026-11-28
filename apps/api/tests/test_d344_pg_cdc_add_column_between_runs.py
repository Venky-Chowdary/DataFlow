"""D344 (PARTIAL, PG-CDC leg): a source ADD COLUMN between two scheduled CDC runs.

QA (Nov 10, PostgreSQL -> MySQL CDC schedule, propagate_columns): after
``ADD COLUMN tier int DEFAULT 1``, update id 5 (tier 9), delete id 6, insert
id 9 (tier 4), update id 9 (tier 5) - all four changes applied, the
destination never got ``tier`` and the run said "CDC applied 13 change(s)".

``test_d344_cdc_added_column_propagate_or_stop.py`` proves the column added
*during* a capture. A schedule's ALTER almost always lands *between* runs,
where the next run already sees the column in the source catalog when it
opens, so it is not "added mid-capture": this is that leg, end to end through
``UniversalTransferEngine.execute_tracked`` on live PostgreSQL (logical WAL)
and MySQL.

* AC-1 propagate_columns: the column is added at the destination and the
  tier values land in commit order (5 -> 9, 9 -> 4 then 5; 6 deleted).
  Existing rows that only received the source DEFAULT (no change event) are
  never reported as success: the run stops on the CDC image check and names
  the repair, instead of "CDC applied N change(s)" over rows it never got.
* AC-2 manual_review / strict: the run parks with a schema-drift finding
  naming ``tier``, and writes nothing.
"""

from __future__ import annotations

import uuid

import pytest

from tests.test_d344_cdc_added_column_propagate_or_stop import (  # noqa: F401 - fixture
    _add_column_and_change,
    _cleanup,
    _dest_columns,
    _dest_rows,
    _my_sql,
    _pg_sql,
    _request,
    _run,
    isolated,
    needs_live,
)


def _first_two_runs(fake, table: str, policy: str) -> None:
    _pg_sql(f'CREATE TABLE "{table}" (id INT PRIMARY KEY, name TEXT, status TEXT)')
    _pg_sql(f'ALTER TABLE "{table}" REPLICA IDENTITY FULL')
    for i in range(1, 7):
        _pg_sql(f'INSERT INTO "{table}" VALUES (%s, %s, %s)', (i, f"n{i}", "new"))
    first = _run(fake, table, "cdc", policy)
    assert first.success, first.error
    _pg_sql(f"UPDATE \"{table}\" SET status = 'paid' WHERE id = 2")
    second = _run(fake, table, "cdc_incremental", policy)
    assert second.success, second.error


@needs_live
def test_propagate_adds_the_column_and_lands_tier_in_commit_order(isolated):
    table = "d344b_" + uuid.uuid4().hex[:8]
    try:
        _first_two_runs(isolated, table, "propagate_columns")
        _add_column_and_change(table, default="")  # between runs, as on a schedule

        third = _run(isolated, table, "cdc_incremental", "propagate_columns")

        assert third.success, third.error
        assert "tier" in _dest_columns(table)
        rows = _dest_rows(table)
        assert 6 not in rows
        assert rows[5] == ("n5", "pending", 9)
        assert rows[9] == ("renamed", "new", 5)  # 4 then 5: the later change wins
        assert rows[2] == ("n2", "paid", None)
    finally:
        _cleanup(table)


@needs_live
def test_a_source_default_on_existing_rows_is_never_reported_as_success(isolated):
    table = "d344bd_" + uuid.uuid4().hex[:8]
    try:
        _first_two_runs(isolated, table, "propagate_columns")
        _add_column_and_change(table, default=" DEFAULT 1")

        third = _run(isolated, table, "cdc_incremental", "propagate_columns")

        assert "tier" in _dest_columns(table)
        rows = _dest_rows(table)
        assert 6 not in rows
        assert rows[5] == ("n5", "pending", 9)
        assert rows[9] == ("renamed", "new", 5)
        if third.success:
            # An at-least-once route upserts the source image itself.
            assert all(rows[i][2] == 1 for i in (1, 2, 3, 4)), rows
        else:
            assert "repair_cdc_drift" in str(third.error), third.error
            assert "4 rows" in str(third.error), third.error
    finally:
        _cleanup(table)


@needs_live
@pytest.mark.parametrize("policy", ["manual_review", "strict"])
def test_without_propagation_the_run_parks_naming_the_column(isolated, policy):
    table = "d344bm_" + uuid.uuid4().hex[:8]
    try:
        _first_two_runs(isolated, table, policy)
        before = _my_sql(f"SELECT id, name, status FROM `{table}_dst` ORDER BY id", fetch=True)
        _add_column_and_change(table, default=" DEFAULT 1")

        third = _run(isolated, table, "cdc_incremental", policy)

        assert not third.success
        assert "tier" in str(third.error), third.error
        assert "Nothing was written" in str(third.error), third.error
        assert "tier" not in _dest_columns(table)
        after = _my_sql(f"SELECT id, name, status FROM `{table}_dst` ORDER BY id", fetch=True)
        assert after == before
    finally:
        _cleanup(table)
