"""Gate-8's NULL-marker probe is bounded (review F3).

The probe full-scanned every text column of the destination on every run, in
every validation mode: an upsert of ten rows into a table of millions read the
whole table, with no statement timeout, and a capped count was compared as if
it were a census (1,000 against 1,000 "matched" whatever lay beyond the cap).

* the probe reads the rows this run wrote when the route can name them (the
  run's keys, or a table the run wrote whole); otherwise it runs only in
  strict/maximum and says so;
* every probe statement carries a timeout;
* a timeout or a cap hit is recorded as "not measured" (the verification is
  unproven), never as a pass.
"""

from __future__ import annotations

import uuid

import pytest

from services.value_serializer import SQL_NULL_SENTINEL
from tests.c3_live_helpers import pg_endpoint, pg_sql
from tests.test_reversibility_review3 import fake  # noqa: F401

MAPS = [{"source": "id", "target": "id"}, {"source": "note", "target": "note"}]


@pytest.fixture()
def tables():
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    tok = uuid.uuid4().hex[:8]
    src, dst = f"mbs_{tok}", f"mbd_{tok}"
    for t in (src, dst):
        pg_sql(f'CREATE TABLE public."{t}" (id INT PRIMARY KEY, note TEXT)')
    try:
        yield src, dst
    finally:
        for t in (src, dst):
            pg_sql(f'DROP TABLE IF EXISTS public."{t}"')


def _gate(dst, *, src=None, records=None, rows_written=0, **kw):
    from services.null_marker_guard import apply_null_marker_gate

    summary = {"table": dst, **kw.pop("summary", {})}
    return apply_null_marker_gate(
        {"passed": True, "message": "Checksum matches."},
        endpoint=pg_endpoint(dst), source_endpoint=pg_endpoint(src) if src else None,
        mappings=MAPS, dest_summary=summary, records=records, rows_written=rows_written, **kw,
    )


def _seed_upsert(dst):
    # Row 1 is an old row this run did not write; rows 2 and 3 are this run's.
    pg_sql(f'INSERT INTO public."{dst}" VALUES (1, %s), (2, %s), (3, %s)', (SQL_NULL_SENTINEL, "b", "c"))
    return [{"id": 2, "note": "b"}, {"id": 3, "note": "c"}]


UPSERT = {"primary_key_columns": ["id"], "target_rows_before": 1, "sync_mode": "upsert"}


def test_an_upsert_probes_only_the_keys_it_wrote(tables):
    _src, dst = tables
    records = _seed_upsert(dst)
    recon = _gate(dst, records=records, rows_written=2, summary={**UPSERT, "written_ids": ["2", "3"]})
    check = recon["null_marker_check"]
    assert recon["passed"] is True, recon
    assert check["scope"] == "run_keys" and check["status"] == "passed", check
    assert check["destination_rows"] == 0, check


def test_the_run_keys_come_from_the_records_when_the_writer_named_none(tables):
    _src, dst = tables
    records = _seed_upsert(dst)
    check = _gate(dst, records=records, rows_written=2, summary=UPSERT)["null_marker_check"]
    assert check["scope"] == "run_keys" and check["destination_rows"] == 0, check


def test_a_marker_among_the_keys_this_run_wrote_still_fails(tables):
    _src, dst = tables
    _seed_upsert(dst)
    records = [{"id": 1, "note": None}, {"id": 2, "note": "b"}]
    recon = _gate(dst, records=records, rows_written=2, summary={**UPSERT, "written_ids": ["1", "2"]})
    assert recon["passed"] is False, recon
    assert recon["null_marker_check"]["scope"] == "run_keys"
    assert recon["null_marker_check"]["status"] == "leaked"


def test_an_unscoped_append_is_probed_only_in_strict_or_maximum(tables):
    _src, dst = tables
    records = _seed_upsert(dst)
    summary = {"target_rows_before": 1, "sync_mode": "append"}
    balanced = _gate(dst, records=records, rows_written=2, summary=summary, validation_mode="balanced")
    check = balanced["null_marker_check"]
    assert check["status"] == "skipped" and "strict" in check["reason"], check
    assert balanced["passed"] is True
    for mode in ("strict", "maximum"):
        strict = _gate(dst, records=records, rows_written=2, summary=summary, validation_mode=mode)
        assert strict["null_marker_check"]["scope"] == "whole_table", strict["null_marker_check"]
        assert strict["passed"] is False, strict


def test_a_table_this_run_wrote_whole_is_probed_in_any_mode(tables):
    _src, dst = tables
    pg_sql(f'INSERT INTO public."{dst}" VALUES (1, %s)', (SQL_NULL_SENTINEL,))
    recon = _gate(
        dst, records=[{"id": 1, "note": None}], rows_written=1,
        summary={"target_rows_before": 0, "sync_mode": "append"}, validation_mode="balanced",
    )
    assert recon["null_marker_check"]["scope"] == "table_written_by_run", recon["null_marker_check"]
    assert recon["passed"] is False


def test_a_probe_that_times_out_is_not_measured(tables, monkeypatch):
    _src, dst = tables
    pg_sql(f"INSERT INTO public.\"{dst}\" SELECT g, 'n' || g FROM generate_series(1, 400000) g")
    monkeypatch.setenv("DATAFLOW_NULL_MARKER_PROBE_SECONDS", "0.001")
    recon = _gate(dst, validation_mode="strict", summary={"target_rows_before": 5, "sync_mode": "append"})
    check = recon["null_marker_check"]
    assert check["status"] == "not_measured" and "timed out" in check["reason"], check
    assert recon["unproven"] is True and recon["migration_proven"] is False, recon
    assert "not measured" in recon["message"], recon["message"]


def test_a_probe_that_reaches_its_cap_is_not_measured(tables):
    src, dst = tables
    # 1,100 marker rows in the destination against 1,050 the source really holds.
    pg_sql(f'INSERT INTO public."{dst}" SELECT g, %s FROM generate_series(1, 1100) g', (SQL_NULL_SENTINEL,))
    pg_sql(f'INSERT INTO public."{src}" SELECT g, %s FROM generate_series(1, 1050) g', (SQL_NULL_SENTINEL,))
    recon = _gate(dst, src=src, summary={"target_rows_before": 0, "sync_mode": "append"})
    check = recon["null_marker_check"]
    assert check["status"] == "not_measured" and "cap" in check["reason"], check
    assert recon["unproven"] is True, recon


def test_engine_upsert_into_a_large_table_probes_only_this_runs_rows(tmp_path, tables, fake):  # noqa: F811
    """The real engine path: SQLite → PostgreSQL upsert, balanced mode."""
    from src.transfer.engine import UniversalTransferEngine
    from tests.test_reversibility_review3 import _ID, _exec, _note, _sqlite_request

    _src, dst = tables
    pg_sql(f'ALTER TABLE public."{dst}" ALTER COLUMN id TYPE BIGINT')  # SQLite INTEGER is 64-bit
    pg_sql(f'INSERT INTO public."{dst}" VALUES (1, %s)', (SQL_NULL_SENTINEL,))  # an old row
    src = tmp_path / "s.sqlite"
    _exec(src, f"CREATE TABLE {dst} (id INTEGER PRIMARY KEY, note TEXT)")
    _exec(src, f"INSERT INTO {dst} VALUES (?, ?)", [(2, "b"), (3, "c")])
    request = _sqlite_request(
        src, tmp_path / "unused.sqlite", table=dst, destination=pg_endpoint(dst), sync_mode="upsert",
        validation_mode="balanced", require_reversibility=False, mappings=[dict(_ID), _note()],
    )
    result = UniversalTransferEngine().execute_tracked(request, "mb" + uuid.uuid4().hex[:10])
    check = ((result.destination_summary or {}).get("reconciliation") or {}).get("null_marker_check") \
        or (getattr(result, "reconciliation", None) or {}).get("null_marker_check")
    assert result.success is True, result.error
    assert check and check["scope"] == "run_keys" and check["status"] == "passed", check
