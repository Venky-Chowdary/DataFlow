"""Parity at Scale phase 3: the scan compares exactly what the run was responsible for (review defect 6).

* A run that read a window, a query, a limited number of rows, or rows its
  filter or recipe dropped is not compared as a whole table: no scan runs and
  the reason is said plainly (Gate-8's own record decides).
* Inside an eligible run, what Gate-8 explained stays explained: quarantined
  rows and a destination's rows from before an append are merged with the
  scan's counts, never turned into unexplained differences — and an
  explained table never becomes "proven" by the scan alone.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parity_engines  # noqa: E402
from test_parity_scale_verification_live import N, _endpoint, _env, _report, _sql, tables  # noqa: E402,F401

live = pytest.mark.skipif(not parity_engines.available("postgresql"),
                          reason="PostgreSQL not reachable (DATAFLOW_PG_TEST_DSN)")


def _job(src: str, dst: str, *, request: dict[str, Any] | None = None, recon: dict[str, Any] | None = None,
         summary: dict[str, Any] | None = None) -> str:
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service().create_transfer_job({
        "status": "completed",
        "transfer_request": {
            "source": _endpoint(src), "destination": _endpoint(dst),
            "mappings": [{"source": c, "target": c} for c in ("id", "name", "amt", "ts")],
            "sync_mode": "full_refresh_overwrite", **(request or {}),
        },
        "destination_summary": {"table": dst, "primary_key_columns": ["id"], **(summary or {})},
        "mapping_proof": {"mapping_hash": "m" * 64},
        "connector_versions": {"source": "postgresql 16.15", "destination": "postgresql 16.15"},
        "reconciliation": {"passed": True, "source_rows": N, "target_rows": N, "phase": "post_write",
                           "coverage_limit": {"rows": N, "limit": 100},
                           "verification_verdict_qualifier": "coverage_limited", **(recon or {})},
    })


@pytest.mark.parametrize("request_over, summary_over, words", [
    ({"sync_mode": "incremental_append", "cursor_column": "ts"}, {}, "window of the source"),
    ({"limit": 500}, {}, "limited number of rows"),
    ({"source_filter": {"column": "amt", "op": "gt", "value": 10}}, {}, "filtered source rows"),
    ({"shape_recipe": {"steps": [{"op": "filter_rows", "options": {"expression": "amt > 10"}}]}}, {},
     "recipe filtered or diverted"),
    ({}, {"rows_shape_diverted": 3}, "recipe filtered or diverted"),
])
def test_a_run_responsible_for_less_than_the_table_is_not_compared_as_a_whole(
        request_over: dict[str, Any], summary_over: dict[str, Any], words: str) -> None:
    from services.parity_scale.scope import scope_refusal

    job = {"transfer_request": {"sync_mode": "full_refresh_overwrite", **request_over},
           "destination_summary": dict(summary_over)}
    assert words in scope_refusal(job)
    assert scope_refusal({"transfer_request": {"sync_mode": "full_refresh_overwrite"}}) == ""


@live
def test_an_incremental_run_gets_no_whole_table_scan(tables) -> None:
    from services.parity_scale.verification import start_for_job

    job_id = _job(*tables, request={"sync_mode": "incremental_append", "cursor_column": "ts"})
    block = start_for_job(job_id, background=False)
    assert block["state"] == "not_eligible"
    assert "window of the source" in block["reason"]
    assert _report(job_id)["verdict"] != "proven"


@live
def test_quarantined_rows_stay_explained(tables) -> None:
    from services.parity_scale.verification import start_for_job

    src, dst = tables
    _sql(f'DELETE FROM "{dst}" WHERE id IN (5, 6)')  # the two rows the run quarantined
    job_id = _job(src, dst, recon={"rejected_rows": 2, "target_rows": N - 2})
    assert start_for_job(job_id, background=False)["state"] == "failed"  # the scan counts them missing
    report = _report(job_id)
    assert report["verdict"] == "explained", report["verdict_reason"]  # never unexplained, never proven
    assert report["parity"]["unexplained"] == 0 and report["parity"]["explained"] == 2


@live
def test_a_real_loss_beside_quarantine_is_still_unexplained(tables) -> None:
    from services.parity_scale.verification import start_for_job

    src, dst = tables
    _sql(f'DELETE FROM "{dst}" WHERE id IN (5, 6, 9)')  # two quarantined, one lost
    job_id = _job(src, dst, recon={"rejected_rows": 2, "target_rows": N - 3})
    start_for_job(job_id, background=False)
    report = _report(job_id)
    assert report["verdict"] == "unexplained"
    assert report["parity"]["unexplained"] == 1 and report["parity"]["explained"] == 2


@live
def test_rows_from_before_an_append_stay_out_of_scope(tables) -> None:
    from services.parity_scale.verification import start_for_job

    src, dst = tables
    _sql(f"INSERT INTO \"{dst}\" VALUES (-1, 'older', 1, timestamp '2023-01-01'), "
         "(-2, 'older', 2, timestamp '2023-01-01')")
    job_id = _job(src, dst, request={"sync_mode": "full_refresh_append"},
                  recon={"target_rows": N + 2, "target_rows_before": 2})
    start_for_job(job_id, background=False)
    report = _report(job_id)
    assert report["verdict"] == "explained", report["verdict_reason"]
    assert report["parity"]["unexplained"] == 0


def test_explained_value_observations_survive_the_merge() -> None:
    from services.parity_evidence import Observation
    from services.parity_scale.scope import merge_explained

    explained = {"code": "trim", "label": "Your trim transform", "explained": True, "detail": ""}
    prior = [Observation(table="t", kind="value", column="note", rows=8, origin="sample", cause=explained),
             Observation(table="t", kind="value", column="amt", rows=4, origin="sample",
                         cause={"code": "unexplained", "label": "x", "explained": False, "detail": ""})]
    merged = merge_explained(prior, [])
    assert [(o.column, o.rows) for o in merged] == [("note", 8)]  # Gate-8's unexplained sample is superseded


@live
def test_rows_held_by_the_write_contract_keep_the_table_explained_and_balance() -> None:
    """Review 2, 6a: decimal-fit held rows were trimmed away and the table read proven."""
    import uuid

    from services.parity_scale.verification import start_for_job

    src, dst = f"psd1_h_s_{uuid.uuid4().hex[:8]}", f"psd1_h_d_{uuid.uuid4().hex[:8]}"
    _sql(f'CREATE TABLE "{src}" (id bigint PRIMARY KEY, name varchar(40), amt numeric(14,2), ts timestamp(6))',
         f"INSERT INTO \"{src}\" SELECT g, 'n' || g, CASE WHEN g <= 3 THEN 12345.67 ELSE g % 900 END, "
         f"timestamp '2024-01-01' FROM generate_series(1, {N}) g",
         f'CREATE TABLE "{dst}" (id bigint PRIMARY KEY, name varchar(40), amt numeric(5,2), ts timestamp(6))',
         f'INSERT INTO "{dst}" SELECT * FROM "{src}" WHERE id > 3')
    try:
        job_id = _job(src, dst, recon={"rejected_rows": 3, "target_rows": N - 3})
        start_for_job(job_id, background=False)
        report = _report(job_id)
        assert report["verdict"] == "explained", report["verdict_reason"]
        parity = report["parity"]
        assert parity["unexplained"] == 0 and parity["explained"] == 3
        # read = landed + held
        assert parity["rows_source"] == parity["rows_destination"] + 3 == N
    finally:
        _sql(f'DROP TABLE IF EXISTS "{src}"', f'DROP TABLE IF EXISTS "{dst}"')


@live
def test_quarantined_rows_are_counted_exactly_when_only_some_are_named(tables) -> None:
    """Review 2, 6b: three quarantined and missing, one key named, the report showed one row."""
    from services.parity_scale.verification import start_for_job

    src, dst = tables
    _sql(f'DELETE FROM "{dst}" WHERE id IN (5, 6, 7)')
    job_id = _job(src, dst, recon={"rejected_rows": 3, "target_rows": N - 3},
                  summary={"rejected_details": [{"row": {"id": 5}, "error": "failed validation"}]})
    start_for_job(job_id, background=False)
    report = _report(job_id)
    assert report["verdict"] == "explained", report["verdict_reason"]
    assert report["parity"]["rows_differ"] == 3 and report["parity"]["explained"] == 3
    assert report["parity"]["rows_source"] == report["parity"]["rows_destination"] + 3
