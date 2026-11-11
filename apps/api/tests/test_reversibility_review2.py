"""Reversibility gate: regressions from the second adversarial review (review 2).

Each test names the finding it pins: N1 a query / procedure source or a file
read window, N2 Execute's sample is not the population, N3 type mappings still
read as lossless, N7 NULL-aware transforms, and the gate message (gap 2).
"""

from __future__ import annotations

import os
import uuid
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.reversibility import LOSSY  # noqa: E402
from tests.test_reversibility_review_fixes import (  # noqa: E402,F401  (fixtures)
    _TYPED,
    _blocked_by_g24,
    _dest_rows,
    _execute,
    _m,
    fake_store,
    people,
)


def _req(source_extra=None, read_options=None, **kw):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=":memory:", table="t",
                              extra=dict(source_extra or {})),
        destination=EndpointConfig(kind="database", format="sqlite", database=":memory:", table="t"),
        mappings=_TYPED, require_reversibility=True, read_options=dict(read_options or {}), **kw)


_PLANNED = (_TYPED, {"id": "INTEGER", "email": "TEXT"}, {"id": "INTEGER", "email": "TEXT"})


def _row_changes(report):
    return [c for t in report["tables"] for c in t["columns"] if c["reason_code"] == "row_change"]


# --------------------------------------- N1: a query / procedure source, a file read window


@pytest.mark.parametrize("extra,op", [
    ({"source_read_mode": "query", "source_query": "SELECT id, email FROM t WHERE id <= 3"}, "source_query"),
    ({"source_query": "SELECT id, email FROM t"}, "source_query"),
    ({"source_read_mode": "procedure", "procedure_call": "CALL p()"}, "source_procedure"),
])
def test_n1_a_query_or_procedure_source_is_a_lossy_row_change(extra, op):
    from services.reversibility_sources import execute_gate, request_report

    report = request_report(_req(extra), *_PLANNED)
    [entry] = _row_changes(report)
    assert entry["invertibility"] == LOSSY and entry["transforms"] == [op]
    assert entry["source"] is None and entry["target"] is None
    assert "cannot be rebuilt" in entry["reason"]
    assert execute_gate(_req(extra), _PLANNED)["status"] == "block"


@pytest.mark.parametrize("options,ops", [
    ({"skip_rows": 2}, ["skip_rows"]),
    ({"skip_footer": 3}, ["skip_footer"]),
    ({"skip_rows": 2, "skip_footer": 3}, ["skip_rows", "skip_footer"]),
])
def test_n1_a_file_read_window_that_drops_data_rows_is_a_lossy_row_change(options, ops):
    from services.reversibility_sources import execute_gate, request_report

    report = request_report(_req(read_options=options), *_PLANNED)
    assert [e["transforms"][0] for e in _row_changes(report)] == ops
    assert execute_gate(_req(read_options=options), _PLANNED)["status"] == "block"


def test_n1_a_table_read_with_a_default_window_adds_nothing():
    from services.reversibility_sources import execute_gate, request_report

    report = request_report(_req({"source_read_mode": "table"}, {"skip_rows": 0, "header_row": 1}), *_PLANNED)
    assert not _row_changes(report)
    assert execute_gate(_req(), _PLANNED)["status"] == "pass"


def test_n1_job_and_plan_reports_read_the_read_mode_and_the_window():
    from services.reversibility_sources import job_report, plan_report

    job = {"transfer_request": {
        "source": {"format": "sqlite", "table": "t",
                   "extra": {"source_read_mode": "query", "source_query": "SELECT id FROM t"}},
        "destination": {"format": "sqlite", "table": "t"},
        "column_types": {"id": "INTEGER"}, "mappings": [_m("id", ttype="INTEGER")], "require_reversibility": True}}
    assert [c["transforms"] for c in _row_changes(job_report(job, "j"))] == [["source_query"]]
    job["transfer_request"]["source"] = {"format": "csv"}
    job["transfer_request"]["read_options"] = {"skip_footer": 1}
    assert [c["transforms"] for c in _row_changes(job_report(job, "j"))] == [["skip_footer"]]

    rev = SimpleNamespace(mappings=[_m("id")])
    plan = SimpleNamespace(
        id="p", source={"format": "sqlite", "table": "t", "extra": {"source_read_mode": "procedure",
                                                                     "procedure_call": "CALL p()"}},
        destination={"format": "sqlite", "table": "t"}, source_columns=["id"], source_schema={"id": "INTEGER"},
        target_schema={"id": "INTEGER"}, sample_rows=[], policies={"require_reversibility": True},
        row_count_estimate=0, active_revision=lambda: rev)
    report = plan_report(plan)
    assert report["gate"]["status"] == "block"
    assert [c["transforms"] for c in _row_changes(report)] == [["source_procedure"]]


def test_n1_engine_blocks_a_required_reversible_run_from_a_source_query(people, fake_store):  # noqa: F811
    from src.transfer.models import EndpointConfig

    src, dst = people
    endpoint = EndpointConfig(kind="database", format="sqlite", database=str(src), table="people",
                              extra={"source_read_mode": "query",
                                     "source_query": "SELECT id, email FROM people WHERE id <= 3"})
    result = _execute(fake_store, src, dst, source=endpoint)
    assert not result.success and _blocked_by_g24(result), result.error
    assert _dest_rows(dst) is None


def test_n1_engine_blocks_a_required_reversible_csv_read_that_skips_rows(tmp_path, fake_store):  # noqa: F811
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    body = "id,email\n" + "".join(f"{i},u{i}@example.com\n" for i in range(1, 11))
    dst = tmp_path / "dst.sqlite"
    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"), source_filename="people.csv",
        source_content=body.encode(),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="people"),
        mappings=[dict(m, source_type="TEXT", target_type="TEXT", confidence=0.99) for m in _TYPED],
        stream_contracts=[{"name": "people", "primary_key": ["id"], "selected": True}],
        sync_mode="full_refresh_overwrite", validation_mode="strict", require_reversibility=True,
        compliance_acknowledged=True, read_options={"skip_rows": 2, "skip_footer": 3})
    job_id = "rev" + uuid.uuid4().hex[:12]
    fake_store.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    assert not result.success and _blocked_by_g24(result), result.error
    assert _dest_rows(dst) is None
