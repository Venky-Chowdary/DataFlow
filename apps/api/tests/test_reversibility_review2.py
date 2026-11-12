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

    body = dict(
        source=EndpointConfig(kind="database", format="sqlite", database=":memory:", table="t",
                              extra=dict(source_extra or {})),
        destination=EndpointConfig(kind="database", format="sqlite", database=":memory:", table="t"),
        mappings=_TYPED, require_reversibility=True, read_options=dict(read_options or {}))
    body.update(kw)
    return TransferRequest(**body)


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


# ------------------------- N2: a sample that fits is not the population (fail closed at Execute)


_UPPER = [dict(_m("id", stype="INTEGER", ttype="INTEGER"), confidence=0.99),
          dict(_m("email", transform="uppercase", stype="TEXT", ttype="TEXT"), confidence=0.99)]


def _upper_table(**kw):
    from services.reversibility import analyze_table

    return analyze_table(table="people", mappings=_UPPER, source_columns=["id", "email"],
                         sample_rows=[{"id": 1, "email": "A@X.COM"}, {"id": 2, "email": "B@X.COM"}], **kw)


def test_n2_a_conditional_column_fits_only_when_the_check_covered_the_whole_population():
    from services.reversibility import CONDITIONAL, PARTLY, REVERSIBLE

    whole = _upper_table(population_rows=2)
    email = whole["columns"][1]
    assert email["check"]["status"] == "fits" and email["check"]["covers_population"] is True
    assert whole["verdict"] == REVERSIBLE

    sampled = _upper_table(population_rows=3000)
    email = sampled["columns"][1]
    assert email["invertibility"] == CONDITIONAL and email["check"]["status"] == "fits"
    assert email["check"]["covers_population"] is False
    assert "fits the sample" in email["reason"]
    assert "will be blocked at Execute unless the whole table is checked" in email["reason"]
    assert sampled["verdict"] == PARTLY
    # An unknown population is not assumed to be the sample (fail closed).
    assert _upper_table()["verdict"] == PARTLY


def test_n2_execute_blocks_when_its_sample_is_not_the_whole_table():
    from services.reversibility_sources import execute_gate

    def req():
        return _req(mappings=_UPPER)

    types = {"id": "INTEGER", "email": "TEXT"}
    rows = [{"id": 1, "email": "A@X.COM"}, {"id": 2, "email": "B@X.COM"}]
    assert execute_gate(req(), (_UPPER, types, types, rows, 2))["status"] == "pass"
    blocked = execute_gate(req(), (_UPPER, types, types, rows, 3000))
    assert blocked["status"] == "block" and "t.email" in blocked["message"]
    assert "whole table" in blocked["message"]
    # Without a source row count Execute cannot know the sample is the table: it blocks.
    assert execute_gate(req(), (_UPPER, types, types, rows))["status"] == "block"


def test_n2_validate_says_the_same_as_execute_for_a_sampled_plan():
    from services.reversibility_sources import plan_report

    rev = SimpleNamespace(mappings=_UPPER)
    plan = SimpleNamespace(
        id="p", source={"format": "postgresql", "table": "people"},
        destination={"format": "postgresql", "table": "people"}, source_columns=["id", "email"],
        source_schema={"id": "INTEGER", "email": "TEXT"}, target_schema={"id": "INTEGER", "email": "TEXT"},
        sample_rows=[{"id": 1, "email": "A@X.COM"}], policies={"require_reversibility": True},
        row_count_estimate=3000, active_revision=lambda: rev)
    report = plan_report(plan)
    assert report["gate"]["status"] == "block"
    assert report["data_checked"] == {"rows": 1, "source": "plan_sample", "population_rows": 3000}
    email = report["tables"][0]["columns"][1]
    assert "will be blocked at Execute unless the whole table is checked" in email["reason"]
    plan.row_count_estimate = 1
    assert plan_report(plan)["gate"]["status"] == "pass"


def test_n2_engine_blocks_a_transform_that_fits_only_the_design_sample(tmp_path, fake_store):  # noqa: F811
    from tests.test_reversibility_review_fixes import _exec

    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)")
    rows = [(i, f"U{i}@EXAMPLE.COM") for i in range(1, 3000)] + [(3000, "mixed3000@Example.com")]
    _exec(src, "INSERT INTO people VALUES (?, ?)", rows)
    result = _execute(fake_store, src, dst, mappings=_UPPER)
    assert not result.success and _blocked_by_g24(result), result.error
    assert _dest_rows(dst) is None


# ------------------------------------------- N3: type mappings still read as lossless


def _pair(stype, ttype, source, dest):
    from services.reversibility_types import type_pair

    return type_pair(stype, ttype, dest_db=dest, source_db=source)


@pytest.mark.parametrize("stype,ttype,source,dest,code", [
    # integer -> decimal: precision minus scale against the integer's digits
    ("BIGINT", "NUMBER(10)", "postgresql", "oracle", "narrowing"),
    ("BIGINT", "NUMBER(18)", "postgresql", "oracle", "narrowing"),
    ("bigint", "numeric(10)", "postgresql", "postgresql", "narrowing"),
    ("integer", "numeric(5)", "postgresql", "postgresql", "narrowing"),
    ("integer", "numeric(10,2)", "postgresql", "postgresql", "narrowing"),
    ("bigint", "decimal", "postgresql", "mysql", "narrowing"),  # a bare MySQL DECIMAL is DECIMAL(10,0)
    # integer storage intervals, not bit widths
    ("INT UNSIGNED", "INT", "mysql", "mysql", "narrowing"),
    ("INT UNSIGNED", "INTEGER", "mysql", "postgresql", "narrowing"),
    ("TINYINT", "TINYINT", "mysql", "mssql", "narrowing"),  # -128..127 into 0..255
    ("int", "bigint", "snowflake", "postgresql", "narrowing"),  # Snowflake INT is NUMBER(38,0)
    # same spelling, another engine
    ("text", "text", "postgresql", "mysql", "narrowing"),
    ("text", "mediumtext", "postgresql", "mysql", "narrowing"),
    ("DATE", "DATE", "oracle", "postgresql", "truncation"),
    # normalising JSON carriers
    ("json", "JSON", "postgresql", "mysql", "json_normalize"),
    ("json", "JSON", "postgresql", "bigquery", "json_normalize"),
    ("json", "variant", "postgresql", "snowflake", "json_normalize"),
    # offset-keeping types
    ("TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE", "oracle", "oracle", "timezone"),
    ("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMPTZ", "oracle", "postgresql", "timezone"),
    ("DATETIMEOFFSET(6)", "timestamptz", "mssql", "postgresql", "timezone"),
    # unbounded or typed text into a narrow column
    ("varchar", "varchar(10)", "postgresql", "postgresql", "narrowing"),
    ("inet", "varchar(15)", "postgresql", "postgresql", "narrowing"),
])
def test_n3_lossy_type_mappings_are_named_lossy(stype, ttype, source, dest, code):
    assert _pair(stype, ttype, source, dest) == (LOSSY, code)


@pytest.mark.parametrize("stype,ttype,source,dest,code,fits,does_not_fit", [
    ("VARCHAR2(10 CHAR)", "VARCHAR2(10 BYTE)", "oracle", "oracle", "byte_length", ["plain"], ["ééééééé"]),
    ("VARCHAR2(40 CHAR)", "VARCHAR2(40)", "oracle", "oracle", "byte_length", ["plain"], ["€" * 14]),
    ("varchar(10)", "VARCHAR2(10)", "postgresql", "oracle", "byte_length", ["abcdefghij"], ["abcdefghié"]),
    ("TINYINT(1)", "BOOLEAN", "mysql", "postgresql", "boolean_domain", [0, 1, True], [2]),
    ("DATETIME", "TIMESTAMP", "mysql", "mysql", "timestamp_range",
     ["2024-05-01 10:00:00"], ["1960-01-01 00:00:00"]),
    ("date", "TIMESTAMP", "postgresql", "mysql", "timestamp_range", ["2024-05-01"], ["2040-01-01"]),
])
def test_n3_conditional_type_mappings_are_checked_value_by_value(stype, ttype, source, dest, code, fits,
                                                                 does_not_fit):
    from services.reversibility import CONDITIONAL, classify_mapping

    def col(observed=None):
        return classify_mapping(_m("c", stype=stype, ttype=ttype), dest_db=dest, source_db=source,
                                observed=observed)

    assert _pair(stype, ttype, source, dest) == (CONDITIONAL, code)
    assert col()["check"]["status"] == "not_checked"
    assert col(fits)["check"]["status"] == "fits"
    bad = col(fits + does_not_fit)
    assert bad["check"]["status"] == "does_not_fit" and bad["check"]["rows_not_fitting"] == len(does_not_fit)
    assert "?" not in bad["reason"]


@pytest.mark.parametrize("stype,ttype,source,dest", [
    ("integer", "numeric(12,2)", "postgresql", "postgresql"),
    ("bigint", "decimal(19,0)", "postgresql", "mysql"),
    ("BIGINT", "NUMBER(19)", "postgresql", "oracle"),
    ("INTEGER", "NUMBER", "postgresql", "oracle"),
    ("bigint", "numeric", "postgresql", "postgresql"),
    ("INT UNSIGNED", "BIGINT", "mysql", "postgresql"),
    ("TINYINT UNSIGNED", "TINYINT", "mysql", "mssql"),  # 0..255 into 0..255
    ("text", "longtext", "postgresql", "mysql"),
    ("text", "text", "postgresql", "sqlite"),
    ("DATE", "DATE", "oracle", "oracle"),
    ("DATE", "TIMESTAMP", "oracle", "postgresql"),
    ("date", "datetime", "oracle", "mysql"),
    ("json", "json", "postgresql", "postgresql"),
    ("json", "text", "postgresql", "postgresql"),
    ("jsonb", "JSON", "postgresql", "mysql"),
    ("timestamptz", "TIMESTAMP WITH TIME ZONE", "postgresql", "oracle"),
    ("DATETIMEOFFSET", "TIMESTAMP_TZ", "mssql", "snowflake"),
    ("VARCHAR2(10 BYTE)", "VARCHAR2(10 CHAR)", "oracle", "oracle"),
    ("varchar(10)", "VARCHAR2(40)", "postgresql", "oracle"),
    ("inet", "varchar(64)", "postgresql", "postgresql"),
    ("BOOLEAN", "TINYINT(1)", "postgresql", "mysql"),
    ("TINYINT(1)", "TINYINT(1)", "mysql", "mysql"),
    ("TINYINT(1)", "BOOLEAN", "mysql", "mysql"),
    ("DATETIME", "DATETIME(6)", "mysql", "mysql"),
])
def test_n3_lossless_type_mappings_stay_invertible(stype, ttype, source, dest):
    from services.reversibility import INVERTIBLE

    assert _pair(stype, ttype, source, dest)[0] == INVERTIBLE


# ------------------------------------------------- N7: NULL-aware transforms


def test_n7_default_on_a_column_whose_sample_holds_nulls_does_not_fit():
    from services.reversibility import classify_mapping

    mapping = _m("note", transform="default:N/A", stype="TEXT", ttype="TEXT")
    nulls = classify_mapping(mapping, observed=[None, "a", "b"])
    assert nulls["check"] == {"status": "does_not_fit", "rows_checked": 3, "rows_not_fitting": 1}
    assert classify_mapping(mapping, observed=["a", "b"])["check"]["status"] == "fits"
    # A transform that keeps NULL as NULL still skips it.
    trim = classify_mapping(_m("note", transform="trim", stype="TEXT", ttype="TEXT"), observed=[None, "a"])
    assert trim["check"] == {"status": "fits", "rows_checked": 1, "rows_not_fitting": 0}


def test_n7_validate_blocks_default_on_a_sampled_null_like_execute():
    from services.reversibility_sources import plan_report

    rev = SimpleNamespace(mappings=[_m("id"), _m("note", transform="default:N/A")])
    plan = SimpleNamespace(
        id="p", source={"format": "sqlite", "table": "t"}, destination={"format": "sqlite", "table": "t"},
        source_columns=["id", "note"], source_schema={"id": "INTEGER", "note": "TEXT"},
        target_schema={"id": "INTEGER", "note": "TEXT"},
        sample_rows=[{"id": 1, "note": None}, {"id": 2, "note": "a"}], row_count_estimate=2,
        policies={"require_reversibility": True}, active_revision=lambda: rev)
    report = plan_report(plan)
    note = report["tables"][0]["columns"][1]
    assert note["check"]["status"] == "does_not_fit"
    assert report["gate"]["status"] == "block"


# ------------------------------------------------- gap 2: the g24 message names row changes once


def test_gap2_the_gate_message_names_each_table_s_row_changes_once_by_kind():
    from services.reversibility import analyze_table, build_report

    table = analyze_table(
        table="events", mappings=_TYPED, source_columns=["id", "email"], row_limit=5,
        source_filter={"column": "id", "op": "lte", "value": 3},
        recipe_steps=[{"op": "rank_rows", "options": {"partition_by": ["id"], "order_by": "id", "keep": 1}}],
    )
    report = build_report([table], scope="job", subject_id="j", require=True, data_rows=0, data_source="none")
    message = report["gate"]["message"]
    assert "(rows)" not in message
    assert message.count("events") == 1, message
    clause = message.split("it is not: ", 1)[1].split(".", 1)[0]
    assert clause.startswith("events: ")
    assert sorted(clause[len("events: "):].split(", ")) == ["row cap", "source filter", "top rows only"]
    assert len(report["lossy_columns"]) == 3  # every row change is still listed with its reason


# ------------------------------------------- Low: a JSONL file export keeps every value's own text


_UNTYPED = [{"source": "id", "target": "id"}, {"source": "email", "target": "email"}]


def _file_req(fmt):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", database="db", table="t"),
        destination=EndpointConfig(kind="file_export", format=fmt, output_path=f"out.{fmt}"),
        mappings=_UNTYPED, require_reversibility=True)


def test_low_a_jsonl_file_export_takes_its_destination_types_from_the_source():
    from services.reversibility_sources import request_report

    types = {"id": "BIGINT", "email": "TEXT"}
    report = request_report(_file_req("jsonl"), _UNTYPED, types, None)
    assert report["verdict"] == "reversible", [c["reason"] for c in report["tables"][0]["columns"]]
    doc = request_report(_file_req("jsonl"), [{"source": "doc", "target": "doc"}], {"doc": "json"}, None)
    assert doc["tables"][0]["columns"][0]["reason_code"] == "json_normalize"


def test_low_a_csv_file_export_stays_type_unknown_because_null_and_empty_text_collapse():
    from services.reversibility_sources import request_report

    report = request_report(_file_req("csv"), _UNTYPED, {"id": "BIGINT", "email": "TEXT"}, None)
    codes = {c["reason_code"] for c in report["tables"][0]["columns"]}
    assert codes == {"type_unknown"} and report["verdict"] != "reversible"


@pytest.mark.parametrize("fmt,passes", [("jsonl", True), ("csv", False)])
def test_low_engine_g24_on_a_file_export_follows_the_format(people, fake_store, tmp_path, monkeypatch, fmt,  # noqa: F811
                                                            passes):
    from src.transfer.models import EndpointConfig

    monkeypatch.setenv("DATAWRAP_EXPORTS_DIR", str(tmp_path / "exports"))
    src, _dst = people
    name = f"people_{uuid.uuid4().hex[:8]}.{fmt}"
    result = _execute(fake_store, src, tmp_path / name, mappings=[dict(m, confidence=0.99) for m in _UNTYPED],
                      destination=EndpointConfig(kind="file_export", format=fmt, output_path=name))
    if passes:
        assert result.success, result.error
    else:
        assert not result.success and _blocked_by_g24(result), result.error


# --------------------------------- pre-existing: direct Validate computes g24 when asked (Validate == Execute)


@pytest.fixture
def validate_client(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    # The app's start-up sweepers are not this test's: no orphan pass, and none left running after it.
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    from services.orphan_jobs import stop_orphan_sweeper
    from src.main import app

    stop_orphan_sweeper()
    with TestClient(app) as c:
        yield c
    stop_orphan_sweeper()


@pytest.mark.parametrize("transform,status", [("hash_pii", "block"), ("none", "pass")])
def test_direct_validate_adds_g24_when_reversibility_is_required(validate_client, tmp_path, transform, status):
    body = {
        "columns": ["id", "email"], "column_types": {"id": "INTEGER", "email": "TEXT"}, "row_count": 2,
        "sample_rows": [{"id": 1, "email": "a@x.com"}, {"id": 2, "email": "b@x.com"}],
        "source_kind": "file", "source_type": "csv", "dest_kind": "database", "dest_type": "sqlite",
        "dest_database": str(tmp_path / "dst.sqlite"), "dest_table": "people",
        "destination_column_types": {"id": "INTEGER", "email": "TEXT"},
        "mappings": [{"source": "id", "target": "id", "confidence": 0.99},
                     {"source": "email", "target": "email", "confidence": 0.99, "transform": transform}],
        "compliance_acknowledged": True, "acknowledgment_actor": "ops lead",
        "acknowledgment_reason": "test of the reversibility gate",
        "require_reversibility": True,
    }
    res = validate_client.post("/api/v1/preflight/run", json=body)
    assert res.status_code == 200, res.text
    out = res.json()
    gate = next((g for g in out.get("gates") or [] if g.get("id") == "g24_reversibility"), None)
    assert gate is not None and gate["status"] == status, gate
    assert out["reversibility"]["gate"]["status"] == status
    # Not asked: no g24 (as before).
    body.pop("require_reversibility")
    plain = validate_client.post("/api/v1/preflight/run", json=body).json()
    assert not any(g.get("id") == "g24_reversibility" for g in plain.get("gates") or [])
