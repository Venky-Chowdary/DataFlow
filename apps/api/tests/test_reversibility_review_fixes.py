"""Reversibility gate: regressions from the adversarial review of the Reversibility Proof.

Each test names the finding it pins (C1 row caps / filters, C2 recipe steps that
overwrite a column, C3 lossy type mappings the type system reads as widening, ...).
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.reversibility import (  # noqa: E402
    CONDITIONAL,
    INVERTIBLE,
    LOSSY,
    PARTLY,
    REVERSIBLE,
    analyze_table,
    classify_mapping,
)


def _m(source, target=None, transform="none", stype="", ttype=""):
    return {"source": source, "target": target or source, "transform": transform,
            "source_type": stype, "target_type": ttype}


_TYPED = [_m("id", stype="INTEGER", ttype="INTEGER"), _m("email", stype="TEXT", ttype="TEXT")]


def _row_changes(table):
    return [c for c in table["columns"] if c["reason_code"] == "row_change"]


# ------------------------------------------------- C1: row caps and filters


def test_c1_a_row_cap_is_a_lossy_row_change():
    table = analyze_table(table="people", mappings=_TYPED, source_columns=["id", "email"], row_limit=2)
    [entry] = _row_changes(table)
    assert entry["invertibility"] == LOSSY and entry["transforms"] == ["limit"]
    assert entry["source"] is None and entry["target"] is None
    assert "2" in entry["reason"]
    assert table["verdict"] != REVERSIBLE


def test_c1_a_source_filter_is_a_lossy_row_change():
    table = analyze_table(table="people", mappings=_TYPED, source_columns=["id", "email"],
                          source_filter={"column": "id", "op": "lte", "value": 3})
    [entry] = _row_changes(table)
    assert entry["invertibility"] == LOSSY and entry["transforms"] == ["source_filter"]
    assert table["verdict"] == PARTLY


def test_c1_no_cap_and_no_filter_add_nothing():
    table = analyze_table(table="people", mappings=_TYPED, source_columns=["id", "email"],
                          row_limit=0, source_filter={})
    assert not _row_changes(table) and table["verdict"] == REVERSIBLE


def test_c1_execute_request_with_a_limit_or_filter_blocks_g24():
    from services.reversibility_sources import execute_gate
    from src.transfer.models import EndpointConfig, TransferRequest

    def req(**kw):
        return TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=":memory:", table="t"),
            destination=EndpointConfig(kind="database", format="sqlite", database=":memory:", table="t"),
            mappings=_TYPED, require_reversibility=True, **kw)

    planned = (_TYPED, {"id": "INTEGER", "email": "TEXT"}, {"id": "INTEGER", "email": "TEXT"})
    assert execute_gate(req(), planned)["status"] == "pass"
    capped = execute_gate(req(limit=2), planned)
    assert capped["status"] == "block" and "(rows)" in capped["message"]
    assert capped["details"]["lossy_columns"][0]["reason_code"] == "row_change"
    filtered = execute_gate(req(source_filter={"column": "id", "op": "lte", "value": 3}), planned)
    assert filtered["status"] == "block"


def test_c1_job_and_plan_reports_read_the_cap_and_filter():
    from services.reversibility_sources import job_report, plan_report

    job = {"transfer_request": {
        "source": {"format": "sqlite", "table": "t"}, "destination": {"format": "sqlite", "table": "t"},
        "column_types": {"id": "INTEGER"}, "mappings": [_m("id", ttype="INTEGER")],
        "limit": 5, "require_reversibility": True}}
    assert job_report(job, "j")["gate"]["status"] == "block"
    job["transfer_request"]["limit"] = 0
    job["transfer_request"]["source_filter"] = {"column": "id", "op": "eq", "value": 1}
    assert job_report(job, "j")["gate"]["status"] == "block"

    rev = SimpleNamespace(mappings=[_m("id")])
    plan = SimpleNamespace(
        id="p", source={"format": "sqlite", "table": "t"}, destination={"format": "sqlite", "table": "t"},
        source_columns=["id"], source_schema={"id": "INTEGER"}, target_schema={"id": "INTEGER"},
        sample_rows=[], policies={"require_reversibility": True, "row_limit": 10},
        active_revision=lambda: rev)
    report = plan_report(plan)
    assert report["gate"]["status"] == "block"
    assert any(c["reason_code"] == "row_change" for c in report["lossy_columns"])
    plan.policies = {"require_reversibility": True}
    assert plan_report(plan)["gate"]["status"] == "pass"


# ----------------------------------------------------- in-process engine e2e


def _exec(path: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            conn.executemany(sql, rows) if rows is not None else conn.execute(sql)
    finally:
        conn.close()


@pytest.fixture()
def fake_store(monkeypatch):
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kwargs):
            return super().update_job_fields(job_id, fields)

    fake = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


@pytest.fixture()
def people(tmp_path):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)")
    _exec(src, "INSERT INTO people VALUES (?, ?)", [(i, f"u{i}@example.com") for i in range(1, 11)])
    return src, dst


def _execute(fake, src: Path, dst: Path, **over):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    body = dict(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="people"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="people"),
        mappings=[dict(m, confidence=0.99) for m in _TYPED],
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{"name": "people", "primary_key": ["id"], "selected": True}],
        validation_mode="strict", skip_preflight=False, require_reversibility=True,
        compliance_acknowledged=True,
    )
    body.update(over)
    job_id = "rev" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(TransferRequest(**body), job_id)


def _dest_rows(dst: Path) -> int | None:
    if not dst.exists():
        return None
    conn = sqlite3.connect(str(dst))
    try:
        if not conn.execute("SELECT name FROM sqlite_master WHERE name='people'").fetchall():
            return None
        return conn.execute("SELECT count(*) FROM people").fetchone()[0]
    finally:
        conn.close()


def _blocked_by_g24(result) -> bool:
    return "g24_reversibility" in str(result.error_details or "")


def test_c1_engine_blocks_a_required_reversible_run_with_a_row_cap(people, fake_store):
    src, dst = people
    result = _execute(fake_store, src, dst, limit=2)
    assert not result.success and _blocked_by_g24(result), result.error
    assert _dest_rows(dst) is None


def test_c1_engine_blocks_a_required_reversible_run_with_a_source_filter(people, fake_store):
    src, dst = people
    result = _execute(fake_store, src, dst, source_filter={"column": "id", "op": "lte", "value": 3})
    assert not result.success and _blocked_by_g24(result), result.error
    assert _dest_rows(dst) is None


# ------------------------------------- C2: a recipe step that overwrites a column


def _recipe_columns(table):
    return [c for c in table["columns"] if c["reason_code"].startswith("recipe_")]


@pytest.mark.parametrize("step", [
    {"op": "constant_column", "options": {"to": "email", "value": "x"}},
    {"op": "derive_column", "options": {"to": "email", "expression": "id"}},
    {"op": "surrogate_key", "options": {"to": "email"}},
    {"op": "flatten_json", "column": "doc", "options": {"keys": ["email"]}},
])
def test_c2_a_producing_step_that_overwrites_a_source_column_is_lossy(step):
    table = analyze_table(table="people", mappings=_TYPED + [_m("doc", stype="TEXT", ttype="TEXT")],
                          source_columns=["id", "email", "doc"], recipe_steps=[step],
                          recipe_input_columns=["id", "email", "doc"])
    [entry] = _recipe_columns(table)
    assert entry["invertibility"] == LOSSY and entry["reason_code"] == "recipe_lossy", entry
    assert entry["source"] == "email" and "overwrit" in entry["reason"]
    assert table["verdict"] != REVERSIBLE


def test_c2_a_producing_step_into_a_new_column_stays_invertible():
    table = analyze_table(table="people", mappings=_TYPED + [_m("full", stype="TEXT", ttype="TEXT")],
                          source_columns=["id", "email", "full"],
                          recipe_steps=[{"op": "derive_column", "options": {"to": "full", "expression": "email"}}],
                          recipe_input_columns=["id", "email"])
    [entry] = _recipe_columns(table)
    assert entry["invertibility"] == INVERTIBLE and entry["source"] == "full"
    assert table["verdict"] == REVERSIBLE


def test_c2_a_column_made_by_an_earlier_step_then_overwritten_is_lossy():
    steps = [{"op": "derive_column", "options": {"to": "full", "expression": "email"}},
             {"op": "constant_column", "options": {"to": "full", "value": "x"}}]
    table = analyze_table(table="people", mappings=_TYPED, source_columns=["id", "email"],
                          recipe_steps=steps, recipe_input_columns=["id", "email"])
    kinds = [c["invertibility"] for c in _recipe_columns(table)]
    assert kinds == [INVERTIBLE, LOSSY]


def test_c2_without_the_pre_recipe_columns_a_known_target_is_not_assumed_new():
    table = analyze_table(table="people", mappings=_TYPED, source_columns=["id", "email"],
                          recipe_steps=[{"op": "constant_column", "options": {"to": "email", "value": "x"}}])
    [entry] = _recipe_columns(table)
    assert entry["invertibility"] == CONDITIONAL and entry["check"]["status"] == "not_checked"
    assert table["verdict"] != REVERSIBLE


def test_c2_rank_rows_keeps_only_some_rows():
    table = analyze_table(table="people", mappings=_TYPED, source_columns=["id", "email"],
                          recipe_steps=[{"op": "rank_rows", "options": {"order_by": "id", "limit": 3}}],
                          recipe_input_columns=["id", "email"])
    [entry] = _row_changes(table)
    assert entry["transforms"] == ["rank_rows"] and entry["invertibility"] == LOSSY


def test_c2_engine_blocks_a_recipe_that_overwrites_a_mapped_column(people, fake_store):
    src, dst = people
    recipe = {"steps": [{"op": "constant_column", "options": {"to": "email", "value": "x"}}]}
    result = _execute(fake_store, src, dst, shape_recipe=recipe)
    assert not result.success and _blocked_by_g24(result), result.error
    assert "people.email" in str(result.error)
    assert _dest_rows(dst) is None


def test_c2_engine_lets_a_recipe_that_adds_a_column_pass_g24(people, fake_store):
    src, dst = people
    recipe = {"steps": [{"op": "derive_column", "options": {"to": "full", "expression": "email"}}]}
    mappings = [dict(m, confidence=0.99) for m in _TYPED + [_m("full", stype="TEXT", ttype="TEXT")]]
    result = _execute(fake_store, src, dst, shape_recipe=recipe, mappings=mappings)
    assert result.success, result.error
    assert _dest_rows(dst) == 10


# ------------------------------------------------ C3: lossy type mappings


def _col(stype, ttype, dest="postgresql", source="postgresql", observed=None):
    return classify_mapping(_m("c", stype=stype, ttype=ttype), dest_db=dest, source_db=source, observed=observed)


@pytest.mark.parametrize("stype,ttype,dest,source,code", [
    ("bigint", "integer", "postgresql", "postgresql", "narrowing"),
    ("bigint", "int", "mysql", "mysql", "narrowing"),
    ("bigint", "int", "mssql", "mssql", "narrowing"),
    ("BIGINT", "INTEGER", "postgresql", "postgresql", "narrowing"),
    ("integer", "smallint", "postgresql", "postgresql", "narrowing"),
    ("timestamp with time zone", "timestamp without time zone", "postgresql", "postgresql", "timezone"),
    ("timestamptz", "datetime(6)", "mysql", "postgresql", "timezone"),
    ("time with time zone", "time", "postgresql", "postgresql", "timezone"),
    ("timestamp", "datetime", "mysql", "postgresql", "narrowing"),
    ("double", "float", "mysql", "mysql", "narrowing"),
    ("json", "jsonb", "postgresql", "postgresql", "json_normalize"),
    ("timestamp", "varchar(10)", "postgresql", "postgresql", "narrowing"),
    ("double precision", "varchar(5)", "postgresql", "postgresql", "narrowing"),
    ("bigint", "varchar(3)", "postgresql", "postgresql", "narrowing"),
    ("numeric(12,2)", "varchar(8)", "postgresql", "postgresql", "narrowing"),
])
def test_c3_lossy_type_mappings_are_named_lossy(stype, ttype, dest, source, code):
    col = _col(stype, ttype, dest, source)
    assert (col["invertibility"], col["reason_code"]) == (LOSSY, code), col
    assert "every value fits" not in col["reason"]


@pytest.mark.parametrize("stype,ttype,dest,code,fits,does_not_fit", [
    ("nvarchar(50)", "varchar(50)", "mssql", "charset", ["plain"], ["café"]),
    ("varchar(10)", "enum('a','b')", "mysql", "enum", ["a", "b"], ["c"]),
    ("varchar(10)", "char(10)", "postgresql", "blank_pad", ["abc"], ["abc "]),
])
def test_c3_conditional_type_mappings_are_checked_against_the_data(stype, ttype, dest, code, fits, does_not_fit):
    unchecked = _col(stype, ttype, dest, dest)
    assert (unchecked["invertibility"], unchecked["reason_code"]) == (CONDITIONAL, code), unchecked
    assert unchecked["check"]["status"] == "not_checked"
    assert _col(stype, ttype, dest, dest, observed=fits)["check"]["status"] == "fits"
    bad = _col(stype, ttype, dest, dest, observed=fits + does_not_fit)
    assert bad["check"] == {"status": "does_not_fit", "rows_checked": len(fits) + 1, "rows_not_fitting": 1}


@pytest.mark.parametrize("stype,ttype,dest,source,code", [
    ("integer", "bigint", "postgresql", "postgresql", "widen"),
    ("bigint", "bigint", "postgresql", "postgresql", "identity"),
    ("int", "integer", "mysql", "mysql", "widen"),
    ("timestamp(3)", "timestamp(6)", "postgresql", "postgresql", "widen"),
    ("timestamp", "datetime(6)", "mysql", "postgresql", "widen"),
    ("real", "double precision", "postgresql", "postgresql", "widen"),
    ("float", "double", "mysql", "mysql", "widen"),
    ("jsonb", "json", "postgresql", "postgresql", "widen"),
    ("enum('a','b')", "varchar(10)", "mysql", "mysql", "widen"),
    ("nvarchar(50)", "varchar(50)", "postgresql", "mssql", "widen"),
    ("char(10)", "char(10)", "postgresql", "postgresql", "identity"),
    ("bigint", "varchar(20)", "postgresql", "postgresql", "lossless_cast"),
    ("integer", "varchar(11)", "postgresql", "postgresql", "lossless_cast"),
    ("timestamp", "varchar(40)", "postgresql", "postgresql", "lossless_cast"),
    ("uuid", "varchar(36)", "postgresql", "postgresql", "lossless_cast"),
    ("numeric(12,2)", "varchar(14)", "postgresql", "postgresql", "lossless_cast"),
    ("date", "text", "postgresql", "postgresql", "lossless_cast"),
])
def test_c3_lossless_type_mappings_stay_invertible(stype, ttype, dest, source, code):
    col = _col(stype, ttype, dest, source)
    assert (col["invertibility"], col["reason_code"]) == (INVERTIBLE, code), col


def test_c3_a_cast_into_text_of_unknown_rendered_width_is_checked_by_width():
    col = _col("interval", "varchar(8)")
    assert (col["invertibility"], col["reason_code"]) == (CONDITIONAL, "text_width"), col
    assert _col("interval", "varchar(8)", observed=["1 day"])["check"]["status"] == "fits"
    assert _col("interval", "varchar(8)", observed=["1 day 02:00:00"])["check"]["status"] == "does_not_fit"


# ----------------------------------- H5: a schedule carries require_reversibility to its runs

_SCHED_CONN = {"_id": "src-pg", "id": "src-pg", "type": "postgresql", "host": "h", "port": 5432,
               "database": "db", "schema": "public", "username": "u", "password": "p"}


@pytest.fixture
def schedule_store(tmp_path, monkeypatch):
    import services.schedule_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    return store


def _schedule_body(**extra):
    return {"name": f"rev-{uuid.uuid4().hex[:6]}", "source_connector_id": "src-pg", "source_table": "orders",
            "dest_connector_id": "dst-pg", "dest_table": f"orders_{uuid.uuid4().hex[:6]}", "interval": "hourly",
            "sync_mode": "full_refresh_overwrite", "mappings": [{"source": "id", "target": "id"}], **extra}


def test_h5_a_schedule_stores_require_reversibility_and_its_runs_carry_it(schedule_store):
    import services.schedule_runner as runner

    dst = {**_SCHED_CONN, "id": "dst-pg", "_id": "dst-pg"}
    plain = schedule_store.create_schedule(_schedule_body())
    assert plain.require_reversibility is False
    assert runner.build_schedule_request(plain, _SCHED_CONN, dst).require_reversibility is False
    required = schedule_store.create_schedule(_schedule_body(require_reversibility=True))
    reloaded = schedule_store.get_schedule(required.id)
    assert reloaded.require_reversibility is True and reloaded.to_dict()["require_reversibility"] is True
    assert runner.build_schedule_request(reloaded, _SCHED_CONN, dst).require_reversibility is True
    schedule_store.update_schedule(plain.id, {"require_reversibility": True})
    assert schedule_store.get_schedule(plain.id).require_reversibility is True


def test_h5_the_schedule_routes_accept_and_return_the_flag(schedule_store):
    from src.routers.schedules_router import (
        ScheduleCreate,
        ScheduleResponse,
        ScheduleSummaryResponse,
        ScheduleUpdate,
    )

    assert ScheduleCreate(**_schedule_body(require_reversibility=True)).model_dump()["require_reversibility"] is True
    assert ScheduleUpdate(require_reversibility=True).model_dump(exclude_none=True) == {"require_reversibility": True}
    sched = schedule_store.create_schedule(_schedule_body(require_reversibility=True))
    assert ScheduleResponse.from_schedule(sched).require_reversibility is True
    assert ScheduleSummaryResponse.from_schedule(sched).require_reversibility is True


# ------------------------------------- H6: tests for the mutants the suite did not catch


def test_h6_a_lossy_key_declared_on_the_stream_contract_makes_every_report_not_reversible():
    from services.reversibility import NOT_REVERSIBLE
    from services.reversibility_sources import execute_gate, job_report, plan_report
    from src.transfer.models import EndpointConfig, TransferRequest

    maps = [_m("email", transform="hash_pii", stype="TEXT", ttype="TEXT"), _m("note", stype="TEXT", ttype="TEXT")]
    contracts = [{"name": "people", "selected": True, "primary_key": ["email"]}]
    job = {"transfer_request": {
        "source": {"format": "postgresql", "table": "people"}, "destination": {"format": "postgresql", "table": "people"},
        "column_types": {"email": "TEXT", "note": "TEXT"}, "mappings": maps, "stream_contracts": contracts}}
    assert job_report(job, "j")["verdict"] == NOT_REVERSIBLE
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table="people"),
        destination=EndpointConfig(kind="database", format="postgresql", table="people"),
        mappings=maps, stream_contracts=contracts, require_reversibility=True)
    gate = execute_gate(req, (maps, {"email": "TEXT", "note": "TEXT"}, {"email": "TEXT", "note": "TEXT"}))
    assert gate["details"]["verdict"] == NOT_REVERSIBLE
    rev = SimpleNamespace(mappings=maps)
    plan = SimpleNamespace(
        id="p", source={"format": "postgresql", "table": "people"},
        destination={"format": "postgresql", "table": "people"}, source_columns=["email", "note"],
        source_schema={"email": "TEXT", "note": "TEXT"}, target_schema={"email": "TEXT", "note": "TEXT"},
        sample_rows=[], policies={"stream_contracts": contracts}, active_revision=lambda: rev)
    assert plan_report(plan)["verdict"] == NOT_REVERSIBLE


def test_h6_a_lossless_run_passes_g24_at_execute_and_lands_every_row(people, fake_store, monkeypatch):
    import services.reversibility_sources as rs

    seen = []
    real = rs.execute_gate

    def spy(request, planned):
        gate = real(request, planned)
        seen.append(gate)
        return gate

    monkeypatch.setattr(rs, "execute_gate", spy)
    src, dst = people
    result = _execute(fake_store, src, dst)
    assert result.success, result.error
    assert _dest_rows(dst) == 10
    assert seen and all(g["status"] == "pass" for g in seen), seen


# --------------------- M4: Execute checks conditional columns against the rows it read (Validate == Execute)


@pytest.fixture()
def names(tmp_path):
    def make(values):
        src, dst = tmp_path / f"src_{uuid.uuid4().hex[:6]}.sqlite", tmp_path / f"dst_{uuid.uuid4().hex[:6]}.sqlite"
        _exec(src, "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)")
        _exec(src, "INSERT INTO people VALUES (?, ?)", list(enumerate(values, start=1)))
        return src, dst
    return make


_TRIM_MAPS = [dict(_m("id", stype="INTEGER", ttype="INTEGER"), confidence=0.99),
              dict(_m("email", transform="trim", stype="TEXT", ttype="TEXT"), confidence=0.99)]


def test_m4_execute_passes_a_normalising_transform_whose_read_rows_round_trip(names, fake_store):
    src, dst = names(["a@example.com", "b@example.com", "c@example.com"])
    result = _execute(fake_store, src, dst, mappings=_TRIM_MAPS)
    assert result.success, result.error
    assert _dest_rows(dst) == 3


def test_m4_execute_blocks_a_normalising_transform_whose_read_rows_change(names, fake_store):
    src, dst = names(["a@example.com", " b@example.com", "c@example.com"])
    result = _execute(fake_store, src, dst, mappings=_TRIM_MAPS)
    assert not result.success and _blocked_by_g24(result), result.error
    assert "people.email" in str(result.error)
