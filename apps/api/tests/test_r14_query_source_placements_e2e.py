"""QA C3 / MySQL D9: a query source is honoured from every accepted placement or refused.

QA (job 6ae6988b…): ``source_read_mode``/``source_query`` sent to REST
``/api/v1/transfer/run`` as form fields (the route did not declare them, so
FastAPI dropped them) or inside ``stream_contracts_json`` (stored, never read)
were ignored: the whole 1,000-row table was copied and "Row fidelity
verified". Only ``source_extra_json`` worked.

Now ``services.procedure_source.resolve_callable_placements`` resolves form
fields, ``source_extra_json`` and one stream contract into the source the
engine runs, and refuses (422) what it cannot honour. MCP refuses an argument
it does not declare (``stream_contracts``/``source_extra``) instead of
dropping it. Live MySQL and MariaDB -> PostgreSQL, run 1 and run 2.
"""

from __future__ import annotations

import json

import pytest

from services.procedure_source import CallablePlacementError, resolve_callable_placements
from tests import r4_route_harness as h
from tests.test_r11_mysql_pg_fk_ddl_e2e import (  # noqa: F401 — fixture re-export
    KINDS,
    _connector,
    _wait,
    client,
)
from tests.test_r11_mysql_query_mode_pg_e2e import (  # noqa: F401 — fixture re-export
    EXPECTED,
    _rows,
    query_source,
)


def _form(src: str, dst: str, names: dict, **extra: str) -> dict:
    return {
        "source_kind": "database", "source_format": names["kind"],
        "source_connector_id": src, "source_table": names["src"],
        "dest_kind": "database", "dest_format": "postgresql", "dest_connector_id": dst,
        "dest_schema": "public", "dest_table": names["dst"],
        "mappings_json": json.dumps(
            [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "v")]),
        "sync_mode": "full_refresh_overwrite", "validation_mode": "strict",
        "async_mode": "false", **extra,
    }


def _placements(query: str) -> dict[str, dict[str, str]]:
    return {
        "form_fields": {"source_read_mode": "query", "source_query": query},
        "form_query_only": {"source_query": query},
        "stream_contract": {"stream_contracts_json": json.dumps(
            [{"name": "q", "source_read_mode": "query", "source_query": query,
              "primary_key": ["id"]}])},
        "source_extra_json": {"source_extra_json": json.dumps(
            {"source_read_mode": "query", "source_query": query})},
        "form_and_extra_agree": {"source_query": query, "source_extra_json": json.dumps(
            {"source_read_mode": "query", "source_query": query})},
    }


@pytest.mark.parametrize("placement", list(_placements("q")))
@pytest.mark.parametrize("query_source", KINDS, indirect=True)
def test_every_accepted_placement_runs_the_query_run1_run2(client, query_source, placement):
    src = _connector(query_source["kind"], "source")
    dst = _connector("postgresql", "destination")
    extra = _placements(query_source["query"])[placement]
    for _run in (1, 2):
        resp = client.post("/api/v1/transfer/run", data=_form(src, dst, query_source, **extra))
        assert resp.status_code == 200, resp.text
        job = _wait(str(resp.json().get("job_id") or ""))
        assert job.get("status") == "completed", {
            k: job.get(k) for k in ("status", "error", "message")}
        # The hidden row (id 5) and the hidden column are not copied.
        assert _rows(query_source["dst"]) == EXPECTED


@pytest.mark.parametrize("query_source", ["mysql"], indirect=True)
def test_unhonourable_placements_are_422_and_nothing_runs(client, query_source):
    src = _connector(query_source["kind"], "source")
    dst = _connector("postgresql", "destination")
    q = query_source["query"]
    bad = {
        "disagree": {"source_query": q, "source_extra_json": json.dumps(
            {"source_query": f"SELECT id, v FROM {query_source['src']}"})},
        "query_mode_no_query": {"source_read_mode": "query"},
        "contracts_not_json": {"stream_contracts_json": "[{"},
    }
    for name, extra in bad.items():
        resp = client.post("/api/v1/transfer/run", data=_form(src, dst, query_source, **extra))
        assert resp.status_code == 422, (name, resp.status_code, resp.text)
    assert not h.query("postgresql", f"SELECT to_regclass('public.{query_source['dst']}')")[0][0]


def test_file_transfer_refuses_a_query_in_its_contracts(client, tmp_path):
    resp = client.post(
        "/api/v1/connectors/transfer",
        data={"destination_database": "dataflow", "destination_collection": "r14c3_file",
              "dest_type": "postgresql",
              "stream_contracts_json": json.dumps([{"source_query": "SELECT 1"}])},
        files={"file": ("a.csv", b"id,v\n1,x\n", "text/csv")},
    )
    assert resp.status_code == 422, resp.text


def test_mcp_refuses_placements_it_does_not_declare(client):
    for arg in ({"stream_contracts": [{"source_query": "SELECT 1"}]},
                {"source_extra": {"source_query": "SELECT 1"}}):
        ok, out = h.mcp_call(client, "start_transfer", {
            "source_connector_id": "x", "dest_connector_id": "y", "dest_table": "t", **arg})
        assert not ok and "does not accept" in str(out), out


def test_resolver_lifts_a_single_contract_query_and_keeps_the_rest():
    extra, contracts = resolve_callable_placements(
        form={"source_read_mode": "", "source_query": ""},
        source_extra={"enable_ocr": True},
        stream_contracts=[{"name": "s", "source_query": "SELECT 1", "primary_key": ["id"]}],
    )
    assert extra == {"enable_ocr": True, "source_query": "SELECT 1"}
    assert contracts == [{"name": "s", "primary_key": ["id"]}]
    extra, contracts = resolve_callable_placements(
        form={}, source_extra={}, stream_contracts=None)
    assert extra == {} and contracts is None
    with pytest.raises(CallablePlacementError):
        resolve_callable_placements(form={"procedure_params": "[1]", "procedure_call": "CALL p()"},
                                    source_extra={}, stream_contracts=None)


# ------------------------------------------------ several streams (Studio shape)


def _multi_form(src: str, dst: str, kind: str, contracts: list[dict], **extra: str) -> dict:
    return {
        "source_kind": "database", "source_format": kind, "source_connector_id": src,
        "source_table": "", "dest_kind": "database", "dest_format": "postgresql",
        "dest_connector_id": dst, "dest_schema": "public", "dest_table": "",
        "stream_contracts_json": json.dumps(contracts), "sync_mode": "full_refresh_overwrite",
        "validation_mode": "strict", "async_mode": "false", **extra,
    }


def _contract(name: str, **extra) -> dict:
    return {"name": name, "selected": True, "sync_mode": "full_refresh_overwrite",
            "primary_key": "id", **extra}


@pytest.fixture
def two_tables():
    import uuid

    from tests.test_r11_mysql_pg_fk_ddl_e2e import _my, _sql

    tag = uuid.uuid4().hex[:6]
    names = [f"r14ms_a_{tag}", f"r14ms_b_{tag}"]
    for t in names:
        _my("mysql", f"CREATE TABLE {t} (id INT UNSIGNED PRIMARY KEY, v VARCHAR(20), hidden INT)"
                     " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            f"INSERT INTO {t} VALUES (1,'x',0),(2,NULL,0),(3,'',1),(4,' 日本 ',0)")
    try:
        yield names
    finally:
        for t in names:
            _my("mysql", f"DROP TABLE IF EXISTS {t}")
            _sql(f"DROP TABLE IF EXISTS {t}")


def _pg_rows(table: str) -> list[tuple]:
    return sorted(h.query("postgresql", f'SELECT id, v FROM "{table}"'))


_ALL = [(1, "x"), (2, None), (3, ""), (4, " 日本 ")]
_VISIBLE = [(1, "x"), (2, None), (4, " 日本 ")]


def _run_ok(client, form: dict) -> dict:
    resp = client.post("/api/v1/transfer/run", data=form)
    assert resp.status_code == 200, resp.text
    job = _wait(str(resp.json().get("job_id") or ""))
    assert job.get("status") == "completed", {k: job.get(k) for k in ("status", "error", "message")}
    return job


def test_each_stream_runs_its_own_query(client, two_tables):
    a, b = two_tables
    src, dst = _connector("mysql", "source"), _connector("postgresql", "destination")
    contracts = [
        _contract(a, source_read_mode="query", source_query=f"SELECT id, v FROM {a} WHERE hidden = 0"),
        _contract(b, source_read_mode="query", source_query=f"SELECT id, v FROM {b} WHERE id <= 2"),
    ]
    for _run in (1, 2):
        job = _run_ok(client, _multi_form(src, dst, "mysql", contracts))
        assert _pg_rows(a) == _VISIBLE and _pg_rows(b) == [(1, "x"), (2, None)]
        streams = (job.get("destination_summary") or {}).get("streams") or job.get("streams") or []
        assert streams and all((s.get("row_accounting") or {}).get("balanced") for s in streams), streams


def test_one_query_stream_beside_a_table_stream(client, two_tables):
    a, b = two_tables
    src, dst = _connector("mysql", "source"), _connector("postgresql", "destination")
    maps = [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "v", "hidden")]
    contracts = [_contract(a, source_read_mode="query",
                           source_query=f"SELECT id, v FROM {a} WHERE hidden = 0",
                           mappings=maps[:2]),
                 _contract(b, mappings=maps)]
    _run_ok(client, _multi_form(src, dst, "mysql", contracts))
    assert _pg_rows(a) == _VISIBLE
    assert sorted(r[:2] for r in h.query("postgresql", f'SELECT id, v FROM "{b}"')) == _ALL


@pytest.mark.parametrize("query_source", ["mysql"], indirect=True)
def test_table_mode_contracts_params_and_legacy_modes_are_not_refused(client, query_source):
    src = _connector("mysql", "source")
    dst = _connector("postgresql", "destination")
    q = query_source["query"]
    # A contract that only says "table" is a table copy, not a placement.
    resp = client.post("/api/v1/transfer/run", data=_form(src, dst, query_source, **{
        "dest_table": query_source["dst"] + "_t",
        "mappings_json": json.dumps([{"source": c, "target": c, "confidence": 1.0}
                                     for c in ("id", "v", "hidden")]),
        "stream_contracts_json": json.dumps([{"name": query_source["src"],
                                              "source_read_mode": "table",
                                              "primary_key": ["id"]}])}))
    assert resp.status_code == 200, resp.text
    assert _wait(str(resp.json()["job_id"])).get("status") == "completed"
    assert len(h.query("postgresql", f'SELECT 1 FROM "{query_source["dst"]}_t"')) == 5
    h.drop_table("postgresql", query_source["dst"] + "_t")
    # An unrecognised mode word is inferred as before: the query runs.
    resp = client.post("/api/v1/transfer/run", data=_form(
        src, dst, query_source, source_read_mode="sql", source_query=q))
    assert resp.status_code == 200, resp.text
    assert _wait(str(resp.json()["job_id"])).get("status") == "completed"
    assert _rows(query_source["dst"]) == EXPECTED


def test_procedure_params_compare_as_objects():
    extra, _ = resolve_callable_placements(
        form={"procedure_call": "CALL p(:n)", "procedure_params": '{"n": 3}'},
        source_extra={"procedure_call": "CALL p(:n)", "procedure_params": {"n": 3}},
        stream_contracts=None,
    )
    assert extra["procedure_params"] == {"n": 3}
    two = [{"name": "a", "source_query": "SELECT 1"}, {"name": "b", "source_query": "SELECT 2"}]
    extra, contracts = resolve_callable_placements(form={}, source_extra={}, stream_contracts=two)
    assert "source_query" not in extra and contracts == two  # left for stream_multi


def test_legacy_mode_word_and_text_binds_on_several_streams(client, two_tables):
    """'sql' is inferred as the query it carries (not a whole-table copy), and a
    stream's procedure_params sent as JSON text are its binds (not dropped)."""
    a, b = two_tables
    src, dst = _connector("mysql", "source"), _connector("postgresql", "destination")
    contracts = [
        _contract(a, source_read_mode="sql", source_query=f"SELECT id, v FROM {a} WHERE hidden = 0"),
        _contract(b, source_read_mode="query", source_query=f"SELECT id, v FROM {b} WHERE id <= :n",
                  procedure_params='{"n": 2}'),
    ]
    _run_ok(client, _multi_form(src, dst, "mysql", contracts))
    assert _pg_rows(a) == _VISIBLE
    assert _pg_rows(b) == [(1, "x"), (2, None)]


def test_stream_patches_normalise_mode_words_and_params():
    from services.multi_stream_plan import patches_for_stream

    src, _dst = patches_for_stream({"source_read_mode": "sql", "source_query": "SELECT 1 AS x"})
    assert src and src["source_read_mode"] == "query" and src["source_query"] == "SELECT 1 AS x"
    src, _dst = patches_for_stream({"procedure_call": "CALL p(:n)", "procedure_params": '{"n": 3}'})
    assert src and src["procedure_params"] == {"n": 3}
    _extra, contracts = resolve_callable_placements(
        form={}, source_extra={},
        stream_contracts=[{"name": "a", "source_query": "SELECT :n", "procedure_params": '{"n": 1}'},
                          {"name": "b", "source_query": "SELECT 2"}])
    assert contracts[0]["procedure_params"] == {"n": 1}
    with pytest.raises(CallablePlacementError):
        resolve_callable_placements(form={}, source_extra={}, stream_contracts=[
            {"name": "a", "procedure_params": "[1]"}, {"name": "b"}])
