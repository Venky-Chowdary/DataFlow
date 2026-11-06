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
        "two_streams": {"stream_contracts_json": json.dumps(
            [{"name": "a", "source_query": q}, {"name": "b", "source_query": q}])},
        "bad_mode": {"source_read_mode": "sql"},
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
