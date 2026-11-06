"""QA C3 (third review): one intake decides "table, query or procedure" on every route.

Review repro: POST ``/transfer/execute`` with ``source.table=a`` and ONE stream
contract ``{source_read_mode: "query", source_query: "SELECT … FROM b …"}``
answered 200 and copied the four rows of ``a``. ``{source_read_mode: "call",
procedure_call: ""}`` did the same, and so did a saved one-contract schedule.
Only the form route (``/transfer/run``) resolved the placements. An unknown
mode word, or a CALL and a SELECT with no word, was HTTP 500 on the JSON route.
A query mode whose SELECT sat under ``procedure_call`` ran there and was
refused on the form route.

Now ``services.callable_intake.apply_callable_intake`` runs on every route
that accepts a source: JSON execute, form run, schedule save and fire,
preflight, chat Confirm. ``job_submission.create_pending_job`` runs it as
well, so a route that skips it still cannot queue an unresolved source.
Live MySQL -> PostgreSQL.
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r11_mysql_pg_fk_ddl_e2e import (  # noqa: F401 — fixture re-export
    _connector,
    _my,
    _sql,
    _wait,
    client,
)

pytestmark = pytest.mark.timeout(600)


@pytest.fixture
def two_tables():
    tag = uuid.uuid4().hex[:6]
    names = [f"r14ci_a_{tag}", f"r14ci_b_{tag}"]
    for t in names:
        _my("mysql", f"CREATE TABLE {t} (id INT UNSIGNED PRIMARY KEY, v VARCHAR(20))"
                     " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            f"INSERT INTO {t} VALUES (1,'x'),(2,NULL),(3,''),(4,' 日本 ')")
    try:
        yield names
    finally:
        for t in names:
            _my("mysql", f"DROP TABLE IF EXISTS {t}")
            _sql(f"DROP TABLE IF EXISTS {t}")


def _pg_rows(table: str) -> list[tuple]:
    return sorted(h.query("postgresql", f'SELECT id, v FROM "{table}"'))


def _pg_exists(table: str) -> bool:
    return bool(h.query("postgresql", f"SELECT to_regclass('public.\"{table}\"')")[0][0])


_MAPS = [{"source": c, "target": c, "confidence": 1.0} for c in ("id", "v")]


def _execute_body(src: str, dst: str, table: str, *, contracts=None, extra=None) -> dict:
    return {
        "source": {"kind": "database", "format": "mysql", "connector_id": src, "table": table,
                   **({"extra": extra} if extra is not None else {})},
        "destination": {"kind": "database", "format": "postgresql", "connector_id": dst,
                        "table": table, "schema": "public"},
        "mappings": _MAPS, "stream_contracts": list(contracts or []),
        "sync_mode": "full_refresh_overwrite", "validation_mode": "strict", "async_mode": False,
    }


def test_json_execute_runs_a_single_contract_query_not_the_table(client, two_tables):
    a, b = two_tables
    src, dst = _connector("mysql", "source"), _connector("postgresql", "destination")
    contract = {"name": a, "selected": True, "primary_key": "id",
                "source_read_mode": "query", "source_query": f"SELECT id, v FROM {b} WHERE id = 4"}
    for _run in (1, 2):
        resp = client.post("/api/v1/transfer/execute", json=_execute_body(src, dst, a,
                                                                           contracts=[contract]))
        assert resp.status_code == 200, resp.text
        job = _wait(str(resp.json().get("job_id") or ""))
        assert job.get("status") == "completed", {k: job.get(k) for k in ("status", "error")}
        # b's row 4 only — never the four rows of a.
        assert _pg_rows(a) == [(4, " 日本 ")]


@pytest.mark.parametrize("case", ["call_empty", "unknown_word", "call_and_select_no_word",
                                  "query_word_select_under_procedure_call"])
def test_json_execute_refuses_unhonourable_sources_with_422(client, two_tables, case):
    a, b = two_tables
    src, dst = _connector("mysql", "source"), _connector("postgresql", "destination")
    shapes = {
        "call_empty": {"contracts": [{"name": a, "source_read_mode": "call",
                                      "procedure_call": ""}]},
        "unknown_word": {"extra": {"source_read_mode": "tabel",
                                   "source_query": f"SELECT id, v FROM {b}"}},
        "call_and_select_no_word": {"extra": {"procedure_call": "CALL nothing_here()",
                                              "source_query": f"SELECT id, v FROM {b}"}},
        "query_word_select_under_procedure_call": {"contracts": [{
            "name": a, "source_read_mode": "query",
            "procedure_call": f"SELECT id, v FROM {b} WHERE id = 4"}]},
    }
    resp = client.post("/api/v1/transfer/execute", json=_execute_body(src, dst, a, **shapes[case]))
    assert resp.status_code == 422, (resp.status_code, resp.text[:400])
    assert not _pg_exists(a)


def test_form_and_json_agree_on_a_select_under_procedure_call(client, two_tables):
    a, b = two_tables
    src, dst = _connector("mysql", "source"), _connector("postgresql", "destination")
    form = {
        "source_kind": "database", "source_format": "mysql", "source_connector_id": src,
        "source_table": a, "dest_kind": "database", "dest_format": "postgresql",
        "dest_connector_id": dst, "dest_schema": "public", "dest_table": a,
        "mappings_json": json.dumps(_MAPS), "sync_mode": "full_refresh_overwrite",
        "validation_mode": "strict", "async_mode": "false",
        "source_extra_json": json.dumps({"source_read_mode": "query",
                                         "procedure_call": f"SELECT id, v FROM {b}"}),
    }
    assert client.post("/api/v1/transfer/run", data=form).status_code == 422
    resp = client.post("/api/v1/transfer/execute", json=_execute_body(
        src, dst, a, extra={"source_read_mode": "query",
                            "procedure_call": f"SELECT id, v FROM {b}"}))
    assert resp.status_code == 422, resp.text[:400]
    assert not _pg_exists(a)


def test_preflight_refuses_an_unknown_word_and_an_empty_call_with_422(client, two_tables):
    a, _b = two_tables
    src = _connector("mysql", "source")
    base = {"columns": ["id", "v"], "mappings": _MAPS, "source_kind": "database",
            "source_type": "mysql", "source_connector_id": src, "source_table": a,
            "dest_type": "postgresql", "dest_table": a}
    for body in ({"stream_contracts": [{"name": a, "source_read_mode": "call"}]},
                 {"source_config": {"source_read_mode": "tabel", "source_query": "SELECT 1"}}):
        resp = client.post("/api/v1/preflight/run", json={**base, **body})
        assert resp.status_code == 422, (body, resp.status_code, resp.text[:400])


# ------------------------------------------------------------------ schedules


@pytest.fixture
def sched_client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _schedule_body(src: str, dst: str, table: str, contract: dict) -> dict:
    return {"name": f"r14ci {uuid.uuid4().hex[:6]}", "source_connector_id": src,
            "source_table": table, "dest_connector_id": dst, "dest_table": table,
            "interval": "hourly", "sync_mode": "full_refresh_overwrite", "mappings": _MAPS,
            "stream_contracts": [contract], "enabled": True}


def test_a_saved_one_contract_schedule_runs_its_query(sched_client, two_tables):
    from tests.test_r12_mongo_incremental_schedule_e2e import _beat

    a, b = two_tables
    src, dst = _connector("mysql", "source"), _connector("postgresql", "destination")
    contract = {"name": a, "selected": True, "primary_key": "id",
                "source_read_mode": "query", "source_query": f"SELECT id, v FROM {b} WHERE id = 4"}
    resp = sched_client.post("/api/v1/schedules/", json=_schedule_body(src, dst, a, contract))
    assert resp.status_code == 201, resp.text
    run = _beat(resp.json()["id"])
    job = run["job"]
    assert job.get("status") == "completed", (run["started"], job.get("error"),
                                              getattr(run["schedule"], "last_status", None))
    assert _pg_rows(a) == [(4, " 日本 ")]


def test_a_schedule_with_an_unhonourable_contract_is_422_at_save(sched_client, two_tables):
    a, b = two_tables
    src, dst = _connector("mysql", "source"), _connector("postgresql", "destination")
    for contract in ({"name": a, "source_read_mode": "call", "procedure_call": ""},
                     {"name": a, "source_read_mode": "tabel", "source_query": f"SELECT * FROM {b}"}):
        resp = sched_client.post("/api/v1/schedules/", json=_schedule_body(src, dst, a, contract))
        assert resp.status_code == 422, (contract, resp.status_code, resp.text[:400])


def test_a_stored_unhonourable_schedule_fails_with_a_readable_reason(sched_client, two_tables,
                                                                     monkeypatch):
    """A schedule saved before this check fires into a readable refusal, not a table copy."""
    from services import schedule_store
    from services.schedule_store import get_schedule, update_schedule
    from tests.test_r12_mongo_incremental_schedule_e2e import _beat

    a, b = two_tables
    src, dst = _connector("mysql", "source"), _connector("postgresql", "destination")
    plain = {"name": a, "selected": True, "primary_key": "id"}
    resp = sched_client.post("/api/v1/schedules/", json=_schedule_body(src, dst, a, plain))
    assert resp.status_code == 201, resp.text
    sid = resp.json()["id"]
    # Stored as an older build (no save-time check) would have stored it.
    with monkeypatch.context() as m:
        m.setattr(schedule_store, "_assert_callable_schedule_sync", lambda *_a, **_k: None)
        update_schedule(sid, {"stream_contracts": [{**plain, "source_read_mode": "call",
                                                    "procedure_call": ""}]})
    run = _beat(sid)
    assert not _pg_exists(a)
    sched = get_schedule(sid)
    text = json.dumps(sched.to_dict(), default=str)
    assert "set to procedure" in text, text[:600]


# ------------------------------------------------------------------ backstop


def test_job_creation_refuses_an_unresolved_source_on_any_route(client):
    from services.procedure_source import ProcedureSourceError
    from src.transfer.engine import get_transfer_engine
    from src.transfer.models import EndpointConfig, TransferRequest

    req = TransferRequest(
        source=EndpointConfig(kind="database", format="mysql", table="a",
                              extra={"source_read_mode": "tabel", "source_query": "SELECT 1"}),
        destination=EndpointConfig(kind="database", format="postgresql", table="a"),
    )
    with pytest.raises(ProcedureSourceError, match="unknown source_read_mode"):
        get_transfer_engine()._create_pending_job(req)
    req = TransferRequest(
        source=EndpointConfig(kind="database", format="mysql", table="a"),
        destination=EndpointConfig(kind="database", format="postgresql", table="a"),
        stream_contracts=[{"name": "a", "source_query": "SELECT 1 AS id"}],
    )
    from services.callable_intake import apply_callable_intake

    apply_callable_intake(req)
    apply_callable_intake(req)  # idempotent: a second pass changes nothing
    assert req.source.extra.get("source_query") == "SELECT 1 AS id"
    assert req.stream_contracts == [{"name": "a"}]


# ------------------------------------------- 422, never 500, and one schedule vocabulary


def test_a_procedure_source_error_anywhere_in_a_handler_is_422(client):
    """A mode error raised outside the intake (an unguarded is_callable_source) is a 422."""
    from services.procedure_source import is_callable_source
    from src.main import app

    path = f"/api/v1/__r14_callable_probe_{uuid.uuid4().hex[:6]}"

    def _probe():
        return {"callable": is_callable_source({"extra": {"source_read_mode": "tabel"}})}

    app.add_api_route(path, _probe, methods=["GET"])
    try:
        resp = client.get(path)
        assert resp.status_code == 422, (resp.status_code, resp.text[:300])
        assert "unknown source_read_mode" in resp.text
    finally:
        app.router.routes[:] = [r for r in app.router.routes if getattr(r, "path", "") != path]


def test_a_schedule_stamps_its_source_with_the_one_vocabulary():
    from types import SimpleNamespace

    from services.procedure_source import UnknownReadModeError
    from services.schedule_runner import _apply_callable_schedule_source

    def _stamp(**sched):
        src = SimpleNamespace(extra={})
        _apply_callable_schedule_source(src, SimpleNamespace(**{
            "source_read_mode": "", "procedure_call": "", "source_query": "",
            "procedure_params": {}, **sched}))
        return src.extra

    assert _stamp(source_read_mode="sql", source_query="SELECT 1")["source_read_mode"] == "query"
    assert _stamp(source_read_mode="call", procedure_call="CALL p()")["source_read_mode"] == \
        "procedure"
    # An explicit table read is a table read, even next to a leftover CALL.
    assert _stamp(source_read_mode="table", procedure_call="CALL p()") == {}
    with pytest.raises(UnknownReadModeError):
        _stamp(source_read_mode="tabel", source_query="SELECT 1")
