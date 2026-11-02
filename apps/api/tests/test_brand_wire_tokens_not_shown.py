"""The internal wire tokens (``__DF_SQL_NULL__``, ``__DF_ESC__``) never reach a person.

They carry SQL NULL and a colliding literal between reader and writer and are
decoded at the writer leaf, so nothing in the customer's destination holds
them (the brand-leak probe found the literal only where it was the user's own
data). Two places still showed them:

* the job API (``GET /jobs/{id}``) returned ``destination_summary.
  reconcile_sample`` as stored: a NULL cell read ``__DF_SQL_NULL__`` and a user
  literal read ``__DF_ESC____DF_SQL_NULL__``;
* MCP ``get_job`` quarantine samples printed ``str(value)``: a NULL cell read
  ``__DF_SQL_NULL__`` (and an absent value printed as ``""``, i.e. NULL and
  empty string looked the same).

The stored job keeps the wire (Gate-8 and replay read it); only what is shown
is projected. Real engines: PostgreSQL -> MySQL through execute_tracked.
"""

from __future__ import annotations

import socket
import uuid

import pytest

PG = {"host": "127.0.0.1", "port": 5432, "database": "dataflow", "username": "dataflow", "password": "dataflow"}
MY = {"host": "127.0.0.1", "port": 3306, "database": "dataflow", "username": "dataflow", "password": "dataflow"}
LITERAL = "__DF_SQL_NULL__"


def _up(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


@pytest.fixture()
def pg_source():
    if not (_up(5432) and _up(3306)):
        pytest.skip("PostgreSQL / MySQL down")
    import psycopg2

    conn = psycopg2.connect(host="127.0.0.1", user="dataflow", password="dataflow", dbname="dataflow")
    conn.autocommit = True
    table = "brandwire_" + uuid.uuid4().hex[:8]
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{table}" (id int PRIMARY KEY, note text, amt numeric(12,2))')
        cur.execute(
            f'INSERT INTO public."{table}" VALUES (1, NULL, NULL), (2, %s, 1.50), '
            "(3, '', 0), (4, '  ', 2), (5, 'é中\U0001f600', 3)",
            (LITERAL,),
        )
    yield table
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    conn.close()
    import pymysql

    my = pymysql.connect(host="127.0.0.1", user="dataflow", password="dataflow", database="dataflow", autocommit=True)
    my.cursor().execute(f"DROP TABLE IF EXISTS `{table}_d`")
    my.close()


def test_job_api_shows_null_and_the_users_literal_not_wire_tokens(pg_source, monkeypatch):
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest, sanitize_job_for_api
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    job_id = "brandwire" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema="public", table=pg_source, **PG),
        destination=EndpointConfig(kind="database", format="mysql", table=pg_source + "_d", **MY),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{"name": pg_source, "sync_mode": "upsert", "primary_key": "id", "selected": True}],
    )
    for run in (1, 2):  # run 2 lands in the table run 1 created
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success, result.error
        stored = fake.jobs[job_id]
        sample = (stored.get("destination_summary") or {}).get("reconcile_sample") or []
        assert sample, f"run {run}: no reconcile_sample stored"
        shown = sanitize_job_for_api(stored)
        rows = {str(r.get("id")): r for r in shown["destination_summary"]["reconcile_sample"]}
        assert rows["1"]["note"] is None and rows["1"]["amt"] is None, rows["1"]
        assert rows["2"]["note"] == LITERAL, rows["2"]  # the user's own text
        assert rows["3"]["note"] == "" and rows["4"]["note"] == "  "
        assert rows["5"]["note"] == "é中\U0001f600"
        assert "__DF_ESC__" not in repr(shown)
        # The stored job keeps the wire for Gate-8 / replay.
        assert any("__DF_" in repr(v) for r in sample for v in r.values())


def test_mcp_get_job_quarantine_samples_show_null_not_the_token(monkeypatch):
    from services.value_serializer import SQL_NULL_SENTINEL, escape_wire_text
    from src.ai.copilot import job_reads
    from src.ai.copilot.tools import DataPilotTools

    job = {
        "_id": "brandq1",
        "status": "completed_with_warnings",
        "rejected_rows": 3,
        "rejected_details": [
            {"row": 1, "column": "note", "value": SQL_NULL_SENTINEL, "reason": "bad"},
            {"row": 2, "column": "note", "value": escape_wire_text(LITERAL), "reason": "bad"},
            {"row": 3, "column": "note", "value": "", "reason": "bad"},
        ],
        "transfer_request": {},
    }
    monkeypatch.setattr(job_reads, "read_transfer_job", lambda _id: dict(job))
    res = DataPilotTools().execute("get_job", {"job_id": "brandq1"})
    assert res.success, res.error
    text = repr(res.output)
    assert "__DF_ESC__" not in text
    samples = {s["row"]: s["value"] for s in res.output.get("quarantine_samples") or res.output.get("samples") or []}
    assert samples, res.output.keys()
    assert samples[1] is None
    assert samples[2] == LITERAL
    assert samples[3] == ""
