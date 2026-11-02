"""SQL query source through the Studio REST routes, on real PostgreSQL.

QA (demo videos, production 557f556c): Studio -> Database -> Orders Postgres ->
Source extract "SQL query". Preview and Validate pass with the right result
columns, then Execute fails every time with "Decision Artifact source schema
drifted since Validate" and writes 0 rows. Stored-procedure mode with the
equivalent set-returning function executes.

Root cause: Studio names a query stream after its first relation
(``procedureStreamName("SELECT ... FROM orders")`` == ``orders``), so the plan's
``source.table`` is the base *table*. Plan Validate re-described the source by
that table name and merged every catalog column of ``orders`` into the query's
result types; Execute describes the query's result set. The two fingerprints
were taken over different column sets. A set-returning function is not a
table, so the catalog lookup came back empty and procedure mode matched.

The same lookup also dropped ``:name`` binds: Preview, Validate's describe and
population scans, Execute and the Gate-8 re-read must all bind the operator's
values as typed parameters.

Routes driven, as the web app calls them: ``/api/v1/transfer/introspect``
(Preview), ``/api/v1/transfer/map``, ``/api/v1/transfer/plans`` +
``/plans/{id}/mappings`` + ``/plans/{id}/preflight`` (Validate),
``/api/v1/transfer/run`` (Execute, Decision Artifact from Validate).
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


# NULL, '', whitespace and unicode in every projected text column.
_ROWS = [
    (1, "ann", "US", "150.00", "paid", "x"),
    (2, "bob", "DE", "50.00", "open", None),
    (3, None, "US", "99.99", "paid", ""),
    (4, "Łukasz 山田", "US", "100.00", "paid", "  "),
    (5, "  ", "US", "250.50", "open", "🎉"),
]


def _create_orders(table: str) -> None:
    h.execute(
        "postgresql",
        f"CREATE TABLE {table} (id int PRIMARY KEY, customer varchar(80), "  # nosec B608
        "country char(2), amount numeric(12,2), status varchar(20), note text, "
        "created_at timestamptz DEFAULT now(), is_paid boolean, meta jsonb)",
    )
    for rid, cust, country, amt, status, note in _ROWS:
        def lit(v):
            return "NULL" if v is None else "'" + v.replace("'", "''") + "'"
        h.execute(
            "postgresql",
            f"INSERT INTO {table} (id, customer, country, amount, status, note) "  # nosec B608
            f"VALUES ({rid}, {lit(cust)}, {lit(country)}, {amt}, {lit(status)}, {lit(note)})",
        )


def _connectors(token: str):
    from services.connector_store import create_connector

    src = create_connector({
        "name": f"r7q-src-{token}", "role": "source", "workspace_id": "", "ssl": False,
        **h.dest_config("postgresql"),
    })
    dst = create_connector({
        "name": f"r7q-dst-{token}", "role": "destination", "workspace_id": "", "ssl": False,
        **h.dest_config("postgresql"),
    })
    return src, dst


def _stream_name(query: str) -> str:
    """apps/web sourceReadMode.ts procedureStreamName: first FROM relation."""
    import re

    m = re.search(r"\b(?:CALL|EXEC(?:UTE)?|FROM)\s+([A-Za-z_][A-Za-z0-9_.]*)", query, re.I)
    return (m.group(1).split(".")[-1]) if m else "procedure_result"


def _source_endpoint(src_id: str, query: str, params: dict[str, str]) -> dict[str, Any]:
    """TransferPage buildSourceEndpoint() for sourceReadMode == "query"."""
    return {
        "kind": "database", "format": "postgresql", "connector_id": src_id,
        "database": h.PG["database"], "table": _stream_name(query),
        "source_read_mode": "query", "source_query": query,
        "procedure_params": params or None,
        "extra": {"source_read_mode": "query", "procedure_call": "",
                  "source_query": query, "procedure_params": params},
    }


def _studio_run(client, src_id: str, dst_id: str, query: str, dest_table: str,
                params: dict[str, str] | None = None) -> dict[str, Any]:
    """Preview -> Map -> Validate (plan route) -> Execute, as Studio does."""
    params = dict(params or {})
    source = _source_endpoint(src_id, query, params)
    destination = {"kind": "database", "format": "postgresql", "connector_id": dst_id,
                   "database": h.PG["database"], "table": dest_table}

    resp = client.post("/api/v1/transfer/introspect",
                       json={"source": source, "destination": destination})
    assert resp.status_code == 200, resp.text
    preview = resp.json()["source"]
    dest_info = resp.json()["destination"] or {}
    dest_exists = bool(dest_info.get("table_exists"))
    dest_cols = list(dest_info.get("columns") or []) if dest_exists else []
    dest_types = dict(dest_info.get("schema") or {}) if dest_exists else {}
    assert preview.get("connected") is not False, preview
    cols = list(preview.get("columns") or [])
    types = dict(preview.get("schema") or {})
    samples = list(preview.get("sample_data") or preview.get("data") or [])
    assert cols, preview

    resp = client.post("/api/v1/transfer/map", json={
        "source_columns": cols, "source_schema": types, "use_llm": False,
        "source_samples": {c: [str(r.get(c)) for r in samples] for c in cols},
        "destination_db_type": "postgresql", "sync_mode": "full_refresh_overwrite",
        "destination_table_exists": dest_exists, "target_columns": dest_cols,
        "target_schema": dest_types, "source_kind": "database", "source_db_type": "postgresql",
        "validation_mode": "strict",
    })
    assert resp.status_code == 200, resp.text
    mappings = resp.json()["mappings"]

    policies = {"sync_mode": "full_refresh_overwrite", "schema_policy": "manual_review",
                "validation_mode": "strict", "stream_contracts": []}
    resp = client.post("/api/v1/transfer/plans", json={
        "name": source["table"], "source": source, "destination": destination,
        "source_columns": cols, "source_schema": types, "target_columns": dest_cols,
        "target_schema": dest_types, "row_count_estimate": len(samples),
        "sample_rows": samples[:25], "policies": policies,
    })
    assert resp.status_code == 200, resp.text
    plan_id = resp.json()["plan"]["id"]
    resp = client.post(f"/api/v1/transfer/plans/{plan_id}/mappings", json={"mappings": mappings})
    assert resp.status_code == 200, resp.text
    resp = client.post(f"/api/v1/transfer/plans/{plan_id}/preflight", json={})
    assert resp.status_code == 200, resp.text
    pf = resp.json()
    blockers = [b for b in (pf.get("blockers") or []) if isinstance(b, dict)]
    assert pf.get("passed") is True, [(b.get("id"), b.get("message")) for b in blockers]
    proof = pf.get("proof_bundle") or {}
    artifact = proof.get("decision_artifact") or {}
    assert artifact.get("content_hash"), proof.keys()

    form = {
        "source_kind": "database", "source_format": "postgresql",
        "source_connector_id": src_id, "source_database": h.PG["database"],
        "source_table": source["table"],
        "dest_kind": "database", "dest_format": "postgresql",
        "dest_database": h.PG["database"], "dest_table": dest_table,
        "dest_collection": dest_table, "dest_connector_id": dst_id,
        "skip_preflight": "false", "async_mode": "false", "plan_id": plan_id,
        "mappings_json": json.dumps(mappings), "sync_mode": "full_refresh_overwrite",
        "validation_mode": "strict", "schema_policy": "manual_review",
        "source_extra_json": json.dumps({
            "source_read_mode": "query", "source_query": query,
            **({"procedure_params": params} if params else {}),
        }),
        "approved_decision_artifact_hash":
            proof.get("decision_artifact_hash") or artifact["content_hash"],
        "approved_ddl_identity_hash": (proof.get("ddl_identity") or {}).get("ddl_identity_hash", ""),
        "decision_artifact_json": json.dumps(artifact),
    }
    resp = client.post("/api/v1/transfer/run", data=form)
    body = resp.json()
    assert resp.status_code == 200, body
    assert body.get("success") is True, body
    return {"preview": preview, "preflight": pf, "run": body}


def _dest(table: str, cols: str) -> list[tuple]:
    return h.query("postgresql", f"SELECT {cols} FROM {table} ORDER BY 1")  # nosec B608


@pytest.fixture
def orders():
    token = uuid.uuid4().hex[:8]
    src_table = f"r7q_orders_{token}"
    dest_table = f"r7q_dest_{token}"
    _create_orders(src_table)
    try:
        yield token, src_table, dest_table
    finally:
        h.drop_table("postgresql", src_table)
        h.drop_table("postgresql", dest_table)


def _text(rows, *idx):
    return [tuple(r[i] for i in idx) for r in rows]


def test_projection_query_executes_after_validate_and_reruns(client, orders):
    token, src_table, dest_table = orders
    src, dst = _connectors(token)
    q = f"SELECT id, customer, amount, note FROM {src_table} WHERE country = 'US'"
    out = _studio_run(client, src.id, dst.id, q, dest_table)
    assert out["run"]["records_transferred"] == 4
    want = [(r[0], r[1], r[3], r[5]) for r in _ROWS if r[2] == "US"]
    got = [(int(a), b, f"{c:.2f}", d) for a, b, c, d in _dest(dest_table, "id, customer, amount, note")]
    assert got == want
    # Only the query's columns land — never the base table's.
    cols = [r[0] for r in h.query(
        "postgresql",
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{dest_table}' ORDER BY ordinal_position",  # nosec B608
    )]
    assert cols == ["id", "customer", "amount", "note"]

    # Second run into the table DataFlow created.
    out2 = _studio_run(client, src.id, dst.id, q, dest_table)
    assert out2["run"]["records_transferred"] == 4
    got = [(int(a), b, f"{c:.2f}", d) for a, b, c, d in _dest(dest_table, "id, customer, amount, note")]
    assert got == want


def test_aggregate_query_executes_after_validate(client, orders):
    token, src_table, dest_table = orders
    src, dst = _connectors(token)
    q = (f"SELECT country, COUNT(*) AS orders, SUM(amount) AS total "
         f"FROM {src_table} GROUP BY country")
    out = _studio_run(client, src.id, dst.id, q, dest_table)
    assert out["run"]["records_transferred"] == 2
    got = [(a.strip(), int(b), f"{c:.2f}") for a, b, c in _dest(dest_table, "country, orders, total")]
    assert got == [("DE", 1, "50.00"), ("US", 4, "600.49")]


def test_named_bind_reaches_preview_validate_execute_and_gate8(client, orders):
    token, src_table, dest_table = orders
    src, dst = _connectors(token)
    q = (f"SELECT id, customer, amount, note FROM {src_table} "
         "WHERE amount >= :min_amount AND country = :cc")
    out = _studio_run(client, src.id, dst.id, q, dest_table,
                      params={"min_amount": "100", "cc": "US"})
    want = [(r[0], r[1], r[3], r[5]) for r in _ROWS if r[2] == "US" and float(r[3]) >= 100]
    # Preview bound the values (rows are filtered, not refused).
    assert sorted(int(r["id"]) for r in out["preview"]["sample_data"]) == [w[0] for w in want]
    assert out["run"]["records_transferred"] == len(want)
    got = [(int(a), b, f"{c:.2f}", d) for a, b, c, d in _dest(dest_table, "id, customer, amount, note")]
    assert got == want
    # Gate-8 re-read ran the same bound query and matched.
    from services.mongodb_service import get_mongodb_service

    job = get_mongodb_service().get_job(str(out["run"]["job_id"])) or {}
    assert str(job.get("status")) == "completed", job.get("error") or job.get("message")
    assert "verified" in str(job.get("message") or "").lower(), job.get("message")


def test_bind_value_is_a_typed_parameter_not_interpolated(client, orders):
    """A quote in the bound value is data, never SQL."""
    token, src_table, dest_table = orders
    src, dst = _connectors(token)
    q = f"SELECT id, customer FROM {src_table} WHERE customer = :who OR id = :rid"
    out = _studio_run(client, src.id, dst.id, q, dest_table,
                      params={"who": "x' OR '1'='1", "rid": "4"})
    assert out["run"]["records_transferred"] == 1
    assert [(int(a), b) for a, b in _dest(dest_table, "id, customer")] == [(4, "Łukasz 山田")]


def test_unbound_name_is_refused_in_operator_words_before_the_engine(client, orders):
    """No Bound value: say which :name needs one — not SQLAlchemy's error."""
    token, src_table, _dest_table = orders
    src, _dst = _connectors(token)
    q = f"SELECT id, amount FROM {src_table} WHERE amount >= :min_amount"
    for params in ({}, {"other": "1"}):
        resp = client.post("/api/v1/transfer/introspect", json={
            "source": _source_endpoint(src.id, q, params),
            "destination": {"kind": "file_export", "format": "json"},
        })
        assert resp.status_code == 200, resp.text
        info = resp.json()["source"]
        assert not info.get("columns"), info
        msg = str(info.get("message") or "")
        assert ":min_amount" in msg and "Bound value" in msg, msg
        assert "sqlalchemy" not in msg.lower() and "sqlalche.me" not in msg, msg


def test_listings_hide_internal_objects(client):
    """The error state's "Use <table>" chips list the catalog — never our own tables."""
    token = uuid.uuid4().hex[:8]
    schema = f"r7q_sch_{token}"
    internal = ["dataflow_signal", "datawrap_signal", "orders_df_quarantine",
                "_df_stg_orders", "_dataflow_write_ledger", "orders__df_bak_ab12cd"]
    h.execute("postgresql", f"CREATE SCHEMA {schema}")
    try:
        for name in ["orders", "orders_dfq_notes", *internal]:
            h.execute("postgresql", f'CREATE TABLE {schema}."{name}" (id int)')  # nosec B608
        from services.connector_store import create_connector

        src = create_connector({
            "name": f"r7q-sch-{token}", "role": "source", "workspace_id": "", "ssl": False,
            **h.dest_config("postgresql"), "schema": schema,
        })
        q = "SELECT id FROM orders WHERE id >= :min_id"
        for source in (
            _source_endpoint(src.id, q, {}),  # the failing query: error state + chips
            {"kind": "database", "format": "postgresql", "connector_id": src.id,
             "database": h.PG["database"], "table": "orders"},  # Table mode picker
        ):
            resp = client.post("/api/v1/transfer/introspect", json={
                "source": source, "destination": {"kind": "file_export", "format": "json"},
            })
            assert resp.status_code == 200, resp.text
            names = [o.get("name") for o in resp.json()["source"].get("objects") or []]
            assert "orders" in names and "orders_dfq_notes" in names, names
            assert not set(internal) & set(names), names
    finally:
        h.execute("postgresql", f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_validate_note_is_worded_for_sql_query_mode(client, orders):
    token, src_table, dest_table = orders
    src, dst = _connectors(token)
    q = f"SELECT id, customer FROM {src_table} WHERE country = 'US'"
    out = _studio_run(client, src.id, dst.id, q, dest_table)
    note = str((out["preflight"].get("callable_extract") or {}).get("note") or "")
    assert note, out["preflight"].get("callable_extract")
    assert "procedure" not in note.lower(), note
    assert "query" in note.lower(), note
