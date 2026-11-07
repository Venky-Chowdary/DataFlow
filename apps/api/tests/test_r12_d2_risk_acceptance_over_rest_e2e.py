"""QA r12 MongoDB D2 / Snowflake D5 + D6: quarantine opt-in over REST.

QA (production 8f7d246d): MongoDB -> CSV with one bad Decimal was refused with
"Sample transform / cast failures ... pass risk_acceptance ... to
plan_transfer / start_transfer, or to rerun_job". No REST input accepted
risk_acceptance: ``POST /transfer/run`` had no such form field (also tried as
``risk_acceptance_json``), ``POST /jobs/{id}/retry`` took no body, and
``rerun_job`` refuses a *failed* job, so the advice was a dead end.

Now ``/transfer/run`` takes ``risk_acceptance`` (alias ``risk_acceptance_json``,
JSON-encoded form field) and ``/jobs/{id}/retry`` (like ``/rerun``) takes it in
the JSON body; both sign the same continue-policy Risk Contract MCP signs. The
failed job's error names retry (``retry_job`` / ``/retry``), which accepts it.

Live: MongoDB on 127.0.0.1:27017, PostgreSQL (``PG*`` env) for the
PG -> Snowflake (fakesnow) leg. Each skips when unreachable.
"""

from __future__ import annotations

import csv
import io
import json
import os
import uuid
from decimal import Decimal

import pytest

from tests import r4_route_harness as h

pymongo = pytest.importorskip("pymongo")
from bson.decimal128 import Decimal128  # noqa: E402

pytestmark = pytest.mark.timeout(600)

ACCEPTANCE = {
    "approved_by": "qa@example.com",
    "reason": "one source row carries a non-numeric amount; hold it out",
    "execution_policy": "CAST_AND_CONTINUE",
    "on_cast_failure": "quarantine",
    "columns": ["amount"],
}

# NULL, '', whitespace and unicode names; decimals at full scale; id 6 is bad.
_GOOD = [
    (1, "ann", "1234.5678"),
    (2, None, "0.0001"),
    (3, "", "-999999.9999"),
    (4, "   ", "12.3400"),
    (5, "Zoë 東京 ✓", "100.0000"),
    (7, "eve", "7.0007"),
]
_BAD = (6, "bad", "abc")

_MAPPINGS = [
    {"source": "_id", "target": "_id", "source_type": "INTEGER",
     "target_type": "INTEGER", "confidence": 0.99},
    {"source": "name", "target": "name", "source_type": "VARCHAR",
     "target_type": "VARCHAR(100)", "confidence": 0.99},
    {"source": "amount", "target": "amount", "source_type": "VARCHAR",
     "target_type": "DECIMAL(12,4)", "transform": "decimal", "confidence": 0.99},
]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(tmp_path / "exports"))
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.fixture
def collection():
    if not h.port_up(27017):
        pytest.skip("MongoDB not reachable on :27017")
    mc = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    coll = mc["dataflow_test"]["r12d2_" + uuid.uuid4().hex[:8]]
    docs = [{"_id": i, "name": n, "amount": Decimal128(a)} for i, n, a in _GOOD]
    docs.append({"_id": _BAD[0], "name": _BAD[1], "amount": _BAD[2]})
    coll.insert_many(docs)
    try:
        yield coll
    finally:
        coll.drop()
        mc.close()


def _mongo_form(coll, out_name: str, **extra) -> dict:
    return {
        "source_kind": "database", "source_format": "mongodb",
        "source_connection_string": "mongodb://127.0.0.1:27017",
        "source_database": "dataflow_test",
        "source_collection": coll.name, "source_table": coll.name,
        "dest_kind": "file", "dest_format": "csv",
        "dest_output_path": out_name, "dest_table": out_name.rsplit(".", 1)[0],
        "async_mode": "false", "sync_mode": "full_refresh_overwrite",
        "mappings_json": json.dumps(_MAPPINGS),
        **extra,
    }


def _csv_rows(path: str) -> dict[int, tuple]:
    with open(path, encoding="utf-8", newline="") as fh:
        text = fh.read()
    out = {}
    for row in csv.DictReader(io.StringIO(text)):
        out[int(row["_id"])] = (row["name"], Decimal(row["amount"]))
    return out


def _assert_csv(path: str) -> None:
    got = _csv_rows(path)
    # CSV has no NULL: a missing name and '' both read back as ''.
    want = {i: ("" if n is None else n, Decimal(a)) for i, n, a in _GOOD}
    assert got == want, (got, want)


_TERMINAL = {"completed", "completed_with_quarantine", "failed", "partial", "cancelled"}


def _wait(job_id: str, timeout_s: float = 240.0) -> dict:
    import time

    from services.mongodb_service import get_mongodb_service

    deadline = time.monotonic() + timeout_s
    job: dict = {}
    while time.monotonic() < deadline:
        job = get_mongodb_service().get_job(job_id) or {}
        if str(job.get("status") or "") in _TERMINAL:
            return job
        time.sleep(0.25)
    return job


def _detail(resp) -> str:
    body = resp.json()
    detail = body.get("detail", body)
    return json.dumps(detail) if not isinstance(detail, str) else detail


def test_block_names_a_retry_route_that_exists(client, collection):
    resp = client.post("/api/v1/transfer/run", data=_mongo_form(collection, "d2_block.csv"))
    assert resp.status_code == 422, resp.text
    text = _detail(resp)
    assert "Sample transform / cast failures" in text
    assert "retry_job" in text and "/retry" in text, text
    assert "risk_acceptance form field" in text, text
    # rerun_job refuses a failed job: the advice must not send operators there.
    assert "rerun_job" not in text, text


@pytest.mark.parametrize("field", ["risk_acceptance", "risk_acceptance_json"])
def test_run_with_risk_acceptance_quarantines_bad_row(client, collection, field):
    resp = client.post(
        "/api/v1/transfer/run",
        data=_mongo_form(collection, f"d2_{field}.csv", **{field: json.dumps(ACCEPTANCE)}),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    acct = body["destination"]["row_accounting"]
    assert acct["rows_written"] == len(_GOOD), acct
    assert acct["rows_quarantined"] == 1, acct
    _assert_csv(body["destination"]["path"])
    job = _wait(body["job_id"])
    contract = {m["source"]: m.get("risk_contract") for m in job["transfer_request"]["mappings"]}
    assert contract["amount"]["execution_policy"] == "CAST_AND_CONTINUE"
    assert contract["_id"] is None


def test_retry_with_risk_acceptance_quarantines_bad_row(client, collection):
    resp = client.post("/api/v1/transfer/run", data=_mongo_form(collection, "d2_retry.csv"))
    assert resp.status_code == 422, resp.text
    failed_id = resp.json()["detail"]["job_id"]
    assert _wait(failed_id)["status"] == "failed"

    resp = client.post(
        f"/api/v1/connectors/jobs/{failed_id}/retry", json={"risk_acceptance": ACCEPTANCE}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["retry_of"] == failed_id
    job = _wait(body["job_id"])
    assert job["status"] in {"completed", "completed_with_quarantine"}, (
        job.get("status"), job.get("error"),
    )
    assert job["confirmation"]["risk_accepted_columns"] == ["amount"]
    from services.export_paths import exports_dir

    _assert_csv(str(exports_dir() / "d2_retry.csv"))


@pytest.mark.parametrize(
    "value,needle",
    [
        ("yes", "not valid JSON"),
        ('"yes"', "must be an object"),
        (json.dumps({"execution_policy": "CAST_AND_CONTINUE"}), "approved_by"),
        (json.dumps({**ACCEPTANCE, "execution_policy": "FAIL_JOB"}), "does not clear a gate"),
    ],
)
def test_malformed_acceptance_is_refused_before_a_job(client, collection, value, needle):
    resp = client.post(
        "/api/v1/transfer/run",
        data=_mongo_form(collection, "d2_bad.csv", risk_acceptance=value),
    )
    assert resp.status_code == 400, resp.text
    assert needle in _detail(resp)


def test_retry_body_with_malformed_acceptance_is_refused(client, collection):
    resp = client.post("/api/v1/transfer/run", data=_mongo_form(collection, "d2_rb.csv"))
    failed_id = resp.json()["detail"]["job_id"]
    resp = client.post(
        f"/api/v1/connectors/jobs/{failed_id}/retry",
        json={"risk_acceptance_json": json.dumps({"reason": "x"})},
    )
    assert resp.status_code == 400, resp.text
    assert "approved_by" in resp.text


def test_mcp_retry_job_stages_the_acceptance(monkeypatch):
    from src.ai.copilot import lifecycle_tools
    from src.ai.copilot.tools import get_pilot_tools

    monkeypatch.setattr(
        lifecycle_tools,
        "resolve_job",
        lambda job_id, selector: ({"job_id": "job-r12d2", "status": "failed"}, ""),
    )
    result = get_pilot_tools().execute(
        "retry_job", {"job_id": "job-r12d2", "risk_acceptance": ACCEPTANCE}
    )
    assert result.success, result.error
    assert result.output["risk_acceptance"] == ACCEPTANCE
    bad = get_pilot_tools().execute("retry_job", {"job_id": "job-r12d2", "risk_acceptance": "yes"})
    assert not bad.success


# --------------------------------------------------------------------------
# Snowflake D5: the same family on PG -> Snowflake (fakesnow stands in).
# --------------------------------------------------------------------------


@pytest.fixture
def pg_snowflake(tmp_path, monkeypatch, client):
    pytest.importorskip("fakesnow")
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    table = f"r12d2_src_{token}"
    h.pg_conn().close()
    h.execute("postgresql", f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT, amount TEXT)")
    for i, n, a in [*_GOOD, _BAD]:
        name = "NULL" if n is None else "'" + n.replace("'", "''") + "'"
        h.execute("postgresql", f"INSERT INTO {table} VALUES ({i}, {name}, '{a}')")  # nosec B608
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    src = create_connector({"name": f"pg-{token}", "type": "postgresql", "schema": "public",
                            **h.dest_config("postgresql")})
    dst = create_connector({
        "name": f"sf-{token}", "type": "snowflake", "host": "localhost", "port": 443,
        "database": "dataflow", "username": "test", "password": "test",
        "schema": "public", "warehouse": "COMPUTE_WH",
    })
    try:
        yield {"src": src.id, "dst": dst.id, "table": table, "dest": f"r12d2_dst_{token}"}
    finally:
        h.drop_table("postgresql", table)


def _sf_form(env: dict, **extra) -> dict:
    mappings = [dict(m) for m in _MAPPINGS]
    mappings[0] = {**mappings[0], "source": "id", "target": "id"}
    mappings[2] = {**mappings[2], "source_type": "TEXT"}
    mappings[1] = {**mappings[1], "source_type": "TEXT", "target_type": "TEXT"}
    return {
        "source_kind": "database", "source_format": "postgresql",
        "source_connector_id": env["src"], "source_table": env["table"],
        "source_database": h.PG["database"],
        "dest_kind": "database", "dest_format": "snowflake",
        "dest_connector_id": env["dst"], "dest_database": "dataflow",
        "dest_schema": "public", "dest_table": env["dest"],
        "async_mode": "false", "sync_mode": "full_refresh_overwrite",
        "mappings_json": json.dumps(mappings),
        **extra,
    }


def _sf_rows(dest: str) -> dict[int, tuple]:
    import connectors.snowflake_conn as sc

    conn = sc._open_connection(
        account="localhost", username="test", password="test", database="dataflow",
        schema="PUBLIC", warehouse="", connection_string="",
    )
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM DATAFLOW.PUBLIC."{dest}"')
        cols = [d[0].lower() for d in cur.description]
        out = {}
        for raw in cur.fetchall():
            row = dict(zip(cols, raw))
            out[int(row["id"])] = (row["name"], Decimal(str(row["amount"])))
        return out
    finally:
        conn.close()


def _assert_sf(dest: str) -> None:
    want = {i: (n, Decimal(a)) for i, n, a in _GOOD}
    assert _sf_rows(dest) == want


@pytest.mark.parametrize("field", ["risk_acceptance", "risk_acceptance_json"])
def test_pg_to_snowflake_run_with_risk_acceptance(client, pg_snowflake, field):
    resp = client.post(
        "/api/v1/transfer/run",
        data=_sf_form(pg_snowflake, **{field: json.dumps(ACCEPTANCE)}),
    )
    assert resp.status_code == 200, resp.text
    _assert_sf(pg_snowflake["dest"])
    # Second run into the table DataFlow created.
    resp = client.post(
        "/api/v1/transfer/run",
        data=_sf_form(pg_snowflake, **{field: json.dumps(ACCEPTANCE)}),
    )
    assert resp.status_code == 200, resp.text
    _assert_sf(pg_snowflake["dest"])


def test_pg_to_snowflake_retry_with_risk_acceptance(client, pg_snowflake):
    resp = client.post("/api/v1/transfer/run", data=_sf_form(pg_snowflake))
    assert resp.status_code == 422, resp.text
    assert "retry_job" in _detail(resp) and "rerun_job" not in _detail(resp)
    failed_id = resp.json()["detail"]["job_id"]
    resp = client.post(
        f"/api/v1/connectors/jobs/{failed_id}/retry",
        json={"risk_acceptance_json": ACCEPTANCE},
    )
    assert resp.status_code == 200, resp.text
    job = _wait(resp.json()["job_id"])
    assert job["status"] in {"completed", "completed_with_quarantine"}, job.get("error")
    _assert_sf(pg_snowflake["dest"])
