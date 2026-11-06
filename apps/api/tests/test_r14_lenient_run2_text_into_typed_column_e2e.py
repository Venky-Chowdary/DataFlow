"""Run 1 and run 2 of the same CSV agree (QA r14 neighbour of C2).

``price`` holds twenty numbers and one ``'abc'``, so inference reads the file
column as VARCHAR and the plan casts it into NUMERIC. Lenient run 1 (the
table does not exist) loads 20 rows and quarantines ``'abc'``: the integrity
audit decides the text->numeric pair per cell. Lenient run 2 into the table
run 1 created met the same pair as a live NUMERIC column and Target DDL (G6)
and schema drift refused it: "Lossy type coercion price (VARCHAR) → price
(DECIMAL)" plus "Breaking schema change: type_change" — though nothing
changed.

Now one rule (validation_mode_contract.mode_holds_out_bad_cells) decides the
pair in every gate: lenient run 2 loads the 20 rows and quarantines ``'abc'``
with its reason, exactly like run 1. Strict and balanced hold no cell out,
so they refuse run 2 with run 1's own cast-failure reason and write nothing.
Live PostgreSQL through ``POST /connectors/transfer`` (the upload route).
"""

from __future__ import annotations

import json
import uuid

import pytest

PG = {"host": "127.0.0.1", "port": 5432, "user": "dataflow", "password": "dataflow", "dbname": "dataflow"}
BAD_REASON = "Invalid currency: 'abc'"


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "test-pii-key")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(tmp_path / "uploads"))
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)

    import services.mongodb_service as mongo_mod
    from fastapi.testclient import TestClient
    from services.mongodb_service import MemoryMongoDBService
    from src.main import app

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    with TestClient(app) as c:
        c.memory = memory
        yield c


@pytest.fixture
def pg():
    import psycopg2

    try:
        conn = psycopg2.connect(**PG)
    except Exception as exc:  # pragma: no cover - environment
        pytest.fail(f"PostgreSQL is required for this e2e: {exc}")
    conn.autocommit = True
    tables: list[str] = []
    yield conn, tables
    cur = conn.cursor()
    for t in tables:
        cur.execute(f'DROP TABLE IF EXISTS public."{t}"')  # nosec B608
    conn.close()


def _csv() -> bytes:
    lines = ["sku,name,price"]
    for i in range(1, 21):
        name = {3: "", 4: " ", 5: "Ünïcode 東京 ✓"}.get(i, f"item {i}")
        lines.append(f"SKU-{i:05d},{name},{i}.50")
    lines.append("SKU-00030,bad price,abc")
    return ("\n".join(lines) + "\n").encode("utf-8")


def _run(client, table: str, mode: str):
    form = {
        "destination_database": PG["dbname"], "destination_collection": table,
        "dest_type": "postgresql", "dest_host": PG["host"], "dest_port": str(PG["port"]),
        "dest_schema": "public", "dest_username": PG["user"], "dest_password": PG["password"],
        "async_mode": "false", "sync_mode": "full_refresh_overwrite",
        "validation_mode": mode, "schema_policy": "propagate_columns",
    }
    resp = client.post(
        "/api/v1/connectors/transfer", data=form, files={"file": ("bad.csv", _csv(), "text/csv")}
    )
    body = resp.json()
    job_id = str((body.get("detail") or {}).get("job_id") if isinstance(body.get("detail"), dict) else body.get("job_id"))
    return resp, client.memory.get_job(job_id) or {}, job_id


def _rows(conn, table: str) -> dict:
    cur = conn.cursor()
    cur.execute(f'SELECT sku, name, price FROM public."{table}"')  # nosec B608
    return {r[0]: (r[1], None if r[2] is None else str(r[2])) for r in cur.fetchall()}


def _assert_loaded_and_quarantined(client, conn, table: str, resp, job, job_id) -> None:
    assert resp.status_code == 200, resp.text[:1200]
    assert job.get("status") == "completed_with_quarantine", (job.get("status"), job.get("error"))
    rows = _rows(conn, table)
    assert len(rows) == 20 and "SKU-00030" not in rows, sorted(rows)
    assert rows["SKU-00007"] == ("item 7", "7.50")
    assert rows["SKU-00005"][0] == "Ünïcode 東京 ✓"
    assert rows["SKU-00004"][0] == " "
    q = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine").json()
    assert q["rejected_rows"] == 1, q
    (finding,) = q["quarantine"]
    assert finding["column"] == "price" and finding["value"] == "abc", finding
    assert BAD_REASON in finding["reason"], finding


def test_lenient_run_2_into_the_table_run_1_created_loads_like_run_1(client, pg):
    conn, tables = pg
    table = f"r14r2_len_{uuid.uuid4().hex[:6]}"
    tables.append(table)
    for run in (1, 2):
        resp, job, job_id = _run(client, table, "lenient")
        _assert_loaded_and_quarantined(client, conn, table, resp, job, job_id)
        text = json.dumps(job.get("preflight") or {}, ensure_ascii=False)
        assert "Breaking schema change" not in text, (run, text[:500])


@pytest.mark.parametrize("mode", ["strict", "balanced"])
def test_strict_and_balanced_refuse_run_2_as_they_refuse_run_1(client, pg, mode):
    conn, tables = pg
    fresh = f"r14r2_{mode[:3]}1_{uuid.uuid4().hex[:6]}"
    created = f"r14r2_{mode[:3]}2_{uuid.uuid4().hex[:6]}"
    tables.extend([fresh, created])

    # Run 1 in this mode (no table yet) refuses on the 'abc' cell.
    resp1, job1, _ = _run(client, fresh, mode)
    assert resp1.status_code == 422, resp1.text[:800]
    assert job1.get("status") == "failed"
    error1 = str(job1.get("error") or "")
    assert BAD_REASON in error1 and "price" in error1, error1

    # The table a lenient run 1 created; run 2 in this mode refuses the same way.
    lenient_resp, lenient_job, lenient_id = _run(client, created, "lenient")
    _assert_loaded_and_quarantined(client, conn, created, lenient_resp, lenient_job, lenient_id)
    before = _rows(conn, created)
    resp2, job2, _ = _run(client, created, mode)
    assert resp2.status_code == 422, resp2.text[:800]
    assert job2.get("status") == "failed"
    error2 = str(job2.get("error") or "")
    assert BAD_REASON in error2 and "price" in error2, error2
    # The same refusal: the planned NUMERIC is spelled DECIMAL by the live catalog.
    for phrase in (
        "Lossy / fidelity collapse across type path: 1 column(s) collapse fidelity "
        "on write (price VARCHAR →",
        "Sample transform / cast failures",
    ):
        assert phrase in error1 and phrase in error2, (phrase, error1, error2)
    assert _rows(conn, created) == before  # nothing written by the refused run
