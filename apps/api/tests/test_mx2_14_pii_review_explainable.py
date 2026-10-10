"""MX2-14 — PII/compliance review must be explainable, correct and acknowledgeable.

QA: start_transfer on a synthetic numeric table was refused with an opaque
"Preflight is review-grade" (reason "PII/compliance review required", no
column listed) and the Pilot had no acknowledge path.
"""

from __future__ import annotations

import socket
import uuid

import pytest


def _pg_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1.5):
            return True
    except OSError:
        return False


def test_surrogate_account_id_is_not_high_risk_account_number():
    from services.compliance_guard import score_compliance_risk

    rows = [{"id": i, "account_id": 100 + i, "amount": i * 1.5} for i in range(1, 21)]
    out = score_compliance_risk(["id", "account_id", "amount"], rows)
    assert out["requires_review"] is False, out
    assert out["field_risk"]["account_id"] == ["identifier"]
    # Real account numbers are still high risk.
    rows = [{"account_number": f"12345678{i:04d}"} for i in range(5)]
    assert score_compliance_risk(["account_number"], rows)["requires_review"] is True
    # No samples to contradict the name: stays conservative.
    assert score_compliance_risk(["account_id"], [])["requires_review"] is True


def test_review_finding_names_columns_rule_and_ack_path():
    from services.preflight_proof_bundle import build_preflight_proof_bundle

    rows = [{"id": i, "ssn": f"123-45-{6000 + i}"} for i in range(5)]
    bundle = build_preflight_proof_bundle(columns=["id", "ssn"], sample_rows=rows)
    reason = bundle["transfer_decision"]["reason"]
    assert reason.startswith("PII/compliance review required"), reason
    assert "ssn (ssn by column name" in reason, reason
    assert "pii_acknowledgement" in reason, reason
    assert bundle["compliance"]["findings"][0]["column"] == "ssn"

    acked = build_preflight_proof_bundle(
        columns=["id", "ssn"], sample_rows=rows, compliance_acknowledged=True,
        acknowledgment_actor="qa", acknowledgment_reason="synthetic data",
    )
    assert not any("PII" in b for b in acked["transfer_decision"]["blockers"])
    assert acked["compliance"]["acknowledgment"]["reason"] == "synthetic data"


@pytest.fixture
def pg_route():
    import psycopg2
    from services.connector_store import create_connector, delete_connector

    sfx = uuid.uuid4().hex[:6]
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    name = f"MX214{sfx}"
    saved = create_connector({
        "name": name, "type": "postgresql", "host": "localhost", "port": 5432,
        "database": "dataflow", "username": "dataflow", "password": "dataflow",
        "schema": "public",
    })
    tables: list[str] = []
    try:
        yield {"conn": conn, "name": name, "sfx": sfx, "tables": tables}
    finally:
        with conn.cursor() as cur:
            for t in tables:
                cur.execute(f"DROP TABLE IF EXISTS {t}")
        conn.close()
        delete_connector(saved.id)


def _stage(route, src: str, dst: str, **extra):
    from src.ai.copilot.tools import DataPilotTools

    route["tables"].extend([src, dst])
    return DataPilotTools().execute("start_transfer", {
        "source_connector_name": route["name"], "source_table": src,
        "dest_connector_name": route["name"], "dest_table": dst,
        "sync_mode": "full_refresh_overwrite", **extra,
    })


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_synthetic_numeric_table_stages(pg_route):
    src = f"mx214_num_{pg_route['sfx']}"
    with pg_route["conn"].cursor() as cur:
        cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, account_id INT, amount NUMERIC(10,2), qty INT)")
        cur.executemany(
            f"INSERT INTO {src} VALUES (%s,%s,%s,%s)",
            [(i, 100 + i, i * 1.5, i) for i in range(1, 21)],
        )
    staged = _stage(pg_route, src, f"{src}_dst")
    assert staged.success, staged.error


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_real_pii_explains_and_acknowledges(pg_route):
    src = f"mx214_pii_{pg_route['sfx']}"
    with pg_route["conn"].cursor() as cur:
        cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, ssn VARCHAR(11))")
        cur.executemany(
            f"INSERT INTO {src} VALUES (%s,%s)", [(i, f"123-45-{6000 + i}") for i in range(1, 21)]
        )
    refused = _stage(pg_route, src, f"{src}_dst")
    assert refused.success is False
    assert "ssn (ssn by column name" in refused.error, refused.error
    assert "pii_acknowledgement" in refused.error, refused.error

    unsigned = _stage(pg_route, src, f"{src}_dst", pii_acknowledgement={"approved_by": "qa"})
    assert unsigned.success is False and "approved_by and reason" in unsigned.error

    acked = _stage(
        pg_route, src, f"{src}_dst",
        pii_acknowledgement={"approved_by": "qa", "reason": "synthetic test data"},
    )
    assert acked.success, acked.error
    assert acked.output["preview"]["pii_acknowledgement"] == {
        "approved_by": "qa", "reason": "synthetic test data",
    }
    # Confirm: Execute re-runs Validate with the same ack and keeps the trail.
    import json
    import time

    from conftest import spend_pilot_ack
    from services.mongodb_service import get_mongodb_service
    from src.ai.copilot.tools import DataPilotTools

    job_id = spend_pilot_ack(acked.output["ack_id"], "mx214-test")["job_id"]
    deadline = time.time() + 120
    while time.time() < deadline:
        job = dict(DataPilotTools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(1.0)
    assert job.get("status") == "completed", job
    raw = json.dumps(get_mongodb_service().get_job(job_id) or {}, default=str)
    assert '"compliance_acknowledged": true' in raw
    assert "synthetic test data" in raw
