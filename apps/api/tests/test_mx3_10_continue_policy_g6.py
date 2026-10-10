"""MX3-10 — signed continue-policy Risk Contracts must be reachable.

A destination narrower than the source (NUMERIC(6,2) / VARCHAR(10)) with a
signed QUARANTINE_ROW / CAST_AND_CONTINUE / SKIP_ROW contract was blocked by
G6 ("Decimal capacity overflow ... needs ~6,2 vs 6,2") and by schema_drift
("policy 'type_locked' requires review") on an identical schema. The contract
was the operator's signed instruction for exactly those rows, so neither gate
may block — while the no-contract path must still fail closed.
"""

from __future__ import annotations

import socket
import time
import uuid

import pytest


def _pg_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1.5):
            return True
    except OSError:
        return False


def _ddl(mappings):
    from services.ddl_compatibility import evaluate_ddl_compatibility

    return evaluate_ddl_compatibility(
        mappings=mappings,
        source_schema={"amt": "DECIMAL(10,2)"},
        target_schema={"amt": "DECIMAL(6,2)"},
        sample_rows=[{"amt": "12.50"}, {"amt": "123456.78"}],
        table_exists=True,
        dest_connected=True,
        dest_db_type="postgresql",
    )


def _amt_mapping():
    return {
        "source": "amt",
        "target": "amt",
        "source_type": "DECIMAL(10,2)",
        "target_type": "DECIMAL(6,2)",
        "confidence": 0.99,
    }


def test_capacity_overflow_message_reports_needed_precision():
    ok, issues = _ddl([_amt_mapping()])
    assert ok is False
    # 123456.78 needs precision 8 / scale 2 — not "~6,2 vs 6,2".
    assert any("needs ~8,2 vs 6,2" in i for i in issues), issues


def test_capacity_overflow_still_blocks_without_contract():
    ok, issues = _ddl([_amt_mapping()])
    assert ok is False
    assert any("Decimal capacity overflow" in i for i in issues), issues


@pytest.mark.parametrize("policy", ["QUARANTINE_ROW", "CAST_AND_CONTINUE", "SKIP_ROW"])
def test_signed_continue_contract_clears_value_overflow(policy):
    from src.ai.copilot.transfer_tools import _sign_required_risk_contracts

    signed = _sign_required_risk_contracts(
        [_amt_mapping()],
        {"execution_policy": policy, "approved_by": "qa", "reason": "accepted"},
        table="t",
    )
    assert signed[0].get("risk_contract"), signed
    ok, issues = _ddl(signed)
    assert ok is True, issues


def test_tampered_contract_does_not_clear_value_overflow():
    from src.ai.copilot.transfer_tools import _sign_required_risk_contracts

    signed = _sign_required_risk_contracts(
        [_amt_mapping()],
        {"execution_policy": "QUARANTINE_ROW", "approved_by": "qa", "reason": "accepted"},
        table="t",
    )
    contract = dict(signed[0]["risk_contract"])
    contract["approved_by"] = "someone-else"
    signed[0]["risk_contract"] = contract
    ok, issues = _ddl(signed)
    assert ok is False
    assert any("Decimal capacity overflow" in i for i in issues), issues


def test_type_locked_identical_schema_is_not_a_change():
    from services.schema_drift import resolve_schema_evolution

    evo = resolve_schema_evolution(
        {"additive": [], "breaking": [], "severity": "none", "renamed": []},
        schema_policy="type_locked",
    )
    assert evo["action"] != "review", evo
    assert "type_locked_review_required" not in evo["reasons"]


def test_type_locked_still_reviews_additive_change():
    from services.schema_drift import resolve_schema_evolution

    evo = resolve_schema_evolution(
        {
            "additive": [{"kind": "add_column", "column": "c", "nullable": True}],
            "breaking": [],
            "severity": "additive",
            "renamed": [],
        },
        schema_policy="type_locked",
    )
    assert evo["action"] == "review", evo


# --------------------------------------------------------------------------
# Live: PostgreSQL → existing narrower PostgreSQL destination
# --------------------------------------------------------------------------


def _pg_conn():
    import psycopg2

    return psycopg2.connect(
        host="localhost", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )


@pytest.fixture
def narrow_route():
    from services.connector_store import create_connector, delete_connector

    sfx = uuid.uuid4().hex[:6]
    src, dst = f"mx310_src_{sfx}", f"mx310_dst_{sfx}"
    conn = _pg_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, amt NUMERIC(10,2), name VARCHAR(40))")
        cur.executemany(
            f"INSERT INTO {src} VALUES (%s,%s,%s)",
            [
                (1, "12.50", "ab"),
                (2, "123456.78", "cd"),
                (3, "99.99", "a very long name exceeding"),
                (4, "1.00", "x"),
            ],
        )
        cur.execute(f"CREATE TABLE {dst} (id INT PRIMARY KEY, amt NUMERIC(6,2), name VARCHAR(10))")
    name = f"MX310{sfx}"
    saved = create_connector({
        "name": name, "type": "postgresql", "host": "localhost", "port": 5432,
        "database": "dataflow", "username": "dataflow", "password": "dataflow",
        "schema": "public",
    })
    try:
        yield {"conn": conn, "name": name, "src": src, "dst": dst}
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")
            cur.execute(f"DROP TABLE IF EXISTS {dst}")
        conn.close()
        delete_connector(saved.id)


def _wait_job(job_id: str, timeout: float = 120.0) -> dict:
    from src.ai.copilot.tools import DataPilotTools

    deadline = time.time() + timeout
    job: dict = {}
    while time.time() < deadline:
        res = DataPilotTools().execute("get_job", {"job_id": job_id})
        job = dict(res.output or {})
        status = str(job.get("status") or (job.get("job") or {}).get("status") or "")
        if status and status not in {"pending", "queued", "running", "starting"}:
            return job
        time.sleep(1.0)
    return job


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
@pytest.mark.parametrize("policy", ["QUARANTINE_ROW", "CAST_AND_CONTINUE", "SKIP_ROW"])
def test_live_signed_continue_policy_loads_into_narrower_destination(narrow_route, policy):
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    r = narrow_route
    staged = DataPilotTools().execute("start_transfer", {
        "source_connector_name": r["name"],
        "source_table": r["src"],
        "dest_connector_name": r["name"],
        "dest_table": r["dst"],
        "sync_mode": "full_refresh_append",
        "schema_policy": "type_locked",
        "validation_mode": "balanced",
        "risk_acceptance": {"execution_policy": policy, "approved_by": "qa", "reason": "accepted"},
    })
    assert staged.success, staged.error
    confirmed = spend_pilot_ack(staged.output["ack_id"], "mx310-test")
    assert confirmed["ok"] is True, confirmed
    job = _wait_job(confirmed["job_id"])

    with r["conn"].cursor() as cur:
        cur.execute(f"SELECT id FROM {r['dst']} ORDER BY id")
        ids = [row[0] for row in cur.fetchall()]
    # Fitting rows land; the overflowing rows are held out per the contract.
    assert ids == [1, 4], (ids, job)
    assert job.get("status") == "completed_with_quarantine", job
