"""MX3-14 — CAST_AND_CONTINUE must have an effect distinct from QUARANTINE_ROW.

QA: a signed CAST_AND_CONTINUE produced byte-identical output to QUARANTINE_ROW
(row held out, coerced_null_rows=0, no visible disposition). The catalog says
CAST_AND_CONTINUE holds the row out as ``cast_failure`` by default and writes
NULL only when the contract's quarantine_policy asks for it — but Pilot had
no way to sign that NULL variant and get_job hid the disposition.
"""

from __future__ import annotations

import socket
import sys
import time
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))


def _pg_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 5432), timeout=1.5):
            return True
    except OSError:
        return False


def _mapping():
    return {"source": "flt", "target": "flt", "source_type": "TEXT",
            "target_type": "DOUBLE PRECISION", "confidence": 0.99}


def _sign(acceptance):
    from src.ai.copilot.transfer_tools import _sign_required_risk_contracts

    return _sign_required_risk_contracts(
        [_mapping()], {"approved_by": "qa", "reason": "accepted", **acceptance}, table="t"
    )


def test_on_cast_failure_null_signs_a_coerce_null_contract():
    from services.migration_risk_contract import resolve_write_action_for_mapping

    default = _sign({"execution_policy": "CAST_AND_CONTINUE"})[0]
    nulled = _sign({"execution_policy": "CAST_AND_CONTINUE", "on_cast_failure": "null"})[0]
    assert resolve_write_action_for_mapping(default, "quarantine")[0] == "quarantine"
    assert resolve_write_action_for_mapping(nulled, "quarantine")[0] == "coerce_null"


@pytest.mark.parametrize(
    "acceptance",
    [
        {"execution_policy": "QUARANTINE_ROW", "on_cast_failure": "null"},
        {"execution_policy": "CAST_AND_CONTINUE", "on_cast_failure": "zero"},
    ],
)
def test_on_cast_failure_is_validated(acceptance):
    with pytest.raises(ValueError, match="on_cast_failure"):
        _sign(acceptance)


@pytest.fixture
def cast_route():
    import psycopg2

    from services.connector_store import create_connector, delete_connector

    sfx = uuid.uuid4().hex[:6]
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    name = f"MX314{sfx}"
    saved = create_connector({
        "name": name, "type": "postgresql", "host": "localhost", "port": 5432,
        "database": "dataflow", "username": "dataflow", "password": "dataflow",
        "schema": "public",
    })
    made: list[str] = []

    def tables(tag: str):
        src, dst = f"mx314s_{sfx}_{tag}", f"mx314d_{sfx}_{tag}"
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, flt TEXT)")
            cur.executemany(
                f"INSERT INTO {src} VALUES (%s,%s)",
                [(1, "1.5"), (2, "2.5"), (3, "not-a-number"), (4, "4.0")],
            )
            cur.execute(f"CREATE TABLE {dst} (id INT PRIMARY KEY, flt DOUBLE PRECISION)")
        made.extend([src, dst])
        return src, dst

    try:
        yield {"conn": conn, "name": name, "tables": tables}
    finally:
        with conn.cursor() as cur:
            for t in made:
                cur.execute(f"DROP TABLE IF EXISTS {t}")
        conn.close()
        delete_connector(saved.id)


def _run(route, acceptance, tag):
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    src, dst = route["tables"](tag)
    staged = DataPilotTools().execute("start_transfer", {
        "source_connector_name": route["name"], "source_table": src,
        "dest_connector_name": route["name"], "dest_table": dst,
        "sync_mode": "full_refresh_append", "validation_mode": "balanced",
        "risk_acceptance": {"approved_by": "qa", "reason": "accepted", **acceptance},
    })
    assert staged.success, staged.error
    confirmed = spend_pilot_ack(staged.output["ack_id"], "mx314-test")
    assert confirmed["ok"] is True, confirmed
    deadline = time.time() + 120
    job: dict = {}
    while time.time() < deadline:
        job = dict(DataPilotTools().execute("get_job", {"job_id": confirmed["job_id"]}).output or {})
        if str(job.get("status") or "") not in {"", "pending", "queued", "running", "starting"}:
            break
        time.sleep(1.0)
    with route["conn"].cursor() as cur:
        cur.execute(f"SELECT id, flt FROM {dst} ORDER BY id")
        rows = cur.fetchall()
    return job, rows


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_cast_and_continue_default_is_a_named_cast_failure_holdout(cast_route):
    q_job, q_rows = _run(cast_route, {"execution_policy": "QUARANTINE_ROW"}, "q")
    c_job, c_rows = _run(cast_route, {"execution_policy": "CAST_AND_CONTINUE"}, "c")
    assert q_rows == c_rows == [(1, 1.5), (2, 2.5), (4, 4.0)]
    assert q_job["quarantine_samples"][0]["disposition"] == "quarantined", q_job
    sample = c_job["quarantine_samples"][0]
    assert sample["disposition"] == "cast_failure", c_job
    assert sample["execution_policy"] == "CAST_AND_CONTINUE"


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_cast_and_continue_null_writes_null_and_counts_it(cast_route):
    job, rows = _run(
        cast_route, {"execution_policy": "CAST_AND_CONTINUE", "on_cast_failure": "null"}, "n"
    )
    assert rows == [(1, 1.5), (2, 2.5), (3, None), (4, 4.0)], (rows, job)
    assert job["coerced_null_rows"] == 1, job
    assert job["rejected_rows"] == 0, job
    assert str(job["status"]).startswith("completed"), job
