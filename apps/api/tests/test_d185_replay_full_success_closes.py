"""D185 - a fully successful quarantine replay ends completed and closes its record.

QA fixed the destination column that quarantined two rows (INTEGER -> DOUBLE
PRECISION) and replayed. The writer landed both rows, but Gate-8 re-mapped the
replayed rows against the target types stored on the parent job's mappings
(``INT4``, the destination as it was when the job planned). Every fractional
value was held out of the *source* digest as "Invalid integer", the source
digest became the digest of nothing, and a correct write failed Gate-8: the
replay job ended ``failed`` and the quarantine record stayed ``diverging``.
"""

from __future__ import annotations

import socket
import time
import uuid

import pytest


def _pg_cfg() -> dict:
    from conftest import pg_test_cfg

    return pg_test_cfg()


def _pg_reachable() -> bool:
    cfg = _pg_cfg()
    try:
        with socket.create_connection((cfg["host"], cfg["port"]), timeout=1.5):
            return True
    except OSError:
        return False


def test_refresh_restates_a_stale_plan_type_from_the_live_catalog(monkeypatch):
    import services.dest_physical_types as dpt

    monkeypatch.setattr(
        dpt,
        "physical_column_types",
        lambda *_a, **_k: {"ID": "integer", "flt": "double precision"},
    )
    stored = [
        {"source": "id", "target": "id", "target_type": "INT4"},
        {"source": "flt", "target": "flt", "target_type": "INT4"},
        {"source": "note", "target": "note"},
    ]
    out = dpt.refresh_mapping_target_types(stored, "postgresql", {}, table="t")
    assert [m.get("target_type") for m in out] == ["integer", "double precision", None]
    assert stored[1]["target_type"] == "INT4"  # the stored plan is not mutated


def test_refresh_keeps_the_plan_when_the_catalog_cannot_answer(monkeypatch):
    import services.dest_physical_types as dpt

    monkeypatch.setattr(dpt, "physical_column_types", lambda *_a, **_k: {})
    stored = [{"source": "flt", "target": "flt", "target_type": "INT4"}]
    assert dpt.refresh_mapping_target_types(stored, "postgresql", {}, table="t") == stored


@pytest.fixture
def two_bad_rows():
    import psycopg2

    from services.connector_store import create_connector, delete_connector

    cfg = _pg_cfg()
    sfx = uuid.uuid4().hex[:6]
    src, dst = f"d185_src_{sfx}", f"d185_dst_{sfx}"
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
        user=cfg["username"], password=cfg["password"],
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, flt DOUBLE PRECISION)")
        cur.executemany(
            f"INSERT INTO {src} VALUES (%s,%s)",
            [(1, 1.0), (2, 2.0), (3, 3.7), (4, 4.0), (5, 5.5), (6, 6.0)],
        )
        cur.execute(f"CREATE TABLE {dst} (id INT PRIMARY KEY, flt INTEGER)")
    name = f"D185{sfx}"
    saved = create_connector({
        "name": name, "type": "postgresql", "host": cfg["host"],
        "port": cfg["port"], "database": cfg["database"],
        "username": cfg["username"], "password": cfg["password"],
        "schema": "public",
    })
    try:
        yield {"name": name, "src": src, "dst": dst, "conn": conn}
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")
            cur.execute(f"DROP TABLE IF EXISTS {dst}")
            cur.execute(f'DROP TABLE IF EXISTS "{dst}_df_quarantine"')
        conn.close()
        delete_connector(saved.id)


def _quarantined_job(route) -> str:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    tools = DataPilotTools()
    staged = tools.execute("start_transfer", {
        "source_connector_name": route["name"], "source_table": route["src"],
        "dest_connector_name": route["name"], "dest_table": route["dst"],
        "sync_mode": "full_refresh_append", "validation_mode": "lenient",
        "risk_acceptance": {
            "execution_policy": "QUARANTINE_ROW", "approved_by": "qa", "reason": "accepted",
        },
    })
    assert staged.success, staged.error
    job_id = spend_pilot_ack(staged.output["ack_id"], "d185-test")["job_id"]
    deadline = time.time() + 120
    job: dict = {}
    while time.time() < deadline:
        job = dict(tools.execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(1.0)
    assert job.get("status") == "completed_with_quarantine", job
    return job_id


def _replay(job_id: str) -> dict:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    tools = DataPilotTools()
    staged = tools.execute("replay_quarantine", {"job_id": job_id})
    assert staged.success, staged.error
    return spend_pilot_ack(staged.output["ack_id"], "d185-test")


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_replay_after_the_destination_column_is_widened_completes_and_closes(two_bad_rows):
    from src.ai.copilot.tools import DataPilotTools

    job_id = _quarantined_job(two_bad_rows)
    with two_bad_rows["conn"].cursor() as cur:
        cur.execute(f"ALTER TABLE {two_bad_rows['dst']} ALTER COLUMN flt TYPE DOUBLE PRECISION")

    out = _replay(job_id)

    assert out["rows_written"] == 2 and out["rejected"] == 0, out
    assert out["status"] == "completed", out["reconciliation"].get("message")
    assert out["success"] is True
    assert out["reconciliation"]["passed"] is True, out["reconciliation"].get("message")
    assert out["quarantine_closure"]["verdict"] == "closed", out["quarantine_closure"]
    assert out["quarantine_closure"]["open_count"] == 0

    child = dict(DataPilotTools().execute("get_job", {"job_id": out["job_id"]}).output or {})
    assert child.get("status") == "completed", child
    from services.mongodb_service import get_mongodb_service
    from services.quarantine_dlq import job_quarantine_closure

    parent = get_mongodb_service().get_job(job_id)
    assert (job_quarantine_closure(parent) or {}).get("verdict") == "closed", parent

    with two_bad_rows["conn"].cursor() as cur:
        cur.execute(f"SELECT id, flt FROM {two_bad_rows['dst']} WHERE id IN (3, 5) ORDER BY id")
        assert cur.fetchall() == [(3, 3.7), (5, 5.5)]


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_replay_without_a_fix_does_not_close_the_record(two_bad_rows):
    job_id = _quarantined_job(two_bad_rows)

    out = _replay(job_id)

    assert out["status"] != "completed", out
    assert out["quarantine_closure"]["verdict"] != "closed", out["quarantine_closure"]
    assert out["quarantine_closure"]["open_count"] == 2
