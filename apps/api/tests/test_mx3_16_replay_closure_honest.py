"""MX3-16 — a quarantine replay must not report the ledger ``closed`` when the
destination DLQ promote stamp failed, and a SKIP_ROW-only job must not stage a
replay (its rows are contract skips kept for audit, not replay quarantine).

QA saw ``quarantine_closure.verdict == "closed"`` next to
``dest_dlq_promoted.error: UndefinedTable ... _df_quarantine``.
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


def test_promote_error_holds_closure_at_diverging():
    from services.quarantine_dlq import VERDICT_CLOSED, closure_with_promotion_outcome

    closed = {"verdict": VERDICT_CLOSED, "open_count": 0, "promoted_count": 1}
    out = closure_with_promotion_outcome(
        closed, {"updated": 0, "error": "relation does not exist", "table": "t_df_quarantine"}
    )
    assert out["verdict"] == "diverging"
    assert "relation does not exist" in out["promotion_error"]
    assert "t_df_quarantine" in out["next_action"]
    # Clean stamp leaves the verdict alone.
    assert closure_with_promotion_outcome(closed, {"updated": 1})["verdict"] == VERDICT_CLOSED


def _pg():
    import psycopg2

    cfg = _pg_cfg()
    c = psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
        user=cfg["username"], password=cfg["password"],
    )
    c.autocommit = True
    return c


@pytest.fixture
def frac_route():
    from services.connector_store import create_connector, delete_connector

    sfx = uuid.uuid4().hex[:6]
    src, dst = f"mx316_src_{sfx}", f"mx316_dst_{sfx}"
    conn = _pg()
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, flt DOUBLE PRECISION)")
        cur.executemany(
            f"INSERT INTO {src} VALUES (%s,%s)",
            [(1, 1.0), (2, 2.0), (3, 3.7), (4, 4.0), (5, 5.0), (6, 6.0)],
        )
        cur.execute(f"CREATE TABLE {dst} (id INT PRIMARY KEY, flt INTEGER)")
    name = f"MX316{sfx}"
    cfg = _pg_cfg()
    saved = create_connector({
        "name": name, "type": "postgresql", "host": cfg["host"],
        "port": cfg["port"], "database": cfg["database"],
        "username": cfg["username"], "password": cfg["password"],
        "schema": "public",
    })
    try:
        yield {"conn": conn, "name": name, "src": src, "dst": dst}
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")
            cur.execute(f"DROP TABLE IF EXISTS {dst}")
            cur.execute(f'DROP TABLE IF EXISTS "{dst}_df_quarantine"')
        conn.close()
        delete_connector(saved.id)


def _run(route, policy: str) -> str:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    staged = DataPilotTools().execute("start_transfer", {
        "source_connector_name": route["name"],
        "source_table": route["src"],
        "dest_connector_name": route["name"],
        "dest_table": route["dst"],
        "sync_mode": "full_refresh_append",
        "validation_mode": "lenient",
        "risk_acceptance": {"execution_policy": policy, "approved_by": "qa", "reason": "accepted"},
    })
    assert staged.success, staged.error
    job_id = spend_pilot_ack(staged.output["ack_id"], "mx316-test")["job_id"]
    deadline = time.time() + 120
    while time.time() < deadline:
        job = dict(DataPilotTools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(1.0)
    assert job.get("status") == "completed_with_quarantine", job
    return job_id


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_replay_skips_missing_dest_quarantine_table_honestly(frac_route):
    """R3: shape-divert findings live in the control-plane DLQ — a missing
    ``<dest>_df_quarantine`` is a skipped promote step, not UndefinedTable."""
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    job_id = _run(frac_route, "QUARANTINE_ROW")
    with frac_route["conn"].cursor() as cur:
        cur.execute(f'DROP TABLE "{frac_route["dst"]}_df_quarantine"')
    rp = DataPilotTools().execute(
        "replay_quarantine", {"job_id": job_id, "rows": [{"id": 3, "flt": 4}]}
    )
    assert rp.success, rp.error
    out = spend_pilot_ack(rp.output["ack_id"], "mx316-test")
    promoted = out.get("dest_dlq_promoted") or {}
    assert promoted.get("skipped") is True, promoted
    assert "error" not in promoted, promoted
    assert out["quarantine_closure"]["verdict"] == "closed", out["quarantine_closure"]


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_replay_with_failed_dlq_stamp_is_not_closed(frac_route):
    """A promote that fails for a real reason (no permission to stamp) still
    holds the ledger at diverging — only a missing table skips cleanly."""
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    job_id = _run(frac_route, "QUARANTINE_ROW")
    # Leave the quarantine table in place but drop its promoted_at column —
    # the stamp UPDATE fails for a real reason, so closure must not close.
    with frac_route["conn"].cursor() as cur:
        cur.execute(
            f'ALTER TABLE "{frac_route["dst"]}_df_quarantine" '
            'DROP COLUMN "_df_promoted_at"'
        )
    rp = DataPilotTools().execute(
        "replay_quarantine", {"job_id": job_id, "rows": [{"id": 3, "flt": 4}]}
    )
    assert rp.success, rp.error
    out = spend_pilot_ack(rp.output["ack_id"], "mx316-test")
    promoted = out.get("dest_dlq_promoted") or {}
    assert promoted.get("skipped") or "error" in promoted, promoted
    if "error" in promoted:
        closure = out["quarantine_closure"]
        assert closure["verdict"] != "closed", closure


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_skip_row_only_job_refuses_replay_staging(frac_route):
    from src.ai.copilot.tools import DataPilotTools

    job_id = _run(frac_route, "SKIP_ROW")
    rp = DataPilotTools().execute("replay_quarantine", {"job_id": job_id})
    assert rp.success is False
    assert "SKIP_ROW" in (rp.error or ""), rp.error
