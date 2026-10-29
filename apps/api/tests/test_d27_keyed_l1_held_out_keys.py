"""D27 - a partial quarantine replay said "Row fidelity verified" while the
verification ladder it carried had failed.

Root cause: the writer's key census is taken before the write, so a key the
writer then holds out (quarantines) is still counted as an insert in
``expected_delta``. L1 compared the destination COUNT(*) delta to that census
exactly: a replay of two edited rows where one still violates the column type
lands one row, the census promises two, and L1 failed a correct write. The
row-level proof (L3 over the keys that landed) matched, so the report kept
"verified" next to ``verification_ladder.passed == false``.

A held-out key is not classified as new or existing (``row_conservation``
says so for the ledger), so L1 can only bound the delta: every landed insert
is a census insert, and at most ``dropped`` of the census inserts were held out.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.verification_ladder import layer_l1_row_balance  # noqa: E402


def _l1(*, delta: int, expected: int, dropped: int):
    return layer_l1_row_balance(
        source_rows=2,
        target_rows=10 + delta,
        rejected_rows=dropped,
        allow_extra_rows=True,
        target_rows_before=10,
        keyed_cardinality=True,
        keyed_expected_delta=expected,
    )


def test_held_out_insert_is_not_a_missing_row():
    # census: 2 new keys; one was quarantined by the writer, one landed.
    assert _l1(delta=1, expected=2, dropped=1).passed is True


def test_held_out_update_leaves_the_census_delta_intact():
    # the quarantined key was one the destination already held.
    assert _l1(delta=2, expected=2, dropped=1).passed is True


def test_a_landed_row_that_vanished_still_fails():
    assert _l1(delta=0, expected=2, dropped=1).passed is False


def test_more_rows_than_the_census_promised_still_fails():
    assert _l1(delta=3, expected=2, dropped=1).passed is False


def test_without_holdouts_the_identity_is_exact():
    assert _l1(delta=2, expected=2, dropped=0).passed is True
    assert _l1(delta=1, expected=2, dropped=0).passed is False


def _pg_reachable() -> bool:
    import socket

    from conftest import pg_test_cfg

    cfg = pg_test_cfg()
    try:
        with socket.create_connection((cfg["host"], cfg["port"]), timeout=1.5):
            return True
    except OSError:
        return False


@pytest.fixture
def two_bad_rows():
    import psycopg2

    from conftest import pg_test_cfg
    from services.connector_store import create_connector, delete_connector

    cfg = pg_test_cfg()
    sfx = uuid.uuid4().hex[:6]
    src, dst = f"d27_src_{sfx}", f"d27_dst_{sfx}"
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
    name = f"D27{sfx}"
    saved = create_connector({
        "name": name, "type": "postgresql", "host": cfg["host"],
        "port": cfg["port"], "database": cfg["database"],
        "username": cfg["username"], "password": cfg["password"],
        "schema": "public",
    })
    try:
        yield {"name": name, "src": src, "dst": dst}
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")
            cur.execute(f"DROP TABLE IF EXISTS {dst}")
            cur.execute(f'DROP TABLE IF EXISTS "{dst}_df_quarantine"')
        conn.close()
        delete_connector(saved.id)


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_partial_replay_never_says_verified_over_a_failed_ladder(two_bad_rows):
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    staged = DataPilotTools().execute("start_transfer", {
        "source_connector_name": two_bad_rows["name"],
        "source_table": two_bad_rows["src"],
        "dest_connector_name": two_bad_rows["name"],
        "dest_table": two_bad_rows["dst"],
        "sync_mode": "full_refresh_append",
        "validation_mode": "lenient",
        "risk_acceptance": {
            "execution_policy": "QUARANTINE_ROW", "approved_by": "qa", "reason": "accepted",
        },
    })
    assert staged.success, staged.error
    job_id = spend_pilot_ack(staged.output["ack_id"], "d27-test")["job_id"]
    deadline = time.time() + 120
    while time.time() < deadline:
        job = dict(DataPilotTools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(1.0)
    assert job.get("status") == "completed_with_quarantine", job

    # Row 3 is fixed; row 5 is still a fraction in an INTEGER column.
    rp = DataPilotTools().execute(
        "replay_quarantine",
        {"job_id": job_id, "rows": [{"id": 3, "flt": 4}, {"id": 5, "flt": 5.5}]},
    )
    assert rp.success, rp.error
    out = spend_pilot_ack(rp.output["ack_id"], "d27-test")
    recon = out["reconciliation"]
    ladder = recon["verification_ladder"]
    assert out["rows_written"] == 1 and out["rejected"] == 1, out
    verified_claim = "verified" in str(recon.get("message") or "").lower()
    # One verdict: the ladder and the message cannot disagree.
    assert not (verified_claim and ladder["passed"] is False), (
        recon["message"], ladder["layers"]["L1"],
    )
    assert ladder["layers"]["L1"]["passed"] is True, ladder["layers"]["L1"]
