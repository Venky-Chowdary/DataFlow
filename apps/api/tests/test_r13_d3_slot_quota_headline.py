"""D-3 — PG replication-slot exhaustion was headlined as a CDC cursor gap.

``all replication slots are in use`` matched the cursor-gap rule's
``"replication slot"`` needle, so the operator was told history was purged
(retention / failover) and to Resume, when the fix is to free a slot or raise
``max_replication_slots``. Quota now classifies as ``replication_slot_quota``
with the same remedy ``cdc_capability`` gives, and real gaps stay gaps.
"""

from __future__ import annotations

import pytest

from services.cdc_capability import CAUSE_SLOT_QUOTA, classify_log_capture_failure
from services.cdc_cursor_gap import job_has_cursor_gap
from services.error_handling import humanize_transfer_failure

_PG_SLOT_EXHAUSTED = (
    "all replication slots are in use\n"
    "HINT:  Free one or increase max_replication_slots.\n"
)


@pytest.mark.parametrize(
    "raw",
    [
        _PG_SLOT_EXHAUSTED,
        "all replication slots are in use (max_replication_slots reached) — "
        "no slot is free for this stream",
        'replication slot "df_orders" could not be created: '
        "all replication slots are in use",
        "FATAL:  number of requested standby connections exceeds max_wal_senders",
    ],
)
def test_slot_quota_is_not_a_cursor_gap(raw):
    out = humanize_transfer_failure(raw)
    assert out["code"] == CAUSE_SLOT_QUOTA
    assert "quota" in out["title"].lower()
    assert "cursor gap" not in out["title"].lower()
    assert "pg_drop_replication_slot" in out["fix"]
    assert "max_replication_slots" in out["fix"]
    assert not job_has_cursor_gap({"error_code": out["code"]})


@pytest.mark.parametrize(
    "raw",
    [
        'replication slot "df_orders" has been invalidated (wal_status=lost)',
        "cdc_slot_gap: resume LSN 0/16B3748 is before restart_lsn 0/1A00000",
        "cdc_cursor_gap: requested WAL segment has already been removed",
    ],
)
def test_real_slot_gaps_stay_cursor_gaps(raw):
    out = humanize_transfer_failure(raw)
    assert out["code"] == "cdc_cursor_gap"
    assert out["title"] == "CDC cursor gap (retention / failover)"


def test_wal_level_message_naming_slot_settings_stays_wal_level():
    out = humanize_transfer_failure(
        "logical decoding requires wal_level >= logical; "
        "set max_replication_slots and max_wal_senders above 0"
    )
    assert out["code"] == "cdc_wal_level"


def test_capability_classifier_still_reads_setting_names_as_quota():
    refusal = classify_log_capture_failure("postgresql", "max_wal_senders is 0")
    assert refusal.cause == CAUSE_SLOT_QUOTA


def test_live_postgres_slot_exhaustion_headline():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            user="dataflow",
            password="dataflow",
            dbname="dataflow",
            connect_timeout=3,
        )
    except psycopg2.OperationalError:
        pytest.skip("PostgreSQL not reachable (docker compose up -d postgres)")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SHOW wal_level")
    if cur.fetchone()[0] != "logical":
        conn.close()
        pytest.skip("wal_level is not logical")
    cur.execute(
        "SELECT current_setting('max_replication_slots')::int "
        "- (SELECT count(*) FROM pg_replication_slots)"
    )
    free = int(cur.fetchone()[0])
    made: list[str] = []
    err: psycopg2.Error | None = None
    try:
        for i in range(free + 1):
            name = f"d3_quota_probe_{i}"
            try:
                cur.execute(
                    "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (name,)
                )
                made.append(name)
            except psycopg2.Error as exc:
                err = exc
                break
    finally:
        for name in made:
            cur.execute("SELECT pg_drop_replication_slot(%s)", (name,))
        conn.close()
    assert err is not None, "server never refused a slot"
    out = humanize_transfer_failure(err)
    assert out["code"] == CAUSE_SLOT_QUOTA, out
    assert out["title"] == "Replication slot quota reached"
