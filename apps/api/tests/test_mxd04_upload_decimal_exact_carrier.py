"""MXD04 — upload CSV -> SQLite keeps currency exact; WORKS-AS-DESIGNED pin.

QA saw ``typeof(AMT) = 'text'``. SQLite has no fixed-point type: a
``DECIMAL(8,2)`` column has NUMERIC affinity, which stores ``'1500.00'`` as the
integer 1500, ``'275.50'`` as the REAL 275.5 and rounds a 17-digit amount
through IEEE-754. TEXT is the exact digit carrier (type_system SQLite DDL map,
``sqlite_writer._sqlite_create_ddl``). This pins that the upload route keeps
every digit and the scale, and that the identifier still lands as INTEGER.
"""

from __future__ import annotations

import sqlite3

import tests.test_mcp_sqlite_file_transfer as _mcp_harness
from tests.test_mcp_sqlite_file_transfer import _mcp, _wait_job

mcp_client = _mcp_harness.mcp_client


def test_numeric_affinity_would_lose_the_scale_text_keeps_it():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE d (a DECIMAL(8,2))")
    conn.execute("CREATE TABLE t (a TEXT)")
    values = ["1500.00", "275.50", "12345678901234567.89"]
    for v in values:
        conn.execute("INSERT INTO d VALUES (?)", (v,))
        conn.execute("INSERT INTO t VALUES (?)", (v,))
    assert [r[0] for r in conn.execute("SELECT a FROM d")] != values
    assert [r[0] for r in conn.execute("SELECT a FROM t")] == values


def test_upload_payments_overwrite_keeps_amount_digits_exact(mcp_client):
    client, db_path = mcp_client
    staged = _mcp(client, "create_connector", {
        "name": "qe-mxd04", "type": "sqlite", "database": str(db_path), "test_first": True,
    })
    assert _mcp(client, "confirm_action", {"ack_id": staged["ack_id"], "reason": "qe"})["ok"]
    transfer = _mcp(client, "start_dataset_transfer", {
        "dataset_name": "sample_payments", "dest_connector_name": "qe-mxd04",
        "dest_table": "QA_E2E_RT_pay", "sync_mode": "full_refresh_overwrite",
    })
    assert transfer["requires_confirm"] is True, transfer
    confirmed = _mcp(client, "confirm_action", {"ack_id": transfer["ack_id"], "reason": "qe"})
    job = _wait_job(confirmed["job_id"])
    assert job.get("status") == "completed", job.get("error")

    with sqlite3.connect(db_path) as conn:
        cols = {r[1]: r[2] for r in conn.execute('PRAGMA table_info("QA_E2E_RT_pay")')}
        amounts = [r[0] for r in conn.execute('SELECT "AMT" FROM "QA_E2E_RT_pay" ORDER BY "CUST_ID"')]
        cust_kind = conn.execute('SELECT DISTINCT typeof("CUST_ID") FROM "QA_E2E_RT_pay"').fetchall()
        total = conn.execute('SELECT sum("AMT") FROM "QA_E2E_RT_pay"').fetchone()[0]
    assert cols["CUST_ID"] == "INTEGER"
    assert cust_kind == [("integer",)]
    assert cols["AMT"] == "TEXT"
    assert amounts[:2] == ["1500.00", "275.50"]  # scale kept, byte-for-byte the file
    assert round(total, 2) == 146786.24  # QA's own sum still computes
