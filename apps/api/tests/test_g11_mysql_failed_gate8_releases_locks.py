"""OPS-F1: a MySQL run that fails Gate-8 leaves no open transaction or lock.

Observed on the pre-G11 code: a SQLite -> MySQL run whose Gate-8 sample read
failed (``1054 Unknown column``) kept a pooled connection inside an open
transaction holding the table's metadata lock, so the next ``DROP TABLE`` on
that table blocked indefinitely (a ``Sleep`` connection in the processlist).

The failure is forced the same way (the sample reader asks for a column name
the table does not have); afterwards no transaction of this process is open
on the server and ``DROP TABLE`` succeeds under a short ``lock_wait_timeout``.
"""

from __future__ import annotations

import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

LONG_64 = "customer_lifetime_value_after_discounts_and_refunds_in_eur_cents"


def _mysql():
    import pymysql

    return pymysql.connect(host="localhost", port=3306, user="dataflow", password="dataflow",
                           database="dataflow", autocommit=True)


def _mysql_up() -> bool:
    try:
        socket.create_connection(("localhost", 3306), timeout=1).close()
        _mysql().close()
    except Exception:  # noqa: BLE001 - any failure means "not available"
        return False
    return True


@pytest.mark.skipif(not _mysql_up(), reason="no MySQL on :3306")
def test_failed_gate8_leaves_no_open_transaction_or_metadata_lock(tmp_path: Path, monkeypatch):
    import connectors.destination_naming as naming
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.typed_fidelity_helpers import mysql_endpoint

    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, 10), (2, 20)])
    conn.close()
    table = f"g11o_{uuid.uuid4().hex[:10]}"
    # Force the Gate-8 sample read onto a column the table does not have.
    real_cap = naming.name_cap
    monkeypatch.setattr(naming, "name_cap",
                        lambda kind: 63 if naming.dest_kind(kind) == "mysql" else real_cap(kind))
    try:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(kind="database", format="sqlite", database=str(src),
                                      table="t"),
                destination=mysql_endpoint(table),
                mappings=[{"source": "id", "target": "id", "confidence": 1.0},
                          {"source": "v", "target": LONG_64, "confidence": 1.0}],
                sync_mode="full_refresh_overwrite",
                skip_preflight=True,
            ),
            "g11o" + uuid.uuid4().hex[:16],
        )
        assert not (result.reconciliation or {}).get("passed"), result.reconciliation
        assert "Unknown column" in str(result.error), result.error

        db = _mysql()
        try:
            with db.cursor() as cur:
                try:  # needs PROCESS; the DROP below is the check that always runs
                    cur.execute(
                        "SELECT COUNT(*) FROM information_schema.innodb_trx t "
                        "JOIN information_schema.processlist p ON p.id = t.trx_mysql_thread_id "
                        "WHERE p.user = CURRENT_USER() AND p.id <> CONNECTION_ID()"
                    )
                    assert cur.fetchone()[0] == 0, "an open transaction survived the failed run"
                except Exception as exc:  # noqa: BLE001 - privilege missing: rely on the DROP
                    if "1227" not in str(exc):
                        raise
                cur.execute("SET SESSION lock_wait_timeout = 5")
                cur.execute(f"DROP TABLE IF EXISTS `{table}`")  # must not wait on a lock
        finally:
            db.close()
    finally:
        db = _mysql()
        try:
            with db.cursor() as cur:
                cur.execute("SET SESSION lock_wait_timeout = 5")
                cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        except Exception:  # noqa: BLE001 - cleanup only
            pass
        finally:
            db.close()
