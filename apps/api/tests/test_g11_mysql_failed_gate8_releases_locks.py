"""OPS-F1: a MySQL run that fails Gate-8 leaves no open transaction or lock.

Observed on the pre-G11 code: a SQLite -> MySQL run whose Gate-8 sample read
failed (``1054 Unknown column``) kept a pooled connection inside an open
transaction holding the table's metadata lock, so the next ``DROP TABLE`` on
that table blocked indefinitely (a ``Sleep`` connection in the processlist).

The Gate-8 failure is induced explicitly on each route, so the test does not
depend on which route the server allows (CI PR 426, api-tests 4/6: CI's MySQL
has ``local_infile`` on, the run took the LOAD DATA fast path, whose Gate-8
is a COUNT(*) proof with no sample read, and the failure the test relied on
never happened):

* ``row_path``: the copy fast path is declined and the Gate-8 sample read asks
  for a column the table does not have (``1054 Unknown column`` on a real
  connection, inside the read's transaction: the OPS-F1 shape).
* ``fast_path``: the LOAD DATA fast path (needs the server's ``local_infile``;
  skipped where it is off) and a row removed from the destination after the
  write, so Gate-8's independent readback disagrees.

Either way the run fails Gate-8; afterwards no transaction of this process is
open on the server and ``DROP TABLE`` succeeds under a short
``lock_wait_timeout``. ``DATAFLOW_TEST_MYSQL_PORT`` points it at another
server (a ``local_infile=1`` one simulates CI).
"""

from __future__ import annotations

import contextlib
import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

LONG_64 = "customer_lifetime_value_after_discounts_and_refunds_in_eur_cents"
PORT = int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT") or 3306)
MISSING = "g11o_no_such_column"


def _mysql():
    import pymysql

    return pymysql.connect(host="127.0.0.1", port=PORT, user="dataflow", password="dataflow",
                           database="dataflow", autocommit=True)


def _mysql_up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=1).close()
        _mysql().close()
    except Exception:  # noqa: BLE001 - any failure means "not available"
        return False
    return True


def _local_infile_on() -> bool:
    db = _mysql()
    try:
        with db.cursor() as cur:
            cur.execute("SELECT @@GLOBAL.local_infile")
            return str(cur.fetchone()[0]).lower() in {"1", "on"}
    finally:
        db.close()


def _row_path(monkeypatch, table: str) -> None:
    """Decline the copy fast path; Gate-8's sample read names a missing column."""
    import services.target_sample as target_sample
    import src.transfer.copy_route as copy_route

    monkeypatch.setattr(copy_route, "_try_sqlite_mysql_copy_fast_path", lambda **_k: None)
    real = target_sample._read_destination_sample

    def failing_read(db_type, dest, *, table_name, columns=None, **kwargs):
        if db_type == "mysql" and table_name == table:
            columns = [c for c in (columns or []) if c != "*"] + [MISSING]
        return real(db_type, dest, table_name=table_name, columns=columns, **kwargs)

    monkeypatch.setattr(target_sample, "_read_destination_sample", failing_read)


def _fast_path(monkeypatch, table: str) -> None:
    """The LOAD DATA fast path, then a row removed behind the run's back."""
    import src.transfer.copy_route as copy_route

    real = copy_route._try_sqlite_mysql_copy_fast_path

    def copy_then_tamper(**kwargs):
        out = real(**kwargs)
        assert out is not None, "the LOAD DATA fast path declined"
        db = _mysql()
        try:
            with db.cursor() as cur:
                cur.execute(f"DELETE FROM `{table}` WHERE id = 2")  # nosec B608
        finally:
            db.close()
        return out

    monkeypatch.setattr(copy_route, "_try_sqlite_mysql_copy_fast_path", copy_then_tamper)


@pytest.mark.skipif(not _mysql_up(), reason=f"no MySQL on :{PORT}")
@pytest.mark.parametrize("route", ["row_path", "fast_path"])
def test_failed_gate8_leaves_no_open_transaction_or_metadata_lock(
    tmp_path: Path, monkeypatch, route: str
):
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
    if route == "fast_path" and not _local_infile_on():
        pytest.skip("server local_infile is off: no LOAD DATA fast path here")
    (_row_path if route == "row_path" else _fast_path)(monkeypatch, table)
    dest = mysql_endpoint(table)
    dest.host, dest.port = "127.0.0.1", PORT
    try:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(kind="database", format="sqlite", database=str(src),
                                      table="t"),
                destination=dest,
                mappings=[{"source": "id", "target": "id", "confidence": 1.0},
                          {"source": "v", "target": LONG_64, "confidence": 1.0}],
                sync_mode="full_refresh_overwrite",
                skip_preflight=True,
            ),
            "g11o" + uuid.uuid4().hex[:16],
        )
        summary = result.destination_summary or {}
        assert (summary.get("copy_fast_path") == "used") == (route == "fast_path"), summary
        assert not (result.reconciliation or {}).get("passed"), result.reconciliation
        if route == "row_path":
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
            with db.cursor() as cur, contextlib.suppress(Exception):  # cleanup only
                cur.execute("SET SESSION lock_wait_timeout = 5")
                cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        finally:
            db.close()
