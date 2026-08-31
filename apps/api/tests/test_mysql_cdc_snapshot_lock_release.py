"""A MySQL CDC snapshot lock never outlives the coordinate capture.

`FLUSH TABLES WITH READ LOCK` freezes every write on the instance. It is held
only to pin a read view and read the binlog coordinates; if it survives into the
dump, a `mysql -> mysql` route whose destination shares the server waits on its
own snapshot until `lock_wait_timeout` (`1205, Lock wait timeout exceeded`).
These cases pin the release to every path, including the ones where pinning the
read view or reading the coordinates fails.
"""

from __future__ import annotations

from typing import Any

import pytest

from connectors.mysql_change_stream import MySqlChangeStreamCdc

CFG = {
    "host": "localhost",
    "port": 3306,
    "database": "test",
    "username": "root",
    "password": "",
}


class FakeCursor:
    def __init__(self, conn: FakeConnection) -> None:
        self.conn = conn

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str) -> None:
        self.conn.sql.append(sql)
        upper = sql.strip().upper()
        if upper.startswith("FLUSH TABLES WITH READ LOCK"):
            if self.conn.fail_global_lock:
                raise RuntimeError("no RELOAD privilege")
            self.conn.locks_held = True
        elif upper.startswith("LOCK TABLES"):
            if self.conn.fail_table_lock:
                raise RuntimeError("no LOCK TABLES privilege")
            self.conn.locks_held = True
        elif upper.startswith("UNLOCK TABLES"):
            self.conn.locks_held = False
        elif upper.startswith("START TRANSACTION"):
            if self.conn.fail_start_txn:
                raise RuntimeError("cannot start transaction")
            self.conn.in_txn = True
        elif upper.startswith("SHOW ") and self.conn.fail_coords:
            raise RuntimeError("coordinates unavailable")

    def fetchone(self) -> tuple[Any, ...] | None:
        last = self.conn.sql[-1].strip().upper()
        if last.startswith("SHOW MASTER STATUS") or last.startswith(
            "SHOW BINARY LOG STATUS"
        ):
            return ("mysql-bin.000004", 1234)
        return None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return []

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(
        self,
        *,
        fail_global_lock: bool = False,
        fail_table_lock: bool = False,
        fail_start_txn: bool = False,
        fail_coords: bool = False,
    ) -> None:
        self.sql: list[str] = []
        self.locks_held = False
        self.in_txn = False
        self.closed = False
        self.rolled_back = False
        self.fail_global_lock = fail_global_lock
        self.fail_table_lock = fail_table_lock
        self.fail_start_txn = fail_start_txn
        self.fail_coords = fail_coords

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def autocommit(self, _value: bool) -> None:
        return None

    def rollback(self) -> None:
        self.rolled_back = True
        self.in_txn = False

    def close(self) -> None:
        self.closed = True
        self.locks_held = False


def _run_snapshot(
    monkeypatch: pytest.MonkeyPatch, conn: FakeConnection
) -> dict[str, Any]:
    """Consume a snapshot with the paging stubbed; report state at dump time."""
    cdc = MySqlChangeStreamCdc(CFG, table="orders", primary_key="id")
    observed: dict[str, Any] = {}

    monkeypatch.setattr(
        "connectors.mysql_change_stream.get_connection", lambda **_kw: conn
    )
    monkeypatch.setattr(cdc, "_acquire_cdc_lease", lambda: None)
    monkeypatch.setattr(cdc, "_ensure_decode_schema", lambda **_kw: {})
    monkeypatch.setattr(cdc, "heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(
        cdc,
        "_current_binlog_position",
        lambda: {"file": "mysql-bin.000009", "pos": 99},
    )

    def fake_pages(table: str, **kwargs: Any):
        observed["locks_held_during_dump"] = conn.locks_held
        observed["lock_conn_passed"] = kwargs.get("lock_conn") is conn
        observed["start_pos"] = dict(kwargs["start_pos"])
        return iter(())

    monkeypatch.setattr(cdc, "_snapshot_table_pages", fake_pages)

    batches = list(cdc.snapshot())
    observed["handoff"] = batches[-1].resume_token if batches else None
    observed["sql"] = list(conn.sql)
    observed["locks_held_after"] = conn.locks_held
    return observed


def test_global_lock_is_released_before_the_dump(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = FakeConnection()
    seen = _run_snapshot(monkeypatch, conn)

    assert "FLUSH TABLES WITH READ LOCK" in seen["sql"]
    assert "UNLOCK TABLES" in seen["sql"]
    # The read view is pinned before the lock goes, and the lock goes before any
    # row is read.
    assert seen["sql"].index("START TRANSACTION WITH CONSISTENT SNAPSHOT") < seen[
        "sql"
    ].index("UNLOCK TABLES")
    assert seen["locks_held_during_dump"] is False
    assert seen["locks_held_after"] is False
    # The dump reads inside the pinned read view.
    assert seen["lock_conn_passed"] is True
    assert seen["start_pos"]["file"] == "mysql-bin.000004"


def test_lock_is_released_when_the_read_view_cannot_be_pinned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = FakeConnection(fail_start_txn=True)
    seen = _run_snapshot(monkeypatch, conn)

    # Pinning failed, so the snapshot degrades — but it degrades without holding
    # the instance-wide lock through the dump.
    assert "UNLOCK TABLES" in seen["sql"]
    assert seen["locks_held_during_dump"] is False
    assert seen["locks_held_after"] is False
    assert seen["lock_conn_passed"] is False


def test_lock_is_released_when_coordinate_capture_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = FakeConnection(fail_coords=True)
    seen = _run_snapshot(monkeypatch, conn)

    assert seen["locks_held_during_dump"] is False
    assert seen["locks_held_after"] is False
    # A read view with no coordinates of its own is given up rather than dumped
    # against a position recaptured later (which would leave a silent gap).
    assert conn.rolled_back is True
    assert conn.closed is True
    assert seen["lock_conn_passed"] is False
    assert seen["start_pos"]["file"] == "mysql-bin.000009"


def test_lock_connection_is_closed_when_the_lock_phase_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ExplodingCursorConnection(FakeConnection):
        def cursor(self) -> FakeCursor:
            if self.sql:
                raise RuntimeError("connection lost mid-capture")
            return FakeCursor(self)

    conn = ExplodingCursorConnection()
    seen = _run_snapshot(monkeypatch, conn)

    # Dropping the reference is not a release: the server frees a session's
    # locks when the socket closes, so the connection is closed explicitly.
    assert conn.closed is True
    assert seen["locks_held_during_dump"] is False
    assert seen["lock_conn_passed"] is False


def test_table_lock_fallback_does_not_unlock_its_own_read_view(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Without RELOAD the snapshot falls back to LOCK TABLES ... READ. MySQL
    # releases those locks when the transaction begins, and UNLOCK TABLES would
    # commit the read view away, so none is issued.
    conn = FakeConnection(fail_global_lock=True)
    seen = _run_snapshot(monkeypatch, conn)

    assert any(sql.startswith("LOCK TABLES") for sql in seen["sql"])
    assert "UNLOCK TABLES" not in seen["sql"]
    assert seen["lock_conn_passed"] is True
    assert seen["start_pos"]["file"] == "mysql-bin.000004"


def test_no_lock_privilege_at_all_still_dumps_unpinned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = FakeConnection(fail_global_lock=True, fail_table_lock=True)
    seen = _run_snapshot(monkeypatch, conn)

    assert seen["locks_held_during_dump"] is False
    assert seen["lock_conn_passed"] is False
    # Coordinates come from a fresh read, which the dump cannot run ahead of.
    assert seen["start_pos"]["file"] == "mysql-bin.000009"
