"""A commit racing the per-table fallback snapshot is never dropped.

Without `RELOAD` the snapshot locks each table with `LOCK TABLES ... READ`.
MySQL releases those locks the moment a transaction begins, so a coordinate
capture placed *after* `START TRANSACTION WITH CONSISTENT SNAPSHOT` reads a
position that is later than the read view: a commit landing in between is
behind the replay position and ahead of the dump, and neither phase carries it.

This drives the real compose MySQL down the fallback path and commits a row
exactly at the coordinate capture. The row must reach the destination, read on
a connection the transfer engine never touched.

Skips when the compose MySQL with ROW binlog is not reachable.
"""

from __future__ import annotations

import threading
import uuid
from typing import Any

import pytest

from tests.test_cdc_mysql_to_mysql_same_instance import (  # noqa: E402
    CFG,
    _cleanup,
    _exec,
    _run_transfer,
    _seed,
    pytestmark as _live_mysql_required,
)

pytestmark = _live_mysql_required

RACING_ID = 10_000_000


def _independent_row(table: str, row_id: int) -> tuple[Any, ...] | None:
    import pymysql

    conn = pymysql.connect(
        host=CFG["host"],
        port=int(CFG["port"]),
        user=CFG["username"],
        password=CFG["password"],
        database=CFG["database"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, name FROM {table} WHERE id = %s", (row_id,))
            return cur.fetchone()
    finally:
        conn.close()


def test_a_commit_racing_the_fallback_coordinate_capture_still_lands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from connectors import mysql_change_stream as mcs

    real_get_connection = mcs.get_connection
    suffix = uuid.uuid4().hex[:8]
    src_table = f"cdc_race_src_{suffix}"
    dst_table = f"cdc_race_dst_{suffix}"
    committed = threading.Event()
    injected = threading.Event()

    def commit_racing_row() -> None:
        _exec(
            f"INSERT INTO {src_table} (id, name, amount) VALUES (%s, %s, %s)",
            (RACING_ID, "racer", "1.00"),
        )
        committed.set()

    class FallbackOnlyConnection:
        """Real connection with `RELOAD` withdrawn and a writer at the capture."""

        def __init__(self, inner: Any) -> None:
            self._inner = inner

        def cursor(self) -> Any:
            inner_cursor = self._inner.cursor()

            class Cursor:
                def __enter__(self) -> Any:
                    inner_cursor.__enter__()
                    return self

                def __exit__(self, *exc: object) -> None:
                    inner_cursor.__exit__(*exc)

                def execute(self, sql: str, *args: Any) -> Any:
                    upper = sql.strip().upper()
                    if upper.startswith("FLUSH TABLES WITH READ LOCK"):
                        raise RuntimeError("injected: no RELOAD privilege")
                    if (
                        upper.startswith(("SHOW MASTER STATUS", "SHOW BINARY LOG STATUS"))
                        and not injected.is_set()
                    ):
                        injected.set()
                        # Started, not awaited: with the fix the table lock is
                        # still held here, so this write waits for the read view
                        # to begin and is replayed from the captured position.
                        threading.Thread(target=commit_racing_row, daemon=True).start()
                        committed.wait(timeout=1.5)
                    return inner_cursor.execute(sql, *args)

                def __getattr__(self, name: str) -> Any:
                    return getattr(inner_cursor, name)

            return Cursor()

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

    monkeypatch.setattr(
        mcs, "get_connection", lambda **kw: FallbackOnlyConnection(real_get_connection(**kw))
    )

    _seed(src_table)
    try:
        _run_transfer(src_table, dst_table, f"mysql-race-{suffix}")
        assert injected.is_set(), "the fallback coordinate capture never ran"
        assert committed.wait(timeout=30), "the racing commit never completed"
        assert _independent_row(dst_table, RACING_ID) == (RACING_ID, "racer")
    finally:
        _cleanup(src_table, dst_table)
