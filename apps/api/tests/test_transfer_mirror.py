"""Inferred-delete (full_refresh_mirror) transfer tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _csv_bytes(rows: list[tuple[str, str]]) -> bytes:
    lines = ["id,name"]
    for rid, name in rows:
        lines.append(f"{rid},{name}")
    return "\n".join(lines).encode("utf-8")


def _active_rows(db_path: Path) -> list[tuple]:
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute("SELECT id, name, _deleted FROM mirror_test ORDER BY id")
        return cur.fetchall()
    finally:
        conn.close()


def test_file_to_sqlite_mirror_soft_deletes_and_reactivates(tmp_path: Path) -> None:
    db_path = tmp_path / "mirror.db"
    engine = UniversalTransferEngine()

    # First snapshot: ids 1, 2, 3
    request1 = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=_csv_bytes([("1", "Alice"), ("2", "Bob"), ("3", "Charlie")]),
        source_filename="snapshot1.csv",
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            connection_string=str(db_path),
            table="mirror_test",
        ),
        sync_mode="full_refresh_mirror",
        skip_preflight=True,
        validation_mode="strict",
    )
    result1 = engine.execute_tracked(request1, f"mirror_01_{os.getpid():06d}")
    assert result1.success, result1.error

    rows1 = _active_rows(db_path)
    assert len(rows1) == 3
    assert {str(r[0]) for r in rows1} == {"1", "2", "3"}
    assert all(r[2] in (0, False, None) for r in rows1)

    # Second snapshot: 1 is gone, 2 and 3 updated, 4 is new
    request2 = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=_csv_bytes([("2", "Bob2"), ("3", "Charlie2"), ("4", "Dave")]),
        source_filename="snapshot2.csv",
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            connection_string=str(db_path),
            table="mirror_test",
        ),
        sync_mode="full_refresh_mirror",
        skip_preflight=True,
        validation_mode="strict",
    )
    result2 = engine.execute_tracked(request2, f"mirror_02_{os.getpid():06d}")
    assert result2.success, result2.error

    conn = __import__("sqlite3").connect(str(db_path))
    try:
        cur = conn.execute("SELECT id, name, _deleted FROM mirror_test ORDER BY id")
        all_rows = cur.fetchall()
    finally:
        conn.close()

    active = [r for r in all_rows if r[2] in (0, False, None)]
    deleted = [r for r in all_rows if r[2] not in (0, False, None)]

    assert len(active) == 3, active
    assert {str(r[0]) for r in active} == {"2", "3", "4"}
    assert {r[1] for r in active} == {"Bob2", "Charlie2", "Dave"}
    assert len(deleted) == 1
    assert str(deleted[0][0]) == "1"
