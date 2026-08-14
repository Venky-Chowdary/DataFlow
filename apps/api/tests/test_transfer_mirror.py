"""Inferred-delete (full_refresh_mirror) transfer tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path


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

    ledger = result2.row_accounting or {}
    assert ledger.get("conservation_kind") == "mirror", ledger
    assert ledger.get("balanced") is True, ledger
    assert ledger.get("active_count") == 3, ledger
    assert ledger.get("rows_written") == 3, ledger
    assert ledger.get("dest_count") == 4, ledger
    assert ledger.get("inferred_deletes") == 1, ledger
    assert ledger.get("reactivated") == 0, ledger
    assert ledger.get("rows_written_source") == "gate8_dest_active_readback", ledger

    # Bringing id 1 back must land as active. File→SQLite dest has no unique
    # key on first create, so upsert may delete+insert and materialize
    # ``_deleted = 0`` before the inferred-delete pass. This-run reactivate
    # census is dest-engine transitions *remaining for that pass* (0 here).
    # Dest-before tombstone ∩ snapshot is a future enhancement of this kernel.
    request3 = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=_csv_bytes([("1", "Alice"), ("2", "Bob2"), ("3", "Charlie2"), ("4", "Dave")]),
        source_filename="snapshot3.csv",
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
    result3 = engine.execute_tracked(request3, f"mirror_03_{os.getpid():06d}")
    assert result3.success, result3.error
    rows3 = _active_rows(db_path)
    assert {str(r[0]) for r in rows3} == {"1", "2", "3", "4"}
    assert all(r[2] in (0, False, None) for r in rows3)
    ledger3 = result3.row_accounting or {}
    assert ledger3.get("inferred_deletes") == 0, ledger3
    assert ledger3.get("active_count") == 4, ledger3


def test_staging_inferred_deletes_count_transitions_not_already_active(tmp_path: Path) -> None:
    """Dest-engine COUNT of transitions: already-active keys in staging are not reactivates."""
    import sqlalchemy as sa

    from services.mirror_engine import apply_inferred_deletes_via_staging

    db = tmp_path / "mirror_staging.db"
    engine = sa.create_engine(f"sqlite:///{db}")
    with engine.connect() as conn:
        conn.execute(sa.text("CREATE TABLE dst (id TEXT, name TEXT, _deleted INTEGER)"))
        conn.execute(sa.text("CREATE TABLE stg (id TEXT, name TEXT)"))
        conn.execute(
            sa.text(
                "INSERT INTO dst (id, name, _deleted) VALUES "
                "('1','a',0),('2','b',0),('3','c',1)"
            )
        )
        conn.execute(
            sa.text("INSERT INTO stg (id, name) VALUES ('1','a'),('3','c'),('4','d')")
        )
        conn.commit()
        census = apply_inferred_deletes_via_staging(
            conn, "dst", "stg", ["id"], dialect="sqlite"
        )
        conn.commit()
        rows = {
            str(r[0]): int(r[1])
            for r in conn.execute(sa.text("SELECT id, _deleted FROM dst")).fetchall()
        }
    assert census["reactivated"] == 1
    assert census["soft_deleted"] == 1
    assert rows == {"1": 0, "2": 1, "3": 0}
