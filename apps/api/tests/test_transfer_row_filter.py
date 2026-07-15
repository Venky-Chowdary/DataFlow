"""Row-filter integration tests for the universal transfer engine."""

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


def _csv_bytes(rows: list[tuple[str, str, str]]) -> bytes:
    lines = ["id,status,amount"]
    for rid, status, amount in rows:
        lines.append(f"{rid},{status},{amount}")
    return "\n".join(lines).encode("utf-8")


def test_full_refresh_with_source_filter_only_active_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "filter.db"
    engine = UniversalTransferEngine()

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=_csv_bytes([
            ("1", "active", "100"),
            ("2", "inactive", "250"),
            ("3", "active", "75"),
            ("4", "pending", "300"),
        ]),
        source_filename="orders.csv",
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            connection_string=str(db_path),
            table="orders",
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        source_filter={"column": "status", "operator": "eq", "value": "active"},
    )
    result = engine.execute_tracked(request, f"filter_01_{os.getpid():06d}")
    assert result.success, result.error

    conn = __import__("sqlite3").connect(str(db_path))
    try:
        rows = conn.execute("SELECT id, status FROM orders ORDER BY id").fetchall()
    finally:
        conn.close()

    assert [str(r[0]) for r in rows] == ["1", "3"]
    assert len(rows) == 2


def test_source_filter_with_and_predicate(tmp_path: Path) -> None:
    db_path = tmp_path / "filter_and.db"
    engine = UniversalTransferEngine()

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_content=_csv_bytes([
            ("1", "active", "100"),
            ("2", "active", "50"),
            ("3", "inactive", "100"),
            ("4", "active", "200"),
        ]),
        source_filename="orders.csv",
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            connection_string=str(db_path),
            table="orders",
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        source_filter={
            "and": [
                {"column": "status", "operator": "eq", "value": "active"},
                {"column": "amount", "operator": "gte", "value": "100"},
            ]
        },
    )
    result = engine.execute_tracked(request, f"filter_02_{os.getpid():06d}")
    assert result.success, result.error

    conn = __import__("sqlite3").connect(str(db_path))
    try:
        ids = [r[0] for r in conn.execute("SELECT id FROM orders ORDER BY id").fetchall()]
    finally:
        conn.close()

    assert ids == ["1", "4"] if isinstance(ids[0], str) else [1, 4]
