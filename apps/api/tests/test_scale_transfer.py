"""Generate large CSV fixture and benchmark parse throughput."""

from __future__ import annotations

import csv
import io
import os
import sys
import time
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

FIXTURE = _API_ROOT / "tests" / "fixtures" / "scale_100k.csv"
ROW_COUNT = int(os.getenv("SCALE_ROWS", "100000"))


def _generate_csv(rows: int) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id", "full_name", "email", "amount", "created_at", "is_active"])
    for i in range(rows):
        w.writerow([
            i + 1,
            f"User {i}",
            f"user{i}@example.com",
            round(10.5 + (i % 1000) * 0.01, 2),
            f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
            i % 2 == 0,
        ])
    return buf.getvalue().encode()


def test_generate_scale_fixture():
    if FIXTURE.exists() and ROW_COUNT == 100000:
        return
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_bytes(_generate_csv(ROW_COUNT))


def test_parse_100k_rows():
    if not FIXTURE.exists():
        test_generate_scale_fixture()
    from src.services.file_parser import FileParser

    content = FIXTURE.read_bytes()
    t0 = time.perf_counter()
    result = FileParser.parse(content, "scale_100k.csv")
    elapsed = time.perf_counter() - t0
    assert result.success, result.error
    assert result.row_count == ROW_COUNT
    assert elapsed < 20, f"Parse took {elapsed:.1f}s for {ROW_COUNT} rows"


def test_100k_csv_to_sqlite_scale_transfer(tmp_path: Path) -> None:
    """End-to-end 100k CSV -> SQLite with strict reconciliation must pass."""
    if not FIXTURE.exists():
        pytest.skip("Scale fixture not generated")

    db_path = tmp_path / "scale.db"
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_path=str(FIXTURE),
        source_filename="scale_100k.csv",
        destination=EndpointConfig(
            kind="database",
            format="sqlite",
            connection_string=str(db_path),
            table="scale_test",
        ),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )

    engine = UniversalTransferEngine()
    t0 = time.perf_counter()
    result = engine.execute_tracked(request, f"scale_{os.getpid():06d}")
    elapsed = time.perf_counter() - t0

    assert result.success, result.error
    assert result.records_transferred == ROW_COUNT
    assert result.reconciliation.get("passed") is True
    assert result.reconciliation.get("source_rows") == ROW_COUNT
    assert result.reconciliation.get("target_rows") == ROW_COUNT
    assert result.reconciliation.get("source_checksum") == result.reconciliation.get("target_checksum")
    assert elapsed < 30, f"100k transfer took {elapsed:.1f}s"
