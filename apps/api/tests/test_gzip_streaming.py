"""Transparent gzip streaming for CSV/JSONL file uploads."""
from __future__ import annotations

import gzip
import io
import json
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[2]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

sys.path.insert(0, str(_API_ROOT / "tests"))
from test_execute_tracked_universal_matrix import _build_db_endpoint  # noqa: E402


def test_gzip_csv_file_to_sqlite(tmp_path: Path) -> None:
    plain_csv = "id,amount\n1,1000.00\n2,2000.50\n3,3000.00\n".encode("utf-8")
    gzipped = gzip.compress(plain_csv)

    source = EndpointConfig(kind="file", format="csv")
    destination = _build_db_endpoint("sqlite", tmp_path, "gzip_csv", uuid.uuid4().hex[:8])
    request = TransferRequest(
        source=source,
        destination=destination,
        source_content=gzipped,
        source_filename="data.csv.gz",
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        mappings=[{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])

    assert result.success, f"{result.error}"
    assert result.records_transferred == 3
    assert result.reconciliation.get("passed") is True


def test_gzip_path_csv_to_sqlite(tmp_path: Path) -> None:
    plain_csv = "id,amount\n10,100.00\n20,200.00\n".encode("utf-8")
    gz_path = tmp_path / "data.csv.gz"
    gz_path.write_bytes(gzip.compress(plain_csv))

    source = EndpointConfig(kind="file", format="csv")
    destination = _build_db_endpoint("sqlite", tmp_path, "gzip_path_csv", uuid.uuid4().hex[:8])
    request = TransferRequest(
        source=source,
        destination=destination,
        source_path=str(gz_path),
        source_filename="data.csv.gz",
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        mappings=[{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])

    assert result.success, f"{result.error}"
    assert result.records_transferred == 2
    assert result.reconciliation.get("passed") is True


def test_gzip_jsonl_file_to_sqlite(tmp_path: Path) -> None:
    lines = [json.dumps({"id": str(i), "amount": f"{i * 100}.00"}) for i in range(1, 4)]
    plain = ("\n".join(lines) + "\n").encode("utf-8")
    gzipped = gzip.compress(plain)

    source = EndpointConfig(kind="file", format="jsonl")
    destination = _build_db_endpoint("sqlite", tmp_path, "gzip_jsonl", uuid.uuid4().hex[:8])
    request = TransferRequest(
        source=source,
        destination=destination,
        source_content=gzipped,
        source_filename="data.jsonl.gz",
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        mappings=[{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])

    assert result.success, f"{result.error}"
    assert result.records_transferred == 3
    assert result.reconciliation.get("passed") is True
