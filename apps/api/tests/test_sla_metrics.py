"""SLA metrics are surfaced on every transfer result."""
from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[2]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.adapters import write_destination_database  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

sys.path.insert(0, str(_API_ROOT / "tests"))
from test_execute_tracked_universal_matrix import _build_db_endpoint  # noqa: E402


def test_transfer_result_includes_sla_metrics(tmp_path: Path) -> None:
    columns = ["id", "amount"]
    schema = {"id": "INTEGER", "amount": "DECIMAL"}
    records = [{"id": "1", "amount": "1000.00"}, {"id": "2", "amount": "2000.50"}]
    mappings = [{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}]

    source = _build_db_endpoint("sqlite", tmp_path, "sla_src", uuid.uuid4().hex[:8])
    rows, _, _ = write_destination_database(
        source, records, columns, schema, mappings
    )
    assert rows == len(records)

    destination = _build_db_endpoint("sqlite", tmp_path, "sla_dst", uuid.uuid4().hex[:8])
    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        mappings=mappings,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])

    assert result.success
    assert result.records_transferred == len(records)
    assert result.elapsed_seconds >= 0
    assert result.records_per_second >= 0
    assert result.peak_memory_bytes >= 0
    assert result.destination_summary.get("elapsed_seconds") == result.elapsed_seconds
    assert result.destination_summary.get("records_per_second") == result.records_per_second
    assert result.destination_summary.get("peak_memory_bytes") == result.peak_memory_bytes
