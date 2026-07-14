"""File → file_export conversion end-to-end."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def test_csv_to_parquet_export():
    csv_content = b"id,name\n1,alice\n2,bob\n"

    request = TransferRequest(
        source=EndpointConfig(
            kind="file", format="csv",
        ),
        destination=EndpointConfig(
            kind="file_export", format="parquet",
        ),
        source_filename="users.csv",
        source_content=csv_content,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2
    assert result.destination_summary.get("format") == "parquet"
    assert result.destination_summary.get("filename", "").endswith(".parquet")
