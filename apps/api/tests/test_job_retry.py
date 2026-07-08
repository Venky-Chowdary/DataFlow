"""Job retry serialization tests."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_transfer_request_roundtrip():
    from src.transfer.models import (
        EndpointConfig,
        TransferRequest,
        transfer_request_from_dict,
        transfer_request_to_dict,
    )

    req = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", connector_id="abc", table="orders"),
        destination=EndpointConfig(kind="database", format="mysql", connector_id="def", table="orders"),
        skip_preflight=True,
    )
    payload = transfer_request_to_dict(req)
    assert payload["requires_file_reupload"] is False
    restored = transfer_request_from_dict(payload)
    assert restored.source.table == "orders"
    assert restored.destination.format == "mysql"


def test_file_job_marks_reupload_required():
    from src.transfer.models import (
        EndpointConfig,
        TransferRequest,
        transfer_request_to_dict,
    )

    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="postgresql", table="t"),
        source_filename="data.csv",
        source_content=b"id,name\n1,a",
    )
    assert transfer_request_to_dict(req)["requires_file_reupload"] is True
