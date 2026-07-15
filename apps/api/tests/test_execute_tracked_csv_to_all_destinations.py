"""CSV file → every supported local emulator destination.

This complements `test_execute_tracked_all_sources_to_postgresql.py` by proving
`execute_tracked` can write to every destination class from a standard file source.
"""

from __future__ import annotations

import contextlib
import socket
import sys
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

from tests.test_live_emulator_matrix import CASES as _CASES, _is_reachable


def _prepare_destination(dest: EndpointConfig) -> EndpointConfig:
    suffix = uuid.uuid4().hex[:8]
    if dest.format in ("s3", "gcs", "adls"):
        new_table = f"payments_{dest.format}_{suffix}.json"
    else:
        new_table = f"payments_{dest.format}_{suffix}"
    return replace(dest, table=new_table)


@pytest.mark.parametrize("dest", [pytest.param(c.values[0], id=c.id) for c in _CASES])
def test_csv_to_all_destinations(dest: EndpointConfig):
    if dest.format == "snowflake":
        pytest.importorskip("fakesnow")
    elif not _is_reachable(dest.host, dest.port):
        pytest.skip(f"{dest.format} emulator not reachable on {dest.host}:{dest.port}")

    dest = _prepare_destination(dest)

    csv_content = b"id,amount\n1,1000.00\n2,2000.50\n"
    request = TransferRequest(
        source=EndpointConfig(
            kind="file", format="csv",
        ),
        destination=dest,
        source_filename="payments.csv",
        source_content=csv_content,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, f"{dest.format}: {result.error}"
    assert result.records_transferred == 2, f"{dest.format}: expected 2 transferred, got {result.records_transferred}"
