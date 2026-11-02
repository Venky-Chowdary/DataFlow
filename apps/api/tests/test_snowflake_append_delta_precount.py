"""A correct append into a populated Snowflake table must prove its own delta.

Reported from the field: 200 rows loaded, then 100 more appended to the same
table. The rows landed (dest COUNT(*) 300) and the run was still failed —
"Append delta unverified: destination held an unknown number of rows before
this write". Dest-*after* is counted through the native Snowflake driver while
dest-*before* went through SQLAlchemy, whose ``snowflake`` dialect the product
does not ship, so the pre-count came back unknowable on every Snowflake append.
"""

from __future__ import annotations

import csv
import io
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.dest_precount import PRECOUNT_KEY, precount_table  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _csv_bytes(start: int, count: int) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "amount"])
    writer.writeheader()
    for i in range(start, start + count):
        writer.writerow({"id": str(i), "amount": "10.50"})
    return buf.getvalue().encode("utf-8")


def _destination(table_name: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="snowflake",
        host="localhost",
        port=443,
        database="dataflow",
        username="test",
        password="test",
        schema="public",
        table=table_name,
    )


def _append_request(table_name: str, content: bytes) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="employees.csv",
        source_content=content,
        destination=_destination(table_name),
        sync_mode="full_refresh_append",
        skip_preflight=True,
    )


def _snowflake_cfg() -> dict[str, str]:
    return {
        "type": "snowflake",
        "host": "localhost",
        "database": "dataflow",
        "username": "test",
        "password": "test",
        "schema": "public",
        "warehouse": "",
    }


def test_append_into_populated_snowflake_table_proves_its_delta():
    pytest.importorskip("fakesnow")
    table_name = "employee_exceldata_" + uuid.uuid4().hex[:8]
    engine = UniversalTransferEngine()

    first = engine.execute_tracked(
        _append_request(table_name, _csv_bytes(1, 200)), uuid.uuid4().hex[:24]
    )
    assert first.success, first.error
    assert first.records_transferred == 200

    second = engine.execute_tracked(
        _append_request(table_name, _csv_bytes(201, 100)), uuid.uuid4().hex[:24]
    )
    recon = second.reconciliation or {}
    assert second.success, second.error or recon.get("message")
    assert recon.get("target_rows_before") == 200
    assert recon.get("target_rows") == 300
    assert recon.get("passed") is True, recon.get("message")
    assert "unverified" not in str(recon.get("message", ""))


def test_snowflake_precount_counts_the_stored_table_name():
    pytest.importorskip("fakesnow")
    table_name = "sunday0816_" + uuid.uuid4().hex[:8]
    cfg = _snowflake_cfg()

    # A table that does not exist yet is a known-empty destination, not unknown.
    assert precount_table("snowflake", cfg, table_name) == 0

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(
        _append_request(table_name, _csv_bytes(1, 12)), uuid.uuid4().hex[:24]
    )
    assert result.success, result.error
    assert precount_table("snowflake", cfg, table_name) == 12
    assert precount_table("snowflake", cfg, table_name.upper()) == 12


def test_append_precount_is_stamped_on_the_destination_summary():
    pytest.importorskip("fakesnow")
    table_name = "precount_stamp_" + uuid.uuid4().hex[:8]
    engine = UniversalTransferEngine()

    engine.execute_tracked(
        _append_request(table_name, _csv_bytes(1, 5)), uuid.uuid4().hex[:24]
    )
    second = engine.execute_tracked(
        _append_request(table_name, _csv_bytes(6, 5)), uuid.uuid4().hex[:24]
    )
    assert (second.reconciliation or {}).get(PRECOUNT_KEY) == 5
