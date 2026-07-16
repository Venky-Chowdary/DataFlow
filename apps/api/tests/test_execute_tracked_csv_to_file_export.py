"""File → file_export conversion end-to-end for multiple formats."""

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


def _run_export(dest_format: str, expected_ext: str, expected_mime_prefix: str) -> None:
    csv_content = b"id,name\n1,alice\n2,bob\n"

    request = TransferRequest(
        source=EndpointConfig(
            kind="file", format="csv",
        ),
        destination=EndpointConfig(
            kind="file_export", format=dest_format,
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
    assert result.destination_summary.get("format") == dest_format
    assert result.destination_summary.get("filename", "").endswith(f".{expected_ext}")
    mime = result.destination_summary.get("mime", "")
    assert mime.startswith(expected_mime_prefix), mime


def test_csv_to_parquet_export():
    _run_export("parquet", "parquet", "application/vnd.apache.parquet")


def test_csv_to_json_export():
    _run_export("json", "json", "application/json")


def test_csv_to_jsonl_export():
    _run_export("jsonl", "jsonl", "application/x-ndjson")


def test_csv_to_csv_export():
    _run_export("csv", "csv", "text/csv")


def test_csv_to_csv_export_with_output_path():
    out_path = "exports/test_output_path.csv"
    out_file = Path(_API_ROOT) / out_path
    csv_content = b"id,name\n1,alice\n2,bob\n"
    try:
        request = TransferRequest(
            source=EndpointConfig(kind="file", format="csv"),
            destination=EndpointConfig(kind="file_export", format="csv", output_path=out_path),
            source_filename="users.csv",
            source_content=csv_content,
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        )
        engine = UniversalTransferEngine()
        result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
        assert result.success is True, result.error
        assert result.records_transferred == 2
        assert out_file.exists(), result.destination_summary
        assert out_file.read_text().startswith("id,name")
        assert result.destination_summary.get("filename") == "test_output_path.csv"
    finally:
        if out_file.exists():
            out_file.unlink()
