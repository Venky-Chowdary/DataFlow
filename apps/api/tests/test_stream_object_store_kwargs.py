"""Streaming object-store writes must forward MinIO/custom S3 endpoint settings."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.models import EndpointConfig
from src.transfer.stream import _write_batch


def test_write_batch_s3_forwards_endpoint_url_and_path_style() -> None:
    captured: dict = {}

    def _fake_write(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            ok=True,
            rows_written=2,
            checksum="abc",
            driver="s3",
            error=None,
            rejected_rows=0,
            coerced_null_rows=0,
            rejected_details=[],
            warnings=[],
            load_method="put",
            chunk_size=2,
            batches=1,
        )

    dest = EndpointConfig(
        kind="database",
        format="s3",
        database="my-bucket",
        table="exports/out.json",
        endpoint_url="http://localhost:9000",
        path_style=True,
        region="us-east-1",
    )
    cfg = {
        "host": "localhost",
        "port": 9000,
        "database": "my-bucket",
        "schema": "",
        "username": "minio",
        "password": "minio123",
        "connection_string": "",
        "ssl": False,
        "endpoint_url": "http://localhost:9000",
        "path_style": True,
        "region": "us-east-1",
    }

    with patch("importlib.import_module") as import_mod:
        mod = MagicMock()
        mod.write_mapped_rows = _fake_write
        import_mod.return_value = mod
        rows, checksum, summary = _write_batch(
            "s3",
            dest,
            cfg,
            table_name="exports/out.json",
            headers=["id", "name"],
            data_rows=[["1", "a"], ["2", "b"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
            ],
            column_types={"id": "integer", "name": "string"},
            create_table=True,
            on_checkpoint=None,
            chunk_idx=1,
            total_chunks=1,
            rows_so_far=0,
        )

    assert rows == 2
    assert checksum == "abc"
    assert summary["type"] == "s3"
    assert captured["endpoint_url"] == "http://localhost:9000"
    assert captured["path_style"] is True
    assert captured["region"] == "us-east-1"


def test_write_batch_dynamodb_forwards_endpoint_url() -> None:
    captured: dict = {}

    def _fake_write(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            ok=True,
            rows_written=1,
            checksum="d",
            driver="dynamodb",
            error=None,
            rejected_rows=0,
            coerced_null_rows=0,
            rejected_details=[],
            warnings=[],
            load_method="batch",
            chunk_size=1,
            batches=1,
        )

    dest = EndpointConfig(kind="database", format="dynamodb", database="orders")
    cfg = {
        "host": "localhost",
        "port": 5555,
        "database": "orders",
        "schema": "",
        "username": "",
        "password": "",
        "connection_string": "",
        "ssl": False,
        "endpoint_url": "http://localhost:5555",
        "path_style": False,
        "region": "us-east-1",
    }

    with patch("importlib.import_module") as import_mod:
        mod = MagicMock()
        mod.write_mapped_rows = _fake_write
        import_mod.return_value = mod
        _write_batch(
            "dynamodb",
            dest,
            cfg,
            table_name="orders",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "string"},
            create_table=True,
            on_checkpoint=None,
            chunk_idx=1,
            total_chunks=1,
            rows_so_far=0,
        )

    assert captured["endpoint_url"] == "http://localhost:5555"
    assert "path_style" in captured
    assert captured["region"] == "us-east-1"
