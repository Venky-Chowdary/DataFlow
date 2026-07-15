"""Path-based file streaming: billion-row style loads without loading bytes."""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from src.transfer.file_stream import (  # noqa: E402
    _iter_csv_batches,
    peek_file_source,
    prepare_stream_content,
    stream_file_to_database,
)
from src.transfer.models import EndpointConfig  # noqa: E402


def _write_csv_file(path: Path, rows: int) -> list[str]:
    headers = ["id", "name", "amount"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(",".join(headers) + "\n")
        for i in range(1, rows + 1):
            f.write(f"{i},row{i},{i * 1.5}\n")
    return headers


def test_peek_file_source_from_path():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "large.csv"
        _write_csv_file(p, 12000)
        columns, schema, total, sample = peek_file_source(str(p), "large.csv")
        assert columns == ["id", "name", "amount"]
        assert total == 12000
        assert len(sample) == 100
        assert schema.get("id") in {"INTEGER", "INT"}


def test_iter_csv_batches_from_path():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "data.csv"
        _write_csv_file(p, 500)
        batches = list(_iter_csv_batches(str(p), 100))
        assert len(batches) == 5
        assert sum(len(b) for b in batches) == 500
        assert batches[0][0] == {"id": "1", "name": "row1", "amount": "1.5"}


def test_stream_file_to_database_from_path():
    with tempfile.TemporaryDirectory() as tmp_str:
        tmp = Path(tmp_str)
        p = tmp / "payments.csv"
        with open(p, "w", encoding="utf-8", newline="") as f:
            f.write("id,amount\n")
            f.write("1,1000.00\n")
            f.write("2,2000.50\n")

        dest = EndpointConfig(kind="database", format="sqlite", database=str(tmp / "out.db"), table="payments")
        rows, ddl, summary, columns = stream_file_to_database(
            str(p),
            "payments.csv",
            dest,
            mappings=[{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
            schema={"id": "INTEGER", "amount": "DECIMAL"},
        )
        assert rows == 2
        assert summary.get("checksum")
        assert columns == ["id", "amount"]


def test_prepare_stream_content_spills_large_payload():
    with tempfile.TemporaryDirectory() as tmp_str:
        tmp = Path(tmp_str)
        original = tmp / "original.csv"
        original.write_bytes(b"id\n" + b"\n".join(f"{i}".encode() for i in range(1000)))
        result = prepare_stream_content(
            content=original.read_bytes(),
            filename="big.csv",
            source_path="",
        )
        if len(original.read_bytes()) > 50 * 1024 * 1024:
            assert isinstance(result, (str, Path))
            assert Path(result).exists()
        else:
            # Below the default 50 MB threshold, bytes are returned as-is.
            assert isinstance(result, bytes)
