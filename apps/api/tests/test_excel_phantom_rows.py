"""An Excel sheet's used range is not its data.

openpyxl reports every row inside the worksheet's used range, and formatting
alone grows that range. Loading those rows produced destination rows whose
every column was NULL — a two-row sheet landing as hundreds of empty rows —
so the reader must count and yield only rows that carry values.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.excel_parser import (  # noqa: E402
    count_excel_rows,
    is_blank_row,
    iter_excel_batches,
    parse_excel_preview,
    sheet_headers,
)
from services.csv_profiler import (  # noqa: E402
    count_csv_rows,
    parse_csv_full,
    parse_csv_preview,
)
from src.transfer.file_stream import (  # noqa: E402
    _excel_batches,
    _excel_count,
    _excel_preview,
    _iter_csv_batches,
)

openpyxl = pytest.importorskip("openpyxl")


def _sheet_with_phantom_range(data_rows: int = 2, phantom_rows: int = 17) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["country", "year", "total"])
    for i in range(data_rows):
        ws.append([f"country_{i}", 2020 + i, 88.5 + i])
    # Formatting-only cells below the data: no values, but inside the used range.
    for r in range(2 + data_rows, 2 + data_rows + phantom_rows):
        ws.cell(row=r, column=1).number_format = "0.00"
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_blank_row_detection_treats_whitespace_as_empty():
    assert is_blank_row((None, None, None))
    assert is_blank_row((None, "", "   "))
    assert not is_blank_row((None, "", "0"))
    assert not is_blank_row((0, None))


def test_unlabelled_header_cells_are_named_positionally():
    assert sheet_headers(("country", None, "total")) == ["country", "col_1", "total"]


def test_preview_excludes_formatting_only_rows():
    content = _sheet_with_phantom_range()
    headers, rows, total = parse_excel_preview(content)
    assert headers == ["country", "year", "total"]
    assert total == 2
    assert [r[0] for r in rows] == ["country_0", "country_1"]


def test_count_is_rows_with_values_not_used_range():
    content = _sheet_with_phantom_range(data_rows=3, phantom_rows=500)
    assert count_excel_rows(content) == 3


def test_batches_do_not_yield_all_null_records():
    content = _sheet_with_phantom_range()
    records = [r for batch in iter_excel_batches(content, 100) for r in batch]
    assert len(records) == 2
    assert all(any(v for v in rec.values()) for rec in records)


def test_streaming_reader_agrees_with_parser(tmp_path):
    content = _sheet_with_phantom_range(data_rows=4, phantom_rows=25)
    path = tmp_path / "phantom.xlsx"
    path.write_bytes(content)

    headers, rows, total = _excel_preview(str(path))
    streamed = [r for batch in _excel_batches(str(path), 100) for r in batch]

    assert headers == ["country", "year", "total"]
    assert total == 4
    assert len(rows) == 4
    assert _excel_count(str(path)) == 4
    assert len(streamed) == 4
    # Count, preview and stream must agree, or reconciliation compares a read
    # against a cardinality nothing ever read.
    assert _excel_count(str(path)) == total == len(streamed)


def test_interior_blank_separator_row_is_not_a_record():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["country", "total"])
    ws.append(["India", 1])
    ws.append([None, None])
    ws.append(["Kenya", 2])
    buf = io.BytesIO()
    wb.save(buf)
    content = buf.getvalue()

    records = [r for batch in iter_excel_batches(content, 100) for r in batch]
    assert [r["country"] for r in records] == ["India", "Kenya"]
    assert count_excel_rows(content) == 2


def test_csv_blank_and_delimiter_only_lines_are_not_records():
    # A sheet exported to CSV ends in ",," lines for the same reason.
    content = b"a,b\n1,2\n\n,\n3,4\n\n"
    headers, preview, _enc, _delim = parse_csv_preview(content)
    assert headers == ["a", "b"]
    assert preview == [["1", "2"], ["3", "4"]]
    assert count_csv_rows(content) == 2
    assert parse_csv_full(content)[1] == [["1", "2"], ["3", "4"]]


def test_csv_stream_batches_match_the_count():
    content = b"a,b\n1,2\n,\n3,4\n\n"
    streamed = [r for batch in _iter_csv_batches(content, 100) for r in batch]
    assert len(streamed) == count_csv_rows(content) == 2


def test_row_of_zeroes_is_kept():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["a", "b"])
    ws.append([0, 0])
    ws.append([False, ""])
    buf = io.BytesIO()
    wb.save(buf)
    content = buf.getvalue()

    records = [r for batch in iter_excel_batches(content, 100) for r in batch]
    assert len(records) == 2
    assert count_excel_rows(content) == 2
