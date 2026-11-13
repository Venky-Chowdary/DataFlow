"""G7 review M2: an XLSX timestamp cell never silently drops sub-millisecond digits.

Excel holds a date-time to the millisecond. Before: ``2024-01-02 03:04:05.123456``
became a date cell holding ``.123000`` (``.1235`` -> ``.124000``, a DATETIME2
seventh digit vanished) and Gate-8 still passed, because it compares at the
destination engine's native precision. Rule: keep the exact text whenever
Excel cannot hold the value exactly — any non-zero digit beyond milliseconds.
"""

from __future__ import annotations

import datetime as dt
import io
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

openpyxl = pytest.importorskip("openpyxl")


def _export(values: list[str], ddl: str) -> tuple[bytes, list[dict], list[dict]]:
    from src.transfer.adapters import write_destination_file
    from src.transfer.models import EndpointConfig

    records = [{"id": str(i), "v": v} for i, v in enumerate(values, start=1)]
    types = {"id": "integer", "v": ddl}
    mappings = [{"source": c, "target": c, "target_type": types[c]} for c in ("id", "v")]
    content, _name, _summary = write_destination_file(
        EndpointConfig(kind="file_export", format="excel"),
        [dict(r) for r in records],
        ["id", "v"],
        source_format="csv",
        mappings=mappings,
        column_types=types,
    )
    return content, records, mappings


def _column(content: bytes) -> list[tuple[object, str]]:
    ws = openpyxl.load_workbook(io.BytesIO(content)).active
    return [(row[1].value, row[1].data_type) for row in ws.iter_rows(min_row=2)]


@pytest.mark.parametrize(
    "ddl, value, digits",
    [
        ("timestamp", "2024-01-02 03:04:05.123456", "123456"),
        ("timestamp", "2024-01-02 03:04:05.1235", "1235"),
        ("datetime2", "2024-01-02T03:04:05.1234567", "1234567"),
        ("timestamp(6)", "2024-01-02 03:04:05.000001", "000001"),
    ],
)
def test_sub_millisecond_timestamp_stays_exact_text(ddl, value, digits):
    from src.transfer.adapters import write_destination_file
    from src.transfer.models import EndpointConfig

    content, records, mappings = _export([value], ddl)
    [(cell, dtype)] = _column(content)
    assert dtype == "s", (cell, dtype)
    # The XLSX text is exactly what the delimited export of the same row says.
    csv_bytes, _n, _s = write_destination_file(
        EndpointConfig(kind="file_export", format="csv"),
        records, ["id", "v"], source_format="csv", mappings=mappings,
        column_types={"id": "integer", "v": ddl},
    )
    assert cell == csv_bytes.decode("utf-8").splitlines()[1].split(",", 1)[1]
    assert digits[:6] in str(cell)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2024-01-02 03:04:05.123", dt.datetime.fromisoformat("2024-01-02T03:04:05.123000")),
        ("2024-01-02 03:04:05.120000", dt.datetime.fromisoformat("2024-01-02T03:04:05.120000")),
        ("2024-01-02 03:04:05", dt.datetime.fromisoformat("2024-01-02T03:04:05")),
    ],
)
def test_millisecond_timestamp_is_a_date_cell(value, expected):
    content, _records, _mappings = _export([value], "timestamp")
    assert _column(content) == [(expected, "d")]


def test_python_datetime_fallback_keeps_microseconds_as_text():
    from services.format_converter import convert_rows

    exact = dt.datetime.fromisoformat("2024-01-02T03:04:05.123456")
    ms = dt.datetime.fromisoformat("2024-01-02T03:04:05.123000")
    content, _ = convert_rows(["a", "b"], [[exact, ms]], source_format="csv", target_format="excel")
    ws = openpyxl.load_workbook(io.BytesIO(content)).active
    a, b = ws.cell(2, 1), ws.cell(2, 2)
    assert (a.data_type, a.value) == ("s", "2024-01-02T03:04:05.123456")
    assert (b.data_type, b.value) == ("d", ms)


def test_gate8_passes_on_the_exact_text_cells(tmp_path):
    from src.transfer.reconcile_step import _file_export_cell_readback

    content, records, mappings = _export(
        ["2024-01-02 03:04:05.123456", "2024-01-02 03:04:05.123"], "timestamp"
    )
    path = tmp_path / "export.xlsx"
    path.write_bytes(content)
    compare = _file_export_cell_readback(
        records, ["id", "v"], mappings, {"path": str(path), "primary_key_columns": ["id"]}, fmt="excel"
    )
    assert compare is not None and compare.get("passed") is True, compare
    assert compare.get("rows_compared") == 2, compare
