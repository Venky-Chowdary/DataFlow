"""G7 (CWE-1236): an XLSX export never turns data into a live formula.

Before the fix ``_write_excel_bytes`` did ``ws.append(row)`` with raw strings:
openpyxl stores any text starting with ``=`` as a formula (``data_type='f'``),
so ``=HYPERLINK("http://evil…")`` opened as a clickable link and a harmless
``=== VIP ===`` became a broken formula. Numeric text such as ``12.50`` was a
text cell even when the column is numeric.

Product rule: every text value is an explicit string cell; numbers, decimals,
dates and booleans are typed cells according to the column's export type
(mapping/destination type), falling back to the Python type of the value.
"""

from __future__ import annotations

import datetime as dt
import io
import sys
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

openpyxl = pytest.importorskip("openpyxl")

HYPERLINK = '=HYPERLINK("http://evil.example/x","Click")'


def _naive(text: str) -> dt.datetime:
    """Excel date cells are zone-less by design: compare with a naive datetime."""
    return dt.datetime.fromisoformat(text)


def _cells(content: bytes) -> list[list[tuple[object, str]]]:
    wb = openpyxl.load_workbook(io.BytesIO(content))
    ws = wb.active
    return [[(c.value, c.data_type) for c in row] for row in ws.iter_rows()]


def _export(records, columns, *, mappings=None, column_types=None) -> bytes:
    from src.transfer.adapters import write_destination_file
    from src.transfer.models import EndpointConfig

    content, filename, _summary = write_destination_file(
        EndpointConfig(kind="file_export", format="excel"),
        records,
        columns,
        source_format="csv",
        mappings=mappings,
        column_types=column_types,
    )
    assert filename.endswith(".xlsx")
    return content


def test_formula_text_is_a_string_cell_never_a_formula():
    from services.format_converter import convert_rows

    content, _ = convert_rows(
        ["note", "banner", "err"],
        [[HYPERLINK, "=== VIP ===", "#N/A"], ["=1+1", "@SUM(A1)", "+cmd"]],
        source_format="csv",
        target_format="excel",
    )
    rows = _cells(content)
    assert rows[1] == [(HYPERLINK, "s"), ("=== VIP ===", "s"), ("#N/A", "s")]
    assert rows[2] == [("=1+1", "s"), ("@SUM(A1)", "s"), ("+cmd", "s")]
    # Header cells come from source column names: also never formulas.
    assert all(dtype == "s" for _v, dtype in rows[0])


def test_formula_header_is_a_string_cell():
    from services.format_converter import convert_rows

    content, _ = convert_rows(["=cmd|'/c calc'!A1"], [["x"]], source_format="csv", target_format="excel")
    assert _cells(content)[0] == [("=cmd|'/c calc'!A1", "s")]


def test_typed_columns_from_mapping_target_types_round_trip():
    records = [
        {"id": "1", "name": HYPERLINK, "amount": "12.50", "delta": "-5", "ratio": "-1.5",
         "born": "2024-03-01", "seen": "2024-03-01T10:20:30", "active": "true"},
        {"id": "2", "name": "=== VIP ===", "amount": "0.10", "delta": "7", "ratio": "2.25",
         "born": "1999-12-31", "seen": "2001-02-03 04:05:06", "active": "false"},
    ]
    cols = list(records[0])
    types = {
        "id": "integer", "name": "text", "amount": "numeric(12,2)", "delta": "bigint",
        "ratio": "double precision", "born": "date", "seen": "timestamp", "active": "boolean",
    }
    mappings = [{"source": c, "target": c, "target_type": types[c]} for c in cols]
    rows = _cells(_export(records, cols, mappings=mappings, column_types=types))

    first = dict(zip(cols, rows[1]))
    assert first["id"] == (1, "n")
    assert first["name"] == (HYPERLINK, "s")
    assert first["amount"][1] == "n" and Decimal(str(first["amount"][0])) == Decimal("12.5")
    assert first["delta"] == (-5, "n")
    assert first["ratio"] == (-1.5, "n")
    assert first["born"][1] == "d" and first["born"][0] == _naive("2024-03-01T00:00:00")
    assert first["seen"] == (_naive("2024-03-01T10:20:30"), "d")
    assert first["active"] == (True, "b")

    second = dict(zip(cols, rows[2]))
    assert second["name"] == ("=== VIP ===", "s")
    assert second["seen"] == (_naive("2001-02-03T04:05:06"), "d")
    assert second["active"] == (False, "b")


def test_unmapped_export_types_cells_from_column_types():
    records = [{"code": "007", "qty": "-3", "label": "-3"}]
    rows = _cells(
        _export(records, ["code", "qty", "label"], column_types={"code": "string", "qty": "integer", "label": "string"})
    )
    # Text columns keep text exactly — leading zeros and a minus-leading label alike.
    assert rows[1] == [("007", "s"), (-3, "n"), ("-3", "s")]


def test_numeric_text_that_excel_cannot_hold_exactly_stays_text():
    """Excel holds 15 significant digits: a wider number is kept as exact text, not rounded."""
    records = [{"big": "12345678901234567890", "dec": "1234567890.123456789", "lead": "0012"}]
    types = {"big": "bigint", "dec": "decimal(38,9)", "lead": "integer"}
    rows = _cells(_export(records, list(types), column_types=types))
    assert rows[1] == [
        ("12345678901234567890", "s"),
        ("1234567890.123456789", "s"),
        ("0012", "s"),
    ]


def test_timezone_aware_timestamp_stays_exact_text():
    """Excel has no time zones: an offset timestamp keeps the export's exact text.

    The delimited export renders it in UTC with its offset; the XLSX cell carries
    that same text instead of a naive date that silently drops the zone.
    """
    records = [{"ts": "2024-03-01T10:20:30+02:00"}]
    rows = _cells(_export(records, ["ts"], column_types={"ts": "timestamptz"}))
    assert rows[1] == [("2024-03-01 08:20:30+00:00", "s")]


def test_python_type_fallback_without_column_types():
    from services.format_converter import convert_rows

    content, _ = convert_rows(
        ["i", "f", "d", "b", "day", "s"],
        [[7, -1.5, Decimal("12.50"), True, dt.date(2024, 3, 1), "=2+2"]],
        source_format="csv",
        target_format="excel",
    )
    row = _cells(content)[1]
    assert row[0] == (7, "n")
    assert row[1] == (-1.5, "n")
    assert row[2][1] == "n"
    assert row[3] == (True, "b")
    assert row[4][1] == "d"
    assert row[5] == ("=2+2", "s")


def test_gate8_cell_readback_passes_on_typed_xlsx(tmp_path):
    """Gate-8 re-reads the written workbook: typed cells still match what was sent."""
    from src.transfer.reconcile_step import _file_export_cell_readback

    records = [
        {"id": "1", "name": HYPERLINK, "amount": "12.50", "delta": "-5", "born": "2024-03-01", "active": "true"},
        {"id": "2", "name": "=== VIP ===", "amount": "0.10", "delta": "7", "born": "1999-12-31", "active": "false"},
    ]
    cols = list(records[0])
    types = {"id": "integer", "name": "text", "amount": "numeric(12,2)", "delta": "bigint",
             "born": "date", "active": "boolean"}
    mappings = [{"source": c, "target": c, "target_type": types[c]} for c in cols]
    path = tmp_path / "export.xlsx"
    path.write_bytes(_export(records, cols, mappings=mappings, column_types=types))
    compare = _file_export_cell_readback(
        records, cols, mappings, {"path": str(path), "primary_key_columns": ["id"]}, fmt="excel"
    )
    assert compare is not None
    assert compare.get("passed") is True, compare
    assert compare.get("rows_compared") == 2, compare
