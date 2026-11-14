"""G7 review H2: object-store XLSX (S3/GCS/ADLS/SFTP .xlsx/.xlsm keys) never writes formulas.

Before: ``ObjectStoreEncoder._ensure_xlsx/_append_xlsx`` used a write-only
workbook with ``ws.append`` of raw strings — headers and cells starting with
``=`` became formula cells (``'f'``) and ``#N/A`` an error cell (``'e'``).

Rule (same as the file export): text is an explicit string cell; numbers,
booleans and dates are typed by the destination column type.
"""

from __future__ import annotations

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


def _cells(data: bytes) -> list[list[tuple[object, str]]]:
    ws = openpyxl.load_workbook(io.BytesIO(data)).active
    return [[(c.value, c.data_type) for c in row] for row in ws.iter_rows()]


@pytest.mark.parametrize("key", ["out/report.xlsx", "out/report.xlsm"])
def test_encoder_writes_text_as_string_cells(key):
    from connectors.object_store_materialize import ObjectStoreEncoder

    enc = ObjectStoreEncoder(
        key=key,
        target_cols=['=HYPERLINK("http://h","hdr")', "note", "amount", "qty", "day", "flag"],
        dest_types={"note": "TEXT", "amount": "NUMERIC(12,2)", "qty": "BIGINT", "day": "DATE", "flag": "BOOLEAN"},
    )
    enc.append_rows([
        ("1", HYPERLINK, "12.50", "-5", "2024-03-01", "true"),
        ("2", "#N/A", "-1.25", "7", "1999-12-31", "false"),
        ("3", None, None, None, None, None),
    ])
    rows = _cells(enc.finish().read_all())
    assert rows[0][0] == ('=HYPERLINK("http://h","hdr")', "s")
    assert all(dtype == "s" for _v, dtype in rows[0])
    first = rows[1]
    assert first[1] == (HYPERLINK, "s")
    assert first[2][1] == "n" and Decimal(str(first[2][0])) == Decimal("12.5")
    assert first[3] == (-5, "n")
    assert first[4][1] == "d"
    assert first[5] == (True, "b")
    assert rows[2][1] == ("#N/A", "s")
    assert rows[2][2][1] == "n" and Decimal(str(rows[2][2][0])) == Decimal("-1.25")
    # SQL NULL is a blank cell — never the internal wire sentinel text.
    assert [v for v, _t in rows[3][1:]] == [None] * 5


def test_materializer_xlsx_round_trip():
    from connectors.object_store_materialize import materialize_object_store_export

    mappings = [
        {"source": "id", "target": "id", "target_type": "INTEGER"},
        {"source": "note", "target": "note", "target_type": "TEXT"},
    ]
    mat = materialize_object_store_export(
        key="exports/report.xlsx",
        headers=["id", "note"],
        data_rows=[["1", HYPERLINK], ["2", "=== VIP ==="], ["3", "@SUM(A1)"]],
        mappings=mappings,
        target_cols=["id", "note"],
        column_types={"id": "integer", "note": "text"},
        dest_types={"id": "INTEGER", "note": "TEXT"},
        error_policy="strict",
        dest_kind="s3",
        dialect_label="S3",
        spill_max_size=1024 * 1024,
    )
    assert mat.abort_error is None, mat.abort_error
    rows = _cells(mat.export.read_all())
    assert rows[1:] == [
        [(1, "n"), (HYPERLINK, "s")],
        [(2, "n"), ("=== VIP ===", "s")],
        [(3, "n"), ("@SUM(A1)", "s")],
    ]
