"""BIFF .xls is a workbook, not a name refusal.

GAP-XLS. Unit proof on a real OLE compound document. Not a live file retest.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from services.excel_parser import (
    _OLE_MAGIC,
    count_excel_rows,
    iter_excel_dicts,
    parse_excel_preview,
)

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "legacy_events.xls"


def test_biff_fixture_is_an_ole_workbook():
    payload = _FIXTURE.read_bytes()
    assert payload.startswith(_OLE_MAGIC)


def test_biff_rows_keep_integers_text_and_booleans():
    headers, preview, total = parse_excel_preview(_FIXTURE.read_bytes())
    assert headers == ["id", "name", "active"]
    assert total == 2
    assert preview == [
        ["1", "alpha", "true"],
        ["2", "beta", "false"],
    ]
    rows = list(iter_excel_dicts(_FIXTURE))
    assert rows == [
        {"id": "1", "name": "alpha", "active": "true"},
        {"id": "2", "name": "beta", "active": "false"},
    ]
    assert count_excel_rows(_FIXTURE) == 2


def test_corrupt_ole_names_the_workbook_not_a_zip():
    with pytest.raises(ValueError, match="not a readable .xls workbook"):
        parse_excel_preview(_OLE_MAGIC + b"not-a-workbook")
