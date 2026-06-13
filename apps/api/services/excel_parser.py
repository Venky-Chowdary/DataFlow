"""Excel (.xlsx) parser with streaming row count."""

from __future__ import annotations

from io import BytesIO


def parse_excel_preview(content: bytes, preview_rows: int = 100) -> tuple[list[str], list[list[str]], int]:
    try:
        from openpyxl import load_workbook
    except ImportError:
        raise ValueError("Excel support requires openpyxl — pip install openpyxl") from None

    wb = load_workbook(BytesIO(content), read_only=True, data_only=True)
    ws = wb.active
    if ws is None:
        return [], [], 0

    row_iter = ws.iter_rows(values_only=True)
    first = next(row_iter, None)
    if not first:
        wb.close()
        return [], [], 0

    headers = [str(c).strip() if c is not None else f"col_{i}" for i, c in enumerate(first)]
    preview: list[list[str]] = []
    total = 0

    for row in row_iter:
        total += 1
        if len(preview) < preview_rows:
            preview.append([str(c).strip() if c is not None else "" for c in row])

    wb.close()
    return headers, preview, total
