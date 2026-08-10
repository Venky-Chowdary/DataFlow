"""Excel (.xlsx) parser with streaming row count."""

from __future__ import annotations

from io import BytesIO
from typing import Iterator

from services.tabular_rows import is_blank_row
from services.value_serializer import cell_to_string

__all__ = [
    "count_excel_rows",
    "is_blank_row",
    "iter_excel_batches",
    "parse_excel_preview",
    "sheet_headers",
]


def sheet_headers(first_row: tuple) -> list[str]:
    """Header names for a sheet's first row, naming unlabelled cells col_N."""
    headers: list[str] = []
    for i, c in enumerate(first_row):
        h = cell_to_string(c).strip() if c is not None else ""
        headers.append(h if h else f"col_{i}")
    return headers


def _load_workbook(content: bytes):
    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise ValueError(
            "Excel import is not ready on this platform node. Datawrap bundles file parsers — retry shortly."
        ) from exc
    return load_workbook(BytesIO(content), read_only=True, data_only=True)


def parse_excel_preview(content: bytes, preview_rows: int = 100) -> tuple[list[str], list[list[str]], int]:
    wb = _load_workbook(content)
    ws = wb.active
    if ws is None:
        wb.close()
        return [], [], 0

    row_iter = ws.iter_rows(values_only=True)
    first = next(row_iter, None)
    if not first:
        wb.close()
        return [], [], 0

    headers = sheet_headers(first)
    preview: list[list[str]] = []
    total = 0

    for row in row_iter:
        if is_blank_row(row):
            continue
        total += 1
        if len(preview) < preview_rows:
            preview.append([cell_to_string(c) for c in row])

    wb.close()
    return headers, preview, total


def iter_excel_batches(content: bytes, chunk_size: int) -> Iterator[list[dict]]:
    """Stream Excel rows as dict batches without loading the full sheet into RAM."""
    wb = _load_workbook(content)
    ws = wb.active
    if ws is None:
        wb.close()
        return

    row_iter = ws.iter_rows(values_only=True)
    first = next(row_iter, None)
    if not first:
        wb.close()
        return

    headers = sheet_headers(first)
    batch: list[dict] = []
    try:
        for row in row_iter:
            if is_blank_row(row):
                continue
            if len(row) > len(headers):
                raise ValueError(
                    f"Excel row has {len(row)} cells but header has {len(headers)} "
                    "columns; refuse silent column drop — widen the header row "
                    "or fix the sheet"
                )
            record = {
                headers[i]: cell_to_string(c)
                for i, c in enumerate(row[: len(headers)])
            }
            batch.append(record)
            if len(batch) >= chunk_size:
                yield batch
                batch = []
        if batch:
            yield batch
    finally:
        wb.close()


def count_excel_rows(content: bytes) -> int:
    """Count rows that carry values.

    ``max_row`` is the used range, which formatting inflates; using it as the
    source cardinality makes reconciliation compare against rows that were
    never read.
    """
    wb = _load_workbook(content)
    ws = wb.active
    if ws is None:
        wb.close()
        return 0
    try:
        row_iter = ws.iter_rows(values_only=True)
        if next(row_iter, None) is None:
            return 0
        return sum(1 for row in row_iter if not is_blank_row(row))
    finally:
        wb.close()
