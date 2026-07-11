"""Excel (.xlsx) parser with streaming row count."""

from __future__ import annotations

from io import BytesIO
from typing import Iterator


def _load_workbook(content: bytes):
    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise ValueError(
            "Excel import is not ready on this platform node. DataFlow bundles file parsers — retry shortly."
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

    headers = [str(c).strip() if c is not None else f"col_{i}" for i, c in enumerate(first)]
    preview: list[list[str]] = []
    total = 0

    for row in row_iter:
        total += 1
        if len(preview) < preview_rows:
            preview.append([str(c).strip() if c is not None else "" for c in row])

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

    headers = [str(c).strip() if c is not None else f"col_{i}" for i, c in enumerate(first)]
    batch: list[dict] = []
    try:
        for row in row_iter:
            record = {
                headers[i]: ("" if c is None else str(c).strip())
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
    wb = _load_workbook(content)
    ws = wb.active
    if ws is None:
        wb.close()
        return 0
    total = max(0, (ws.max_row or 1) - 1)
    wb.close()
    return total
