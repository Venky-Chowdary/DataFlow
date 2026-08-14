"""Excel (.xlsx) parser with streaming row count."""

from __future__ import annotations

from io import BytesIO
from typing import Any, Iterator

from services.tabular_rows import is_blank_row
from services.value_serializer import cell_to_string

__all__ = [
    "count_excel_rows",
    "is_blank_row",
    "iter_excel_batches",
    "iter_excel_dicts",
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


def _load_workbook(content: bytes | Any):
    """openpyxl workbook from bytes or a seekable binary handle.

    Dest gzip Excel spools a decompressed image (workbook formats are not
    sequential). ``load_workbook`` already accepts a file-like; wrapping
    that image in a second ``BytesIO`` would be a third copy.
    """
    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise ValueError(
            "Excel import is not ready on this platform node. Datawrap bundles file parsers — retry shortly."
        ) from exc
    if isinstance(content, (bytes, bytearray)):
        stream: Any = BytesIO(content)
    else:
        stream = content
        try:
            stream.seek(0)
        except Exception as exc:
            raise ValueError("Excel workbook source is not seekable") from exc
    return load_workbook(stream, read_only=True, data_only=True)


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


def iter_excel_dicts(content: bytes | Any) -> Iterator[dict]:
    """Value-bearing Excel records. Same population as ``count_excel_rows``.

    Header is not a record. ``is_blank_row`` (formatting-only used-range)
    is not a record. Extra cells beyond the header refuse silent column
    drop — ingest already raised; Gate-8 must not hash a truncated row.
    ``max_row`` is not dest population. Gate-8 cell checksum and dest
    sample walk this iterator.
    """
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
            yield {
                headers[i]: cell_to_string(c)
                for i, c in enumerate(row[: len(headers)])
            }
    finally:
        wb.close()


def iter_excel_batches(content: bytes, chunk_size: int) -> Iterator[list[dict]]:
    """Stream Excel rows as dict batches without loading the full sheet into RAM."""
    batch: list[dict] = []
    for record in iter_excel_dicts(content):
        batch.append(record)
        if len(batch) >= chunk_size:
            yield batch
            batch = []
    if batch:
        yield batch


def count_excel_rows(content: bytes | Any) -> int:
    """Count rows that carry values.

    ``max_row`` is the used range, which formatting inflates; using it as the
    source cardinality makes reconciliation compare against rows that were
    never read. Extra cells beyond the header raise — dest COUNT then
    stays unmeasured rather than hashing a truncated row.
    """
    return sum(1 for _ in iter_excel_dicts(content))
