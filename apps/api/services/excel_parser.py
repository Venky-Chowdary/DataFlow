"""Excel parser with streaming row count.

Bytes decide the workbook. ZIP magic is OOXML (``.xlsx``), including a
file renamed to ``.xls``. OLE compound-document magic is BIFF ``.xls``.
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime
from io import BytesIO
from typing import Any, Iterator

from services.read_options import ReadOptions, ReadOptionsError
from services.tabular_rows import is_blank_row
from services.tabular_window import header_and_rows, synthetic_headers
from services.value_serializer import cell_to_string

XLS_UNSUPPORTED_MSG = (
    "This file is not a readable Excel workbook. "
    "Save an unprotected .xlsx, or retry the original .xls."
)

__all__ = [
    "XLS_UNSUPPORTED_MSG",
    "cell_to_string",
    "count_excel_rows",
    "is_blank_row",
    "iter_excel_batches",
    "iter_excel_dicts",
    "list_excel_sheets",
    "parse_excel_preview",
    "require_xlsx",
    "sheet_headers",
    "synthetic_headers",
]


def explain_unreadable_file(exc: BaseException) -> str:
    """Operator text for a library exception that is not the file's problem.

    A password-protected workbook surfaces as ``File is not a zip file``.
    A truncated object surfaces as a bad magic number. Neither sentence
    tells the operator what to do. A real BIFF ``.xls`` never reaches this
    helper: the OLE magic is loaded before openpyxl sees the bytes.
    """
    text = str(exc or "")
    low = text.lower()
    kind = type(exc).__name__.lower()
    # Already translated. A second pass would see the word "password" inside
    # the zip-file sentence and replace a legacy-.xls explanation with the
    # password-only one.
    if text.startswith(
        (
            "This workbook is password-protected.",
            "This file is not a readable",
            "This file does not start",
        )
    ):
        return text
    if "password" in low or "encrypted" in low:
        return (
            "This workbook is password-protected. Remove the password, "
            "save it as .xlsx, and retry."
        )
    if "not a zip file" in low or "badzipfile" in kind:
        return (
            "This file is not a readable .xlsx workbook. A password-protected "
            "workbook fails this way. Save an unprotected .xlsx and retry."
        )
    if "bad magic number" in low or "bad magic" in low:
        return (
            "This file does not start with the format it was declared as. "
            "A password-protected workbook, a truncated download, or the wrong "
            "format (xls versus xlsx, csv versus parquet) all look like this. "
            "Check the file and retry."
        )
    return text


# Compound File Binary, the container BIFF .xls workbooks use.
_OLE_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"


def require_xlsx(path_or_name: str | os.PathLike[str] | bytes | None) -> None:
    """Names do not decide the workbook format.

    A path ending in ``.xls`` used to be refused here, before any byte was
    read. Callers still invoke this so a future name check has one owner.
    The bytes decide: OLE compound documents are BIFF ``.xls``; ZIP magic
    is ``.xlsx``, including an ``.xlsx`` someone renamed to ``.xls``.
    """
    return None


def sheet_headers(first_row: tuple) -> list[str]:
    """Header names for a sheet's first row, naming unlabelled cells col_N."""
    headers: list[str] = []
    for i, c in enumerate(first_row):
        h = cell_to_string(c).strip() if c is not None else ""
        headers.append(h if h else f"col_{i}")
    return headers


def _select_sheet(wb: Any, options: ReadOptions) -> Any:
    """The worksheet the options name, or a refusal that lists the real names.

    Silently falling back to the active sheet would transfer the wrong data
    under the right job name, which is worse than not transferring at all.
    """
    names = list(getattr(wb, "sheetnames", []) or [])
    if options.sheet:
        if options.sheet in names:
            return wb[options.sheet]
        folded = {str(n).strip().casefold(): n for n in names}
        match = folded.get(options.sheet.strip().casefold())
        if match is not None:
            return wb[match]
        available = ", ".join(f"'{n}'" for n in names) or "none"
        raise ReadOptionsError(
            f"Workbook has no sheet named '{options.sheet}'. Available: {available}"
        )
    if options.sheet_index >= 0:
        if options.sheet_index >= len(names):
            raise ReadOptionsError(
                f"Workbook has {len(names)} sheet(s); sheet_index "
                f"{options.sheet_index} is out of range. Available: "
                + (", ".join(f"[{i}] '{n}'" for i, n in enumerate(names)) or "none")
            )
        return wb[names[options.sheet_index]]
    return wb.active


def _format_without_literals(fmt: str) -> str:
    return re.sub(r'"[^"]*"', "", fmt or "")


def _zero_pad_width(fmt: str) -> int:
    bare = _format_without_literals(fmt).strip()
    match = re.fullmatch(r"0{2,}", bare)
    return len(match.group(0)) if match else 0


def _format_has_time(fmt: str) -> bool:
    bare = _format_without_literals(fmt)
    return bool(re.search(r"[hs]|am/pm|a/p", bare, re.I))


def excel_cell_value(cell: Any) -> Any:
    """Value a file load should see from one Excel cell.

    A date-only cell is a datetime at midnight. Writing that as TIMESTAMP
    then blocks a DATE column. A zero-padded number format (``00000``) is a
    code; the int alone drops the zeros. Read-only workbooks often omit the
    format, and the name heuristic covers those.
    """
    if cell is None:
        return None
    value = getattr(cell, "value", cell)
    fmt = str(getattr(cell, "number_format", "") or "")
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        width = _zero_pad_width(fmt)
        if width:
            return str(value).zfill(width)
        return value
    if isinstance(value, datetime) and value.tzinfo is None:
        if (
            value.hour == value.minute == value.second == value.microsecond == 0
            and not _format_has_time(fmt)
        ):
            return value.date()
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    return value


def _excel_value_rows(ws: Any) -> Iterator[tuple]:
    for row in ws.iter_rows(values_only=False):
        yield tuple(excel_cell_value(cell) for cell in row)


def _sheet_header_and_rows(
    ws: Any, options: ReadOptions
) -> tuple[list[str], Iterator[Any]]:
    """Header names plus the sheet's data rows inside the declared window."""
    return header_and_rows(
        _excel_value_rows(ws),
        options,
        header_names=sheet_headers,
        source_label=f"Sheet '{getattr(ws, 'title', '')}'",
    )


def list_excel_sheets(content: bytes | Any) -> list[dict[str, Any]]:
    """Sheet inventory for the source picker: name, position, and a first-row peek.

    ``max_row``/``max_column`` are the used range and formatting inflates them,
    so they are reported as ``used_rows``/``used_columns`` and never as a row
    count. The row count is a COUNT, and a COUNT scans.
    """
    wb = _load_workbook(content)
    try:
        active = getattr(getattr(wb, "active", None), "title", "")
        sheets: list[dict[str, Any]] = []
        for index, name in enumerate(list(getattr(wb, "sheetnames", []) or [])):
            ws = wb[name]
            first = next(_excel_value_rows(ws), None) or ()
            sheets.append(
                {
                    "name": name,
                    "index": index,
                    "is_active": name == active,
                    "used_rows": int(getattr(ws, "max_row", 0) or 0),
                    "used_columns": int(getattr(ws, "max_column", 0) or 0),
                    "first_row": [cell_to_string(c) for c in first],
                }
            )
        return sheets
    finally:
        wb.close()


class _XlsCell:
    """openpyxl-shaped cell so ``excel_cell_value`` has one owner."""

    __slots__ = ("value", "number_format")

    def __init__(self, value: Any, number_format: str = "") -> None:
        self.value = value
        self.number_format = number_format


class _XlsSheet:
    def __init__(self, sheet: Any, book: Any) -> None:
        self._sheet = sheet
        self._book = book
        self.title = str(getattr(sheet, "name", "") or "")
        self.max_row = int(getattr(sheet, "nrows", 0) or 0)
        self.max_column = int(getattr(sheet, "ncols", 0) or 0)

    def iter_rows(self, values_only: bool = False) -> Iterator[tuple]:
        import xlrd

        datemode = int(getattr(self._book, "datemode", 0) or 0)
        xf_list = list(getattr(self._book, "xf_list", []) or [])
        format_map = getattr(self._book, "format_map", {}) or {}
        for row_index in range(self._sheet.nrows):
            cells: list[_XlsCell] = []
            for col_index in range(self._sheet.ncols):
                cell = self._sheet.cell(row_index, col_index)
                value: Any = cell.value
                number_format = ""
                xf_index = int(getattr(cell, "xf_index", -1) or -1)
                if 0 <= xf_index < len(xf_list):
                    fmt_key = getattr(xf_list[xf_index], "format_key", None)
                    fmt = format_map.get(fmt_key)
                    number_format = str(getattr(fmt, "format_str", "") or "")
                ctype = int(getattr(cell, "ctype", 0) or 0)
                if ctype == xlrd.XL_CELL_EMPTY or ctype == xlrd.XL_CELL_BLANK:
                    value = None
                elif ctype == xlrd.XL_CELL_BOOLEAN:
                    value = bool(value)
                elif ctype == xlrd.XL_CELL_DATE:
                    value = xlrd.xldate_as_datetime(value, datemode)
                elif ctype == xlrd.XL_CELL_NUMBER and isinstance(value, float):
                    if value.is_integer():
                        value = int(value)
                elif ctype == xlrd.XL_CELL_ERROR:
                    value = None
                cells.append(_XlsCell(value, number_format))
            yield tuple(cells)


class _XlsBook:
    def __init__(self, book: Any) -> None:
        self._book = book
        self.sheetnames = list(book.sheet_names())
        self.active = (
            _XlsSheet(book.sheet_by_index(0), book) if book.nsheets else None
        )

    def __getitem__(self, name: str) -> _XlsSheet:
        return _XlsSheet(self._book.sheet_by_name(name), self._book)

    def close(self) -> None:
        release = getattr(self._book, "release_resources", None)
        if callable(release):
            release()


def _load_xls_workbook(payload: bytes) -> _XlsBook:
    try:
        import xlrd
    except ImportError as exc:
        raise ValueError(
            "Legacy .xls import is not ready on this platform node. "
            "Datawrap bundles file parsers — retry shortly."
        ) from exc
    try:
        book = xlrd.open_workbook(file_contents=payload)
    except Exception as exc:
        raise ValueError(
            "This file is not a readable .xls workbook. "
            "A truncated download or a renamed .xlsx both look like this. "
            "Save an unprotected .xlsx, or retry the original .xls."
        ) from exc
    return _XlsBook(book)


def _peek_magic(content: bytes | Any) -> tuple[bytes, bytes | Any]:
    """First bytes plus a stream the matching loader can consume.

    OLE workbooks come back as one ``bytes`` payload. Everything else is
    left positioned at the start for openpyxl.
    """
    if isinstance(content, (bytes, bytearray)):
        payload = bytes(content)
        return payload[:8], payload
    if isinstance(content, (str, os.PathLike)):
        handle = open(os.fspath(content), "rb")
        try:
            magic = handle.read(8)
            if magic.startswith(_OLE_MAGIC):
                rest = handle.read()
                handle.close()
                return magic, magic + rest
            handle.seek(0)
        except Exception:
            handle.close()
            raise
        return magic, handle
    try:
        content.seek(0)
        magic = content.read(8)
        if magic.startswith(_OLE_MAGIC):
            rest = content.read()
            return magic, magic + rest
        content.seek(0)
    except Exception as exc:
        raise ValueError("Excel workbook source is not seekable") from exc
    return magic, content


def _load_workbook(content: bytes | Any):
    """Workbook from bytes, a path, or a seekable binary handle.

    Dest gzip Excel spools a decompressed image (workbook formats are not
    sequential). ``load_workbook`` already accepts a file-like; wrapping
    that image in a second ``BytesIO`` would be a third copy.

    Object-store / SFTP spill names the cache ``.tmp``. openpyxl keys the
    format off that suffix and refuses a real OOXML workbook. A path is
    opened as a handle so the magic decides, not the cache name. OLE
    compound documents load through xlrd; ZIP magic stays on openpyxl,
    including an ``.xlsx`` someone renamed to ``.xls``.
    """
    magic, stream = _peek_magic(content)
    if magic.startswith(_OLE_MAGIC):
        payload = bytes(stream) if isinstance(stream, (bytes, bytearray)) else bytes(stream.read())
        return _load_xls_workbook(payload)

    closer = None
    if isinstance(stream, (bytes, bytearray)):
        file_stream: Any = BytesIO(bytes(stream))
    else:
        file_stream = stream
        if isinstance(content, (str, os.PathLike)):
            closer = stream.close
        try:
            file_stream.seek(0)
        except Exception as exc:
            if closer is not None:
                closer()
            raise ValueError("Excel workbook source is not seekable") from exc
    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        if closer is not None:
            closer()
        raise ValueError(
            "Excel import is not ready on this platform node. Datawrap bundles file parsers — retry shortly."
        ) from exc
    try:
        workbook = load_workbook(file_stream, read_only=True, data_only=True)
    except Exception as exc:
        if closer is not None:
            closer()
        raise ValueError(explain_unreadable_file(exc)) from exc
    if closer is not None:
        original_close = workbook.close

        def _close_with_handle() -> None:
            try:
                original_close()
            finally:
                closer()

        workbook.close = _close_with_handle  # type: ignore[method-assign]
    return workbook


def parse_excel_preview(
    content: bytes,
    preview_rows: int = 100,
    options: ReadOptions | None = None,
) -> tuple[list[str], list[list[str]], int]:
    opts = options or ReadOptions()
    wb = _load_workbook(content)
    try:
        ws = _select_sheet(wb, opts)
        if ws is None:
            return [], [], 0

        headers, rows = _sheet_header_and_rows(ws, opts)
        if not headers:
            return [], [], 0

        preview: list[list[str]] = []
        total = 0
        for row in rows:
            total += 1
            if len(preview) < preview_rows:
                preview.append([cell_to_string(c) for c in row])
        return headers, preview, total
    finally:
        wb.close()


def iter_excel_dicts(content: bytes | Any, options: ReadOptions | None = None) -> Iterator[dict]:
    """Value-bearing Excel records. Same population as ``count_excel_rows``.

    Header is not a record. ``is_blank_row`` (formatting-only used-range)
    is not a record. Extra cells beyond the header refuse silent column
    drop — ingest already raised; Gate-8 must not hash a truncated row.
    ``max_row`` is not dest population. Gate-8 cell checksum and dest
    sample walk this iterator. ``options`` narrows the window (sheet, header
    row, head/tail skips) and every caller must pass the same one, or the
    population Validate profiled is not the population the writer sends.
    """
    opts = options or ReadOptions()
    wb = _load_workbook(content)
    ws = _select_sheet(wb, opts)
    if ws is None:
        wb.close()
        return
    try:
        headers, rows = _sheet_header_and_rows(ws, opts)
        if not headers:
            return
        for row in rows:
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


def iter_excel_batches(
    content: bytes, chunk_size: int, options: ReadOptions | None = None
) -> Iterator[list[dict]]:
    """Stream Excel rows as dict batches without loading the full sheet into RAM."""
    batch: list[dict] = []
    for record in iter_excel_dicts(content, options):
        batch.append(record)
        if len(batch) >= chunk_size:
            yield batch
            batch = []
    if batch:
        yield batch


def count_excel_rows(content: bytes | Any, options: ReadOptions | None = None) -> int:
    """Count rows that carry values.

    ``max_row`` is the used range, which formatting inflates; using it as the
    source cardinality makes reconciliation compare against rows that were
    never read. Extra cells beyond the header raise — dest COUNT then
    stays unmeasured rather than hashing a truncated row.
    """
    return sum(1 for _ in iter_excel_dicts(content, options))
