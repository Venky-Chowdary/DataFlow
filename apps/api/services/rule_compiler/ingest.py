"""Read Excel / CSV / TSV / JSON into row dicts with sheet + line provenance."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from typing import Any

from .normalize import canonical_header, split_qualified

MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_ROWS = 5_000
HEADER_SCAN = 12
SUPPORTED = frozenset({".xlsx", ".xlsm", ".csv", ".tsv", ".txt", ".json", ".ndjson"})


class RuleIngestError(ValueError):
    """The file is not a rule workbook we can read."""


@dataclass
class IngestResult:
    rows: list[dict[str, Any]] = field(default_factory=list)
    truncated: int = 0


def _ext(filename: str) -> str:
    name = (filename or "").strip().lower()
    for suffix in (".xlsx", ".xlsm", ".xls", ".csv", ".tsv", ".txt", ".json", ".ndjson"):
        if name.endswith(suffix):
            return suffix
    return ""


def ingest_rule_file(filename: str, payload: bytes) -> list[dict[str, Any]]:
    """Every data row from the workbook, with ``_sheet`` and ``_row`` set."""
    return ingest_rule_workbook(filename, payload).rows


def ingest_rule_workbook(filename: str, payload: bytes) -> IngestResult:
    if not payload:
        raise RuleIngestError("The file is empty.")
    if len(payload) > MAX_FILE_BYTES:
        raise RuleIngestError(
            f"Rule files are limited to {MAX_FILE_BYTES // (1024 * 1024)} MB."
        )
    ext = _ext(filename)
    if ext == ".xls":
        raise RuleIngestError(
            "Legacy .xls is not read. Save as .xlsx, CSV or JSON."
        )
    if ext not in SUPPORTED:
        raise RuleIngestError(
            "Upload an Excel workbook (.xlsx), a CSV/TSV, or a JSON array of rules."
        )
    if ext in {".xlsx", ".xlsm"}:
        rows = _from_xlsx(payload)
    elif ext in {".csv", ".tsv", ".txt"}:
        rows = _from_delimited(payload, ext)
    else:
        rows = _from_json(payload)
    if not rows:
        raise RuleIngestError(
            "No rule rows were found. The first data row must be a mapping "
            "spec — headers can be any names; cells are inferred against "
            "the selected source and destination."
        )
    truncated = max(0, len(rows) - MAX_ROWS)
    return IngestResult(rows=rows[:MAX_ROWS], truncated=truncated)


def _cell_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def _qualify_source(mapped: dict[str, Any]) -> None:
    """``Customer.fname`` in the source-column cell also names the table."""
    spoken = str(mapped.get("source_column") or "")
    table, column = split_qualified(spoken)
    if table and column:
        mapped["source_column"] = column
        if not mapped.get("source_table"):
            mapped["source_table"] = table


def _project(headers: list[str], values: list[Any], sheet: str, row_number: int) -> dict[str, Any] | None:
    mapped: dict[str, Any] = {}
    cells: dict[str, str] = {}
    for header, value in zip(headers, values):
        name = str(header or "").strip()
        text = _cell_text(value)
        if name:
            cells[name] = text
        key = canonical_header(name)
        if not key:
            continue
        if key == "rule":
            mapped[key] = (mapped.get(key) or "") or text
        elif text and not mapped.get(key):
            mapped[key] = text
    first = _cell_text(values[0]) if values else ""
    if first.startswith("#"):
        return None
    filled = sum(1 for text in cells.values() if text)
    if not any(mapped.get(k) for k in ("source_column", "dest_column", "rule", "lookup_from", "lookup_to")):
        if filled < 1:
            return None
    _qualify_source(mapped)
    mapped["_cells"] = cells
    mapped["_headers"] = [str(h).strip() for h in headers if str(h or "").strip()]
    mapped["_sheet"] = sheet
    mapped["_row"] = row_number
    return mapped


def _header_score(headers: list[str]) -> int:
    """How likely this row is a mapping-spec header, not a title or a data row.

    A single known alias used to win the scan and then lose to the next data
    row (``Target Name`` + ``Orig Field`` scored 1, ``fname,first_name,Direct``
    scored 3). Mixed custom headers must keep their filled-cell weight.
    """
    aliases = sum(1 for header in headers if canonical_header(str(header or "")))
    filled = sum(1 for header in headers if str(header or "").strip())
    if aliases >= 2:
        return aliases
    if aliases == 1 and filled >= 2:
        return filled
    if aliases:
        return aliases
    return filled


def _unique_headers(headers: list[str]) -> list[str]:
    """Duplicate titles become col, col_2 so later cells are not overwritten."""
    seen: dict[str, int] = {}
    out: list[str] = []
    for header in headers:
        name = str(header or "").strip()
        if not name:
            out.append("")
            continue
        count = seen.get(name, 0) + 1
        seen[name] = count
        out.append(name if count == 1 else f"{name}_{count}")
    return out


def _take_header(rows: list[list[Any]]) -> tuple[list[str], list[tuple[int, list[Any]]]]:
    """The first row with two compiler headers; earlier rows are titles."""
    for index, raw in enumerate(rows[:HEADER_SCAN]):
        headers = _unique_headers(["" if cell is None else str(cell) for cell in raw])
        if _header_score(headers) >= 2:
            data_start = index + 2
            numbered = [(data_start + offset, row) for offset, row in enumerate(rows[index + 1:])]
            return headers, numbered
    if rows:
        headers = _unique_headers(["" if cell is None else str(cell) for cell in rows[0]])
        numbered = [(2 + offset, row) for offset, row in enumerate(rows[1:])]
        return headers, numbered
    return [], []


def _decode_text(payload: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode("utf-8", errors="replace")


def _dialect_for(text: str, ext: str) -> csv.Dialect:
    if ext == ".tsv":
        return csv.excel_tab
    sample = text[:4096]
    tabs = sample.count("\t")
    semis = sample.count(";")
    commas = sample.count(",")
    if tabs > commas and tabs > semis:
        return csv.excel_tab
    if semis > commas and semis > tabs:
        class _Semicolon(csv.excel):
            delimiter = ";"
        return _Semicolon()
    try:
        sniffed = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        if sniffed.delimiter in {",", ";", "\t", "|"}:
            return sniffed
    except csv.Error:
        pass
    return csv.excel


def _from_delimited(payload: bytes, ext: str) -> list[dict[str, Any]]:
    text = _decode_text(payload)
    if not text.strip():
        return []
    dialect = _dialect_for(text, ext)
    reader = csv.reader(io.StringIO(text), dialect)
    raw_rows = [list(row) for row in reader]
    headers, numbered = _take_header(raw_rows)
    out: list[dict[str, Any]] = []
    for row_number, raw in numbered:
        row = _project(headers, raw, "csv" if ext != ".tsv" else "tsv", row_number)
        if row:
            out.append(row)
    return out


def _from_json(payload: bytes) -> list[dict[str, Any]]:
    text = _decode_text(payload).strip()
    if not text:
        return []
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = _ndjson(text)
    if isinstance(data, dict):
        data = (
            data.get("rules")
            or data.get("mappings")
            or data.get("rows")
            or data.get("columns")
            or data.get("items")
            or []
        )
    if not isinstance(data, list):
        raise RuleIngestError("JSON must be an array of rule objects, or {\"rules\": [...]}.")
    out: list[dict[str, Any]] = []
    for index, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            continue
        headers = list(item.keys())
        values = [item.get(k) for k in headers]
        row = _project(headers, values, "json", index)
        if row:
            out.append(row)
    return out


def _ndjson(text: str) -> list[Any]:
    rows: list[Any] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise RuleIngestError(f"JSON is not valid: {exc}") from exc
    return rows


def _from_xlsx(payload: bytes) -> list[dict[str, Any]]:
    try:
        from openpyxl import load_workbook
    except ImportError as exc:  # pragma: no cover
        raise RuleIngestError("openpyxl is required to read Excel workbooks.") from exc
    # data_only=False keeps ``=UPPER()`` rule formulas as text.
    wb = load_workbook(io.BytesIO(payload), read_only=True, data_only=False)
    out: list[dict[str, Any]] = []
    try:
        for sheet in wb.worksheets:
            raw_rows = [list(row) for row in sheet.iter_rows(values_only=True)]
            headers, numbered = _take_header(raw_rows)
            if _header_score(headers) < 2:
                nonempty = sum(
                    1 for row in raw_rows
                    if any(_cell_text(cell) for cell in row)
                )
                # Chart/empty tabs stay out. Commentary still reaches compile
                # so it can be classified as notes instead of vanishing.
                if nonempty < 2:
                    continue
            for row_number, raw in numbered:
                row = _project(headers, raw, sheet.title, row_number)
                if row:
                    out.append(row)
    finally:
        wb.close()
    return out
