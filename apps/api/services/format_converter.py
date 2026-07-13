"""File format conversion — CSV, JSON, JSONL, TSV, Excel, Parquet."""

from __future__ import annotations

import csv
import io
import json
from typing import Any

SUPPORTED_CONVERSIONS: dict[str, set[str]] = {
    "csv": {"json", "jsonl", "tsv", "excel", "parquet"},
    "tsv": {"csv", "json", "jsonl", "excel", "parquet"},
    "json": {"csv", "jsonl", "tsv", "excel", "parquet"},
    "jsonl": {"csv", "json", "tsv", "excel", "parquet"},
    "excel": {"csv", "json", "jsonl", "tsv", "parquet"},
    "parquet": {"csv", "json", "jsonl", "tsv", "excel"},
}


def can_convert(source_format: str, target_format: str) -> bool:
    src = (source_format or "").lower()
    tgt = (target_format or "").lower()
    # NDJSON is JSON Lines in every conversion path
    if src == "ndjson":
        src = "jsonl"
    if tgt == "ndjson":
        tgt = "jsonl"
    if src == tgt:
        return True
    return tgt in SUPPORTED_CONVERSIONS.get(src, set())


def _rows_to_objects(headers: list[str], rows: list[list[str]]) -> list[dict[str, str]]:
    return [dict(zip(headers, row)) for row in rows]


def _write_excel_bytes(headers: list[str], rows: list[list[str]]) -> tuple[bytes, str]:
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    if ws is None:
        ws = wb.create_sheet()
    ws.append(headers)
    for row in rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _write_parquet_bytes(headers: list[str], rows: list[list[str]]) -> tuple[bytes, str]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    objects = _rows_to_objects(headers, rows)
    if not objects:
        table = pa.table({h: [] for h in headers}, schema=pa.schema([(h, pa.string()) for h in headers]))
    else:
        # All values are strings at the conversion boundary; downstream writers
        # perform typed transforms if a database destination is selected.
        arrays = {h: [str(obj.get(h, "")) for obj in objects] for h in headers}
        table = pa.table(arrays, schema=pa.schema([(h, pa.string()) for h in headers]))
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    return buf.getvalue(), "application/vnd.apache.parquet"


def convert_rows(
    headers: list[str],
    rows: list[list[str]],
    *,
    source_format: str,
    target_format: str,
) -> tuple[bytes, str]:
    """Convert tabular data between supported file formats."""
    src = (source_format or "csv").lower()
    tgt = (target_format or "csv").lower()
    # NDJSON is JSON Lines in every conversion path
    if src == "ndjson":
        src = "jsonl"
    if tgt == "ndjson":
        tgt = "jsonl"
    if not can_convert(src, tgt):
        raise ValueError(f"Conversion {src} → {tgt} not supported")

    if tgt == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(headers)
        writer.writerows(rows)
        return buf.getvalue().encode("utf-8"), "text/csv"

    if tgt == "tsv":
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter="\t")
        writer.writerow(headers)
        writer.writerows(rows)
        return buf.getvalue().encode("utf-8"), "text/tab-separated-values"

    if tgt == "json":
        objects = _rows_to_objects(headers, rows)
        return json.dumps(objects, indent=2, default=str).encode("utf-8"), "application/json"

    if tgt == "jsonl":
        lines = []
        for row in rows:
            lines.append(json.dumps(dict(zip(headers, row)), ensure_ascii=False, default=str))
        return ("\n".join(lines) + "\n").encode("utf-8"), "application/x-ndjson"

    if tgt == "excel":
        return _write_excel_bytes(headers, rows)

    if tgt == "parquet":
        return _write_parquet_bytes(headers, rows)

    raise ValueError(f"Unsupported target format: {tgt}")


def conversion_matrix() -> dict[str, Any]:
    return {
        "formats": sorted({*SUPPORTED_CONVERSIONS.keys()}),
        "matrix": {k: sorted(v) for k, v in SUPPORTED_CONVERSIONS.items()},
    }
