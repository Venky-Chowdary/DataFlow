"""File format conversion — CSV, JSON, JSONL, TSV, Excel, Parquet, Avro, ORC, XML."""

from __future__ import annotations

import csv
import io
import json
import xml.sax.saxutils as saxutils
from typing import Any

_ALL_FORMATS = {"csv", "tsv", "json", "jsonl", "excel", "parquet", "avro", "orc", "xml"}
SUPPORTED_CONVERSIONS: dict[str, set[str]] = {fmt: _ALL_FORMATS - {fmt} for fmt in _ALL_FORMATS}


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


def _write_avro_bytes(headers: list[str], rows: list[list[str]]) -> tuple[bytes, str]:
    import fastavro
    import io

    fields = [{"name": h, "type": ["null", "string"]} for h in headers]
    schema = {
        "type": "record",
        "name": "DataFlowRow",
        "fields": fields,
    }
    objects = _rows_to_objects(headers, rows)
    parsed = fastavro.parse_schema(schema)
    buf = io.BytesIO()
    fastavro.writer(buf, parsed, objects)
    buf.seek(0)
    return buf.getvalue(), "application/avro"


def _write_orc_bytes(headers: list[str], rows: list[list[str]]) -> tuple[bytes, str]:
    import pyarrow as pa
    import pyarrow.orc as orc

    objects = _rows_to_objects(headers, rows)
    if not objects:
        table = pa.table({h: [] for h in headers}, schema=pa.schema([(h, pa.string()) for h in headers]))
    else:
        arrays = {h: [str(obj.get(h, "")) for obj in objects] for h in headers}
        table = pa.table(arrays, schema=pa.schema([(h, pa.string()) for h in headers]))
    buf = io.BytesIO()
    orc.write_table(table, buf)
    buf.seek(0)
    return buf.getvalue(), "application/vnd.apache.orc"


def _write_xml_bytes(headers: list[str], rows: list[list[str]]) -> tuple[bytes, str]:
    lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<records>"]
    for row in rows:
        lines.append("  <record>")
        for h, v in zip(headers, row):
            tag = saxutils.escape(str(h).replace(" ", "_"))
            value = saxutils.escape(str(v))
            lines.append(f"    <{tag}>{value}</{tag}>")
        lines.append("  </record>")
    lines.append("</records>")
    return "\n".join(lines).encode("utf-8"), "application/xml"


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

    if tgt == "avro":
        return _write_avro_bytes(headers, rows)

    if tgt == "orc":
        return _write_orc_bytes(headers, rows)

    if tgt == "xml":
        return _write_xml_bytes(headers, rows)

    raise ValueError(f"Unsupported target format: {tgt}")


def conversion_matrix() -> dict[str, Any]:
    return {
        "formats": sorted({*SUPPORTED_CONVERSIONS.keys()}),
        "matrix": {k: sorted(v) for k, v in SUPPORTED_CONVERSIONS.items()},
    }
