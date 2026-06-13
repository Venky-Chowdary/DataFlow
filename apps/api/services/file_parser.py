"""Parse uploaded files and infer schema."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from services.csv_profiler import count_csv_rows, parse_csv_preview
from services.schema_inference import infer_columns_from_rows

UPLOAD_DIR = Path(__file__).resolve().parents[1] / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

_file_registry: dict[str, dict] = {}


def detect_format(filename: str, content: bytes) -> str:
    lower = filename.lower()
    if lower.endswith(".csv"):
        return "csv"
    if lower.endswith((".xlsx", ".xls")):
        return "excel"
    if lower.endswith(".json"):
        return "json"
    if lower.endswith(".parquet"):
        return "parquet"
    if lower.endswith((".txt", ".dat")):
        return "fixed_width"
    if content[:1] == b"{" or content[:1] == b"[": 
        return "json"
    if b"," in content[:512]:
        return "csv"
    return "unknown"


def parse_json(content: bytes) -> tuple[list[str], list[list[str]], int]:
    data = json.loads(content.decode("utf-8", errors="replace"))
    if isinstance(data, list) and data:
        if isinstance(data[0], dict):
            headers = list(data[0].keys())
            rows = [[str(item.get(h, "")) for h in headers] for item in data]
            return headers, rows[:100], len(data)
    raise ValueError("JSON must be an array of objects")


def store_upload(filename: str, content: bytes) -> dict:
    fmt = detect_format(filename, content)
    file_id = uuid.uuid4().hex[:16]
    encoding = "utf-8"
    delimiter = ","
    row_count = 0

    headers: list[str] = []
    rows: list[list[str]] = []

    if fmt in {"csv", "unknown", "fixed_width"}:
        headers, rows, encoding, delimiter = parse_csv_preview(content)
        row_count = count_csv_rows(content, encoding)
        fmt = "csv" if fmt == "unknown" else fmt
    elif fmt == "json":
        headers, rows, row_count = parse_json(content)
    elif fmt == "excel":
        from services.excel_parser import parse_excel_preview

        headers, rows, row_count = parse_excel_preview(content)
    else:
        headers, rows, encoding, delimiter = parse_csv_preview(content)
        row_count = count_csv_rows(content, encoding)
        fmt = "csv"

    columns = infer_columns_from_rows(headers, rows)
    preview_rows = rows[:5]
    path = UPLOAD_DIR / f"{file_id}_{filename}"
    path.write_bytes(content)

    from services.object_store import stage_bytes

    object_uri = stage_bytes(f"uploads/{file_id}/{filename}", content)

    record = {
        "file_id": file_id,
        "filename": filename,
        "format": fmt,
        "encoding": encoding,
        "delimiter": delimiter,
        "row_count": row_count,
        "file_size_bytes": len(content),
        "columns": columns,
        "preview_rows": preview_rows,
        "path": str(path),
        "object_uri": object_uri,
    }
    _file_registry[file_id] = record
    return record


def get_file(file_id: str) -> dict | None:
    return _file_registry.get(file_id)
