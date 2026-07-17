"""Parse uploaded files and infer schema."""

from __future__ import annotations

import io
import json
import uuid
from pathlib import Path

from services.csv_profiler import count_csv_rows, parse_csv_preview
from services.platform_config import data_dir, upload_dir
from services.schema_inference import infer_columns_from_rows

UPLOAD_DIR = upload_dir()
REGISTRY_PATH = data_dir() / "upload_registry.json"

_file_registry: dict[str, dict] = {}


def _registry_record_for_disk(record: dict) -> dict:
    """Persist metadata only — preview rows stay in memory until restart."""
    out = dict(record)
    out.pop("preview_rows", None)
    return out


def _load_registry() -> None:
    global _file_registry
    if not REGISTRY_PATH.exists():
        return
    try:
        raw = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
        items = raw.get("files", []) if isinstance(raw, dict) else []
    except Exception:
        return
    for item in items:
        if not isinstance(item, dict) or not item.get("file_id"):
            continue
        path = Path(item.get("path", ""))
        if path.exists():
            _file_registry[item["file_id"]] = item


def _save_registry() -> None:
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "files": [_registry_record_for_disk(r) for r in _file_registry.values()],
        "count": len(_file_registry),
    }
    REGISTRY_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


_load_registry()


def detect_format(filename: str, content: bytes) -> str:
    lower = filename.lower()
    if lower.endswith(".csv"):
        return "csv"
    if lower.endswith(".tsv"):
        return "tsv"
    if lower.endswith((".xlsx", ".xls")):
        return "excel"
    if lower.endswith(".json"):
        return "json"
    if lower.endswith((".jsonl", ".ndjson")):
        return "jsonl"
    if lower.endswith(".parquet"):
        return "parquet"
    if lower.endswith((".txt", ".dat")):
        return "fixed_width"
    if content[:1] == b"{" or content[:1] == b"[":
        return "json"
    if b"\n{" in content[:2048]:
        return "jsonl"
    if b"," in content[:512]:
        return "csv"
    if b"\t" in content[:512]:
        return "tsv"
    return "unknown"


def parse_jsonl(content: bytes) -> tuple[list[str], list[list[str]], int]:
    lines = content.decode("utf-8", errors="replace").strip().splitlines()
    objects = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        objects.append(json.loads(line))
    if not objects:
        raise ValueError("JSONL must contain at least one JSON object per line")
    headers = list(objects[0].keys())
    rows = [
        [json.dumps(item[h]) if isinstance(item.get(h), (dict, list)) else str(item.get(h, "")) for h in headers]
        for item in objects
    ]
    return headers, rows, len(objects)


def parse_json(content: bytes) -> tuple[list[str], list[list[str]], int]:
    data = json.loads(content.decode("utf-8", errors="replace"))
    if isinstance(data, list) and data:
        if isinstance(data[0], dict):
            headers = list(data[0].keys())
            rows = [[str(item.get(h, "")) for h in headers] for item in data]
            return headers, rows[:100], len(data)
    raise ValueError("JSON must be an array of objects")


def _parse_parquet_preview(content: bytes, preview_rows: int = 100) -> tuple[list[str], list[list[str]], int]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ValueError("Parquet support requires pyarrow") from exc
    table = pq.read_table(io.BytesIO(content))
    row_count = table.num_rows
    slice_table = table.slice(0, min(preview_rows, row_count))
    headers = [str(name) for name in slice_table.column_names]
    rows: list[list[str]] = []
    for i in range(slice_table.num_rows):
        row = []
        for col in slice_table.column_names:
            val = slice_table.column(col)[i].as_py()
            row.append("" if val is None else str(val))
        rows.append(row)
    return headers, rows, row_count


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
    elif fmt == "tsv":
        headers, rows, encoding, delimiter = parse_csv_preview(content)
        row_count = count_csv_rows(content, encoding)
    elif fmt == "json":
        headers, rows, row_count = parse_json(content)
    elif fmt == "jsonl":
        headers, rows, row_count = parse_jsonl(content)
    elif fmt == "excel":
        from services.excel_parser import parse_excel_preview

        headers, rows, row_count = parse_excel_preview(content)
    elif fmt == "parquet":
        headers, rows, row_count = _parse_parquet_preview(content)
    else:
        headers, rows, encoding, delimiter = parse_csv_preview(content)
        row_count = count_csv_rows(content, encoding)
        fmt = "csv"

    columns = infer_columns_from_rows(headers, rows)
    preview_rows = rows[:5]
    path = UPLOAD_DIR / f"{file_id}_{filename}"
    path.write_bytes(content)

    validation_report: dict | None = None
    if fmt in ("csv", "tsv"):
        from services.csv_validator import validate_csv_content

        schema_map = {c["name"]: c.get("inferred_type", "VARCHAR") for c in columns}
        validation_report = validate_csv_content(content, headers, schema_map)

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
        "validation": validation_report,
    }
    _file_registry[file_id] = record
    _save_registry()
    return record


def get_file(file_id: str) -> dict | None:
    return _file_registry.get(file_id)


def get_file_chunks(file_id: str, chunk_size: int = 10000):
    """Generator to yield chunks of a file for streaming transfers."""
    record = get_file(file_id)
    if not record:
        raise FileNotFoundError(f"File {file_id} not found in registry")
    
    path = Path(record["path"])
    fmt = record["format"]
    encoding = record["encoding"]
    delimiter = record["delimiter"]
    
    if fmt == "csv":
        import csv
        with open(path, "r", encoding=encoding, errors="replace") as f:
            reader = csv.reader(f, delimiter=delimiter)
            headers = next(reader, [])
            chunk = []
            for row in reader:
                chunk.append(row)
                if len(chunk) >= chunk_size:
                    yield headers, chunk
                    chunk = []
            if chunk:
                yield headers, chunk
    elif fmt == "json":
        import json
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("JSON must be an array of objects")
            if not data:
                return
            headers = list(data[0].keys())
            for i in range(0, len(data), chunk_size):
                batch = data[i:i+chunk_size]
                rows = [[str(item.get(h, "")) for h in headers] for item in batch]
                yield headers, rows
    elif fmt == "jsonl":
        import json
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            chunk = []
            headers = None
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if headers is None:
                    headers = list(obj.keys())
                row = [json.dumps(obj[h]) if isinstance(obj.get(h), (dict, list)) else str(obj.get(h, "")) for h in headers]
                chunk.append(row)
                if len(chunk) >= chunk_size:
                    yield headers, chunk
                    chunk = []
            if chunk:
                yield headers, chunk
    else:
        # Fallback to full load then chunk
        from services.csv_profiler import parse_csv_full
        headers, data_rows, _, _ = parse_csv_full(path.read_bytes(), encoding)
        for i in range(0, len(data_rows), chunk_size):
            yield headers, data_rows[i:i+chunk_size]
