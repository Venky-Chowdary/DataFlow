"""Parse uploaded files and infer schema."""

from __future__ import annotations

import csv
import gzip
import io
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.csv_profiler import count_csv_rows, detect_delimiter, parse_csv_preview
from services.platform_config import data_dir, upload_dir
from services.schema_inference import infer_columns_from_rows
from services.value_serializer import cell_to_string, json_default

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
    # Prefer Mongo so API + Worker share upload metadata.
    try:
        from services.control_plane_store import mongo_collection

        coll = mongo_collection("upload_registry")
        if coll is not None:
            for item in coll.find().limit(2000):
                if not isinstance(item, dict):
                    continue
                fid = str(item.get("file_id") or item.get("_id") or "")
                if not fid:
                    continue
                row = dict(item)
                row.pop("_id", None)
                row["file_id"] = fid
                _file_registry[fid] = row
            return
    except Exception:
        pass
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
        # Keep registry even if path missing — Worker may materialize from object_uri.
        if path.exists() or item.get("object_uri"):
            _file_registry[item["file_id"]] = item


def _save_registry() -> None:
    try:
        from services.control_plane_store import mongo_collection

        coll = mongo_collection("upload_registry")
        if coll is not None:
            for r in _file_registry.values():
                fid = str(r.get("file_id") or "")
                if not fid:
                    continue
                doc = _registry_record_for_disk(r)
                doc["_id"] = fid
                coll.replace_one({"_id": fid}, doc, upsert=True)
            return
    except Exception:
        pass
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "files": [_registry_record_for_disk(r) for r in _file_registry.values()],
        "count": len(_file_registry),
    }
    REGISTRY_PATH.write_text(json.dumps(payload, indent=2, default=json_default), encoding="utf-8")


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
    if lower.endswith(".avro"):
        return "avro"
    if lower.endswith(".orc"):
        return "orc"
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
    try:
        lines = content.decode("utf-8").strip().splitlines()
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"JSONL is not valid UTF-8 ({exc}); refuse silent byte replacement"
        ) from exc
    objects = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        objects.append(json.loads(line))
    if not objects:
        raise ValueError("JSONL must contain at least one JSON object per line")
    if not all(isinstance(item, dict) for item in objects):
        raise ValueError("JSONL must contain one JSON object per line")

    # Union keys across all lines — sparse late fields must appear in Map/Validate.
    headers: list[str] = []
    seen: set[str] = set()
    for item in objects:
        for key in item:
            if key not in seen:
                seen.add(key)
                headers.append(key)

    rows = [[cell_to_string(item.get(h, "")) for h in headers] for item in objects]
    return headers, rows, len(objects)


def parse_json(content: bytes) -> tuple[list[str], list[list[str]], int]:
    from services.json_tabular import load_json_records

    objects = load_json_records(content)
    if not objects:
        return [], [], 0
    headers = list(objects[0].keys())
    # Union keys across sample so wrapped/geojson rows do not drop fields.
    for item in objects[:50]:
        for k in item.keys():
            if k not in headers:
                headers.append(k)
    rows = [[cell_to_string(item.get(h, "")) for h in headers] for item in objects]
    return headers, rows[:100], len(objects)


def _parse_parquet_preview(content: bytes, preview_rows: int = 100) -> tuple[list[str], list[list[str]], int, Any]:
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
            row.append("" if val is None else cell_to_string(val))
        rows.append(row)
    return headers, rows, row_count, table.schema


def store_upload(filename: str, content: bytes) -> dict:
    fmt = detect_format(filename, content)
    file_id = uuid.uuid4().hex[:16]
    encoding = "utf-8"
    delimiter = ","
    row_count = 0

    headers: list[str] = []
    rows: list[list[str]] = []
    arrow_schema: Any = None
    columns_override: list | None = None

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
        headers, rows, row_count, arrow_schema = _parse_parquet_preview(content)
    elif fmt in {"avro", "orc"}:
        # Native parse — never mislabel binary Avro/ORC as CSV.
        parsed = FileParser.parse_avro(content) if fmt == "avro" else FileParser.parse_orc(content)
        if not parsed.success:
            raise ValueError(parsed.error or f"{fmt.upper()} upload parse failed")
        headers = list(parsed.columns or [])
        rows = [
            [cell_to_string(rec.get(h) if isinstance(rec, dict) else rec) for h in headers]
            for rec in (parsed.data or [])[:100]
        ]
        row_count = int(parsed.row_count or len(parsed.data or []))
        if parsed.column_meta:
            columns_override = parsed.column_meta
        elif parsed.schema_map:
            columns_override = [
                {"name": name, "inferred_type": typ}
                for name, typ in parsed.schema_map.items()
            ]
        else:
            columns_override = None
    else:
        headers, rows, encoding, delimiter = parse_csv_preview(content)
        row_count = count_csv_rows(content, encoding)
        fmt = "csv"

    if arrow_schema is not None:
        from services.arrow_schema import columns_from_arrow_schema

        columns = columns_from_arrow_schema(arrow_schema)
    elif columns_override is not None:
        columns = columns_override
    else:
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
    record = _file_registry.get(file_id)
    if not record:
        # Reload from Mongo in case another API replica registered the upload.
        try:
            from services.control_plane_store import mongo_collection

            coll = mongo_collection("upload_registry")
            if coll is not None:
                doc = coll.find_one({"_id": file_id}) or coll.find_one({"file_id": file_id})
                if doc:
                    row = dict(doc)
                    row.pop("_id", None)
                    row["file_id"] = file_id
                    _file_registry[file_id] = row
                    record = row
        except Exception:
            pass
    if not record:
        return None
    path = Path(record.get("path") or "")
    if path.exists():
        return record
    uri = str(record.get("object_uri") or "")
    if uri.startswith("s3://"):
        from services.object_store import materialize_local
        from services.platform_config import upload_dir

        dest = upload_dir() / f"{file_id}_{record.get('filename') or 'upload.bin'}"
        if materialize_local(uri, dest):
            record = dict(record)
            record["path"] = str(dest)
            _file_registry[file_id] = record
            return record
    if path.exists():
        return record
    # Metadata known but bytes unreachable on this replica.
    return record if record.get("object_uri") else None


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
        with open(path, "r", encoding=encoding) as f:
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
        from services.json_tabular import extract_json_records

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        records = extract_json_records(data)
        if not records:
            return
        # Union keys across the full document — never freeze to first 50 records.
        headers: list[str] = []
        seen: set[str] = set()
        for item in records:
            for k in item.keys():
                if k not in seen:
                    seen.add(k)
                    headers.append(k)
        for i in range(0, len(records), chunk_size):
            batch = records[i : i + chunk_size]
            rows = [[cell_to_string(item.get(h, "")) for h in headers] for item in batch]
            yield headers, rows
    elif fmt == "jsonl":
        import json
        # Two-pass: union sparse keys across the whole file, then project rows.
        headers: list[str] = []
        seen: set[str] = set()
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    raise ValueError("JSONL must contain one JSON object per line")
                for k in obj.keys():
                    if k not in seen:
                        seen.add(k)
                        headers.append(k)
        if not headers:
            return
        with open(path, "r", encoding="utf-8") as f:
            chunk = []
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                row = [cell_to_string(obj.get(h, "")) for h in headers]
                chunk.append(row)
                if len(chunk) >= chunk_size:
                    yield headers, chunk
                    chunk = []
            if chunk:
                yield headers, chunk
    elif fmt in {"parquet", "avro", "orc", "excel"}:
        raise ValueError(
            f"{fmt.upper()} cannot use the legacy CSV chunker; route this upload "
            "through the native file-stream transfer engine."
        )
    else:
        # Fallback to full load then chunk
        from services.csv_profiler import parse_csv_full
        headers, data_rows, _, _ = parse_csv_full(path.read_bytes(), encoding)
        for i in range(0, len(data_rows), chunk_size):
            yield headers, data_rows[i:i+chunk_size]


@dataclass
class ParseResult:
    """Result of file parsing"""
    success: bool
    data: list[dict]
    columns: list[str]
    row_count: int
    error: str = ""
    file_type: str = ""
    ocr_used: bool = False
    ocr_page_count: int = 0
    # Native writer schema when available (Avro/Parquet) — not sample-inferred.
    schema_map: dict | None = None
    column_meta: list | None = None


class FileParser:
    """Universal file parser for DataTransfer platform"""

    SUPPORTED_TYPES = [
        "json", "csv", "tsv", "jsonl", "ndjson", "excel", "parquet", "avro", "orc", "xml",
        "pdf", "docx", "html",
    ]

    @staticmethod
    def detect_file_type(filename: str, content: bytes | None = None) -> str:
        """Detect file type from filename, with content sniffing as fallback.

        Handles ``.gz``-suffixed compressed files by inspecting the inner extension
        and, when no filename hint exists, decompresses a small prefix to sniff the
        payload.  This keeps billion-row CSV/JSONL ingest path-compatible.
        """
        filename_lower = (filename or "").lower()

        def _from_extension(name: str) -> str | None:
            if name.endswith(".json"):
                return "json"
            if name.endswith(".csv"):
                return "csv"
            if name.endswith(".tsv"):
                return "tsv"
            if name.endswith((".jsonl", ".ndjson")):
                return "jsonl" if name.endswith(".jsonl") else "ndjson"
            if name.endswith((".xlsx", ".xls")):
                return "excel"
            if name.endswith(".parquet"):
                return "parquet"
            if name.endswith(".xml"):
                return "xml"
            if name.endswith(".avro"):
                return "avro"
            if name.endswith(".orc"):
                return "orc"
            if name.endswith(".pdf"):
                return "pdf"
            if name.endswith(".docx"):
                return "docx"
            if name.endswith((".html", ".htm")):
                return "html"
            return None

        ext_result = _from_extension(filename_lower)
        if ext_result:
            return ext_result

        # Handle data.csv.gz, data.jsonl.gz, etc.
        if filename_lower.endswith(".gz"):
            inner = filename_lower[:-3]
            ext_result = _from_extension(inner)
            if ext_result:
                return ext_result

        # Document sniffing before tabular heuristics.
        try:
            from services.document_chunking import detect_document_type

            doc_kind = detect_document_type(filename or "", content)
            if doc_kind == "html":
                return "html"
            if doc_kind:
                return doc_kind
        except Exception:
            pass

        # Content sniffing — decompress a gzip prefix if needed.
        sample_bytes: bytes = b""
        if content:
            if content[:2] == b"\x1f\x8b":
                try:
                    with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                        sample_bytes = gz.read(4096)
                except Exception:
                    sample_bytes = content[:4096]
            else:
                sample_bytes = content[:4096]

            stripped = sample_bytes.lstrip()
            if stripped[:1] in (b"{", b"["):
                return "json"
            if b"\n{" in sample_bytes or b"\n[" in sample_bytes:
                return "jsonl"
            if b"," in sample_bytes[:512]:
                return "csv"
            if b"\t" in sample_bytes[:512]:
                return "tsv"

        return "unknown"

    @staticmethod
    def parse_json(content: str) -> ParseResult:
        """Parse JSON file content (array, wrapper object, or single record)."""
        try:
            from services.json_tabular import extract_json_records

            data = json.loads(content)
            try:
                records = extract_json_records(data)
            except ValueError as exc:
                return ParseResult(
                    success=False,
                    data=[],
                    columns=[],
                    row_count=0,
                    error=str(exc),
                    file_type="json",
                )

            if not records:
                return ParseResult(
                    success=True,
                    data=[],
                    columns=[],
                    row_count=0,
                    file_type="json",
                )

            columns: set[str] = set()
            object_rows = 0
            for record in records:
                if isinstance(record, dict):
                    object_rows += 1
                    columns.update(record.keys())

            if object_rows == 0:
                return ParseResult(
                    success=False,
                    data=[],
                    columns=[],
                    row_count=0,
                    error="JSON must be an array of objects — each record needs column keys",
                    file_type="json",
                )

            if not columns:
                return ParseResult(
                    success=False,
                    data=[],
                    columns=[],
                    row_count=0,
                    error="No columns detected — ensure each JSON object has consistent field names",
                    file_type="json",
                )

            return ParseResult(
                success=True,
                data=records,
                columns=sorted(list(columns)),
                row_count=len(records),
                file_type="json",
            )

        except json.JSONDecodeError as e:
            return ParseResult(
                success=False,
                data=[],
                columns=[],
                row_count=0,
                error=f"Invalid JSON: {str(e)}",
                file_type="json",
            )

    @staticmethod
    def parse_jsonl(content: str) -> ParseResult:
        """Parse JSON Lines (JSONL/NDJSON) format"""
        try:
            records = []
            columns = set()

            for line_num, line in enumerate(content.strip().split('\n'), 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        return ParseResult(
                            success=False,
                            data=[],
                            columns=[],
                            row_count=0,
                            error=(
                                f"JSONL line {line_num} must be a JSON object; "
                                "scalar records are not supported"
                            ),
                            file_type="jsonl",
                        )
                    records.append(record)
                    columns.update(record.keys())
                except json.JSONDecodeError as e:
                    return ParseResult(
                        success=False,
                        data=[],
                        columns=[],
                        row_count=0,
                        error=f"Invalid JSON at line {line_num}: {str(e)}",
                        file_type="jsonl"
                    )

            return ParseResult(
                success=True,
                data=records,
                columns=sorted(list(columns)),
                row_count=len(records),
                file_type="jsonl"
            )

        except Exception as e:
            return ParseResult(
                success=False,
                data=[],
                columns=[],
                row_count=0,
                error=str(e),
                file_type="jsonl"
            )

    @staticmethod
    def parse_csv(content: str | bytes, delimiter: str = ",") -> ParseResult:
        """Parse CSV/TSV file content — auto-detects delimiter and encoding."""
        try:
            if isinstance(content, bytes):
                # Strict decode — errors="replace" silently corrupts bytes into
                # U+FFFD and looks like a successful faithful ingest.
                try:
                    text = content.decode("utf-8").lstrip("\ufeff")
                except UnicodeDecodeError as exc:
                    return ParseResult(
                        success=False,
                        data=[],
                        columns=[],
                        row_count=0,
                        error=(
                            f"CSV is not valid UTF-8 ({exc}); refuse silent "
                            "byte replacement — re-encode or declare the source encoding"
                        ),
                        file_type="csv",
                    )
            else:
                text = content.lstrip("\ufeff")
            if not text.strip():
                return ParseResult(
                    success=False,
                    data=[],
                    columns=[],
                    row_count=0,
                    error="CSV file is empty",
                    file_type="csv",
                )
            delim = detect_delimiter(text[:8192])
            reader = csv.DictReader(io.StringIO(text), delimiter=delim)
            records = list(reader)
            columns = reader.fieldnames or []
            if not columns:
                return ParseResult(
                    success=False,
                    data=[],
                    columns=[],
                    row_count=0,
                    error="CSV has no header row",
                    file_type="csv",
                )
            file_type = "tsv" if delim == "\t" else "csv"
            return ParseResult(
                success=True,
                data=records,
                columns=list(columns),
                row_count=len(records),
                file_type=file_type,
            )
        except Exception as e:
            return ParseResult(
                success=False,
                data=[],
                columns=[],
                row_count=0,
                error=f"CSV parse error: {e}",
                file_type="csv",
            )

    @staticmethod
    def parse_excel(content: bytes, max_rows: int = 100_000) -> ParseResult:
        """Parse Excel (.xlsx) workbook — first sheet, header row."""
        try:
            import sys
            from pathlib import Path

            root = Path(__file__).resolve().parents[1]
            if str(root) not in sys.path:
                sys.path.insert(0, str(root))
            from services.excel_parser import iter_excel_batches

            records: list[dict] = []
            columns: list[str] = []
            for batch in iter_excel_batches(content, chunk_size=5000):
                if not columns and batch:
                    columns = list(batch[0].keys())
                # Non-streaming Excel must not silently truncate — same honesty bar
                # as Parquet/Avro/ORC/XML (partial success looks like a full ingest).
                if len(records) + len(batch) > max_rows:
                    return ParseResult(
                        success=False,
                        data=[],
                        columns=[],
                        row_count=len(records) + len(batch),
                        error=(
                            f"Excel contains more than {max_rows:,} rows; "
                            "use streaming ingest."
                        ),
                        file_type="excel",
                    )
                records.extend(batch)
            if not columns:
                return ParseResult(
                    success=False,
                    data=[],
                    columns=[],
                    row_count=0,
                    error="Excel sheet is empty or has no header row",
                    file_type="excel",
                )
            return ParseResult(
                success=True,
                data=records,
                columns=columns,
                row_count=len(records),
                file_type="excel",
            )
        except ValueError as exc:
            return ParseResult(
                success=False, data=[], columns=[], row_count=0, error=str(exc), file_type="excel",
            )
        except Exception as exc:
            return ParseResult(
                success=False, data=[], columns=[], row_count=0,
                error=f"Excel parse error: {exc}", file_type="excel",
            )

    @staticmethod
    def parse_parquet(content: bytes, max_rows: int = 100_000) -> ParseResult:
        try:
            import io

            import pyarrow.parquet as pq

            table = pq.read_table(io.BytesIO(content))
            total_rows = int(table.num_rows)
            if total_rows > max_rows:
                return ParseResult(
                    success=False,
                    data=[],
                    columns=[],
                    row_count=total_rows,
                    error=(
                        f"Parquet contains {total_rows:,} rows, exceeding the "
                        f"{max_rows:,}-row non-streaming limit; use streaming ingest."
                    ),
                    file_type="parquet",
                )
            df = table.to_pandas()
            records = df.to_dict(orient="records")
            columns = [str(c) for c in df.columns.tolist()]
            for rec in records:
                for k, v in list(rec.items()):
                    if hasattr(v, "item"):
                        rec[k] = v.item()
                    elif v != v:  # NaN
                        rec[k] = None
            return ParseResult(
                success=True,
                data=records,
                columns=columns,
                row_count=total_rows,
                file_type="parquet",
            )
        except ImportError:
            return ParseResult(
                success=False, data=[], columns=[], row_count=0,
                error="Parquet import is not ready on this platform node. DataFlow bundles file parsers — retry shortly.",
                file_type="parquet",
            )
        except Exception as exc:
            return ParseResult(
                success=False, data=[], columns=[], row_count=0,
                error=f"Parquet parse error: {exc}",
                file_type="parquet",
            )

    @staticmethod
    def parse_avro(content: bytes, max_rows: int = 100_000) -> ParseResult:
        try:
            import io

            import fastavro

            from services.avro_schema import columns_from_avro_schema, schema_map_from_avro

            reader = fastavro.reader(io.BytesIO(content))
            writer_schema = getattr(reader, "writer_schema", None) or getattr(reader, "schema", None)
            schema_map = schema_map_from_avro(writer_schema) if writer_schema else {}
            column_meta = columns_from_avro_schema(writer_schema) if writer_schema else []
            records = []
            seen_keys: set[str] = set(schema_map.keys())
            for i, record in enumerate(reader):
                if i >= max_rows:
                    return ParseResult(
                        success=False,
                        data=[],
                        columns=[],
                        row_count=i + 1,
                        error=(
                            f"Avro contains more than {max_rows:,} rows; "
                            "use the native streaming ingest path."
                        ),
                        file_type="avro",
                        schema_map=schema_map or None,
                        column_meta=column_meta or None,
                    )
                if not isinstance(record, dict):
                    record = {"value": record}
                for k in record.keys():
                    name = str(k)
                    if name not in seen_keys:
                        seen_keys.add(name)
                        schema_map.setdefault(name, "TEXT")
                records.append(record)
            columns = list(schema_map.keys()) if schema_map else sorted(seen_keys)
            if not columns and records and isinstance(records[0], dict):
                columns = sorted(records[0].keys())
            return ParseResult(
                success=True,
                data=records,
                columns=columns,
                row_count=len(records),
                file_type="avro",
                schema_map=schema_map or None,
                column_meta=column_meta or None,
            )
        except ImportError:
            return ParseResult(
                success=False, data=[], columns=[], row_count=0,
                error="Avro parser is not ready on this platform node. DataFlow bundles file parsers — retry shortly.",
                file_type="avro",
            )
        except Exception as exc:
            return ParseResult(
                success=False, data=[], columns=[], row_count=0,
                error=f"Avro parse error: {exc}", file_type="avro",
            )

    @staticmethod
    def parse_orc(content: bytes, max_rows: int = 100_000) -> ParseResult:
        try:
            import io

            import pyarrow.orc as orc

            from services.arrow_schema import columns_from_arrow_schema, schema_from_arrow

            table = orc.read_table(io.BytesIO(content))
            schema_map = schema_from_arrow(table.schema)
            column_meta = columns_from_arrow_schema(table.schema)
            total_rows = int(table.num_rows)
            if total_rows > max_rows:
                return ParseResult(
                    success=False,
                    data=[],
                    columns=[],
                    row_count=total_rows,
                    error=(
                        f"ORC contains {total_rows:,} rows, exceeding the "
                        f"{max_rows:,}-row non-streaming limit; use streaming ingest."
                    ),
                    file_type="orc",
                    schema_map=schema_map or None,
                    column_meta=column_meta or None,
                )
            records = table.to_pylist()
            columns = list(schema_map.keys()) if schema_map else [str(c) for c in table.column_names]
            return ParseResult(
                success=True,
                data=records,
                columns=columns,
                row_count=total_rows,
                file_type="orc",
                schema_map=schema_map or None,
                column_meta=column_meta or None,
            )
        except ImportError:
            return ParseResult(
                success=False, data=[], columns=[], row_count=0,
                error="ORC parser is not ready on this platform node. DataFlow bundles file parsers — retry shortly.",
                file_type="orc",
            )
        except Exception as exc:
            return ParseResult(
                success=False, data=[], columns=[], row_count=0,
                error=f"ORC parse error: {exc}", file_type="orc",
            )

    @staticmethod
    def parse_xml(content: str | bytes, max_rows: int = 100_000) -> ParseResult:
        try:
            import xmltodict

            text = content.decode("utf-8") if isinstance(content, bytes) else content
            root = xmltodict.parse(text)

            records, selected_path, ambiguity = FileParser._extract_xml_records(root)
            if ambiguity:
                return ParseResult(
                    success=False,
                    data=[],
                    columns=[],
                    row_count=0,
                    error=ambiguity,
                    file_type="xml",
                )
            if not records:
                if isinstance(root, dict):
                    records = [dict(root)]
                else:
                    records = [{"value": root}]
            # Non-streaming XML must not silently truncate — that looks like a
            # successful full transfer of only the first max_rows records.
            if len(records) > max_rows:
                return ParseResult(
                    success=False,
                    data=[],
                    columns=[],
                    row_count=len(records),
                    error=(
                        f"XML contains {len(records):,} records, exceeding the "
                        f"{max_rows:,}-row non-streaming limit; select a smaller "
                        "record set or use streaming XML ingest."
                    ),
                    file_type="xml",
                )
            columns: list[str] = []
            seen = set()
            for rec in records:
                for k in rec:
                    if k not in seen:
                        seen.add(k)
                        columns.append(k)
            return ParseResult(
                success=True,
                data=records,
                columns=columns,
                row_count=len(records),
                file_type="xml",
                schema_map={c: "TEXT" for c in columns} if selected_path else None,
                column_meta=(
                    [{"name": c, "inferred_type": "TEXT", "source": "xml", "path": selected_path} for c in columns]
                    if selected_path
                    else None
                ),
            )
        except ImportError:
            return ParseResult(
                success=False, data=[], columns=[], row_count=0,
                error="XML parser is not ready on this platform node. DataFlow bundles file parsers — retry shortly.",
                file_type="xml",
            )
        except Exception as exc:
            return ParseResult(
                success=False, data=[], columns=[], row_count=0,
                error=f"XML parse error: {exc}", file_type="xml",
            )

    @staticmethod
    def _discover_xml_collections(
        node: Any,
        *,
        depth: int = 0,
        path: str = "",
    ) -> list[tuple[str, list[dict]]]:
        """Find all repeating list-of-object collections under an XML dict."""
        if depth > 4 or not isinstance(node, dict):
            return []
        found: list[tuple[str, list[dict]]] = []
        for key, value in node.items():
            child_path = f"{path}/{key}" if path else str(key)
            if isinstance(value, list) and value and isinstance(value[0], dict):
                found.append((child_path, [FileParser._flatten_xml_item(item) for item in value]))
            elif isinstance(value, dict):
                found.extend(
                    FileParser._discover_xml_collections(value, depth=depth + 1, path=child_path)
                )
        return found

    @staticmethod
    def _extract_xml_records(node: Any, depth: int = 0) -> tuple[list[dict] | None, str | None, str | None]:
        """Return ``(records, selected_path, ambiguity_error)``.

        Multiple sibling repeating collections → fail closed (never pick one silently).
        """
        del depth  # discovery walks with its own depth
        if isinstance(node, list):
            records = [
                FileParser._flatten_xml_item(item)
                for item in node
                if isinstance(item, (dict, str, int, float, bool))
            ]
            if any(isinstance(item, dict) for item in node):
                return records, "root[]", None
            return None, None, None

        if not isinstance(node, dict):
            return None, None, None

        collections = FileParser._discover_xml_collections(node)
        if len(collections) > 1:
            # Prefer the shallowest path; if multiple at same depth, refuse.
            depths = [(p.count("/"), p, rows) for p, rows in collections]
            min_depth = min(d for d, _, _ in depths)
            top = [(p, rows) for d, p, rows in depths if d == min_depth]
            if len(top) > 1:
                paths = ", ".join(p for p, _ in top)
                return (
                    None,
                    None,
                    f"XML has multiple repeating record collections ({paths}). "
                    "Select a record path — refuse silent partial ingest.",
                )
            path, rows = top[0]
            return rows, path, None
        if len(collections) == 1:
            path, rows = collections[0]
            return rows, path, None

        # Single-record XMLs: a single child dict becomes one row.
        if len(node) == 1:
            value = list(node.values())[0]
            if isinstance(value, dict):
                return [FileParser._flatten_xml_item(value)], list(node.keys())[0], None
        return [FileParser._flatten_xml_item(node)], None, None

    @staticmethod
    def _flatten_xml_item(item: Any) -> dict:
        """Flatten an XML dict into a single-level record; attributes become @attr keys."""
        if not isinstance(item, dict):
            return {"value": item}
        out: dict[str, Any] = {}
        for k, v in item.items():
            if k.startswith("@"):
                out[k] = v
            elif isinstance(v, dict):
                for sub_k, sub_v in v.items():
                    out[f"{k}.{sub_k}"] = sub_v
            elif isinstance(v, list):
                out[k] = json.dumps(v, default=json_default)
            else:
                out[k] = v
        return out

    @classmethod
    def parse(cls, content: str | bytes, filename: str, *, enable_ocr: bool = False) -> ParseResult:
        """Parse file based on type detection, transparently handling gzip."""
        raw_bytes = content if isinstance(content, bytes) else content.encode("utf-8", errors="replace")

        # Transparent gzip decompression for in-memory payloads.
        if isinstance(content, bytes) and raw_bytes[:2] == b"\x1f\x8b":
            try:
                raw_bytes = gzip.decompress(raw_bytes)
            except Exception:
                pass

        file_type = cls.detect_file_type(filename, raw_bytes)

        if isinstance(content, bytes):
            decoded = raw_bytes
            try:
                content = decoded.decode("utf-8")
            except UnicodeDecodeError as exc:
                # Text tabular formats must not silently latin-1 mojibake.
                if file_type in {"csv", "tsv", "json", "jsonl", "xml", "fixed_width"}:
                    return ParseResult(
                        success=False,
                        data=[],
                        columns=[],
                        row_count=0,
                        error=(
                            f"File is not valid UTF-8 ({exc}); refuse silent "
                            "latin-1 fallback — re-encode or declare the source encoding"
                        ),
                        file_type=file_type,
                    )
                content = decoded.decode("latin-1")

        if file_type == "json":
            return cls.parse_json(content)
        elif file_type == "jsonl":
            return cls.parse_jsonl(content)
        elif file_type == "csv":
            return cls.parse_csv(content, delimiter=",")
        elif file_type == "tsv":
            return cls.parse_csv(content, delimiter="\t")
        elif file_type == "ndjson":
            return cls.parse_jsonl(content)
        elif file_type == "excel":
            return cls.parse_excel(raw_bytes)
        elif file_type == "parquet":
            return cls.parse_parquet(raw_bytes)
        elif file_type == "avro":
            return cls.parse_avro(raw_bytes)
        elif file_type == "orc":
            return cls.parse_orc(raw_bytes)
        elif file_type == "xml":
            return cls.parse_xml(raw_bytes)
        elif file_type in ("pdf", "docx", "html"):
            return cls.parse_document(raw_bytes, filename, file_type, enable_ocr=enable_ocr)
        else:
            return ParseResult(
                success=False,
                data=[],
                columns=[],
                row_count=0,
                error=f"Unsupported file type: {file_type}",
                file_type=file_type
            )

    @staticmethod
    def parse_document(
        content: bytes,
        filename: str,
        file_type: str,
        *,
        enable_ocr: bool = False,
    ) -> ParseResult:
        """Parse PDF / Word / HTML into provenance-aware chunk rows."""
        try:
            from services.document_chunking import document_columns, extract_document_chunks

            rows = extract_document_chunks(
                content,
                filename or f"document.{file_type}",
                doc_type=file_type,
                enable_ocr=enable_ocr,
            )
            if not rows:
                hint = (
                    "Enable “OCR scanned PDFs” in Transfer Studio (requires Tesseract), "
                    "or provide a PDF with an extractable text layer."
                    if file_type == "pdf"
                    else "Document has no extractable text."
                )
                return ParseResult(
                    success=False,
                    data=[],
                    columns=document_columns(),
                    row_count=0,
                    error=f"No extractable text in {file_type.upper()} — {hint}",
                    file_type=file_type,
                )
            ocr_pages = {
                str(r.get("page") or "")
                for r in rows
                if str(r.get("element_type") or "") == "ocr" and r.get("page")
            }
            return ParseResult(
                success=True,
                data=rows,
                columns=document_columns(),
                row_count=len(rows),
                file_type=file_type,
                ocr_used=bool(ocr_pages),
                ocr_page_count=len(ocr_pages),
            )
        except RuntimeError as exc:
            return ParseResult(
                success=False,
                data=[],
                columns=[],
                row_count=0,
                error=str(exc),
                file_type=file_type,
            )
        except Exception as exc:
            return ParseResult(
                success=False,
                data=[],
                columns=[],
                row_count=0,
                error=f"Document parse failed: {exc}",
                file_type=file_type,
            )

    @staticmethod
    def _value_to_string(value: Any) -> str:
        """Convert a typed Python value into a string for statistical inference."""
        return cell_to_string(value)

    @staticmethod
    def infer_schema(records: list[dict]) -> dict[str, str]:
        """Infer rich schema from records using statistical type inference."""
        if not records:
            return {}

        samples: dict[str, list[str]] = {}
        for record in records[:1000]:
            for key, value in record.items():
                if value is None:
                    continue
                if key not in samples:
                    samples[key] = []
                if len(samples[key]) < 100:
                    samples[key].append(FileParser._value_to_string(value))

        from services.schema_inference import infer_schema_map

        schema, _intel = infer_schema_map(samples)
        return schema

