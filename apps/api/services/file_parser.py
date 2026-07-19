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
    rows = [[cell_to_string(item.get(h, "")) for h in headers] for item in objects]
    return headers, rows, len(objects)


def parse_json(content: bytes) -> tuple[list[str], list[list[str]], int]:
    data = json.loads(content.decode("utf-8", errors="replace"))
    if isinstance(data, list) and data:
        if isinstance(data[0], dict):
            headers = list(data[0].keys())
            rows = [[cell_to_string(item.get(h, "")) for h in headers] for item in data]
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
            row.append("" if val is None else cell_to_string(val))
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
                rows = [[cell_to_string(item.get(h, "")) for h in headers] for item in batch]
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
                row = [cell_to_string(obj.get(h, "")) for h in headers]
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


@dataclass
class ParseResult:
    """Result of file parsing"""
    success: bool
    data: list[dict]
    columns: list[str]
    row_count: int
    error: str = ""
    file_type: str = ""


class FileParser:
    """Universal file parser for DataTransfer platform"""

    SUPPORTED_TYPES = ["json", "csv", "tsv", "jsonl", "ndjson", "excel", "parquet", "avro", "orc", "xml"]

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
        """Parse JSON file content"""
        try:
            data = json.loads(content)

            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                if any(isinstance(v, list) for v in data.values()):
                    for key, value in data.items():
                        if isinstance(value, list) and value and isinstance(value[0], dict):
                            records = value
                            break
                    else:
                        records = [data]
                else:
                    records = [data]
            else:
                return ParseResult(
                    success=False,
                    data=[],
                    columns=[],
                    row_count=0,
                    error="JSON must be an array or object",
                    file_type="json"
                )

            if not records:
                return ParseResult(
                    success=True,
                    data=[],
                    columns=[],
                    row_count=0,
                    file_type="json"
                )

            columns = set()
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
                file_type="json"
            )

        except json.JSONDecodeError as e:
            return ParseResult(
                success=False,
                data=[],
                columns=[],
                row_count=0,
                error=f"Invalid JSON: {str(e)}",
                file_type="json"
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
                    if isinstance(record, dict):
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
                text = content.decode("utf-8", errors="replace").lstrip("\ufeff")
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
                records.extend(batch)
                if len(records) >= max_rows:
                    records = records[:max_rows]
                    break
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
            df = table.to_pandas().head(max_rows)
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
                row_count=len(records),
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
                error=f"Parquet parse error: {exc}", file_type="parquet",
            )

    @staticmethod
    def parse_avro(content: bytes, max_rows: int = 100_000) -> ParseResult:
        try:
            import io

            import fastavro

            reader = fastavro.reader(io.BytesIO(content))
            records = []
            columns: list[str] = []
            for i, record in enumerate(reader):
                if i >= max_rows:
                    break
                if not columns and isinstance(record, dict):
                    columns = sorted(record.keys())
                records.append(record)
            return ParseResult(
                success=True,
                data=records,
                columns=columns,
                row_count=len(records),
                file_type="avro",
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

            table = orc.read_table(io.BytesIO(content))
            df = table.to_pandas().head(max_rows)
            columns = [str(c) for c in df.columns.tolist()]
            records = df.to_dict(orient="records")
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
                row_count=len(records),
                file_type="orc",
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

            records = FileParser._extract_xml_records(root)
            if not records:
                if isinstance(root, dict):
                    records = [dict(root)]
                else:
                    records = [{"value": root}]
            records = records[:max_rows]
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
    def _extract_xml_records(node: Any, depth: int = 0) -> list[dict] | None:
        """Recursively find the first list of objects inside an XML dict and flatten each."""
        if depth > 4:
            return None
        if isinstance(node, list):
            records = [FileParser._flatten_xml_item(item) for item in node if isinstance(item, (dict, str, int, float, bool))]
            return records if any(isinstance(item, dict) for item in node) else None
        if isinstance(node, dict):
            # Direct list of dicts under a key is the most common shape.
            for value in node.values():
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    return [FileParser._flatten_xml_item(item) for item in value]
            # Otherwise recurse through nested dicts.
            for value in node.values():
                found = FileParser._extract_xml_records(value, depth + 1)
                if found:
                    return found
            # Single-record XMLs: a single child dict becomes one row.
            if len(node) == 1:
                value = list(node.values())[0]
                if isinstance(value, dict):
                    return [FileParser._flatten_xml_item(value)]
            return [FileParser._flatten_xml_item(node)]
        return None

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
    def parse(cls, content: str | bytes, filename: str) -> ParseResult:
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
            except UnicodeDecodeError:
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

