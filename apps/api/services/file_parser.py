"""Parse uploaded files and infer schema."""

from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.csv_profiler import count_csv_rows, detect_delimiter, parse_csv_preview
from services.platform_config import data_dir, upload_dir
from services.schema_inference import infer_columns_from_rows
from services.tabular_rows import is_blank_row
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
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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


def _jsonl_count_open(content: bytes | str | Path) -> tuple[Any, Any]:
    """Line reader for dest COUNT. Path (including gzip) streams; bytes/str stay in RAM."""
    if isinstance(content, Path):
        from services.dest_precount import open_artifact_binary

        binary, closer = open_artifact_binary(content)
        try:
            text = io.TextIOWrapper(
                binary, encoding="utf-8", errors="strict", newline=""
            )
        except Exception:
            if closer is not None:
                closer()
            raise
        return text, text.close
    if isinstance(content, bytes):
        handle = io.TextIOWrapper(
            io.BytesIO(content), encoding="utf-8", errors="strict", newline=""
        )
        return handle, handle.close
    if isinstance(content, str):
        return io.StringIO(content), None
    if hasattr(content, "read"):
        text = io.TextIOWrapper(
            content, encoding="utf-8", errors="strict", newline=""
        )
        return text, text.close
    raise TypeError("JSONL COUNT expects bytes, str, Path, or a readable stream")


def _iter_jsonl_dicts_from_reader(reader: Any) -> Any:
    """One JSON object per non-blank line. Poison / non-object is unmeasured."""
    from services.dest_precount import UnmeasuredArtifact

    for raw in reader:
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise UnmeasuredArtifact("jsonl_poison_line") from exc
        if not isinstance(obj, dict):
            raise UnmeasuredArtifact("jsonl_non_object")
        yield obj


def iter_jsonl_dicts(content: bytes | str | Path) -> Any:
    """Same JSONL population as ``count_jsonl_records``, as dicts for Gate-8.

    A poison or non-object line raises ``UnmeasuredArtifact`` — never yield
    a prefix (truncated DISTINCT lesson). COUNT is ``sum`` of this walk.
    """
    closer = None
    try:
        reader, closer = _jsonl_count_open(content)
        yield from _iter_jsonl_dicts_from_reader(reader)
    finally:
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


def count_jsonl_records(content: bytes | str | Path) -> int | None:
    """Dest-engine record COUNT of JSON Lines / NDJSON. Never ingest ``parse_jsonl``.

    Population is one JSON object per non-blank line — the same grain
    streaming ingest already writes. Empty file / only blank lines is 0.
    A scalar, array, or malformed line makes the whole artifact
    unmeasured — never COUNT of a prefix (truncated DISTINCT lesson).
    ``parse_jsonl`` ingest raises and materializes; this COUNT streams
    and returns ``None``. Writer JSONL is ``json.dumps`` + newline.

    Walk is one line at a time (O(line), not ``decode`` + ``splitlines`` of
    the document). Path inputs are counted from disk; bytes (object-store
    GET) stream from a buffer already in RAM. Local gzip JSONL streams.
    Object-store GET gzip streams through a caller-owned ``GzipFile``.
    Gate-8 cell checksum walks the same records via ``iter_jsonl_dicts``.
    """
    try:
        return sum(1 for _ in iter_jsonl_dicts(content))
    except (OSError, UnicodeDecodeError, UnicodeEncodeError, TypeError):
        return None
    except Exception:
        return None


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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
                if is_blank_row(row):
                    continue
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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

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
            records = [r for r in reader if not is_blank_row(dict(r).values())]
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
                        v = rec[k]
                    # Keep IEEE NaN/Inf — never invent SQL NULL (silent loss).
                    # Downstream quarantine / sanitize_json_value refuse_nonfinite.
                    if isinstance(v, float) and v != v:
                        continue
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
                error="Parquet import is not ready on this platform node. Datawrap bundles file parsers — retry shortly.",
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

            from services.avro_schema import (
                columns_from_avro_schema,
                schema_map_from_avro,
            )

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
                error="Avro parser is not ready on this platform node. Datawrap bundles file parsers — retry shortly.",
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
            import importlib
            import io

            # Use importlib so tests/sandbox can replace sys.modules["pyarrow.orc"]
            # without the pyarrow package attribute cache shadowing the override.
            orc = importlib.import_module("pyarrow.orc")

            from services.arrow_schema import (
                columns_from_arrow_schema,
                schema_from_arrow,
            )

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
                error="ORC parser is not ready on this platform node. Datawrap bundles file parsers — retry shortly.",
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
            # Phase D6 — reject XXE / entity expansion before xmltodict (expat).
            try:
                from defusedxml import ElementTree as DET

                DET.fromstring(text)  # nosec B314 — defusedxml, not stdlib
            except ImportError:
                pass
            except Exception as exc:
                return ParseResult(
                    success=False,
                    data=[],
                    columns=[],
                    row_count=0,
                    error=f"XML rejected (unsafe or malformed): {exc}",
                    file_type="xml",
                )
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
                error="XML parser is not ready on this platform node. Datawrap bundles file parsers — retry shortly.",
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
    def _unique_xml_collection(
        node: Any,
    ) -> tuple[list[dict] | None, str | None, str | None]:
        """Unique repeating list-of-object, or ambiguity.

        ``(rows, path, None)`` when one collection wins (including
        shallowest-path tie-break). ``(None, None, error)`` when sibling
        collections tie — never pick one silently. ``(None, None, None)``
        when xmltodict collapsed 0/1 record into a dict (no list yet).
        """
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
        return None, None, None

    @staticmethod
    def _extract_xml_records(node: Any, depth: int = 0) -> tuple[list[dict] | None, str | None, str | None]:
        """Return ``(records, selected_path, ambiguity_error)``.

        Multiple sibling repeating collections → fail closed (never pick one silently).
        """
        del depth  # discovery walks with its own depth
        rows, path, err = FileParser._unique_xml_collection(node)
        if err:
            return None, None, err
        if rows is not None:
            return rows, path, None
        if not isinstance(node, dict):
            return None, None, None
        # Ingest fallback: a single child dict becomes one row. Dest COUNT
        # does not use this — a document XML is not a table of one.
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
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

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
            from services.document_chunking import (
                document_columns,
                extract_document_chunks,
            )

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


def _xml_local_name(tag: object) -> str:
    """Clark ``{ns}local`` → local, so sibling collections still collide on name."""
    raw = str(tag or "")
    if raw.startswith("{") and "}" in raw:
        return raw.rsplit("}", 1)[-1]
    return raw


def _xml_end_kind(elem: Any, had_element_child: bool) -> str:
    """dict / empty / scalar — the same three xmltodict shapes COUNT already used."""
    if had_element_child or getattr(elem, "attrib", None):
        return "dict"
    text = (getattr(elem, "text", None) or "").strip()
    return "scalar" if text else "empty"


def _xml_count_open(content: bytes | str | Path) -> tuple[Any, Any]:
    """Byte source for iterparse. Path (including gzip) streams; bytes/str stay in RAM."""
    from services.dest_precount import artifact_byte_source

    return artifact_byte_source(content)


def _xml_count_as_text(content: bytes | str | Path) -> str | None:
    """ImportError fallback only — never the GB-scale COUNT path."""
    try:
        if isinstance(content, Path):
            return content.read_text(encoding="utf-8")
        if isinstance(content, bytes):
            return content.decode("utf-8")
        if isinstance(content, str):
            return content
    except (OSError, UnicodeDecodeError):
        return None
    return None


def _xml_unique_from_parent_stats(
    parent_stats: dict[str, dict[str, list[int]]],
    *,
    root_text_nonempty: bool,
) -> tuple[int, str | None, str | None] | None:
    """Unique shallowest list-of-object as ``(n, parent_path, tag)``.

    A collection is one child tag with ≥2 dict-like occurrences (element
    children or attributes). Nested inner lists (``items`` under ``record``)
    lose to the outer path. Sibling collections at the same depth stay
    unmeasured — never guess. No collection: empty wrapper ``(0, None, None)``;
    one empty or dict-like child tag ``(1, path, tag)`` (xmltodict's
    one-record collapse); scalar-only or mixed sibling fields ``None``,
    not dest=1. COUNT and Gate-8 share this identity; COUNT returns ``n``,
    checksum emits flattened dicts at ``(path, tag)``.
    """
    collections: list[tuple[int, int, str, str]] = []
    for ppath, tags in parent_stats.items():
        depth = ppath.count("/")
        for tag, row in tags.items():
            n, dict_n, _empty_n, _scalar_n = row
            if n >= 2 and dict_n >= 2:
                collections.append((depth, n, ppath, tag))
    if collections:
        min_depth = min(item[0] for item in collections)
        at_min = [item for item in collections if item[0] == min_depth]
        if len(at_min) != 1:
            return None
        _depth, n, ppath, tag = at_min[0]
        return n, ppath, tag
    if not parent_stats:
        return None if root_text_nonempty else (0, None, None)
    min_depth = min(path.count("/") for path in parent_stats)
    roots = [path for path in parent_stats if path.count("/") == min_depth]
    if len(roots) != 1:
        return None
    children = parent_stats[roots[0]]
    if len(children) != 1:
        return None
    tag, row = next(iter(children.items()))
    n, _dict_n, _empty_n, scalar_n = row
    if n == 1 and not scalar_n:
        return 1, roots[0], tag
    return None


def _xml_stax_unique(
    source: Any, xml_iterparse: Any
) -> tuple[int, str | None, str | None] | None:
    """One StAX walk. Unique ``(n, parent_path, tag)``, else unmeasured."""
    parent_stats: dict[str, dict[str, list[int]]] = {}
    stack: list[str] = []
    saw_child: list[bool] = []
    saw_root = False
    root_text_nonempty = False
    for event, elem in xml_iterparse(  # nosec B314
        source,
        events=("start", "end"),
        forbid_dtd=True,
        forbid_entities=True,
        forbid_external=True,
    ):
        if event == "start":
            saw_root = True
            if saw_child:
                saw_child[-1] = True
            stack.append(_xml_local_name(elem.tag))
            saw_child.append(False)
            continue
        kind = _xml_end_kind(elem, bool(saw_child and saw_child[-1]))
        tag = stack.pop() if stack else _xml_local_name(elem.tag)
        if saw_child:
            saw_child.pop()
        if stack:
            parent_path = "/" + "/".join(stack)
            bucket = parent_stats.setdefault(parent_path, {})
            row = bucket.get(tag)
            if row is None:
                row = [0, 0, 0, 0]
                bucket[tag] = row
            row[0] += 1
            if kind == "dict":
                row[1] += 1
            elif kind == "empty":
                row[2] += 1
            else:
                row[3] += 1
        else:
            root_text_nonempty = bool((elem.text or "").strip())
        elem.clear()
    if not saw_root:
        return None
    return _xml_unique_from_parent_stats(
        parent_stats, root_text_nonempty=root_text_nonempty
    )


def _count_xml_records_stax(source: Any, xml_iterparse: Any) -> int | None:
    """StAX unique-path COUNT. ``elem.clear()`` drops text; empty shells stay.

    defusedxml/stdlib ``iterparse`` has no lxml ``getprevious()`` sibling
    unlink, so memory is O(n) empty Element objects under a wide parent,
    not O(document text). O(depth) unlink is a future enhancement of this
    kernel, not a second COUNT. Gate-8 cell dicts are a second pass of
    this same unique path (``iter_xml_dicts``), not a DOM ingest parse.
    """
    found = _xml_stax_unique(source, xml_iterparse)
    return None if found is None else found[0]


def _count_xml_records_dom(text: str) -> int | None:
    """xmltodict COUNT when defusedxml is absent. Same unique-path identity."""
    try:
        import xmltodict
    except ImportError:
        return None
    try:
        root = xmltodict.parse(text)
    except Exception:
        return None
    rows, _path, err = FileParser._unique_xml_collection(root)
    if err:
        return None
    if rows is not None:
        return len(rows)
    return _count_xml_collapsed_table(root)


def count_xml_records(content: bytes | str | Path) -> int | None:
    """Dest-engine record COUNT of tabular XML. Never ingest ``max_rows``.

    Population is the unique repeating list-of-object the ingest parser
    already discovers. Empty ``<records/>`` is 0. One collapsed
    ``<record>`` is 1. Sibling collections at the same depth stay
    unmeasured — never guess a path. A document (scalar fields under a
    wrapper, no record element) is unmeasured, not dest=1. Malformed /
    XXE / missing parser stay unmeasured, not dest=0. ``parse_xml`` ingest
    fallback that treats the whole document as one row is not this COUNT.

    Walk is defusedxml ``iterparse`` (XXE-safe StAX, ``forbid_dtd``).
    ``fromstring`` + xmltodict DOM is not the COUNT — a GB export must
    not become two in-memory trees. A stream error or a DOCTYPE is
    unmeasured; do not then DOM-parse the same poison file. Writer XML
    has no DTD. xmltodict remains the ImportError fallback when
    defusedxml is absent. Path inputs are counted from disk; bytes
    (object-store GET) stream from a buffer already in RAM. Local gzip
    XML streams; the ImportError DOM fallback does not slurp a gzip path.
    Object-store GET gzip streams through a caller-owned ``GzipFile``.
    Gate-8 cell checksum reuses this uniqueness via ``iter_xml_dicts``
    (second StAX pass at the discovered path; one-shot GET is spooled).
    """
    try:
        from defusedxml.ElementTree import iterparse as xml_iterparse
    except ImportError:
        text = _xml_count_as_text(content)
        if text is None:
            return None
        return _count_xml_records_dom(text)
    closer = None
    try:
        source, closer = _xml_count_open(content)
        return _count_xml_records_stax(source, xml_iterparse)
    except (OSError, UnicodeEncodeError, TypeError):
        return None
    except Exception:
        return None
    finally:
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


def _xml_elem_value(elem: Any) -> Any:
    """Element → xmltodict-shaped value (text, attr dict, or nested map/list)."""
    children = list(elem)
    attrib = dict(getattr(elem, "attrib", None) or {})
    text = (getattr(elem, "text", None) or "").strip()
    if not children:
        if attrib:
            rec = {f"@{k}": v for k, v in attrib.items()}
            if text:
                rec["#text"] = text
            return rec
        return text
    rec = {f"@{k}": v for k, v in attrib.items()}
    groups: dict[str, list[Any]] = {}
    for child in children:
        groups.setdefault(_xml_local_name(child.tag), []).append(_xml_elem_value(child))
    for tag, vals in groups.items():
        rec[tag] = vals if len(vals) > 1 else vals[0]
    return rec


def _xml_elem_record(elem: Any) -> dict[str, Any]:
    """Same flatten ingest uses — attributes ``@attr``, nested dicts dotted.

    An empty record element (``<record/>``) is one empty object ``{}``,
    matching xmltodict's collapse and the ImportError DOM fallback — never
    a synthetic ``value`` cell that would split COUNT=1 from Gate-8.
    """
    raw = _xml_elem_value(elem)
    if not isinstance(raw, dict):
        if raw in ("", None):
            return {}
        return {"value": raw}
    return FileParser._flatten_xml_item(raw)


def _iter_xml_dicts_at_path(
    source: Any, xml_iterparse: Any, parent_path: str, tag: str
) -> Any:
    """Second StAX pass: emit flattened dicts at the unique COUNT path.

    Pass 1 (COUNT) may ``elem.clear()`` on every end — it only needs
    dict/empty/scalar kind. Pass 2 must not: clearing a child before its
    parent record ends would serialize empty shells (``id=""``) and still
    COUNT as n. Clear only after the unique-path record is materialized.
    Empty shells of already-emitted records stay under the parent — the
    same O(n) COUNT lesson; O(depth) unlink is a future of this kernel.
    """
    stack: list[str] = []
    for event, elem in xml_iterparse(  # nosec B314
        source,
        events=("start", "end"),
        forbid_dtd=True,
        forbid_entities=True,
        forbid_external=True,
    ):
        if event == "start":
            stack.append(_xml_local_name(elem.tag))
            continue
        local = stack.pop() if stack else _xml_local_name(elem.tag)
        current_parent = "/" + "/".join(stack) if stack else ""
        if current_parent == parent_path and local == tag:
            yield _xml_elem_record(elem)
            elem.clear()


def _iter_xml_dicts_dom(text: str) -> Any:
    """ImportError fallback when defusedxml is absent. Same unique-path as COUNT."""
    from services.dest_precount import UnmeasuredArtifact

    try:
        import xmltodict
    except ImportError as exc:
        raise UnmeasuredArtifact("xml_checksum_needs_parser") from exc
    try:
        root = xmltodict.parse(text)
    except Exception as exc:
        raise UnmeasuredArtifact("xml_unparseable") from exc
    rows, _path, err = FileParser._unique_xml_collection(root)
    if err:
        raise UnmeasuredArtifact("xml_ambiguous_path")
    if rows is not None:
        yield from rows
        return
    n = _count_xml_collapsed_table(root)
    if n == 0:
        return
    if n == 1 and isinstance(root, dict) and len(root) == 1:
        wrapper = next(iter(root.values()))
        if wrapper is None or wrapper == "":
            yield {}
            return
        if isinstance(wrapper, dict) and len(wrapper) == 1:
            inner = next(iter(wrapper.values()))
            if inner is None or inner == "":
                yield {}
                return
            if isinstance(inner, dict):
                yield FileParser._flatten_xml_item(inner)
                return
    raise UnmeasuredArtifact("xml_unmeasured")


def iter_xml_dicts(content: bytes | str | Path) -> Any:
    """Same unique-path population as ``count_xml_records``, as dicts for Gate-8.

    Pass 1 is the COUNT StAX unique-path walk. Pass 2 emits flattened
    records at that path. A one-shot GET is spooled once, then both
    passes read the spool — never a prefix digest, never ingest
    ``parse_xml`` (max_rows / document-as-one). Ambiguous siblings,
    document XML, XXE, and malformed raise ``UnmeasuredArtifact``.
    Empty well-formed yields nothing.
    """
    from services.dest_precount import UnmeasuredArtifact, rewindable_byte_source

    try:
        from defusedxml.ElementTree import iterparse as xml_iterparse
    except ImportError:
        text = _xml_count_as_text(content)
        if text is None:
            raise UnmeasuredArtifact("xml_unreadable")
        yield from _iter_xml_dicts_dom(text)
        return
    closer = None
    spool_closer = None
    try:
        source, closer = _xml_count_open(content)
        rewindable, spool_closer = rewindable_byte_source(source)
        found = _xml_stax_unique(rewindable, xml_iterparse)
        if found is None:
            raise UnmeasuredArtifact("xml_unmeasured")
        n, parent_path, tag = found
        if n == 0 or parent_path is None or tag is None:
            return
        rewindable.seek(0)
        yielded = 0
        for rec in _iter_xml_dicts_at_path(
            rewindable, xml_iterparse, parent_path, tag
        ):
            yielded += 1
            yield rec
        if yielded != n:
            raise UnmeasuredArtifact("xml_checksum_count_mismatch")
    except UnmeasuredArtifact:
        raise
    except (OSError, UnicodeEncodeError, TypeError) as exc:
        raise UnmeasuredArtifact("xml_unreadable") from exc
    except Exception as exc:
        raise UnmeasuredArtifact("xml_unparseable") from exc
    finally:
        if spool_closer is not None:
            try:
                spool_closer()
            except Exception:
                pass
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


def _count_xml_collapsed_table(root: Any) -> int | None:
    """0/1 record after xmltodict collapsed a list. Document XML stays None."""
    if not isinstance(root, dict) or len(root) != 1:
        return None
    value = next(iter(root.values()))
    if value is None or value == "":
        return 0
    if isinstance(value, list):
        if not value:
            return 0
        if all(isinstance(item, dict) for item in value):
            return len(value)
        return None
    if not isinstance(value, dict):
        return None
    if len(value) != 1:
        return None
    inner = next(iter(value.values()))
    if inner is None or inner == "":
        return 1
    if isinstance(inner, dict):
        return 1
    if isinstance(inner, list):
        if not inner:
            return 0
        if all(isinstance(item, dict) for item in inner):
            return len(inner)
        return None
    return None

