"""
DataTransfer.space — File Parser Service
Parse various file formats: CSV, JSON, Excel, Parquet
"""

from __future__ import annotations

import base64
import csv
import io
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

# Ensure apps/api is on PYTHONPATH so root services.* imports resolve before src/services.*
_api_root = Path(__file__).resolve().parents[2]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.csv_profiler import detect_delimiter
from services.schema_inference import infer_type


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
    
    SUPPORTED_TYPES = ["json", "csv", "tsv", "jsonl", "ndjson", "excel", "parquet"]
    
    @staticmethod
    def detect_file_type(filename: str, content: bytes | None = None) -> str:
        """Detect file type from filename, with content sniffing as fallback."""
        filename_lower = (filename or "").lower()

        if filename_lower.endswith(".json"):
            return "json"
        if filename_lower.endswith(".csv"):
            return "csv"
        if filename_lower.endswith(".tsv"):
            return "tsv"
        if filename_lower.endswith((".jsonl", ".ndjson")):
            return "jsonl" if filename_lower.endswith(".jsonl") else "ndjson"
        if filename_lower.endswith((".xlsx", ".xls")):
            return "excel"
        if filename_lower.endswith(".parquet"):
            return "parquet"
        if filename_lower.endswith(".xml"):
            return "xml"

        if content:
            sample = content[:4096]
            stripped = sample.lstrip()
            if stripped[:1] in (b"{", b"["):
                return "json"
            if b"\n{" in sample or b"\n[" in sample:
                return "jsonl"
            if b"," in sample[:512]:
                return "csv"
            if b"\t" in sample[:512]:
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

            root = Path(__file__).resolve().parents[2]
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

    @classmethod
    def parse(cls, content: str | bytes, filename: str) -> ParseResult:
        """Parse file based on type detection"""
        raw_bytes = content if isinstance(content, bytes) else content.encode("utf-8", errors="replace")
        file_type = cls.detect_file_type(filename, raw_bytes)

        if isinstance(content, bytes):
            try:
                content = content.decode("utf-8")
            except UnicodeDecodeError:
                content = content.decode("latin-1")
        
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
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (bytes, bytearray)):
            return base64.b64encode(bytes(value)).decode("ascii")
        if isinstance(value, (dict, list, tuple, set, frozenset)):
            return json.dumps(value, default=str)
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, time):
            return value.isoformat()
        return str(value)

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

        schema = {key: infer_type(samples[key], field_name=key) for key in samples}
        return schema
