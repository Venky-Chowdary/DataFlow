"""
DataTransfer.space — File Parser Service
Parse various file formats: CSV, JSON, Excel, Parquet
"""

from __future__ import annotations

import json
import csv
import io
from typing import Any
from dataclasses import dataclass


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
    
    SUPPORTED_TYPES = ["json", "csv", "tsv", "jsonl", "ndjson"]
    
    @staticmethod
    def detect_file_type(filename: str, content: bytes = None) -> str:
        """Detect file type from filename or content"""
        filename_lower = filename.lower()
        
        if filename_lower.endswith(".json"):
            return "json"
        elif filename_lower.endswith(".csv"):
            return "csv"
        elif filename_lower.endswith(".tsv"):
            return "tsv"
        elif filename_lower.endswith(".jsonl") or filename_lower.endswith(".ndjson"):
            return "jsonl"
        elif filename_lower.endswith(".xlsx") or filename_lower.endswith(".xls"):
            return "excel"
        elif filename_lower.endswith(".parquet"):
            return "parquet"
        elif filename_lower.endswith(".xml"):
            return "xml"
        
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
            for record in records:
                if isinstance(record, dict):
                    columns.update(record.keys())
            
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
    def parse_csv(content: str, delimiter: str = ",") -> ParseResult:
        """Parse CSV file content"""
        try:
            reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
            records = list(reader)
            columns = reader.fieldnames or []
            
            return ParseResult(
                success=True,
                data=records,
                columns=list(columns),
                row_count=len(records),
                file_type="csv" if delimiter == "," else "tsv"
            )
            
        except Exception as e:
            return ParseResult(
                success=False,
                data=[],
                columns=[],
                row_count=0,
                error=f"CSV parse error: {str(e)}",
                file_type="csv"
            )
    
    @classmethod
    def parse(cls, content: str | bytes, filename: str) -> ParseResult:
        """Parse file based on type detection"""
        file_type = cls.detect_file_type(filename)
        
        if isinstance(content, bytes):
            try:
                content = content.decode('utf-8')
            except UnicodeDecodeError:
                content = content.decode('latin-1')
        
        if file_type == "json":
            return cls.parse_json(content)
        elif file_type == "jsonl":
            return cls.parse_jsonl(content)
        elif file_type == "csv":
            return cls.parse_csv(content, delimiter=",")
        elif file_type == "tsv":
            return cls.parse_csv(content, delimiter="\t")
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
    def infer_schema(records: list[dict]) -> dict[str, str]:
        """Infer schema from records"""
        schema = {}
        
        if not records:
            return schema
        
        for record in records[:100]:
            for key, value in record.items():
                if key not in schema:
                    if value is None:
                        schema[key] = "null"
                    elif isinstance(value, bool):
                        schema[key] = "boolean"
                    elif isinstance(value, int):
                        schema[key] = "integer"
                    elif isinstance(value, float):
                        schema[key] = "number"
                    elif isinstance(value, list):
                        schema[key] = "array"
                    elif isinstance(value, dict):
                        schema[key] = "object"
                    else:
                        schema[key] = "string"
        
        return schema
