"""Compatibility shim: canonical file_parser now lives in services.file_parser."""
from __future__ import annotations

from services.file_parser import (
    REGISTRY_PATH,
    UPLOAD_DIR,
    FileParser,
    ParseResult,
    _load_registry,
    _parse_parquet_preview,
    _registry_record_for_disk,
    _save_registry,
    detect_format,
    get_file,
    get_file_chunks,
    parse_json,
    parse_jsonl,
    store_upload,
)

__all__ = ['UPLOAD_DIR', 'REGISTRY_PATH', '_registry_record_for_disk', '_load_registry', '_save_registry', 'detect_format', 'parse_jsonl', 'parse_json', '_parse_parquet_preview', 'store_upload', 'get_file', 'get_file_chunks', 'ParseResult', 'FileParser']
