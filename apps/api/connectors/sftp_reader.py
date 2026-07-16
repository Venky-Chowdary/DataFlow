"""SFTP file reader — download and parse JSON/CSV/JSONL objects."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from connectors.sftp_common import connect_sftp, parse_sftp_config, split_remote_path
from services.value_serializer import cell_to_string


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int = 0
    total_rows: int = 0


def _parse_object_body(body: bytes, filename: str) -> tuple[list[dict], list[str], dict[str, str]]:
    """Parse downloaded bytes using the canonical FileParser."""
    _api_root = Path(__file__).resolve().parents[1]
    if str(_api_root) not in sys.path:
        sys.path.insert(0, str(_api_root))
    try:
        from services.file_parser import FileParser
    except ImportError:
        from src.services.file_parser import FileParser

    result = FileParser.parse(body, filename)
    if not result.success:
        raise ValueError(result.error or f"Cannot parse SFTP file `{filename}`")
    schema = FileParser.infer_schema(result.data)
    return result.data, result.columns, schema


def read_object(
    *,
    cfg: dict[str, Any],
    bucket: str = "",
    key: str = "",
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
) -> ReadBatch:
    """Download a file from SFTP and parse it into a ReadBatch."""
    sftp_cfg = parse_sftp_config(**cfg)
    if not sftp_cfg.host:
        raise ValueError("SFTP host is required")
    if not sftp_cfg.path:
        if bucket and key:
            sftp_cfg.path = f"{bucket.rstrip('/')}/{key.lstrip('/')}"
        else:
            raise ValueError("SFTP remote path is required (connection_string, database, or table)")

    directory, filename = split_remote_path(sftp_cfg.path)
    remote_name = filename or sftp_cfg.path

    transport, sftp = connect_sftp(sftp_cfg)
    try:
        remote_path = sftp_cfg.path if directory else f"/{filename}"
        with sftp.file(remote_path, "rb") as f:
            body = f.read()
    finally:
        sftp.close()
        transport.close()

    records, columns, total = _parse_object_body(body, remote_name)
    if known_total_rows is not None:
        total = known_total_rows
    slice_rows = records[offset : offset + limit]

    def cell(v: Any) -> str:
        return cell_to_string(v)

    rows = [[cell(r.get(c)) for c in columns] for r in slice_rows]
    return ReadBatch(headers=columns, rows=rows, offset=offset, total_rows=total)


def list_files(
    *,
    cfg: dict[str, Any],
    directory: str = "",
    **_kwargs: Any,
) -> list[str]:
    """List files in a remote SFTP directory."""
    sftp_cfg = parse_sftp_config(**cfg)
    if not sftp_cfg.host:
        raise ValueError("SFTP host is required")

    path = directory or sftp_cfg.path or "."
    transport, sftp = connect_sftp(sftp_cfg)
    try:
        return [entry.filename for entry in sftp.listdir_attr(path) if not str(entry.filename).startswith(".")]
    finally:
        sftp.close()
        transport.close()
