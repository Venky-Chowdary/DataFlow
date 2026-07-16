"""SFTP object writer — upload JSON/JSONL/CSV exports."""

from __future__ import annotations

import csv
import io
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from connectors.sftp_common import connect_sftp, parse_sftp_config, split_remote_path
from connectors.writer_common import build_mapped_rows, resolve_target_columns, row_checksum
from services.value_serializer import cell_to_string, json_default

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@dataclass
class WriteResult:
    ok: bool
    rows_written: int
    table_name: str
    target_schema: str
    checksum: str
    chunks_completed: int
    error: str | None = None
    driver: str = "paramiko"
    rejected_rows: int = 0
    warnings: list[str] = field(default_factory=list)


def write_mapped_rows(
    *,
    connection_string: str = "",
    host: str = "",
    port: int = 0,
    database: str = "",
    username: str = "",
    password: str = "",
    schema: str = "",
    table_name: str = "",
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Any | None = None,
    **_kwargs: Any,
) -> WriteResult:
    """Upload mapped rows as a CSV/JSON file to an SFTP server."""
    cfg = parse_sftp_config(
        connection_string=connection_string,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        table=table_name,
    )
    if not cfg.host:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="SFTP host is required. Use an sftp:// URL or the host/port fields.",
        )
    if not cfg.path:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="SFTP remote path is required. Provide it via the connection_string or database/table fields.",
        )

    target_cols, logical_types = resolve_target_columns(mappings, column_types, preserve_case=True)
    mapped_rows, transform_errors = build_mapped_rows(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types={target_cols[i]: logical_types[i] for i in range(len(target_cols))},
        preserve_case=True,
    )

    directory, filename = split_remote_path(cfg.path)
    ext = (filename.rsplit(".", 1)[-1] if "." in filename else "").lower()
    if ext in ("csv", "jsonl", "json", "tsv"):
        fmt = ext
    else:
        fmt = "csv"
        if not filename.endswith(".csv"):
            filename = f"{filename.rstrip('/')}.csv"
            cfg.path = f"{directory.rstrip('/')}/{filename}" if directory else f"/{filename}"

    rejected_rows = len(data_rows) - len(mapped_rows)

    def _to_json_value(value: Any, col: str) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return value
            try:
                from services.type_system import normalize_logical_type
            except Exception:
                normalize_logical_type = lambda x: str(x or "").lower()
            ctype = normalize_logical_type({target_cols[i]: logical_types[i] for i in range(len(target_cols))}.get(col, ""))
            if ctype in {"json", "array", "object", "struct"}:
                try:
                    return json.loads(text, parse_float=float, parse_constant=lambda v: None)
                except json.JSONDecodeError:
                    return value
            if ctype in {"text", "string", "varchar", "uuid", "binary", "date", "datetime", "time"}:
                return value
            try:
                return json.loads(text, parse_float=float, parse_constant=lambda v: None)
            except json.JSONDecodeError:
                return value
        return value

    records = [{c: _to_json_value(v, c) for c, v in zip(target_cols, row)} for row in mapped_rows]

    if fmt == "csv" or fmt == "tsv":
        delimiter = "\t" if fmt == "tsv" else ","
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=target_cols, delimiter=delimiter, extrasaction="ignore")
        writer.writeheader()
        writer.writerows([{c: cell_to_string(v) for c, v in r.items()} for r in records])
        body = buf.getvalue().encode("utf-8")
    elif fmt == "jsonl":
        body = "\n".join(json.dumps(r, default=json_default, ensure_ascii=False, allow_nan=False) for r in records).encode("utf-8")
    else:
        body = json.dumps(records, indent=2, default=json_default, ensure_ascii=False, allow_nan=False).encode("utf-8")

    try:
        transport, sftp = connect_sftp(cfg)
        try:
            if directory:
                try:
                    sftp.stat(directory)
                except Exception:
                    # Best-effort create directory chain
                    parts = [p for p in directory.split("/") if p]
                    current = ""
                    for part in parts:
                        current += f"/{part}"
                        try:
                            sftp.stat(current)
                        except Exception:
                            sftp.mkdir(current)
            with sftp.file(cfg.path, "wb") as f:
                f.write(body)
        finally:
            sftp.close()
            transport.close()

        if on_checkpoint:
            on_checkpoint(1, 1, len(records))

        return WriteResult(
            ok=True,
            rows_written=len(records),
            table_name=filename,
            target_schema=cfg.host,
            checksum=row_checksum(mapped_rows, target_cols),
            chunks_completed=1,
            warnings=transform_errors[:10],
            rejected_rows=rejected_rows,
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=filename if "filename" in locals() else table_name,
            target_schema=cfg.host,
            checksum="",
            chunks_completed=0,
            error=f"SFTP write failed: {exc}",
        )
