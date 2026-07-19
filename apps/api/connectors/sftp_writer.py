"""SFTP object writer — upload JSON/JSONL/CSV exports."""

from __future__ import annotations

import csv
import io
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from connectors.sftp_common import connect_sftp, parse_sftp_config, split_remote_path
from connectors.writer_common import WriteResult as _WriteResult
from connectors.writer_common import (
    _rejected_row_count,
    build_mapped_rows_with_details,
    resolve_target_columns,
    row_checksum,
    to_json_value,
    transform_error_policy,
)
from services.value_serializer import cell_to_string, json_default

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@dataclass
class WriteResult(_WriteResult):
    driver: str = "paramiko"


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
    dest_types = {target_cols[i]: logical_types[i] for i in range(len(target_cols))}
    policy = transform_error_policy(_kwargs.get("error_policy"))
    mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=dest_types,
        error_policy=policy,
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

    rejected_rows = max(
        _rejected_row_count(data_rows, mapped_rows, rejected_details, policy),
        len(data_rows) - len(mapped_rows),
    )

    records = [{c: to_json_value(v, c, dest_types) for c, v in zip(target_cols, row)} for row in mapped_rows]

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
            rejected_details=rejected_details,
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
            rejected_details=rejected_details if "rejected_details" in locals() else [],
        )
