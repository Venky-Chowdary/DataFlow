"""SFTP file reader — stream payloads to disk before parsing."""

from __future__ import annotations

from typing import Any

from connectors.object_store_common import ReadBatch
from connectors.sftp_common import connect_sftp, parse_sftp_config, split_remote_path
from services.object_streaming import (
    download_for_object_store,
    download_object,
    read_rows_from_spill,
)


def read_object(
    *,
    cfg: dict[str, Any],
    bucket: str = "",
    key: str = "",
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
) -> ReadBatch:
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

    cache_key = f"sftp:{sftp_cfg.host}:{sftp_cfg.port}:{sftp_cfg.path}"
    path = download_object(
        cache_key,
        lambda p: download_for_object_store("sftp", p, cfg, bucket, key),
    )
    headers, rows, total = read_rows_from_spill(
        path,
        remote_name,
        offset=offset,
        limit=limit,
        known_total=known_total_rows,
    )
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)


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
