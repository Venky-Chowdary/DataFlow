"""SFTP file reader — stream payloads to disk before parsing."""

from __future__ import annotations

import hashlib
import stat
from typing import Any

from services.object_streaming import (
    download_for_object_store,
    download_object,
    read_rows_and_schema_from_spill,
)

from connectors.object_store_common import ReadBatch, object_native_types_meta
from connectors.sftp_common import connect_sftp, parse_sftp_config, split_remote_path


def materialize_spill(
    *,
    cfg: dict[str, Any],
    bucket: str = "",
    key: str = "",
) -> tuple[Any, str]:
    """Download the remote file once and return ``(local path, remote name)``.

    Page reads and the uniqueness probe share this spill. Parsing each page
    from byte 0 made a 2M-row uniqueness gate take minutes.
    """
    merged = dict(cfg)
    if bucket:
        merged["database"] = bucket
    if key:
        merged["table"] = key
    sftp_cfg = parse_sftp_config(**merged)
    if not sftp_cfg.host:
        raise ValueError("SFTP host is required")
    if not sftp_cfg.path:
        if bucket and key:
            sftp_cfg.path = f"{bucket.rstrip('/')}/{key.lstrip('/')}"
        else:
            raise ValueError("SFTP remote path is required (connection_string, database, or table)")

    directory, filename = split_remote_path(sftp_cfg.path)
    # Uniqueness / dest-sample pass the schedule table as ``key``. A saved
    # connector only stores the directory, so parse without ``table`` left
    # path="/" and the spill was parsed as UTF-8 text — OOXML failed closed.
    remote_name = filename or key or sftp_cfg.path

    cache_key = f"sftp:{sftp_cfg.host}:{sftp_cfg.port}:{sftp_cfg.path}"
    # SFTP has no ETag. Stat size and mtime once per page so a fresh spill is
    # reused; a changed file still downloads. A failed stat keeps a forced fetch.
    identity = _remote_stat(sftp_cfg)
    force = True
    if identity is not None:
        size, mtime = identity
        cache_key = f"{cache_key}:{size}:{mtime}"
        force = False
    path = download_object(
        cache_key,
        lambda p: download_for_object_store("sftp", p, cfg, bucket, key),
        force=force,
    )
    return path, remote_name


def read_object(
    *,
    cfg: dict[str, Any],
    bucket: str = "",
    key: str = "",
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
) -> ReadBatch:
    path, remote_name = materialize_spill(cfg=cfg, bucket=bucket, key=key)
    headers, rows, total, declared = read_rows_and_schema_from_spill(
        path,
        remote_name,
        offset=offset,
        limit=limit,
        known_total=known_total_rows,
    )
    return ReadBatch(
        headers=headers,
        rows=rows,
        offset=offset,
        total_rows=total,
        # SFTP carries the same CSV/JSON/Parquet payload an upload does, so it
        # owes the engine the same declared types. Without this the identical
        # file lands bigint/numeric/date over an upload and three text columns
        # over SFTP — the object-store defect, one transport later.
        meta=object_native_types_meta(headers, rows, declared),
    )


def _resolved_stat_path(sftp: Any, path: str) -> str:
    """Concrete remote path for a cache stat.

    ``big_*.csv`` is not a file. ``stat`` on the glob always fails, so every
    page downloads the object again. Resolve the same first match the
    download uses, without hashing the bytes.
    """
    from services.file_pickup import is_file_pattern, matches_pattern

    if not path or not is_file_pattern(path):
        return path
    directory, _filename = split_remote_path(path)
    listing = directory or "."
    try:
        entries = sftp.listdir_attr(listing)
    except Exception:
        return path
    for entry in entries:
        name = str(getattr(entry, "filename", "") or "")
        if not name or name.startswith("."):
            continue
        mode = int(getattr(entry, "st_mode", 0) or 0)
        if mode and stat.S_ISDIR(mode):
            continue
        full = f"{listing.rstrip('/')}/{name}" if listing not in {"", ".", "/"} else name
        if matches_pattern(name, path) or matches_pattern(full, path):
            return full
    return path


def _remote_stat(sftp_cfg: Any) -> tuple[int, int] | None:
    """Size and mtime for the cache key. ``None`` when the stat call fails."""
    if not getattr(sftp_cfg, "path", ""):
        return None
    transport = None
    sftp = None
    try:
        transport, sftp = connect_sftp(sftp_cfg)
        st = sftp.stat(_resolved_stat_path(sftp, sftp_cfg.path))
        return int(getattr(st, "st_size", 0) or 0), int(getattr(st, "st_mtime", 0) or 0)
    except Exception:
        return None
    finally:
        if sftp is not None:
            try:
                sftp.close()
            except Exception:
                pass
        if transport is not None:
            try:
                transport.close()
            except Exception:
                pass


def _remote_sha256(sftp: Any, path: str) -> str:
    """Stream the remote bytes into sha256. A listing without this cannot see a rewrite."""
    digest = hashlib.sha256()
    with sftp.open(path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def list_file_stats_on(sftp: Any, directory: str, pattern: str = "") -> list[dict[str, Any]]:
    """Name, size, mtime, and sha256 for files in ``directory``.

    A glob is applied here. ``orders_*.csv`` is not a remote path paramiko
    can ``getfo``. Dotfiles and directories are not pickup candidates.
    """
    from services.file_pickup import is_file_pattern, matches_pattern

    path = directory or "."
    rows: list[dict[str, Any]] = []
    for entry in sftp.listdir_attr(path):
        name = str(getattr(entry, "filename", "") or "")
        if not name or name.startswith("."):
            continue
        mode = int(getattr(entry, "st_mode", 0) or 0)
        if mode and stat.S_ISDIR(mode):
            continue
        if pattern and is_file_pattern(pattern) and not matches_pattern(name, pattern):
            # Also match the path the schedule stored, not only the basename.
            full = f"{path.rstrip('/')}/{name}" if path not in {"", ".", "/"} else name
            if not matches_pattern(full, pattern):
                continue
        remote = f"{path.rstrip('/')}/{name}" if path not in {"", ".", "/"} else name
        mtime = getattr(entry, "st_mtime", None)
        rows.append(
            {
                "name": name,
                "size": int(getattr(entry, "st_size", 0) or 0),
                "mtime": "" if mtime is None else str(int(mtime)),
                "checksum": _remote_sha256(sftp, remote),
            }
        )
    return rows


def list_file_stats(
    *,
    cfg: dict[str, Any],
    directory: str = "",
    pattern: str = "",
    **_kwargs: Any,
) -> list[dict[str, Any]]:
    """Processed-file identity for one SFTP directory: name, size, mtime, sha256."""
    sftp_cfg = parse_sftp_config(**cfg)
    if not sftp_cfg.host:
        raise ValueError("SFTP host is required")
    path = directory or sftp_cfg.path or "."
    transport, sftp = connect_sftp(sftp_cfg)
    try:
        return list_file_stats_on(sftp, path, pattern)
    finally:
        sftp.close()
        transport.close()


def list_files(
    *,
    cfg: dict[str, Any],
    directory: str = "",
    pattern: str = "",
    **_kwargs: Any,
) -> list[str]:
    """List files in a remote SFTP directory, optionally filtered by a glob."""
    return [
        str(row["name"])
        for row in list_file_stats(cfg=cfg, directory=directory, pattern=pattern)
    ]
