"""Append semantics for file-export destinations.

A file export used to be written with ``wb`` whatever the sync mode asked for,
so an append into an existing export replaced the operator's file and the run
still reported success — silent loss of everything the previous run had landed.

Only line-delimited text formats can carry rows appended to an existing file.
Container formats (xlsx, parquet, avro, orc, a single JSON array, xml) hold a
footer/root that a byte append corrupts, so append into an existing one is
refused rather than silently turned into a replace.
"""

from __future__ import annotations

import os
from typing import Any

# Line-delimited exports: one record per line, at most one header line.
APPENDABLE_EXPORT_FORMATS: frozenset[str] = frozenset(
    {"csv", "tsv", "jsonl", "ndjson", "fixed_width", "fwf"}
)

_HEADERED_FORMATS: frozenset[str] = frozenset({"csv", "tsv", "fixed_width"})


def normalize_export_format(fmt: str) -> str:
    f = (fmt or "").strip().lower().lstrip(".")
    if f == "ndjson":
        return "jsonl"
    if f == "fwf":
        return "fixed_width"
    return f


def export_append_refusal(fmt: str, path: str) -> str:
    """Why this append cannot run, or ``""`` when it can.

    An append into a file that does not exist yet is a create — always allowed.
    """
    if not path or not os.path.exists(path):
        return ""
    f = normalize_export_format(fmt)
    if f in APPENDABLE_EXPORT_FORMATS:
        return ""
    return (
        f"Append into an existing {f or 'file'} export is not supported — "
        f"{os.path.basename(path)} is a container format whose structure a row "
        "append would corrupt. Choose overwrite, or export to a new path."
    )


def land_export_bytes(
    destination: Any,
    export_bytes: bytes,
    *,
    job_id: str,
    sync_mode: str,
    export_name: str,
) -> dict[str, str]:
    """Write an export to disk and describe where it landed.

    ``ValueError`` is raised for a path outside the exports folder and for an
    append the target file cannot carry — both refuse before touching the
    operator's file rather than replacing it.
    """
    from services.export_paths import generated_export_path, resolve_export_path
    from services.sync_cursor import is_append_sync, resolve_effective_sync_mode

    fmt = str(getattr(destination, "format", "") or "")
    ext = os.path.splitext(export_name)[1].lstrip(".") or fmt or "json"
    configured = str(getattr(destination, "output_path", "") or "").strip()
    if configured:
        target = resolve_export_path(configured, default_ext=ext)
        export_path = str(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        appending = is_append_sync(
            resolve_effective_sync_mode(sync_mode)
        ) and os.path.exists(export_path)
        if appending:
            payload = append_payload(fmt or "csv", export_path, export_bytes)
            with open(export_path, "ab") as fh:
                fh.write(payload)
        else:
            with open(export_path, "wb") as fh:
                fh.write(export_bytes)
        name = target.name
    else:
        name = f"export_{job_id}.{ext}"
        export_path = str(generated_export_path(name))
        with open(export_path, "wb") as fh:
            fh.write(export_bytes)
    return {
        "filename": name,
        "path": export_path,
        "download_url": f"/api/v1/transfer/download/{name}",
    }


def _first_line(data: bytes) -> bytes:
    idx = data.find(b"\n")
    return data if idx < 0 else data[: idx + 1]


def _existing_header(path: str) -> bytes:
    with open(path, "rb") as fh:
        return _first_line(fh.read(65536))


def _strip_trailing_newline(data: bytes) -> bytes:
    return data[:-1] if data.endswith(b"\n") else data


def append_payload(fmt: str, path: str, data: bytes) -> bytes:
    """The bytes to append to ``path`` so the file keeps one header and all rows.

    Raises ``ValueError`` when the existing file's header disagrees with this
    run's columns: appending under a stale header would misalign every value.
    """
    f = normalize_export_format(fmt)
    refusal = export_append_refusal(f, path)
    if refusal:
        raise ValueError(refusal)
    body = data
    if f in _HEADERED_FORMATS:
        new_header = _first_line(data)
        old_header = _existing_header(path)
        if _strip_trailing_newline(old_header) != _strip_trailing_newline(new_header):
            raise ValueError(
                "Append refused: the existing export header "
                f"({_strip_trailing_newline(old_header).decode('utf-8', 'replace')}) "
                "does not match this run's columns "
                f"({_strip_trailing_newline(new_header).decode('utf-8', 'replace')}) "
                "— appending would misalign every value."
            )
        body = data[len(new_header):]
    with open(path, "rb") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        if size:
            fh.seek(size - 1)
            needs_newline = fh.read(1) != b"\n"
        else:
            needs_newline = False
    return (b"\n" + body) if needs_newline else body
