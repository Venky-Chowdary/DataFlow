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

import logging
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
) -> dict[str, Any]:
    """Write an export to disk and describe where it landed.

    ``ValueError`` is raised for a path outside the exports folder and for an
    append the target file cannot carry — both refuse before touching the
    operator's file rather than replacing it.

    Where local-disk destinations are off (the hosted cloud) and an export
    bucket is configured, the export goes to that private bucket instead
    (``services.export_object_store``): one object per job, so an append run
    stores this run's rows as its own object and never rewrites another's.
    """
    from services.export_object_store import (
        exports_use_object_store,
        job_export_workspace,
        store_job_export,
    )
    from services.export_paths import generated_export_path, resolve_export_path
    from services.local_file_policy import local_destination_refusal
    from services.sync_cursor import is_append_sync, resolve_effective_sync_mode

    if exports_use_object_store():
        return store_job_export(destination, export_bytes, job_id=job_id, export_name=export_name)
    # D276: the last door before a job's export lands on this server's own disk
    # (the engine refuses the run before it reads a row; this covers any other caller).
    refusal = local_destination_refusal("file_export", {"kind": "file_export"})
    if refusal:
        raise ValueError(refusal)
    fmt = str(getattr(destination, "format", "") or "")
    ext = os.path.splitext(export_name)[1].lstrip(".") or fmt or "json"
    configured = str(getattr(destination, "output_path", "") or "").strip()
    appending, rows_before = False, None
    legacy_note = ""
    # The export lands in the folder of the job's workspace (audit H-2): two
    # workspaces naming the same ``output_path`` never share a file.
    workspace_id = job_export_workspace(job_id)
    if configured:
        target = resolve_export_path(configured, default_ext=ext, workspace_id=workspace_id)
        export_path = str(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        from services.exit_job_guard import sealed_export_name

        sealed = sealed_export_name(target.name)
        append_mode = is_append_sync(resolve_effective_sync_mode(sync_mode))
        if append_mode and not sealed and not os.path.exists(export_path):
            # Review M3: the file this export has been appending to may still be
            # in the flat folder from before workspace folders. Continue it when
            # this workspace provably owns it; otherwise say a new file started.
            from services.export_migration import adopt_flat_export

            legacy_note = adopt_flat_export(target, workspace_id)
        appending = append_mode and os.path.exists(export_path)
        if sealed:
            # A rehearsal's extract is sealed by its SHA-256: created once, never appended to or replaced.
            try:
                with open(export_path, "xb") as fh:
                    fh.write(export_bytes)
            except FileExistsError as exc:
                raise ValueError(
                    f"{target.name} is the sealed extract of an exit rehearsal and is never overwritten. "
                    "Start a new rehearsal instead."
                ) from exc
        elif appending:
            payload = append_payload(fmt or "csv", export_path, export_bytes)
            rows_before = existing_record_count(fmt or "csv", export_path)
            with open(export_path, "ab") as fh:
                fh.write(payload)
        else:
            with open(export_path, "wb") as fh:
                fh.write(export_bytes)
        name = target.name
    else:
        name = f"export_{job_id}.{ext}"
        export_path = str(generated_export_path(name, workspace_id=workspace_id))
        with open(export_path, "wb") as fh:
            fh.write(export_bytes)
    from pathlib import Path

    from services.transfer_journal import _sha256_file

    sha256, size = _sha256_file(Path(export_path)), os.path.getsize(export_path)
    landed = {
        "filename": name,
        "path": export_path,
        "download_url": f"/api/v1/transfer/download/{name}",
        # The bytes as written: a later read of the file is checked against these.
        "sha256": sha256,
        "size_bytes": size,
    }
    if configured and appending and rows_before is not None:
        from services.dest_precount import PRECOUNT_KEY

        # The file's records before this run appended: the run's own rows are the delta (review 4, H-3).
        landed[PRECOUNT_KEY] = rows_before
    if legacy_note:
        logging.getLogger(__name__).warning("Export %s for job %s: %s", name, job_id, legacy_note)
        landed["legacy_flat_export"] = legacy_note
    return landed


def existing_record_count(fmt: str, path: str) -> int | None:
    """The records an existing export holds (``None``: they could not be counted, so nothing is claimed)."""
    from services.dest_precount import _iter_artifact_records

    try:
        with open(path, "rb") as fh:
            return sum(1 for _ in _iter_artifact_records(fh, name=os.path.basename(path),
                                                         fmt=normalize_export_format(fmt), written_here=True))
    except Exception:  # noqa: BLE001 - an uncounted file leaves the append unproven, never proven
        logging.getLogger(__name__).info("existing export %s could not be counted", os.path.basename(path),
                                         exc_info=True)
        return None


def _first_line(data: bytes) -> bytes:
    idx = data.find(b"\n")
    return data if idx < 0 else data[: idx + 1]


def _existing_head(path: str) -> bytes:
    with open(path, "rb") as fh:
        return fh.read(65536)


def _existing_header(path: str) -> bytes:
    return _first_line(_existing_head(path))


def _strip_trailing_newline(data: bytes) -> bytes:
    """A header line without its line ending, whichever it is (review 4, H-3: LF files from earlier builds)."""
    return data.rstrip(b"\r\n")


#: Field delimiter of each headered delimited format.
_DELIMITERS = {"csv": b",", "tsv": b"\t"}


def _fields(data: bytes, delimiter: bytes):
    """Yield ``(raw, value, quoted, end, well_formed)`` per field of delimited ``data``.

    ``end`` is the bytes that close the field: the delimiter, a record's line
    ending (``\r\n`` or ``\n``) or ``b""`` at the end. A line break inside a
    quoted value stays in the value. ``well_formed`` is false for a quoted
    field whose closing quote is not followed by a delimiter or a line ending.
    """
    i, n = 0, len(data)
    while i < n:
        well_formed = True
        if data[i:i + 1] == b'"':
            j = i + 1
            while j < n:
                if data[j:j + 1] == b'"':
                    if data[j + 1:j + 2] == b'"':
                        j += 2
                        continue
                    break
                j += 1
            raw = data[i:j + 1]
            value = data[i + 1:j].replace(b'""', b'"')
            quoted, k = True, j + 1
            while k < n and data[k:k + 1] not in (delimiter, b"\r", b"\n"):
                well_formed = False
                k += 1
            raw = data[i:k]
        else:
            k = i
            while k < n and data[k:k + 1] not in (delimiter, b"\n") and data[k:k + 2] != b"\r\n":
                k += 1
            raw = value = data[i:k]
            quoted = False
        if k >= n:
            end = b""
        elif data[k:k + 1] == delimiter:
            end = delimiter
        elif data[k:k + 2] == b"\r\n":
            end = b"\r\n"
        else:
            end = data[k:k + 1]
        yield raw, value, quoted, end, well_formed
        i = k + len(end)


_BOM = b"\xef\xbb\xbf"


def _never_quotes(head: bytes, delimiter: bytes) -> bool:
    """Whether an existing file shows it was written without quoting (a quote left bare inside a value)."""
    for _raw, value, quoted, end, well_formed in _fields(head, delimiter):
        if (not quoted and b'"' in value) or not well_formed:
            return True
        if quoted and not end:
            return False  # the sample ends inside a quoted value: nothing more to read
    return False


def _in_existing_dialect(body: bytes, delimiter: bytes, eol: bytes, *, unquoted: bool) -> bytes:
    """This run's rows (written CRLF and quoted) re-spelled in the existing file's line ending and quoting.

    ``unquoted``: the existing file never quoted (an earlier build's TSV), so a
    value quoted only for a quote or for being empty text is written bare as it
    did; a value holding the delimiter or a line break stays quoted, or it
    would not stay one cell.
    """
    out = bytearray()
    for raw, value, quoted, end, _ok in _fields(body, delimiter):
        if unquoted and quoted and not any(c in value for c in (delimiter, b"\r", b"\n")):
            out += value
        else:
            out += raw
        out += eol if end in (b"\r\n", b"\n") else end
    return bytes(out)


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
    separator = b"\n"
    if f in _HEADERED_FORMATS:
        new_header = _first_line(data)
        head = _existing_head(path)
        old_header = _first_line(head)
        # A byte order mark (a file re-saved by Excel) is not part of the first column's name (review 5).
        if _strip_trailing_newline(old_header.removeprefix(_BOM)) != _strip_trailing_newline(new_header):
            raise ValueError(
                "Append refused: the existing export header "
                f"({_strip_trailing_newline(old_header).decode('utf-8', 'replace')}) "
                "does not match this run's columns "
                f"({_strip_trailing_newline(new_header).decode('utf-8', 'replace')}) "
                "— appending would misalign every value."
            )
        body = data[len(new_header):]
        delimiter = _DELIMITERS.get(f)
        # The existing file's line ending; a header with none (a header-only file) takes this run's. The
        # separator before the appended rows uses it too, so one file never mixes line endings (review 5).
        source = old_header if old_header.endswith(b"\n") else new_header
        separator = eol = b"\r\n" if source.endswith(b"\r\n") else b"\n"
        if delimiter is not None and old_header.endswith(b"\n"):
            # Append in the existing file's own line ending, and its quoting (review 4, H-3): an earlier build
            # wrote a file-source export with LF, and never quoted a TSV. This build writes CRLF, so an LF TSV
            # is that build's, whether or not its first bytes show a bare quote (review 5, M-4).
            unquoted = f == "tsv" and (eol == b"\n" or _never_quotes(head[len(old_header):], delimiter))
            if eol != b"\r\n" or unquoted:
                body = _in_existing_dialect(body, delimiter, eol, unquoted=unquoted)
    with open(path, "rb") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        if size:
            fh.seek(size - 1)
            needs_newline = fh.read(1) != b"\n"
        else:
            needs_newline = False
    return (separator + body) if needs_newline else body
