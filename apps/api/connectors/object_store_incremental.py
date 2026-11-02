"""Merge a new object-store batch into the object that already holds rows.

Single-chunk incremental append and deduped writes land on the exact base
key (``orders/export.json``, not a run token). Replacing that key with only
the new batch drops every row the previous run stored. Multi-chunk append
already isolates ``part-*`` keys under a run token and is left alone.
Overwrite still replaces. A body that cannot be parsed is refused — the live
object is not replaced.

Append concatenates. ``incremental_deduped`` / ``upsert`` merge by key
(last-wins, or keep-first when the duplicate policy is skip). ``rows_written``
stays the new batch; this module only changes the bytes that land.
"""

from __future__ import annotations

import csv
import io
import json
from decimal import Decimal
from typing import Any, Callable

from connectors.object_store_common import (
    ObjectStoreExport,
    resolve_object_store_export_format,
)


class ObjectMergeRefused(RuntimeError):
    """Existing object could not be merged. Callers must not replace it."""


def incremental_merge_strategy(
    sync_mode: str | None,
    *,
    total_chunks: int = 1,
    protect_landed: bool = False,
) -> str | None:
    """``concat``, ``key``, or ``None`` when the new batch replaces the key."""
    if protect_landed or int(total_chunks or 1) > 1:
        return None
    from services.sync_cursor import is_append_sync, is_overwrite_sync, normalize_sync_mode

    mode = normalize_sync_mode(sync_mode or "", default="")
    if not mode or is_overwrite_sync(mode):
        return None
    if is_append_sync(mode):
        return "concat"
    if mode in {"incremental_deduped", "upsert"}:
        return "key"
    return None


def fetch_existing_object(fetch: Callable[[], Any]) -> tuple[bytes | None, str]:
    """Read the live object.

    Returns ``(body, status)`` where status is ``ok``, ``absent``,
    ``unsupported`` (client has no read), or ``error``. ``error`` means the
    key may already hold rows and the caller must not replace it.
    """
    try:
        data = fetch()
    except AttributeError:
        return None, "unsupported"
    except Exception as exc:
        if _read_was_absent(exc):
            return None, "absent"
        return None, "error"
    if data is None:
        return None, "absent"
    if isinstance(data, str):
        data = data.encode("utf-8")
    body = bytes(data)
    if not body.strip():
        return None, "absent"
    return body, "ok"


def prepare_incremental_export(
    *,
    key: str,
    export: ObjectStoreExport,
    fetch: Callable[[], Any],
    sync_mode: str | None,
    total_chunks: int,
    protect_landed: bool,
    created_here: bool,
    target_cols: list[str],
    dest_types: dict[str, str] | None,
    conflict_columns: list[str] | None,
    spill_max_size: int,
    backup_put: Callable[[bytes], None] | None = None,
) -> ObjectStoreExport:
    """Merge when this write would otherwise replace a populated key.

    Returns ``export`` unchanged when there is nothing to merge. Raises
    :class:`ObjectMergeRefused` when the live object must be left in place.

    When a merge lands, ``backup_put`` receives the prior object bytes
    before the merged export is returned — the rollback image a failed
    verification restores so a failed append leaves no rows behind (D116).
    If the snapshot cannot be stored the write is refused instead: the
    live object is not replaced.
    """
    strategy = incremental_merge_strategy(
        sync_mode, total_chunks=total_chunks, protect_landed=protect_landed
    )
    if strategy is None or created_here:
        return export
    existing, status = fetch_existing_object(fetch)
    if status == "error":
        raise ObjectMergeRefused(
            f"Object {key!r} could not be read for incremental merge; "
            "it was not replaced."
        )
    if status != "ok" or not existing:
        return export
    merged = merge_object_bytes(
        key=key,
        existing=existing,
        new_export=export,
        target_cols=target_cols,
        dest_types=dest_types,
        conflict_columns=conflict_columns,
        strategy=strategy,
        spill_max_size=spill_max_size,
    )
    if backup_put is not None:
        try:
            backup_put(existing)
        except Exception as exc:
            merged.close()
            raise ObjectMergeRefused(
                f"Object {key!r} pre-append snapshot could not be stored "
                f"({exc}); the existing object was not replaced."
            ) from exc
    export.close()
    return merged


def merge_object_bytes(
    *,
    key: str,
    existing: bytes,
    new_export: ObjectStoreExport,
    target_cols: list[str],
    dest_types: dict[str, str] | None,
    conflict_columns: list[str] | None,
    strategy: str,
    spill_max_size: int,
) -> ObjectStoreExport:
    """Return a new export that contains the prior rows plus this batch."""
    new_bytes = new_export.read_all()
    content_type = str(new_export.content_type or "application/json")
    fmt = resolve_object_store_export_format(key) or "json"
    if strategy == "concat":
        spliced = _concat_bytes(fmt, existing, new_bytes)
        if spliced is None:
            prior = _parse_records(key, existing)
            incoming = _parse_records(key, new_bytes)
            spliced = _encode_records(
                key,
                prior + incoming,
                target_cols,
                dest_types,
                spill_max_size,
            )
            return spliced
        return _export_from_bytes(spliced, content_type, spill_max_size)
    if strategy != "key":
        raise ObjectMergeRefused(
            f"Object {key!r} incremental merge strategy {strategy!r} is unknown; "
            "the existing object was not replaced."
        )
    keys = [str(c) for c in (conflict_columns or []) if str(c).strip()]
    if not keys:
        raise ObjectMergeRefused(
            f"Object {key!r} incremental merge needs a primary key; "
            "the existing object was not replaced."
        )
    prior = _parse_records(key, existing)
    incoming = _parse_records(key, new_bytes)
    merged = _merge_by_key(prior, incoming, keys)
    return _encode_records(key, merged, target_cols, dest_types, spill_max_size)


def _concat_bytes(fmt: str, existing: bytes, new: bytes) -> bytes | None:
    if fmt == "json":
        return _json_concat(existing, new)
    if fmt == "jsonl":
        return _jsonl_concat(existing, new)
    if fmt in {"csv", "tsv"}:
        return _delimited_concat(existing, new)
    return None


def _json_concat(existing: bytes, new: bytes) -> bytes | None:
    old = existing.strip()
    fresh = new.strip()
    if old == b"[]":
        return fresh
    if fresh == b"[]":
        return old
    if (
        old.startswith(b"[\n")
        and old.endswith(b"\n]")
        and fresh.startswith(b"[\n")
        and fresh.endswith(b"\n]")
    ):
        return old[:-2] + b",\n" + fresh[2:]
    return None


def _jsonl_concat(existing: bytes, new: bytes) -> bytes | None:
    old = existing.strip()
    fresh = new.strip()
    if not old:
        return fresh + (b"\n" if fresh else b"")
    if not fresh:
        return old + b"\n"
    return old + b"\n" + fresh + b"\n"


def _delimited_concat(existing: bytes, new: bytes) -> bytes | None:
    old_header, _old_rest = _first_line(existing)
    new_header, new_rest = _first_line(new)
    if not old_header or old_header != new_header:
        return None
    if not new_rest.strip():
        return existing if existing.endswith(b"\n") else existing + b"\n"
    base = existing if existing.endswith(b"\n") else existing + b"\n"
    return base + new_rest


def _first_line(body: bytes) -> tuple[bytes, bytes]:
    # Split at the first line terminator (LF, CRLF, or bare CR) WITHOUT
    # rewriting the rest — the returned tail is appended to the object
    # byte-for-byte, and a quoted CSV field may legally carry CR (D108).
    n = len(body)
    nl = body.find(b"\n")
    cr = body.find(b"\r")
    if nl < 0 and cr < 0:
        return body, b""
    if nl >= 0 and (cr < 0 or nl < cr):
        return body[:nl], body[nl + 1 :]
    if cr + 1 < n and body[cr + 1 : cr + 2] == b"\n":
        return body[:cr], body[cr + 2 :]
    return body[:cr], body[cr + 1 :]


def _merge_by_key(
    prior: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
    key_cols: list[str],
) -> list[dict[str, Any]]:
    from services.duplicate_policy import current_match_action

    keep_first = current_match_action() == "skip"
    merged = list(prior)
    index: dict[tuple, int] = {}
    for i, row in enumerate(merged):
        identity = _row_identity(row, key_cols)
        if identity is not None and identity not in index:
            index[identity] = i
    for row in incoming:
        identity = _row_identity(row, key_cols)
        if identity is None:
            merged.append(row)
            continue
        slot = index.get(identity)
        if slot is None:
            index[identity] = len(merged)
            merged.append(row)
        elif not keep_first:
            merged[slot] = row
    return merged


def _row_identity(row: dict[str, Any], key_cols: list[str]) -> tuple | None:
    parts: list[str] = []
    any_value = False
    for col in key_cols:
        text = _identity_text(row.get(col))
        if text is not None:
            any_value = True
        parts.append("" if text is None else text)
    if not any_value:
        return None
    return tuple(parts)


def _identity_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value, "f")
    text = str(value).strip()
    return text or None


def _parse_records(key: str, body: bytes) -> list[dict[str, Any]]:
    fmt = resolve_object_store_export_format(key) or "json"
    raw = body.strip()
    if not raw:
        return []
    try:
        records = _parse_records_as_stored(fmt, key, raw, body)
    except ObjectMergeRefused:
        raise
    except Exception as exc:
        raise ObjectMergeRefused(
            f"Object {key!r} could not be parsed for incremental merge ({exc}); "
            "the existing object was not replaced."
        ) from exc
    if records is None:
        raise ObjectMergeRefused(
            f"Object {key!r} format {fmt!r} cannot be merged; "
            "the existing object was not replaced."
        )
    from services.value_serializer import payload_record_wire

    # The merged rows re-enter the export encoder, which decodes the wire: a
    # stored text that spells a sentinel must stay that text (D121).
    return [payload_record_wire(rec) for rec in records]


def _parse_records_as_stored(
    fmt: str, key: str, raw: bytes, body: bytes
) -> list[dict[str, Any]] | None:
    if fmt == "json":
        return _parse_json(raw, key)
    if fmt == "jsonl":
        return _parse_jsonl(raw, key)
    if fmt in {"csv", "tsv"}:
        return _parse_delimited(body, "\t" if fmt == "tsv" else ",", key)
    if fmt == "parquet":
        return _parse_parquet(body, key)
    if fmt == "excel":
        return _parse_xlsx(body, key)
    return None


def _parse_json(raw: bytes, key: str) -> list[dict[str, Any]]:
    data = json.loads(raw.decode("utf-8-sig"), parse_float=Decimal)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list) and all(isinstance(item, dict) for item in data):
        return list(data)
    raise ObjectMergeRefused(
        f"Object {key!r} is not a JSON array of records; "
        "the existing object was not replaced."
    )


def _parse_jsonl(raw: bytes, key: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in raw.decode("utf-8-sig").splitlines():
        if not line.strip():
            continue
        item = json.loads(line, parse_float=Decimal)
        if not isinstance(item, dict):
            raise ObjectMergeRefused(
                f"Object {key!r} JSONL line is not an object; "
                "the existing object was not replaced."
            )
        rows.append(item)
    return rows


def _parse_delimited(body: bytes, delimiter: str, key: str) -> list[dict[str, Any]]:
    from services.csv_codec import iter_csv_records

    text = body.decode("utf-8-sig")
    # newline="" — the csv module requires it; the default universal-newlines
    # mode would fold CR/CRLF inside quoted fields to LF on read (D108).
    reader = iter_csv_records(io.StringIO(text, newline=""), delimiter)
    header = next(reader, None)
    if not header:
        raise ObjectMergeRefused(
            f"Object {key!r} has no header row; the existing object was not replaced."
        )
    # C04: codec cells — unquoted empty is None, quoted "" is ''. Every other
    # cell is the text the object holds: the encoder writes NULL as an empty
    # unquoted cell and a literal verbatim, never a wire spelling (D121).
    return [dict(zip(header, row, strict=False)) for row in reader]


def _parse_parquet(body: bytes, key: str) -> list[dict[str, Any]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ObjectMergeRefused(
            f"Object {key!r} is Parquet and pyarrow is unavailable; "
            "the existing object was not replaced."
        ) from exc
    table = pq.read_table(io.BytesIO(body))
    return table.to_pylist()


def _parse_xlsx(body: bytes, key: str) -> list[dict[str, Any]]:
    from openpyxl import load_workbook

    workbook = load_workbook(io.BytesIO(body), read_only=True, data_only=True)
    try:
        sheet = workbook.active
        cursor = sheet.iter_rows(values_only=True)
        header = next(cursor, None)
        if not header:
            raise ObjectMergeRefused(
                f"Object {key!r} workbook has no header; "
                "the existing object was not replaced."
            )
        cols = [str(c) if c is not None else "" for c in header]
        return [
            {col: value for col, value in zip(cols, row) if col}
            for row in cursor
        ]
    finally:
        workbook.close()


def _encode_records(
    key: str,
    records: list[dict[str, Any]],
    target_cols: list[str],
    dest_types: dict[str, str] | None,
    spill_max_size: int,
) -> ObjectStoreExport:
    from connectors.object_store_common import serialize_object_store_export

    cols = list(target_cols) or _columns_from_records(records)
    tuples = [tuple(row.get(col) for col in cols) for row in records]
    return serialize_object_store_export(
        key=key,
        mapped_rows=tuples,
        target_cols=cols,
        dest_types=dest_types,
        spill_max_size=spill_max_size,
    )


def _columns_from_records(records: list[dict[str, Any]]) -> list[str]:
    cols: list[str] = []
    seen: set[str] = set()
    for row in records:
        for key in row:
            if key not in seen:
                seen.add(key)
                cols.append(key)
    return cols


def _export_from_bytes(data: bytes, content_type: str, spill_max_size: int) -> ObjectStoreExport:
    import tempfile

    spool = tempfile.SpooledTemporaryFile(max_size=max(1, int(spill_max_size)), mode="w+b")
    spool.write(data)
    spool.seek(0)
    return ObjectStoreExport(
        content_type=content_type,
        size=len(data),
        spilled=len(data) > spill_max_size,
        _spool=spool,
    )


def _read_was_absent(exc: BaseException) -> bool:
    if isinstance(exc, (KeyError, FileNotFoundError)):
        return True
    response = getattr(exc, "response", None)
    code = ""
    if isinstance(response, dict):
        code = str((response.get("Error") or {}).get("Code") or "")
        http = str((response.get("ResponseMetadata") or {}).get("HTTPStatusCode") or "")
        if code in {"404", "NoSuchKey", "NotFound"} or http == "404":
            return True
    text = f"{type(exc).__name__} {exc} {code}".lower()
    return any(
        token in text
        for token in ("404", "nosuchkey", "notfound", "not found", "no such key", "blobnotfound")
    )
