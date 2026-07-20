"""Normalize JSON payloads into tabular records (array-of-objects).

Single source of truth for **every** file→destination route (Redis, Snowflake,
MySQL, Postgres, …). Preview, upload, buffered execute, and streaming ingest
must all call these helpers so Map/Validate/Run never disagree on JSON shape.
"""

from __future__ import annotations

import json
from typing import Any, Iterator


# Prefer stable, documented wrappers when several array-of-objects keys exist.
_PREFERRED_WRAPPER_KEYS = (
    "data",
    "items",
    "records",
    "results",
    "rows",
    "countries",
    "features",
    "values",
    "payload",
    "content",
    "list",
    "entries",
)


def _array_of_objects(value: Any) -> list[dict[str, Any]] | None:
    if not (isinstance(value, list) and value and isinstance(value[0], dict)):
        return None
    rows = [r for r in value if isinstance(r, dict)]
    return rows or None


def _unwrap_object(data: dict[str, Any], *, depth: int) -> list[dict[str, Any]] | None:
    """Find the first array-of-objects under ``data`` within ``depth`` levels."""
    preferred: list[dict[str, Any]] | None = None
    fallback: list[dict[str, Any]] | None = None
    nested_dicts: list[dict[str, Any]] = []

    for key, value in data.items():
        rows = _array_of_objects(value)
        if rows is not None:
            key_l = str(key).lower()
            if key_l in _PREFERRED_WRAPPER_KEYS or key in _PREFERRED_WRAPPER_KEYS:
                preferred = rows
                break
            if fallback is None:
                fallback = rows
            continue
        if depth > 0 and isinstance(value, dict):
            nested_dicts.append(value)

    if preferred is not None:
        return preferred
    if fallback is not None:
        return fallback

    for nested in nested_dicts:
        found = _unwrap_object(nested, depth=depth - 1)
        if found is not None:
            return found
    return None


def extract_json_records(data: Any) -> list[dict[str, Any]]:
    """Return row objects from a parsed JSON value.

    Accepted shapes
    ---------------
    - ``[{...}, ...]`` — root array of objects
    - ``{"countries": [{...}, ...], ...}`` — preferred / first root key whose
      value is a non-empty array of objects
    - ``{"response":{"data":[{...}]}}`` — nested envelope (depth ≤ 3)
    - GeoJSON ``{"type":"FeatureCollection","features":[...]}``
    - ``{...}`` — single object treated as one row

    Raises
    ------
    ValueError
        When no object rows can be derived (scalars, array of scalars, empty).
    """
    if isinstance(data, list):
        rows = [r for r in data if isinstance(r, dict)]
        if not rows and data:
            raise ValueError(
                "JSON array must contain objects with field names "
                "(got an array of scalars or mixed non-objects)"
            )
        return rows

    if isinstance(data, dict):
        found = _unwrap_object(data, depth=3)
        if found is not None:
            return found
        # Single record object (no nested row array).
        return [data]

    raise ValueError(
        "JSON must be an array of objects, a wrapper object containing that array, "
        "or a single object record"
    )


def load_json_records(raw: bytes | str) -> list[dict[str, Any]]:
    """Parse bytes/text JSON and extract tabular records."""
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="replace")
    else:
        text = raw
    text = text.lstrip("\ufeff").strip()
    if not text:
        raise ValueError("JSON file is empty")
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON: {exc}") from exc
    return extract_json_records(data)


def detect_ijson_records_prefix(head: bytes) -> str | None:
    """Return an ijson path for array-of-object rows, or None if not streamable that way.

    ``item`` → root array. ``countries.item`` → ``{"countries":[...]}``.
    """
    stripped = head.lstrip().lstrip(b"\xef\xbb\xbf")
    if stripped.startswith(b"["):
        return "item"
    if not stripped.startswith(b"{"):
        return None

    # Lightweight scan for `"key": [` without loading values.
    import re

    for key in _PREFERRED_WRAPPER_KEYS:
        # "countries" <ws> : <ws> [
        pat = rb'"' + key.encode("ascii") + rb'"\s*:\s*\['
        if re.search(pat, head[:65536], flags=re.IGNORECASE):
            return f"{key}.item"

    # Any first `"something": [` at root-ish depth (best effort).
    m = re.search(rb'"([^"\\]+)"\s*:\s*\[', head[:65536])
    if m:
        key = m.group(1).decode("utf-8", errors="replace")
        if key and "." not in key:
            return f"{key}.item"
    return None


def iter_json_record_dicts(
    open_binary,
    content: Any,
    *,
    chunk_size: int = 5000,
) -> Iterator[list[dict[str, Any]]]:
    """Yield batches of dict rows from a JSON file (array or wrapped array).

    Falls back to full parse for single-object files, undecidable wrappers,
    or environments without ``ijson``.
    """

    def _read_all() -> bytes:
        if hasattr(content, "read"):
            return content.read()
        if isinstance(content, (bytes, bytearray)):
            return bytes(content)
        from pathlib import Path

        return Path(content).read_bytes()

    head = b""
    try:
        with open_binary(content) as bio:
            head = bio.read(65536)
    except Exception:
        head = b""

    prefix = detect_ijson_records_prefix(head) if head else None
    if prefix:
        try:
            import ijson
        except ImportError:
            prefix = None

    if prefix:
        batch: list[dict[str, Any]] = []
        with open_binary(content) as bio:
            for obj in ijson.items(bio, prefix):
                if not isinstance(obj, dict):
                    continue
                batch.append(obj)
                if len(batch) >= chunk_size:
                    yield batch
                    batch = []
        if batch:
            yield batch
        return

    records = load_json_records(_read_all())
    for i in range(0, len(records), chunk_size):
        yield records[i : i + chunk_size]
