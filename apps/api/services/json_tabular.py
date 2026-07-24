"""Normalize JSON payloads into tabular records (array-of-objects).

Single source of truth for **every** file→destination route (Redis, Snowflake,
MySQL, Postgres, …). Preview, upload, buffered execute, and streaming ingest
must all call these helpers so Map/Validate/Run never disagree on JSON shape.

When multiple array-of-object collections exist, refuse silent partial ingest
unless ``records_path`` selects one (Airbyte-class trap).
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


def _dig_path(data: Any, path: str) -> Any:
    cur = data
    for part in (path or "").split("."):
        if not part:
            continue
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def discover_array_of_object_paths(
    data: dict[str, Any],
    *,
    depth: int = 3,
    prefix: str = "",
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Return ``(dotted_path, rows)`` for every array-of-objects under ``data``."""
    found: list[tuple[str, list[dict[str, Any]]]] = []
    for key, value in data.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        rows = _array_of_objects(value)
        if rows is not None:
            found.append((path, rows))
            continue
        if depth > 0 and isinstance(value, dict):
            found.extend(discover_array_of_object_paths(value, depth=depth - 1, prefix=path))
    return found


def _select_among_candidates(
    candidates: list[tuple[str, list[dict[str, Any]]]],
) -> list[dict[str, Any]]:
    if not candidates:
        raise ValueError("No array-of-object collections found in JSON")
    if len(candidates) == 1:
        return candidates[0][1]

    preferred: list[tuple[str, list[dict[str, Any]], int]] = []
    for path, rows in candidates:
        leaf = path.split(".")[-1].lower()
        if leaf in _PREFERRED_WRAPPER_KEYS:
            rank = _PREFERRED_WRAPPER_KEYS.index(leaf)  # type: ignore[arg-type]
            preferred.append((path, rows, rank))

    if len(preferred) == 1:
        return preferred[0][1]
    if len(preferred) > 1:
        preferred.sort(key=lambda t: t[2])
        # Multiple preferred wrappers (e.g. data + items) — pick canonical order,
        # but only when they share the same parent path depth uniqueness is unclear
        # across siblings. Prefer lowest rank; if two at same rank, fail closed.
        best_rank = preferred[0][2]
        top = [p for p in preferred if p[2] == best_rank]
        if len(top) == 1:
            return top[0][1]

    paths = ", ".join(p for p, _ in candidates)
    raise ValueError(
        f"JSON has multiple array-of-object collections ({paths}). "
        "Set records_path to select one — refuse silent partial ingest."
    )


def extract_json_records(data: Any, *, records_path: str | None = None) -> list[dict[str, Any]]:
    """Return row objects from a parsed JSON value.

    Accepted shapes
    ---------------
    - ``[{...}, ...]`` — root array of objects
    - ``{"countries": [{...}, ...], ...}`` — preferred / single root key whose
      value is a non-empty array of objects
    - ``{"response":{"data":[{...}]}}`` — nested envelope (depth ≤ 3)
    - GeoJSON ``{"type":"FeatureCollection","features":[...]}``
    - ``{...}`` — single object treated as one row

    Raises
    ------
    ValueError
        When no object rows can be derived, or when multiple sibling collections
        exist without an explicit ``records_path``.
    """
    path = (records_path or "").strip()
    if path:
        if isinstance(data, list) and path in {"item", "$", "root"}:
            rows = [r for r in data if isinstance(r, dict)]
        else:
            target = _dig_path(data, path)
            if isinstance(target, list):
                rows = [r for r in target if isinstance(r, dict)]
            else:
                rows = []
        if not rows:
            raise ValueError(f"JSON records_path={path!r} did not resolve to an array of objects")
        return rows

    if isinstance(data, list):
        rows = [r for r in data if isinstance(r, dict)]
        if not rows and data:
            raise ValueError(
                "JSON array must contain objects with field names "
                "(got an array of scalars or mixed non-objects)"
            )
        return rows

    if isinstance(data, dict):
        candidates = discover_array_of_object_paths(data, depth=3)
        if candidates:
            return _select_among_candidates(candidates)
        # Single record object (no nested row array).
        return [data]

    raise ValueError(
        "JSON must be an array of objects, a wrapper object containing that array, "
        "or a single object record"
    )


def load_json_records(raw: bytes | str, *, records_path: str | None = None) -> list[dict[str, Any]]:
    """Parse bytes/text JSON and extract tabular records."""
    if isinstance(raw, bytes):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(
                f"JSON is not valid UTF-8 ({exc}); refuse silent byte replacement"
            ) from exc
    else:
        text = raw
    text = text.lstrip("\ufeff").strip()
    if not text:
        raise ValueError("JSON file is empty")
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON: {exc}") from exc
    return extract_json_records(data, records_path=records_path)


def detect_ijson_records_prefix(head: bytes) -> str | None:
    """Return an ijson path for array-of-object rows, or None if not streamable that way.

    ``item`` → root array. ``countries.item`` → ``{"countries":[...]}``.
    When multiple preferred wrappers appear, prefer canonical order (data before items).
    """
    stripped = head.lstrip().lstrip(b"\xef\xbb\xbf")
    if stripped.startswith(b"["):
        return "item"
    if not stripped.startswith(b"{"):
        return None

    import re

    hits: list[tuple[int, str]] = []
    for key in _PREFERRED_WRAPPER_KEYS:
        pat = rb'"' + key.encode("ascii") + rb'"\s*:\s*\['
        if re.search(pat, head[:65536], flags=re.IGNORECASE):
            hits.append((_PREFERRED_WRAPPER_KEYS.index(key), key))
    if hits:
        hits.sort(key=lambda t: t[0])
        return f"{hits[0][1]}.item"

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
