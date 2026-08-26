"""Normalize JSON payloads into tabular records (array-of-objects).

Single source of truth for **every** file→destination route (Redis, Snowflake,
MySQL, Postgres, …). Preview, upload, buffered execute, and streaming ingest
must all call these helpers so Map/Validate/Run never disagree on JSON shape.

When multiple array-of-object collections exist, refuse silent partial ingest
unless ``records_path`` selects one (Airbyte-class trap).
"""

from __future__ import annotations

import json
from pathlib import Path
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
        from services.value_serializer import json_loads_exact

        data = json_loads_exact(text)
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


def ijson_items_exact(source: Any, prefix: str, **config: Any) -> Iterator[Any]:
    """``ijson.items`` with write-path number identity.

    Default ijson already uses Decimal (``use_float=False``). This helper
    makes that pin explicit and demotes IEEE-exact fractions so StAX matches
    ``json_loads_exact`` (``1.5`` is float; ``1.234567890123456789`` stays
    Decimal). ``use_float=True`` is refused — that is the IEEE invent path.
    """
    import ijson
    from services.value_serializer import demote_exact_json

    if config.get("use_float"):
        raise ValueError(
            "ijson_items_exact refuses use_float=True — that invents IEEE"
        )
    config["use_float"] = False
    for obj in ijson.items(source, prefix, **config):
        yield demote_exact_json(obj)


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
            for obj in ijson_items_exact(bio, prefix):
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


def _json_path_depth(path: str) -> int:
    if not path:
        return 0
    return path.count(".") + 1


def _json_unique_from_stats(
    acc: dict[str, list[int]],
) -> tuple[int, str | None] | None:
    """Unique shallowest array-of-object as ``(n, path)``.

    A collection is an array of objects (maps ≥ 1, no scalars, no nested
    arrays). Nested inner lists lose to the outer path. Sibling collections
    at the same depth stay unmeasured — never guess ``data`` over ``items``.
    Ingest preferred-wrapper ranking and single-object-as-one-row are not
    this identity. Scalar arrays are unmeasured, not dest=N. Empty wrapper
    is ``(0, path)``. COUNT returns ``n``; Gate-8 emits dicts at ``path``.
    """
    objects: list[tuple[int, str, int]] = []
    empties: list[tuple[int, str]] = []
    for path, (map_n, scalar_n, arr_n) in acc.items():
        depth = _json_path_depth(path)
        if map_n >= 1 and scalar_n == 0 and arr_n == 0:
            objects.append((depth, path, map_n))
        elif map_n == 0 and scalar_n == 0 and arr_n == 0:
            empties.append((depth, path))
    if objects:
        min_depth = min(item[0] for item in objects)
        at_min = [item for item in objects if item[0] == min_depth]
        if len(at_min) != 1:
            return None
        return at_min[0][2], at_min[0][1]
    if not empties:
        return None
    min_depth = min(item[0] for item in empties)
    if min_depth > 1:
        return None
    at_min = [item for item in empties if item[0] == min_depth]
    if len(at_min) != 1:
        return None
    return 0, at_min[0][1]


def _json_count_from_stats(acc: dict[str, list[int]]) -> int | None:
    """Unique shallowest array-of-object, else unique empty wrapper 0, else None."""
    found = _json_unique_from_stats(acc)
    return None if found is None else found[0]


def _json_collect_array_stats(data: Any, path: str = "") -> dict[str, list[int]]:
    """DOM fallback: same path stats ijson.parse would accumulate."""
    acc: dict[str, list[int]] = {}
    if isinstance(data, list):
        map_n = scalar_n = arr_n = 0
        for item in data:
            if isinstance(item, dict):
                map_n += 1
            elif isinstance(item, list):
                arr_n += 1
            else:
                scalar_n += 1
        bucket = acc.setdefault(path, [0, 0, 0])
        bucket[0] += map_n
        bucket[1] += scalar_n
        bucket[2] += arr_n
        child = "item" if not path else f"{path}.item"
        for item in data:
            if isinstance(item, dict):
                nested = _json_collect_array_stats(item, child)
                for key, counts in nested.items():
                    dest = acc.setdefault(key, [0, 0, 0])
                    dest[0] += counts[0]
                    dest[1] += counts[1]
                    dest[2] += counts[2]
            elif isinstance(item, list):
                nested = _json_collect_array_stats(item, child)
                for key, counts in nested.items():
                    dest = acc.setdefault(key, [0, 0, 0])
                    dest[0] += counts[0]
                    dest[1] += counts[1]
                    dest[2] += counts[2]
        return acc
    if isinstance(data, dict):
        for key, value in data.items():
            child = str(key) if not path else f"{path}.{key}"
            nested = _json_collect_array_stats(value, child)
            for nested_key, counts in nested.items():
                dest = acc.setdefault(nested_key, [0, 0, 0])
                dest[0] += counts[0]
                dest[1] += counts[1]
                dest[2] += counts[2]
        return acc
    return acc


def _count_json_records_dom(text: str) -> int | None:
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    return _json_count_from_stats(_json_collect_array_stats(data))


def _json_stax_unique(
    source: Any, parse: Any
) -> tuple[int, str | None] | None:
    """ijson.parse walk. Unique ``(n, path)``, else unmeasured."""
    stack: list[tuple[str, list[int], str]] = []
    acc: dict[str, list[int]] = {}
    for prefix, event, _value in parse(source):
        if event == "start_array":
            if stack and stack[-1][0] == "array":
                stack[-1][1][2] += 1
            stack.append(("array", [0, 0, 0], str(prefix or "")))
        elif event == "end_array":
            if not stack:
                return None
            kind, counts, path = stack.pop()
            if kind != "array":
                return None
            bucket = acc.setdefault(path, [0, 0, 0])
            bucket[0] += counts[0]
            bucket[1] += counts[1]
            bucket[2] += counts[2]
        elif event == "start_map":
            if stack and stack[-1][0] == "array":
                stack[-1][1][0] += 1
            stack.append(("map", [0, 0, 0], str(prefix or "")))
        elif event == "end_map":
            if not stack:
                return None
            stack.pop()
        elif event in {"string", "number", "boolean", "null"}:
            if stack and stack[-1][0] == "array":
                stack[-1][1][1] += 1
    if stack:
        return None
    return _json_unique_from_stats(acc)


def _count_json_records_stax(source: Any, parse: Any) -> int | None:
    """ijson.parse walk. One object at a time; empty array shells stay O(depth)."""
    found = _json_stax_unique(source, parse)
    return None if found is None else found[0]


def _json_count_open(content: bytes | str | Path) -> tuple[Any, Any]:
    from services.dest_precount import artifact_byte_source

    return artifact_byte_source(content)


def _json_count_as_text(content: bytes | str | Path) -> str | None:
    try:
        if isinstance(content, Path):
            return content.read_text(encoding="utf-8")
        if isinstance(content, bytes):
            return content.decode("utf-8")
        if isinstance(content, str):
            return content
    except (OSError, UnicodeDecodeError):
        return None
    return None


def count_json_records(content: bytes | str | Path) -> int | None:
    """Dest-engine record COUNT of tabular JSON. Never ingest fallbacks.

    Population is the unique array-of-object the ingest parser already
    discovers — streamed, not ``json.loads`` of the whole export. Empty
    ``[]`` / ``{"records":[]}`` is 0. One object in that array is 1.
    Sibling collections at the same depth stay unmeasured — never rank
    ``data`` over ``items``. A document object is unmeasured, not dest=1
    (ingest ``extract_json_records`` treats a lone object as one row; this
    COUNT does not). An array of scalars is unmeasured, not dest=N.
    Malformed / missing parser stay unmeasured, not dest=0.

    Walk is ``ijson.parse`` (O(record) not O(document)). A stream error is
    unmeasured; do not then ``json.loads`` the same poison file. ``json.loads``
    remains the ImportError fallback when ijson is absent. Path inputs are
    counted from disk; bytes (object-store GET) stream from a buffer already
    in RAM. Local gzip JSON streams; object-store GET gzip streams through
    a caller-owned ``GzipFile``. The ImportError ``json.loads`` fallback
    does not slurp a gzip path (stays unmeasured). Gate-8 cell checksum
    reuses this uniqueness via ``iter_json_dicts`` (second ijson pass at
    the discovered path; one-shot GET is spooled).
    """
    try:
        import ijson
    except ImportError:
        text = _json_count_as_text(content)
        if text is None:
            return None
        return _count_json_records_dom(text)
    closer = None
    try:
        source, closer = _json_count_open(content)
        return _count_json_records_stax(source, ijson.parse)
    except (OSError, UnicodeEncodeError, TypeError):
        return None
    except Exception:
        return None
    finally:
        if closer is not None:
            try:
                closer()
            except Exception:
                pass


def _json_items_prefix(path: str) -> str:
    """ijson.items prefix for objects inside the COUNT array path.

    Root array path ``""`` → ``item``. Wrapped ``records`` → ``records.item``.
    Nested ``[[{...}]]`` COUNT path ``item`` → ``item.item``.
    """
    return "item" if not path else f"{path}.item"


def _json_iter_dicts_at_path(source: Any, items: Any, path: str) -> Any:
    """Second ijson pass: emit objects at the unique COUNT path."""
    from services.dest_precount import UnmeasuredArtifact

    for rec in items(source, _json_items_prefix(path)):
        if not isinstance(rec, dict):
            raise UnmeasuredArtifact("json_array_non_object")
        yield rec


def _json_iter_dicts_at_dom_path(data: Any, target: str, cur: str = "") -> Any:
    """DOM emit of objects COUNT would attribute to ``target``. Same path algebra."""
    if isinstance(data, list):
        if cur == target:
            for item in data:
                if isinstance(item, dict):
                    yield item
            return
        child = "item" if not cur else f"{cur}.item"
        for item in data:
            yield from _json_iter_dicts_at_dom_path(item, target, child)
        return
    if isinstance(data, dict):
        for key, value in data.items():
            child = str(key) if not cur else f"{cur}.{key}"
            yield from _json_iter_dicts_at_dom_path(value, target, child)


def _iter_json_dicts_dom(text: str) -> Any:
    """ImportError fallback when ijson is absent. Same unique-path as COUNT."""
    from services.dest_precount import UnmeasuredArtifact

    try:
        from services.value_serializer import json_loads_exact

        data = json_loads_exact(text)
    except (json.JSONDecodeError, ValueError) as exc:
        raise UnmeasuredArtifact("json_unparseable") from exc
    found = _json_unique_from_stats(_json_collect_array_stats(data))
    if found is None:
        raise UnmeasuredArtifact("json_unmeasured")
    n, path = found
    if n == 0 or path is None:
        return
    yielded = 0
    for rec in _json_iter_dicts_at_dom_path(data, path):
        yielded += 1
        yield rec
    if yielded != n:
        raise UnmeasuredArtifact("json_checksum_count_mismatch")


def iter_json_dicts(content: bytes | str | Path) -> Any:
    """Same unique-path population as ``count_json_records``, as dicts for Gate-8.

    Pass 1 is the COUNT ijson unique-path walk. Pass 2 emits objects at
    that path. A one-shot GET is spooled once, then both passes read the
    spool — never a prefix digest, never ingest ``extract_json_records``
    (preferred-wrapper ranking / lone-object-as-one). Ambiguous siblings,
    document objects, scalar arrays, and malformed raise
    ``UnmeasuredArtifact``. Empty well-formed yields nothing.
    """
    from services.dest_precount import UnmeasuredArtifact, rewindable_byte_source

    try:
        import ijson
    except ImportError:
        text = _json_count_as_text(content)
        if text is None:
            raise UnmeasuredArtifact("json_unreadable")
        yield from _iter_json_dicts_dom(text)
        return
    closer = None
    spool_closer = None
    try:
        source, closer = _json_count_open(content)
        rewindable, spool_closer = rewindable_byte_source(source)
        found = _json_stax_unique(rewindable, ijson.parse)
        if found is None:
            raise UnmeasuredArtifact("json_unmeasured")
        n, path = found
        if n == 0 or path is None:
            return
        rewindable.seek(0)
        yielded = 0
        for rec in _json_iter_dicts_at_path(rewindable, ijson_items_exact, path):
            yielded += 1
            yield rec
        if yielded != n:
            raise UnmeasuredArtifact("json_checksum_count_mismatch")
    except UnmeasuredArtifact:
        raise
    except (OSError, UnicodeEncodeError, TypeError) as exc:
        raise UnmeasuredArtifact("json_unreadable") from exc
    except Exception as exc:
        raise UnmeasuredArtifact("json_unparseable") from exc
    finally:
        if spool_closer is not None:
            try:
                spool_closer()
            except Exception:
                pass
        if closer is not None:
            try:
                closer()
            except Exception:
                pass
