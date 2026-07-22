"""JSON / nested document intelligence — flattening and type recommendations."""

from __future__ import annotations

import json
import os
import re
from typing import Any

DOT_PATH = re.compile(r"^[a-zA-Z_][\w.]*\.\w+")
JSON_PREFIX = re.compile(r"^[\[{]")

# Cap expanded nested keys so sparse Mongo docs cannot explode column counts.
MAX_FLATTENED_KEYS = 128
DEFAULT_FLATTEN_DEPTH = 2


def _looks_like_json(value: Any) -> bool:
    if isinstance(value, (dict, list)):
        return True
    if not isinstance(value, str):
        return False
    s = value.strip()
    return bool(JSON_PREFIX.match(s))


def flatten_column_recommendations(
    columns: list[str],
    sample_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, str]]:
    """Suggest flatten targets for nested JSON Compass would store as BSON."""
    out: list[dict[str, str]] = []
    rows = sample_rows or []

    for col in columns:
        if DOT_PATH.match(col):
            out.append({
                "column": col,
                "kind": "dot_notation",
                "flatten_target": col.replace(".", "_"),
                "detail": "Dot-path field — map to typed warehouse column",
            })
            continue

        sample_val: Any = None
        for row in rows[:20]:
            if col in row and row[col] is not None:
                sample_val = row[col]
                break

        if not _looks_like_json(sample_val):
            continue

        if isinstance(sample_val, list):
            kind = "array"
            detail = "Array — explode rows or JSON-serialize per destination"
        elif isinstance(sample_val, dict):
            kind = "nested_object"
            detail = "Nested object — flatten to columns or VARIANT"
        else:
            try:
                parsed = json.loads(str(sample_val))
                kind = "array" if isinstance(parsed, list) else "nested_object"
                detail = "JSON string — parse and flatten before warehouse load"
            except json.JSONDecodeError:
                continue

        out.append({
            "column": col,
            "kind": kind,
            "flatten_target": f"{col}_json",
            "detail": detail,
        })

    return out[:12]


def flatten_document(
    doc: dict[str, Any],
    *,
    sep: str = "_",
    max_depth: int = DEFAULT_FLATTEN_DEPTH,
    keep_parent: bool = True,
) -> dict[str, Any]:
    """Expand nested dicts into ``parent_child`` columns for SQL / warehouse maps.

    Parent objects are kept (serialized later to VARIANT/JSON) so nothing is
    lost; leaf scalars are promoted so Map can bind ``address_city`` etc.
    Arrays stay on the parent key as JSON — no row explosion.
    """
    if not isinstance(doc, dict):
        return {}
    out: dict[str, Any] = dict(doc) if keep_parent else {}
    added = 0

    def _walk(obj: dict[str, Any], prefix: str, depth: int) -> None:
        nonlocal added
        for key, value in obj.items():
            if added >= MAX_FLATTENED_KEYS:
                return
            name = f"{prefix}{sep}{key}" if prefix else str(key)
            if isinstance(value, dict) and depth < max_depth:
                if keep_parent and name not in out:
                    out[name] = value
                _walk(value, name, depth + 1)
            elif isinstance(value, list):
                if name not in out:
                    out[name] = value
                    added += 1
            else:
                if name not in out or out.get(name) is None:
                    out[name] = value
                    added += 1

    for key, value in doc.items():
        if isinstance(value, dict):
            _walk(value, str(key), 1)
        elif added >= MAX_FLATTENED_KEYS:
            break

    return out


def mongo_flatten_enabled(cfg: dict[str, Any] | None = None) -> bool:
    """Operator / env switch — default on so Mongo→SQL maps nested leaves."""
    if cfg is not None and "flatten_nested" in cfg:
        return bool(cfg.get("flatten_nested"))
    env = (os.environ.get("DATAFLOW_MONGO_FLATTEN_NESTED") or "1").strip().lower()
    return env not in {"0", "false", "no", "off"}


def expand_mongo_documents(
    docs: list[dict[str, Any]],
    *,
    cfg: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Apply nested expansion when enabled; otherwise return docs unchanged."""
    if not docs or not mongo_flatten_enabled(cfg):
        return docs
    return [flatten_document(doc) for doc in docs]
