"""JSON / nested document intelligence — flattening and type recommendations."""

from __future__ import annotations

import json
import re
from typing import Any

DOT_PATH = re.compile(r"^[a-zA-Z_][\w.]*\.\w+")
JSON_PREFIX = re.compile(r"^[\[{]")


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
