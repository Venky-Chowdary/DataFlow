"""Shared row mapping utilities for database writers."""

from __future__ import annotations

import hashlib
import re
from typing import Any, Callable

from services.transform_engine import apply_transform, infer_transform

CHUNK_SIZE = 500


def sanitize_identifier(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip().lower())
    s = re.sub(r"_+", "_", s).strip("_")
    if not s or s[0].isdigit():
        s = f"col_{s or 'field'}"
    return s[:63]


def row_checksum(rows: list[tuple]) -> str:
    h = hashlib.sha256()
    for row in rows:
        h.update("|".join("" if v is None else str(v) for v in row).encode())
    return h.hexdigest()[:16]


def build_mapped_rows(
    *,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    target_cols: list[str],
    column_types: dict[str, str] | None = None,
) -> tuple[list[tuple], list[str]]:
    """Returns mapped rows and any transform errors (first 10)."""
    column_types = column_types or {}
    source_indices = {h: i for i, h in enumerate(headers)}
    mapped: list[tuple] = []
    errors: list[str] = []

    for raw in data_rows:
        row_map: dict[str, Any] = {}
        for m in mappings:
            idx = source_indices.get(m["source"])
            val = raw[idx] if idx is not None and idx < len(raw) else None
            tgt = sanitize_identifier(m["target"])
            transform = m.get("transform") or infer_transform(
                m["source"], m["target"], column_types.get(m["source"], "VARCHAR")
            )
            converted, err = apply_transform(val, transform)
            if err and len(errors) < 10:
                errors.append(f"{m['source']}: {err}")
            row_map[tgt] = converted
        mapped.append(tuple(row_map.get(c) for c in target_cols))

    return mapped, errors


def resolve_target_columns(mappings: list[dict], column_types: dict[str, str]) -> tuple[list[str], list[str]]:
    target_cols: list[str] = []
    source_types: list[str] = []
    for m in mappings:
        tgt = sanitize_identifier(m["target"])
        if tgt not in target_cols:
            target_cols.append(tgt)
            source_types.append(column_types.get(m["source"], "VARCHAR"))
    return target_cols, source_types
