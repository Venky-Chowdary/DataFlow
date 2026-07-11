"""Shared row mapping utilities for database writers."""

from __future__ import annotations

import hashlib
import os
import re
from typing import Any, Callable

from services.transform_engine import apply_transform, infer_transform, infer_transform_for_mapping
from services.transform_resolver import resolve_transform

# Configurable batch size — default 5 000 rows per commit (enterprise scale)
CHUNK_SIZE = int(os.getenv("DATAFLOW_CHUNK_SIZE", "5000"))
TRANSFORM_ERROR_POLICY = os.getenv("DATAFLOW_TRANSFORM_ERROR_POLICY", "quarantine").lower()
VALID_ERROR_POLICIES = {"fail", "quarantine", "coerce_null"}


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


def transform_error_policy(policy: str | None = None) -> str:
    selected = (policy or TRANSFORM_ERROR_POLICY or "quarantine").strip().lower()
    return selected if selected in VALID_ERROR_POLICIES else "quarantine"


_VALIDATION_MODE_POLICIES = {
    "maximum": "fail",
    "strict": "fail",
    "balanced": "quarantine",
}


def transform_error_policy_for_validation_mode(validation_mode: str | None) -> str:
    """Strict/maximum modes fail the transfer on bad cells — no silent row drops."""
    mode = (validation_mode or "strict").strip().lower()
    if mode in _VALIDATION_MODE_POLICIES:
        return _VALIDATION_MODE_POLICIES[mode]
    return transform_error_policy()


def build_mapped_rows(
    *,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    target_cols: list[str],
    column_types: dict[str, str] | None = None,
    error_policy: str | None = None,
) -> tuple[list[tuple], list[str]]:
    """Returns mapped rows and any transform errors (first 10)."""
    column_types = column_types or {}
    policy = transform_error_policy(error_policy)
    source_indices = {h: i for i, h in enumerate(headers)}
    mapped: list[tuple] = []
    errors: list[str] = []

    for row_number, raw in enumerate(data_rows, start=1):
        row_map: dict[str, Any] = {}
        row_has_error = False
        for m in mappings:
            idx = source_indices.get(m["source"])
            val = raw[idx] if idx is not None and idx < len(raw) else None
            tgt = sanitize_identifier(m["target"])
            src_type = column_types.get(m["source"], "VARCHAR")
            tgt_type = m.get("target_type") or column_types.get(m["target"])
            transform = resolve_transform(
                m,
                column_types=column_types,
                dest_types=column_types,
            )
            converted, err = apply_transform(val, transform)
            if err and len(errors) < 10:
                errors.append(f"row {row_number} {m['source']}→{m['target']}: {err}")
            if err:
                row_has_error = True
                if policy == "coerce_null":
                    converted = None
            row_map[tgt] = converted
        if row_has_error and policy in {"fail", "quarantine"}:
            continue
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
