"""Shared row mapping utilities for database writers."""

from __future__ import annotations

import hashlib
import os
import re
from typing import Any, Callable

from services.reconciliation import _iter_fingerprints, checksum_rows
from services.transform_engine import apply_transform
from services.transform_resolver import resolve_transform

# Configurable batch size — default 20 000 rows per commit (enterprise scale)
CHUNK_SIZE = int(os.getenv("DATAFLOW_CHUNK_SIZE", "20000"))
TRANSFORM_ERROR_POLICY = os.getenv("DATAFLOW_TRANSFORM_ERROR_POLICY", "quarantine").lower()
VALID_ERROR_POLICIES = {"fail", "quarantine", "coerce_null"}


def sanitize_identifier(name: str, preserve_case: bool = False) -> str:
    cleaned = name.strip() if preserve_case else name.strip().lower()
    s = re.sub(r"[^a-zA-Z0-9_]", "_", cleaned)
    s = re.sub(r"_+", "_", s).rstrip("_")
    if not s or s[0].isdigit():
        s = f"col_{s or 'field'}"
    return s[:63]


def quote_sql_identifier(name: str, quote_char: str = '"') -> str:
    """Quote a SQL identifier and escape embedded quote characters."""
    escaped = name.replace(quote_char, quote_char + quote_char)
    return f"{quote_char}{escaped}{quote_char}"


def row_checksum(rows: list[Any], columns: list[str] | None = None) -> str:
    return checksum_rows(rows, columns)


def row_fingerprints(rows: list[Any], columns: list[str] | None = None, *, sort_key: str | None = None) -> list[tuple[str, str]]:
    """Return the unsorted (row_key, fingerprint) tuples for a list of rows.

    Streaming producers can accumulate these tuples across batches and then call
    ``services.reconciliation.fingerprint_checksum`` once at the end, avoiding a
    full materialization of every row as a dict/list.
    """
    return list(_iter_fingerprints(rows, columns, sort_key=sort_key))


def dedupe_rows(
    rows: list[tuple],
    conflict_columns: list[str],
    target_cols: list[str],
) -> list[tuple]:
    """Keep the last occurrence of each conflict key, preserving tuple order."""
    if not conflict_columns or not rows:
        return rows
    indices = [target_cols.index(c) for c in conflict_columns if c in target_cols]
    if not indices:
        return rows
    seen: dict[tuple, tuple] = {}
    for row in rows:
        key = tuple(row[i] for i in indices)
        seen[key] = row
    return list(seen.values())


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
    dest_types: dict[str, str] | None = None,
    preserve_case: bool = False,
) -> tuple[list[tuple], list[str]]:
    """Returns mapped rows and any transform errors (first 10)."""
    column_types = column_types or {}
    policy = transform_error_policy(error_policy)
    source_indices = {h: i for i, h in enumerate(headers)}
    sanitized_target_cols = [sanitize_identifier(c, preserve_case=preserve_case) for c in target_cols]
    target_index = {c: i for i, c in enumerate(sanitized_target_cols)}
    errors: list[str] = []

    mapping_infos = []
    for m in mappings:
        src = m["source"]
        tgt = sanitize_identifier(m["target"], preserve_case=preserve_case)
        transform = resolve_transform(
            m,
            column_types=column_types,
            dest_types=dest_types or column_types,
        )
        mapping_infos.append((
            source_indices.get(src),
            target_index.get(tgt, -1),
            transform,
            src,
            tgt,
        ))

    mapped: list[tuple] = []
    for row_number, raw in enumerate(data_rows, start=1):
        out = [None] * len(sanitized_target_cols)
        row_has_error = False
        for source_idx, target_idx, transform, src_name, tgt_name in mapping_infos:
            val = raw[source_idx] if source_idx is not None and source_idx < len(raw) else None
            converted, err = apply_transform(val, transform)
            if err:
                row_has_error = True
                if len(errors) < 10:
                    errors.append(f"row {row_number} {src_name}→{tgt_name}: {err}")
                if policy in {"coerce_null", "quarantine"}:
                    # Quarantine preserves the row; the bad cell becomes NULL and the
                    # error is surfaced as a warning so the transfer does not silently
                    # lose data.
                    converted = None
                else:
                    continue
            if target_idx >= 0:
                out[target_idx] = converted
        if row_has_error and policy == "fail":
            continue
        mapped.append(tuple(out))

    return mapped, errors


def resolve_target_columns(
    mappings: list[dict],
    column_types: dict[str, str],
    preserve_case: bool = False,
    dest_types: dict[str, str] | None = None,
) -> tuple[list[str], list[str]]:
    """Return target column names and their intended logical target types.

    Prefers an explicit ``target_type`` on each mapping, then ``dest_types``,
    then the source logical type, and finally ``VARCHAR``.
    """
    target_cols: list[str] = []
    target_types: list[str] = []
    for m in mappings:
        tgt = sanitize_identifier(m["target"], preserve_case=preserve_case)
        if tgt not in target_cols:
            target_cols.append(tgt)
            target_types.append(
                m.get("target_type")
                or (dest_types or {}).get(tgt)
                or column_types.get(m["source"], "VARCHAR")
            )
    return target_cols, target_types
