"""
Change Data Capture and incremental sync engine.

Algorithms:
  - Typed watermark comparator (int, float, ISO datetime, lexicographic string)
  - Row fingerprinting for change detection
  - Batch diff: inserts / updates / deletes by primary key
  - Incremental filter builder for source queries
  - Monotonic watermark advancement with validation
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any


class WatermarkType(str, Enum):
    INTEGER = "integer"
    FLOAT = "float"
    DATETIME = "datetime"
    STRING = "string"


_ISO_DT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?$"
)
_EPOCH_MS_RE = re.compile(r"^\d{13}$")
_EPOCH_S_RE = re.compile(r"^\d{10}$")


def infer_watermark_type(samples: list[str]) -> WatermarkType:
    """Infer comparator type from cursor column samples."""
    non_empty = [s.strip() for s in samples if s and str(s).strip()]
    if not non_empty:
        return WatermarkType.STRING

    int_hits = sum(1 for s in non_empty if re.match(r"^-?\d+$", s.replace(",", "")))
    float_hits = sum(1 for s in non_empty if _parse_float(s) is not None)
    dt_hits = sum(
        1 for s in non_empty
        if _ISO_DT_RE.match(s) or _EPOCH_MS_RE.match(s) or _EPOCH_S_RE.match(s)
    )

    n = len(non_empty)
    if dt_hits / n >= 0.8:
        return WatermarkType.DATETIME
    if int_hits / n >= 0.9:
        return WatermarkType.INTEGER
    if float_hits / n >= 0.85:
        return WatermarkType.FLOAT
    return WatermarkType.STRING


def _parse_float(s: str) -> float | None:
    try:
        return float(Decimal(s.replace(",", "")))
    except (InvalidOperation, ValueError):
        return None


def _parse_datetime_key(s: str) -> float | None:
    text = s.strip()
    if _EPOCH_MS_RE.match(text):
        return int(text) / 1000.0
    if _EPOCH_S_RE.match(text):
        return float(int(text))
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text.replace("Z", ""), fmt.replace("Z", "")).timestamp()
        except ValueError:
            continue
    return None


def compare_watermarks(a: str, b: str, wm_type: WatermarkType) -> int:
    """
    Compare two watermark values. Returns -1 if a<b, 0 if equal, 1 if a>b.
    Used for monotonic advancement validation and max() selection.
    """
    if wm_type == WatermarkType.INTEGER:
        try:
            ai, bi = int(a.replace(",", "")), int(b.replace(",", ""))
            return (ai > bi) - (ai < bi)
        except ValueError:
            return (a > b) - (a < b)
    if wm_type == WatermarkType.FLOAT:
        fa, fb = _parse_float(a), _parse_float(b)
        if fa is not None and fb is not None:
            return (fa > fb) - (fa < fb)
        return (a > b) - (a < b)
    if wm_type == WatermarkType.DATETIME:
        da, db = _parse_datetime_key(a), _parse_datetime_key(b)
        if da is not None and db is not None:
            return (da > db) - (da < db)
        return (a > b) - (a < b)
    return (a > b) - (a < b)


def max_watermark(values: list[str], wm_type: WatermarkType) -> str | None:
    """Select maximum watermark from a batch using typed comparator."""
    non_empty = [v.strip() for v in values if v and str(v).strip()]
    if not non_empty:
        return None
    best = non_empty[0]
    for v in non_empty[1:]:
        if compare_watermarks(v, best, wm_type) > 0:
            best = v
    return best


def advance_watermark(
    current: str | None,
    batch_values: list[str],
    wm_type: WatermarkType,
) -> tuple[str | None, bool]:
    """
    Advance watermark monotonically. Returns (new_watermark, advanced).
    Rejects batch if any value regresses below current watermark.
    """
    batch_max = max_watermark(batch_values, wm_type)
    if batch_max is None:
        return current, False
    if current is None:
        return batch_max, True
    if compare_watermarks(batch_max, current, wm_type) > 0:
        return batch_max, True
    if compare_watermarks(batch_max, current, wm_type) == 0:
        return current, False
    return current, False  # regression — do not advance


def row_fingerprint(row: dict[str, Any], columns: list[str] | None = None) -> str:
    """Stable SHA-256 fingerprint of a row for change detection."""
    cols = columns or sorted(row.keys())
    parts = []
    for col in cols:
        val = row.get(col)
        parts.append(f"{col}={'' if val is None else str(val).strip()}")
    payload = "\x1f".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


@dataclass
class ChangeBatch:
    inserts: list[dict[str, Any]] = field(default_factory=list)
    updates: list[dict[str, Any]] = field(default_factory=list)
    deletes: list[str] = field(default_factory=list)  # primary key values
    unchanged: int = 0
    resume_token: Any = None  # log-based CDC checkpoint (e.g. MongoDB change stream)

    @property
    def total_changes(self) -> int:
        return len(self.inserts) + len(self.updates) + len(self.deletes)


def diff_by_primary_key(
    previous: dict[str, dict[str, Any]],
    current: dict[str, dict[str, Any]],
    *,
    fingerprint_columns: list[str] | None = None,
) -> ChangeBatch:
    """
    CDC diff algorithm: compare two snapshots keyed by primary key.
    previous/current: {pk_value: row_dict}
    """
    batch = ChangeBatch()
    prev_keys = set(previous.keys())
    curr_keys = set(current.keys())

    for pk in curr_keys - prev_keys:
        batch.inserts.append(current[pk])

    for pk in prev_keys - curr_keys:
        batch.deletes.append(pk)

    for pk in prev_keys & curr_keys:
        fp_prev = row_fingerprint(previous[pk], fingerprint_columns)
        fp_curr = row_fingerprint(current[pk], fingerprint_columns)
        if fp_prev != fp_curr:
            batch.updates.append(current[pk])
        else:
            batch.unchanged += 1

    return batch


def build_incremental_predicate(
    cursor_column: str,
    watermark: str,
    wm_type: WatermarkType,
    *,
    dialect: str = "postgresql",
) -> str:
    """Build SQL WHERE clause for incremental reads (parameterized style)."""
    col = cursor_column
    if wm_type == WatermarkType.DATETIME:
        if dialect in {"postgresql", "redshift", "snowflake"}:
            return f'"{col}" > \'{watermark}\'::timestamptz'
        if dialect == "mysql":
            return f"`{col}` > '{watermark}'"
        return f"{col} > '{watermark}'"
    if wm_type in {WatermarkType.INTEGER, WatermarkType.FLOAT}:
        val = watermark.replace(",", "")
        if dialect == "mysql":
            return f"`{col}` > {val}"
        return f'"{col}" > {val}'
    if dialect == "mysql":
        return f"`{col}` > '{watermark}'"
    return f'"{col}" > \'{watermark}\''


def deduplicate_batch(
    rows: list[dict[str, Any]],
    primary_key: str,
    *,
    keep: str = "last",
) -> tuple[list[dict[str, Any]], int]:
    """
    Deduplicate rows by primary key within a batch.
    keep='last' retains the final occurrence (standard for CDC upsert).
    Returns (deduped_rows, duplicate_count).
    """
    if not primary_key:
        return rows, 0
    seen: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    dupes = 0
    for row in rows:
        pk = str(row.get(primary_key, "")).strip()
        if not pk:
            continue
        if pk in seen:
            dupes += 1
            if keep == "last":
                seen[pk] = row
            continue
        seen[pk] = row
        order.append(pk)
    return [seen[pk] for pk in order], dupes


def validate_sync_contract(contract: dict[str, Any]) -> list[str]:
    """Validate incremental/CDC contract completeness."""
    issues: list[str] = []
    mode = (contract.get("sync_mode") or "").lower()
    if mode not in {"incremental_append", "incremental_deduped", "cdc"}:
        return issues
    if not (contract.get("cursor_field") or contract.get("cursor")):
        issues.append("Incremental sync requires cursor_field")
    if mode in {"incremental_deduped", "cdc"}:
        pk = contract.get("primary_key") or (
            (contract.get("primary_keys") or [None])[0]
            if isinstance(contract.get("primary_keys"), list)
            else None
        )
        if not pk:
            issues.append("Deduped/CDC sync requires primary_key")
    return issues
