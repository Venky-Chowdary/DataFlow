"""Compatibility shim: canonical implementation now lives in services.scd2_engine."""
from __future__ import annotations

from services.scd2_engine import (
    IS_CURRENT_COLUMN,
    ROW_HASH_COLUMN,
    SCD2_COLUMNS,
    VALID_FROM_COLUMN,
    VALID_TO_COLUMN,
    _active_checksum,
    _build_scd_table,
    _ensure_scd_columns,
    _expire_rows,
    _fetch_current_rows,
    _insert_rows,
    _now_utc,
    _qualified_name,
    _row_hash,
    _target_columns,
    apply_scd2,
)

__all__ = ['VALID_FROM_COLUMN', 'VALID_TO_COLUMN', 'IS_CURRENT_COLUMN', 'ROW_HASH_COLUMN', 'SCD2_COLUMNS', '_now_utc', '_qualified_name', '_target_columns', '_row_hash', '_ensure_scd_columns', '_build_scd_table', '_fetch_current_rows', '_insert_rows', '_expire_rows', '_active_checksum', 'apply_scd2']
