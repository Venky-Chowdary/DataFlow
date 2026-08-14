"""Compatibility shim: canonical implementation now lives in services.mirror_engine."""
from __future__ import annotations

from services.mirror_engine import (
    SOFT_DELETE_COLUMN,
    _compute_active_checksum,
    _ensure_soft_delete_column,
    _key_value,
    _qualified_name,
    _target_columns,
    _update_deleted_batch,
    apply_inferred_deletes_via_staging,
    apply_inferred_soft_deletes,
    lattice_column_names,
    quote_sql_identifier,
    strip_lattice_from_upsert,
)

__all__ = [
    "SOFT_DELETE_COLUMN",
    "_qualified_name",
    "_key_value",
    "_target_columns",
    "_ensure_soft_delete_column",
    "_update_deleted_batch",
    "_compute_active_checksum",
    "apply_inferred_deletes_via_staging",
    "apply_inferred_soft_deletes",
    "lattice_column_names",
    "quote_sql_identifier",
    "strip_lattice_from_upsert",
]
