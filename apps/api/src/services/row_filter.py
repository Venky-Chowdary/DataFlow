"""Compatibility shim: canonical implementation now lives in services.row_filter."""
from __future__ import annotations

from services.row_filter import (
    _FILTER_OPS,
    RowFilter,
    _compare_values,
    _is_null,
    _matches,
    _to_scalar,
    apply_row_filter,
    apply_row_filter_to_matrix,
)

__all__ = ['_FILTER_OPS', '_to_scalar', '_is_null', '_compare_values', '_matches', 'apply_row_filter', 'apply_row_filter_to_matrix', 'RowFilter']
