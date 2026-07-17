"""Re-export row filter utilities from the services package."""

from __future__ import annotations

from services.row_filter import (  # noqa: F401
    _FILTER_OPS,
    RowFilter,
    apply_row_filter,
    apply_row_filter_to_matrix,
)
