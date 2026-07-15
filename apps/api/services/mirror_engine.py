"""Re-export the inferred-delete mirror engine from the src services package."""

from __future__ import annotations

from src.services.mirror_engine import (  # noqa: F401
    SOFT_DELETE_COLUMN,
    apply_inferred_soft_deletes,
)
