"""Scale safety guards for transfer reads."""

from __future__ import annotations

import pytest


def test_guard_truncated_read_raises():
    from src.transfer.adapters import _guard_truncated_read

    class Batch:
        total_rows = 2_000_000
        rows = [1] * 100_000

    with pytest.raises(ValueError, match="non-streaming"):
        _guard_truncated_read(Batch(), "postgresql", "orders")


def test_guard_truncated_read_allows_small_tables():
    from src.transfer.adapters import _guard_truncated_read

    class Batch:
        total_rows = 50
        rows = [1] * 50

    _guard_truncated_read(Batch(), "postgresql", "orders")
