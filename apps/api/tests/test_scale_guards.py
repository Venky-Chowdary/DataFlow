"""Scale safety guards for transfer reads."""

from __future__ import annotations

import pytest


def test_guard_truncated_read_raises():
    from src.transfer.adapters import _guard_truncated_read

    class Batch:
        total_rows = 2_000_000
        rows = [1] * 100_000
        meta = None

    with pytest.raises(ValueError, match="non-streaming"):
        _guard_truncated_read(Batch(), "postgresql", "orders")


def test_guard_truncated_read_allows_small_tables():
    from src.transfer.adapters import _guard_truncated_read

    class Batch:
        total_rows = 50
        rows = [1] * 50
        meta = None

    _guard_truncated_read(Batch(), "postgresql", "orders")


def test_guard_truncated_read_raises_on_saas_has_more_stamp():
    from src.transfer.adapters import _guard_truncated_read

    class Batch:
        total_rows = None
        rows = [1] * 100
        meta = {"truncated": True, "has_more": True}

    with pytest.raises(ValueError, match="unread pages|silent truncate"):
        _guard_truncated_read(Batch(), "stripe", "customers")
