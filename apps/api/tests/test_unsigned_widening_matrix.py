"""Matrix: UNSIGNED integer sources into wider signed sinks must not false-block.

``integer_bit_width`` returns ``None`` for the ambiguous ``INT``/``INTEGER``
keyword, so ``unsigned_integer_would_overflow`` used to fail closed on every
bare ``INT UNSIGNED`` source — blocking ``MySQL INT UNSIGNED → BIGINT``, the
most common MySQL→warehouse widening, even though 2^32-1 fits a signed 64-bit
sink. Overflowing pairs must still block: a false negative here is silent
truncation.
"""

from __future__ import annotations

import pytest

from services.type_system import unsigned_integer_would_overflow

# (source_type, target_type, overflows)
MATRIX: list[tuple[str, str, bool]] = [
    # Bare INT UNSIGNED (MySQL 32-bit) → 64-bit signed sinks: value-safe.
    ("INT UNSIGNED", "BIGINT", False),
    ("INTEGER UNSIGNED", "BIGINT", False),
    ("int unsigned", "INT64", False),
    ("INT UNSIGNED", "bigint", False),
    ("INT UNSIGNED", "INT8", False),  # PostgreSQL INT8 ≡ BIGINT
    # Lossless non-integer sinks stay open.
    ("INT UNSIGNED", "DECIMAL(20,0)", False),
    ("BIGINT UNSIGNED", "DECIMAL(38,0)", False),
    ("BIGINT UNSIGNED", "TEXT", False),
    # Same-or-narrower signed sinks still overflow.
    ("INT UNSIGNED", "INTEGER", True),
    ("INT UNSIGNED", "INT", True),
    ("INT UNSIGNED", "SMALLINT", True),
    ("BIGINT UNSIGNED", "BIGINT", True),
    ("BIGINT UNSIGNED", "INTEGER", True),
    # ClickHouse spellings keep their explicit widths.
    ("UInt32", "BIGINT", False),
    ("UInt64", "BIGINT", True),
    ("UInt8", "SMALLINT", False),
    # Signed sources are never in scope.
    ("INT", "SMALLINT", False),
    ("BIGINT", "INTEGER", False),
]


@pytest.mark.parametrize("source_type,target_type,overflows", MATRIX)
def test_unsigned_overflow_matrix(source_type: str, target_type: str, overflows: bool):
    assert unsigned_integer_would_overflow(source_type, target_type) is overflows


def test_unknown_unsigned_carrier_still_fails_closed():
    """Non-bare, unknown-width unsigned carriers must keep failing closed."""
    assert unsigned_integer_would_overflow("NUMBER UNSIGNED", "INTEGER") is True
