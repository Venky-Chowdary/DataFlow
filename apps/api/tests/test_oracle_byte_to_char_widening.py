"""Oracle BYTE→CHAR length semantics: widening is not lossy, narrowing is."""

from __future__ import annotations

import pytest

from services.type_coercion import is_lossy_coercion
from services.type_system import oracle_char_byte_polarity_loss


@pytest.mark.parametrize(
    ("source", "target", "lossy"),
    [
        ("VARCHAR2(8 BYTE)", "VARCHAR2(8 CHAR)", False),
        ("VARCHAR(32 BYTE)", "VARCHAR(32 CHAR)", False),
        ("VARCHAR2(8 BYTE)", "VARCHAR2(7 CHAR)", True),
        ("VARCHAR2(100 CHAR)", "VARCHAR2(100 BYTE)", True),
        ("VARCHAR2(100 CHAR)", "VARCHAR2(399 BYTE)", True),
        ("VARCHAR2(100 CHAR)", "VARCHAR2(400 BYTE)", False),
    ],
)
def test_oracle_char_byte_budget(source: str, target: str, lossy: bool) -> None:
    assert oracle_char_byte_polarity_loss(source, target) is lossy
    assert is_lossy_coercion(source, target) is lossy
