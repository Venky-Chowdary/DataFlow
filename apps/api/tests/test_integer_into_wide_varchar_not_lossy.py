"""An integer column into a VARCHAR wide enough for its longest rendering keeps
every value; a narrower VARCHAR or a padding CHAR(n) stays fail-closed.

Measured: an upsert whose source column changed VARCHAR -> BIGINT was refused
on every server destination as "fidelity collapse (name BIGINT -> VARCHAR(32))",
although BIGINT renders at most 20 chars.
"""

from __future__ import annotations

import pytest

from services.type_coercion import is_lossy_coercion
from services.type_system import integer_max_text_width


@pytest.mark.parametrize(
    ("source", "width"),
    [("SMALLINT", 6), ("INT4", 11), ("BIGINT", 20), ("INTEGER", 20), ("TINYINT", 4)],
)
def test_integer_max_text_width_includes_sign(source: str, width: int) -> None:
    assert integer_max_text_width(source) == width


@pytest.mark.parametrize(
    ("source", "target", "lossy"),
    [
        ("BIGINT", "VARCHAR(32)", False),
        ("BIGINT", "VARCHAR(20)", False),
        ("BIGINT", "NVARCHAR(32)", False),
        ("BIGINT", "CHARACTER VARYING(32)", False),
        ("INT4", "VARCHAR(11)", False),
        ("BIGINT", "VARCHAR(19)", True),
        ("INT4", "VARCHAR(10)", True),
        ("INTEGER", "VARCHAR(11)", True),
        ("INTEGER", "VARCHAR(1)", True),
        ("TINYINT", "CHAR(4)", True),
    ],
)
def test_integer_into_bounded_string(source: str, target: str, lossy: bool) -> None:
    assert is_lossy_coercion(source, target) is lossy
