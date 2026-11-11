"""Re-review 3 (LOW): leading and trailing whitespace labels were swapped."""

from __future__ import annotations

import pytest

from services.parity_classifier import CellContext, whitespace

VARCHAR = CellContext(dest_type="VARCHAR(10)")


@pytest.mark.parametrize("source,dest,label", [
    ("ab", "ab   ", "Trailing"), ("ab  ", "ab", "Trailing"),
    ("ab", "   ab", "Leading"), ("  ab", "ab", "Leading"),
    ("ab", " ab ", "Leading and trailing"),
])
def test_the_side_that_differs_is_named(source, dest, label):
    assert whitespace(source, dest, VARCHAR)["detail"] == f"{label} whitespace differs."
