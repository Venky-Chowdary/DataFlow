"""A non-finite cell must not take the Transform (pre-load) preview down.

``Decimal('NaN').as_tuple().exponent`` is ``'n'`` and ``Decimal('Infinity')``'s
is ``'F'``, so profiling them raised ``ValueError`` inside ``profile_columns``
and ``/api/v1/shape/{profile,preview}`` answered 500 — which disabled Continue
and made any source column holding ``NaN`` unloadable through the wizard.
``NaN`` is not a number this profile can measure; it profiles as text.
"""

from __future__ import annotations

import pytest

from services.shape_suggest import profile_columns, suggest_steps


@pytest.mark.parametrize("token", ["NaN", "nan", "Infinity", "-Infinity", "inf", "-inf"])
def test_a_non_finite_token_profiles_without_raising(token: str) -> None:
    profile = profile_columns([{"v": token}])[0]

    assert profile.rows == 1
    assert profile.numeric_like == 0
    assert profile.max_scale == 0


def test_finite_neighbours_still_measure_their_own_scale() -> None:
    rows = [{"v": "1.5"}, {"v": "NaN"}, {"v": "2.25"}, {"v": "Infinity"}]

    profile = profile_columns(rows)[0]

    assert profile.numeric_like == 2
    assert profile.max_scale == 2
    assert suggest_steps([profile]) is not None
