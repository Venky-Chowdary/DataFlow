"""Property tests for the Parity Studio classifier and confidence bound (hypothesis).

Each property generates the shape a rule claims (or must reject) and checks the
rule's verdict, so a rule cannot quietly start matching pairs another rule owns.
"""

from __future__ import annotations

import math
import unicodedata
from decimal import ROUND_HALF_UP, Decimal

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import assume, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from services import parity_classifier as pc  # noqa: E402
from services.parity_classifier import CellContext  # noqa: E402
from services.parity_stats import binomial_cdf, clopper_pearson_upper  # noqa: E402

PLAIN = CellContext()
TEXT = CellContext(dest_type="TEXT")
words = st.text(alphabet=st.characters(min_codepoint=33, max_codepoint=0x2FF), min_size=1, max_size=20)
SETTINGS = settings(max_examples=150, deadline=None)


@SETTINGS
@given(words, st.integers(1, 4), st.integers(0, 4))
def test_padding_is_whitespace(text, right, left):
    padded = " " * left + text + " " * right
    found = pc.whitespace(padded, text, PLAIN)
    assert found is not None and found["explained"] is False
    assert pc.classify_value(expected=padded, dest=text, ctx=TEXT)["code"] == "whitespace"


@SETTINGS
@given(st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=20))
def test_case_change_is_letter_case(text):
    assert pc.letter_case(text, text.upper(), PLAIN)["code"] == "letter_case"
    assert pc.classify_value(expected=text, dest=text.upper(), ctx=PLAIN)["code"] == "letter_case"


@SETTINGS
@given(words)
def test_nfd_of_nfc_is_explained_unicode_normalization(text):
    nfc, nfd = unicodedata.normalize("NFC", text), unicodedata.normalize("NFD", text)
    assume(nfc != nfd)
    found = pc.classify_value(expected=nfc, dest=nfd, ctx=PLAIN)
    assert found["code"] == "unicode_normalization" and found["explained"] is True


@SETTINGS
@given(st.text(alphabet="abcdefghij", min_size=2, max_size=30), st.data())
def test_a_strict_prefix_is_truncation(text, data):
    cut = data.draw(st.integers(1, len(text) - 1))
    assume(not text[:cut].strip() == "")
    found = pc.classify_value(expected=text, dest=text[:cut], ctx=PLAIN)
    assert found["code"] == "truncation"
    assert found["detail"] == f"Cut at {cut} characters."


@SETTINGS
@given(st.integers(-10**6, 10**6), st.integers(3, 6), st.integers(0, 2))
def test_rounding_to_fewer_places_is_numeric_scale(units, s_scale, d_scale):
    source = Decimal(units).scaleb(-s_scale)
    assume(source.as_tuple().digits[-1] != 0)  # a real digit sits in the last place
    dest = source.quantize(Decimal(1).scaleb(-d_scale), rounding=ROUND_HALF_UP)
    assume(dest != source)
    found = pc.classify_value(expected=str(source), dest=str(dest), ctx=PLAIN)
    assert found["code"] == "numeric_scale" and found["explained"] is False


@SETTINGS
@given(st.integers(-10**6, 10**6), st.integers(1, 5))
def test_unrelated_numbers_are_not_explained(a, delta):
    found = pc.classify_value(expected=str(a), dest=str(a + delta * 7), ctx=PLAIN)
    assert found["code"] == "unexplained"


@SETTINGS
@given(st.floats(allow_nan=False, allow_infinity=False, width=64).filter(lambda x: abs(x) > 1e-300))
def test_same_binary64_is_explained_float_precision(x):
    longer = format(Decimal(x), "f")  # every digit of the double
    shortest = repr(x)
    assume(Decimal(longer) != Decimal(shortest))
    found = pc.classify_value(expected=longer, dest=shortest, ctx=PLAIN)
    assert found["code"] == "float_precision" and found["explained"] is True


@SETTINGS
@given(
    st.datetimes(min_value=__import__("datetime").datetime(2000, 1, 1),
                 max_value=__import__("datetime").datetime(2030, 1, 1)),
    st.integers(-12, 14).filter(bool),
)
def test_same_instant_in_another_offset_is_explained(moment, hours):
    from datetime import timedelta, timezone

    utc = moment.replace(tzinfo=timezone.utc, microsecond=0)
    shifted = utc.astimezone(timezone(timedelta(hours=hours)))
    found = pc.classify_value(expected=utc.isoformat(), dest=shifted.isoformat(), ctx=PLAIN)
    assert found["code"] == "timezone_shift" and found["explained"] is True
    kept_wall = utc.replace(tzinfo=timezone(timedelta(hours=hours))).isoformat()
    moved = pc.classify_value(expected=utc.isoformat(), dest=kept_wall, ctx=PLAIN)
    assert moved["code"] == "timezone_shift" and moved["explained"] is False


@SETTINGS
@given(words)
def test_equal_cells_are_never_a_difference(text):
    assert pc.classify_value(expected=text, dest=text, ctx=PLAIN) is None
    for rule in pc.VALUE_RULES:
        assert rule(text, text, PLAIN) is None or rule is pc.numeric_scale


@SETTINGS
@given(st.integers(1, 3000), st.data())
def test_upper_bound_is_exact_and_monotone(n, data):
    k = data.draw(st.integers(0, min(n, 40)))
    upper = clopper_pearson_upper(k, n)
    assert k / n <= upper <= 1.0
    if k < n:
        # Exactly alpha of the binomial mass sits at or below k at the bound.
        assert math.isclose(binomial_cdf(k, n, upper), 0.05, rel_tol=1e-6, abs_tol=1e-9)
        assert clopper_pearson_upper(k + 1, n) > upper  # more differences, higher bound
    if n < 3000:
        assert clopper_pearson_upper(k, n + 1) < upper or k == n  # more rows, tighter bound
