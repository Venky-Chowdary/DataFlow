"""X-100 — a pad op must never destroy characters.

At 616df100 pad('1234567', width 6) landed '234567' — the leading digit was
destroyed and the run reported verified. Pad now refuses the value under
the step's on_error policy (refuse/divert/null) and counts it in the
transform tally instead of silently truncating — on lpad and rpad, ASCII
and multi-byte.
"""

from __future__ import annotations

import pytest

from services.shape_expr import _fn_pad, EvalError


def test_lpad_overflow_raises_not_truncates():
    with pytest.raises(EvalError, match="pad never truncates"):
        _fn_pad("1234567", 6, "0", left=True)


def test_rpad_overflow_raises_not_truncates():
    with pytest.raises(EvalError, match="pad never truncates"):
        _fn_pad("1234567", 6, "0", left=False)


def test_pad_overflow_multibyte():
    with pytest.raises(EvalError, match="7 character"):
        _fn_pad("é" * 7, 6, "0", left=True)


def test_pad_exact_width_returns_value():
    assert _fn_pad("123456", 6, "0", left=True) == "123456"
    assert _fn_pad("123456", 6, "0", left=False) == "123456"


def test_pad_growth_unchanged():
    assert _fn_pad("7", 6, "0", left=True) == "000007"
    assert _fn_pad("7", 6, "0", left=False) == "700000"
    assert _fn_pad("é", 6, "0", left=True) == "00000é"


def test_pad_null_and_bad_width_unchanged():
    # Blank text is NULL-like to _as_text — unchanged pre/post fix.
    assert _fn_pad("", 6, "0", left=True) is None
    assert _fn_pad(None, 6, "0", left=True) is None
    with pytest.raises(EvalError):
        _fn_pad("x", "notnum", "0", left=True)
    with pytest.raises(EvalError):
        _fn_pad("x", -1, "0", left=True)
