"""Clopper-Pearson one-sided 95% upper bound: known values and edge cases."""

from __future__ import annotations

import math

import pytest

from services import parity_stats
from services.parity_stats import binomial_cdf, clopper_pearson_upper, format_rate_pct


def test_zero_differences_use_the_closed_form():
    assert clopper_pearson_upper(0, 2000) == pytest.approx(0.0014967, abs=5e-8)
    assert clopper_pearson_upper(0, 2000) == pytest.approx(1 - 0.05 ** (1 / 2000), rel=1e-12)
    assert clopper_pearson_upper(0, 1) == pytest.approx(0.95)


def test_textbook_value():
    # One success in ten trials: the exact one-sided 95% upper limit is 0.3942.
    assert clopper_pearson_upper(1, 10) == pytest.approx(0.39416, abs=1e-5)


@pytest.mark.parametrize(("k", "n"), [(5, 1000), (2, 50), (10, 100), (1, 10)])
def test_bound_is_bracketed_by_exact_rational_arithmetic(k, n):
    """P(X <= k) crosses 0.05 at the bound, computed in exact fractions on both sides."""
    from fractions import Fraction

    def cdf(p: Fraction) -> Fraction:
        return sum(Fraction(math.comb(n, i)) * p**i * (1 - p) ** (n - i) for i in range(k + 1))

    upper = Fraction(clopper_pearson_upper(k, n))
    eps = Fraction(1, 10**9)
    assert cdf(upper - eps) > Fraction(1, 20) > cdf(upper + eps)


def test_matches_scipy_when_installed():
    stats = pytest.importorskip("scipy.stats")
    for k, n in ((5, 1000), (1, 10), (37, 4000), (0, 2000), (99, 100)):
        assert clopper_pearson_upper(k, n) == pytest.approx(stats.beta.ppf(0.95, k + 1, n - k) if k < n else 1.0,
                                                          rel=1e-9, abs=1e-12)


def test_bisection_agrees_with_a_direct_binomial_sum(monkeypatch):
    monkeypatch.setattr(parity_stats, "_scipy_upper", lambda *_a: None)
    for k, n in ((5, 1000), (1, 10), (3, 60)):
        upper = clopper_pearson_upper(k, n)
        direct = sum(math.comb(n, i) * upper**i * (1 - upper) ** (n - i) for i in range(k + 1))
        assert direct == pytest.approx(0.05, abs=1e-9)
        assert binomial_cdf(k, n, upper) == pytest.approx(direct, abs=1e-12)


def test_edges_and_refusals():
    assert clopper_pearson_upper(0, 0) is None
    assert clopper_pearson_upper(7, 7) == 1.0
    with pytest.raises(ValueError):
        clopper_pearson_upper(8, 7)
    with pytest.raises(ValueError):
        clopper_pearson_upper(-1, 7)
    with pytest.raises(ValueError):
        clopper_pearson_upper(1, 7, confidence=1.0)
    assert binomial_cdf(-1, 5, 0.3) == 0.0 and binomial_cdf(5, 5, 0.3) == 1.0


def test_rate_formatting():
    assert format_rate_pct(clopper_pearson_upper(0, 2000)) == "0.15%"
    assert format_rate_pct(0.0) == "0.00%"
    assert format_rate_pct(0.0000012) == "0.00012%"
    assert format_rate_pct(None) == "unknown"
