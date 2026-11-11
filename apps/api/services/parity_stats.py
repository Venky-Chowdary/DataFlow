"""Parity Studio: the confidence bound on a sampled value check.

A sampled compare that found ``k`` differing rows in ``n`` does not prove the
rest of the table equal. What it does support is an *upper bound* on the true
difference rate: the exact one-sided Clopper-Pearson bound, the rate ``p`` at
which seeing ``k`` or fewer differences in ``n`` rows would happen only
``alpha`` of the time::

    P(X <= k | n, p) = alpha,  X ~ Binomial(n, p)

which is the ``1 - alpha`` quantile of ``Beta(k + 1, n - k)``. ``k = 0`` has the
closed form ``1 - alpha ** (1 / n)`` (about ``3 / n`` at 95%, the "rule of
three"); ``k = n`` is 1. Otherwise the regularized incomplete beta function is
inverted by bisection, with SciPy's ``beta.ppf`` used instead when it is
installed. The bound is exact, not a normal approximation, so it stays honest
at the small ``k`` a clean migration produces.
"""

from __future__ import annotations

import math

#: Confidence level of every bound Parity Studio states.
CONFIDENCE_LEVEL = 0.95

_EPS = 1e-15
_TINY = 1e-300


def _betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for the incomplete beta function (modified Lentz)."""
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    d = 1.0 / (d if abs(d) > _TINY else _TINY)
    h = d
    for m in range(1, 10_000):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > _TINY else _TINY)
        c = 1.0 + aa / c
        c = c if abs(c) > _TINY else _TINY
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > _TINY else _TINY)
        c = 1.0 + aa / c
        c = c if abs(c) > _TINY else _TINY
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < _EPS:
            break
    return h


def regularized_incomplete_beta(a: float, b: float, x: float) -> float:
    """``I_x(a, b)`` for ``a, b > 0`` and ``0 <= x <= 1``."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    log_front = (
        math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
        + a * math.log(x) + b * math.log1p(-x)
    )
    front = math.exp(log_front)
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _betacf(a, b, x) / a
    return 1.0 - front * _betacf(b, a, 1.0 - x) / b


def binomial_cdf(k: int, n: int, p: float) -> float:
    """``P(X <= k)`` for ``X ~ Binomial(n, p)``: ``I_{1-p}(n - k, k + 1)``."""
    if k < 0:
        return 0.0
    if k >= n:
        return 1.0
    return regularized_incomplete_beta(float(n - k), float(k + 1), 1.0 - p)


def _scipy_upper(k: int, n: int, alpha: float) -> float | None:
    try:
        from scipy.stats import beta  # type: ignore[import-not-found]
    except ImportError:
        return None
    return float(beta.ppf(1.0 - alpha, k + 1, n - k))


def clopper_pearson_upper(k: int, n: int, confidence: float = CONFIDENCE_LEVEL) -> float | None:
    """Exact one-sided upper confidence bound on a binomial rate.

    ``None`` when there is no sample (``n <= 0``): nothing bounds the rate.
    Raises ``ValueError`` for ``k`` outside ``0..n`` or a confidence outside
    ``(0, 1)`` — a caller passing those has mixed up its counts.
    """
    n = int(n)
    k = int(k)
    if n <= 0:
        return None
    if not 0 <= k <= n:
        raise ValueError(f"differences in sample ({k}) must be between 0 and n ({n})")
    if not 0.0 < confidence < 1.0:
        raise ValueError("confidence must be between 0 and 1")
    alpha = 1.0 - confidence
    if k == n:
        return 1.0
    if k == 0:
        return 1.0 - alpha ** (1.0 / n)
    via_scipy = _scipy_upper(k, n, alpha)
    if via_scipy is not None:
        return via_scipy
    # binomial_cdf(k, n, p) falls monotonically from 1 to 0 as p rises.
    lo, hi = k / n, 1.0
    for _ in range(200):
        mid = (lo + hi) / 2.0
        if binomial_cdf(k, n, mid) > alpha:
            lo = mid
        else:
            hi = mid
        if hi - lo < 1e-15:
            break
    return (lo + hi) / 2.0


def format_rate_pct(rate: float | None) -> str:
    """An upper bound as a percentage, rounded **up**: ``0.001444`` -> ``"0.15%"``.

    The figure reads "at most x%", so rounding it down (half-up rounding gave
    "0.14%" for 0.1444%) would state a bound the data does not support. Two
    decimals, or two significant digits below 0.01%.
    """
    if rate is None:
        return "unknown"
    from decimal import ROUND_CEILING, Decimal

    pct = Decimal(repr(float(rate))) * 100
    if pct <= 0:
        return "0.00%"
    if pct >= Decimal("0.01"):
        return f"{pct.quantize(Decimal('0.01'), rounding=ROUND_CEILING)}%"
    step = Decimal(1).scaleb(pct.adjusted() - 1)
    return f"{float(pct.quantize(step, rounding=ROUND_CEILING)):.2g}%"
