"""Audit finding 8 (H6) — the data-quality history gate blocked on noise and missed real shifts.

* One prior load was enough to block a strict run ("volume 400 is below 50%
  of prior median 1,000"). A blocking verdict now needs 8 comparable prior
  loads; with less history every finding is advisory.
* The mean-drift signal used a fixed robust-z cut of 3.5, the large-sample
  value. With the 8-10 loads the gate actually holds it fired on stationary
  data far above its 1% target (``fpr_target`` was accepted and ignored). The
  cut now follows the history length and the target; the simulation below
  measures the rate before and after.
* A 100x volume spike and a 0.1% -> 9% null-rate jump were not flagged.
* A 0.2% move on a very stable history was "13 deviations".
"""

from __future__ import annotations

import random
from decimal import Decimal

import pytest

from services.data_quality_history import (
    MIN_BLOCKING_HISTORY,
    NULL_RATE_METRIC,
    _drift_cutoff,
    _robust_z,
    _schema_version,
    _serialize_profile,
    build_load_history_report,
    profile_batch,
)

_SCHEMA = {"amt": "decimal"}
_LEGACY_Z_CUT = 3.5


def _run(rows: list[dict]) -> tuple[dict, dict]:
    profile = profile_batch(rows, _SCHEMA)
    record = {
        "row_count": len(rows),
        "schema_version": _schema_version(profile),
        "null_rate_metric": NULL_RATE_METRIC,
        "columns": _serialize_profile(profile),
        "quarantine_histogram": {},
    }
    return record, profile


def _batch(rng: random.Random, n: int = 200, mu: float = 100.0, sigma: float = 15.0) -> list[dict]:
    return [{"amt": round(rng.gauss(mu, sigma), 2)} for _ in range(n)]


def _nulls(rng: random.Random, n: int, rate: float) -> list[dict]:
    return [{"amt": (None if rng.random() < rate else 1.0)} for _ in range(n)]


def _history(rng: random.Random, loads: int, **kw) -> list[dict]:
    return [_run(_batch(rng, **kw))[0] for _ in range(loads)]


# ── no blocking verdict below the minimum history ───────────────────────────


@pytest.mark.parametrize("prior_loads", [1, 2, 3, 5, MIN_BLOCKING_HISTORY - 1])
def test_no_block_on_short_history(prior_loads):
    rng = random.Random(11)
    history = _history(rng, prior_loads, n=1000)
    current = _run(_batch(rng, 400))[1]

    report = build_load_history_report(
        current_profile=current, prior_runs=history, current_row_count=400
    )

    assert report["passed"] is True and report["anomalies"] == []
    assert report["blocking"] is False
    assert report["comparable_prior_loads"] == prior_loads
    assert report["min_blocking_history"] == MIN_BLOCKING_HISTORY == 8
    # The finding is still reported, as advice.
    assert any("below 50% of prior median" in a for a in report["advisories"])
    assert all(
        signal.get("advisory") for f in report["column_findings"] for signal in f["signals"]
    )


def test_the_same_finding_blocks_once_the_history_is_long_enough():
    rng = random.Random(12)
    history = _history(rng, MIN_BLOCKING_HISTORY, n=1000)
    current = _run(_batch(rng, 400))[1]
    report = build_load_history_report(
        current_profile=current, prior_runs=history, current_row_count=400
    )
    assert report["blocking"] is True and report["passed"] is False
    assert any("below 50% of prior median" in a for a in report["anomalies"])
    assert report["advisories"] == []


def test_a_single_fallback_baseline_never_blocks():
    rng = random.Random(13)
    baseline = _run(_batch(rng, 500, mu=10.0, sigma=1.0))[1]
    current = _run(_batch(rng, 500, mu=2000.0, sigma=1.0))[1]
    report = build_load_history_report(current_profile=current, fallback_baseline=baseline)
    assert report["passed"] is True and report["anomalies"] == []
    assert any("mean" in a.lower() for a in report["advisories"])


# ── mean drift: measured false-positive rate on stationary data ─────────────


def _mean_drift_rates(prior_loads: int, trials: int, seed: int) -> tuple[float, float]:
    """(rate of the fixed 3.5 cut, rate of the report) on i.i.d. loads of one distribution."""
    rng = random.Random(seed)
    legacy = current_rule = 0
    for _ in range(trials):
        history = _history(rng, prior_loads)
        current = _run(_batch(rng))[1]
        means = [Decimal(str(r["columns"]["amt"]["mean"])) for r in history]
        # The statistic the fixed cut was applied to: 0.6745 * (x - median) / MAD.
        med = sorted(means)[len(means) // 2] if len(means) % 2 else sum(sorted(means)[len(means) // 2 - 1: len(means) // 2 + 1]) / 2
        mad_values = sorted(abs(m - med) for m in means)
        mad = mad_values[len(mad_values) // 2] if len(mad_values) % 2 else sum(mad_values[len(mad_values) // 2 - 1: len(mad_values) // 2 + 1]) / 2
        if mad and abs(Decimal("0.6745") * (Decimal(str(current["amt"].mean)) - med) / mad) > Decimal(str(_LEGACY_Z_CUT)):
            legacy += 1
        report = build_load_history_report(
            current_profile=current, prior_runs=history, current_row_count=200
        )
        current_rule += any("mean drifted" in a for a in report["anomalies"])
    return legacy / trials, current_rule / trials


@pytest.mark.parametrize("prior_loads", [8, 10])
def test_mean_drift_false_positive_rate_below_target(prior_loads, record_property):
    trials = 600
    before, after = _mean_drift_rates(prior_loads, trials, seed=20261111 + prior_loads)
    record_property("fpr_fixed_cut_3_5", before)
    record_property("fpr_calibrated", after)
    print(
        f"mean-drift FPR, stationary N(100,15), {prior_loads} prior loads, {trials} trials: "
        f"fixed cut 3.5 = {before:.1%}, calibrated = {after:.1%}"
    )
    # Before: well above the 1% target. After: at the target (within Monte
    # Carlo noise of 600 trials: 1% +- 0.8% at two sigma).
    assert before >= 0.02, before
    assert after <= 0.02, after
    assert after < before


def test_a_real_mean_shift_is_still_caught():
    rng = random.Random(21)
    history = _history(rng, 10)
    shifted = _run(_batch(rng, mu=130.0))[1]
    report = build_load_history_report(
        current_profile=shifted, prior_runs=history, current_row_count=200
    )
    assert any("mean drifted" in a for a in report["anomalies"])


def test_drift_cutoff_follows_history_length_and_target():
    assert _drift_cutoff(8) > _drift_cutoff(12) > _drift_cutoff(20) == 3.5
    assert _drift_cutoff(100) == 3.5
    assert _drift_cutoff(8, 0.001) > _drift_cutoff(8, 0.01) > _drift_cutoff(8, 0.05)


def test_a_tiny_move_on_a_very_stable_history_is_not_many_deviations():
    stable = [Decimal("100.00"), Decimal("100.01"), Decimal("100.02")] * 3
    assert abs(_robust_z(Decimal("100.20"), stable)) < 1
    assert abs(_robust_z(Decimal("104"), [Decimal("100")] * 9)) == 4
    assert _robust_z(Decimal("100"), [Decimal("100")] * 9) == 0


# ── the shifts the gate missed ──────────────────────────────────────────────


def test_volume_spike_flagged():
    rng = random.Random(31)
    history = _history(rng, 8, n=1000)
    current = _run(_batch(rng, 1000))[1]
    report = build_load_history_report(
        current_profile=current, prior_runs=history, current_row_count=100_000
    )
    assert any("100.0x the prior median" in a for a in report["anomalies"]), report["anomalies"]
    assert report["volume_note"] and report["passed"] is False


def test_a_bursty_history_is_not_a_volume_spike():
    rng = random.Random(32)
    sizes = [50, 4000, 60, 5000, 55, 70, 4500, 65]
    history = [_run(_batch(rng, n))[0] for n in sizes]
    current = _run(_batch(rng, 200))[1]
    report = build_load_history_report(
        current_profile=current, prior_runs=history, current_row_count=8000
    )
    assert not any("the prior median" in a and "x the" in a for a in report["anomalies"])


def test_null_rate_jump_flagged():
    rng = random.Random(41)
    history = [_run(_nulls(rng, 5000, 0.001))[0] for _ in range(8)]
    current = _run(_nulls(rng, 5000, 0.09))[1]
    report = build_load_history_report(
        current_profile=current, prior_runs=history, current_row_count=5000
    )
    assert any("null-rate shifted" in a for a in report["anomalies"]), report


def test_null_rate_noise_is_not_a_jump():
    rng = random.Random(42)
    steady = [_run(_nulls(rng, 5000, 0.05))[0] for _ in range(8)]
    report = build_load_history_report(
        current_profile=_run(_nulls(rng, 5000, 0.06))[1], prior_runs=steady,
        current_row_count=5000,
    )
    assert not any("null-rate" in a for a in report["anomalies"])
    # A history that already swings between 0% and 8% does not make 9% a jump.
    swinging = [_run(_nulls(rng, 5000, r))[0] for r in (0.0, 0.08, 0.01, 0.07, 0.0, 0.08, 0.02, 0.06)]
    report = build_load_history_report(
        current_profile=_run(_nulls(rng, 5000, 0.09))[1], prior_runs=swinging,
        current_row_count=5000,
    )
    assert not any("null-rate" in a for a in report["anomalies"])
