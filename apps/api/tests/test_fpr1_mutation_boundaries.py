"""PR 411 review 1: pin the three boundaries a mutation pass left unpinned.

* the ``'-inf'`` text spelling is negative infinity (mapped to +Infinity, no
  test failed);
* a CDC consumer heartbeat goes stale after 300 s (3000 s passed every test);
* a load is a volume spike above 3x the prior median (30x passed every test).
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

import pytest

from services.cdc_lag_honesty import HEARTBEAT_STALE_SECONDS, observe_cdc_lag
from services.fingerprint_rules import NEG_INFINITY, POS_INFINITY, nonfinite_token
from services.reconciliation import normalize_cell


@pytest.mark.parametrize("spelling", ["-inf", "-INF", " -inf ", "-infinity", "-Infinity"])
def test_minus_inf_spellings_are_negative_infinity(spelling):
    assert nonfinite_token(spelling) == NEG_INFINITY


@pytest.mark.parametrize("ddl", ["DOUBLE PRECISION", "NUMERIC", ""])
def test_minus_inf_text_cell_is_the_negative_infinity_value(ddl):
    cell = normalize_cell("-inf", ddl_type=ddl)
    assert cell == normalize_cell(float("-inf"), ddl_type=ddl) == NEG_INFINITY
    assert cell != normalize_cell("inf", ddl_type=ddl)


def test_plus_inf_text_is_positive_infinity():
    assert nonfinite_token("inf") == nonfinite_token("+inf") == POS_INFINITY


_NOW = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)


def _lag(heartbeat_age_s: float) -> dict:
    return observe_cdc_lag(
        last_heartbeat_at=_NOW - timedelta(seconds=heartbeat_age_s),
        replication_lag_bytes=0,
        now=_NOW,
    )


def test_heartbeat_is_stale_after_300_seconds():
    assert HEARTBEAT_STALE_SECONDS == 300.0
    fresh = _lag(299)
    assert fresh["consumer_alive"] is True and fresh["caught_up"] is True
    stale = _lag(301)
    assert stale["consumer_alive"] is False and stale["caught_up"] is False
    assert stale["cdc_lag_seconds"] is None


def _volume_report(current_rows: int, prior_rows: int = 1000) -> dict:
    from tests.test_audit08_dq_history_gate import _batch, _run

    from services.data_quality_history import build_load_history_report

    rng = random.Random(41)
    history = [_run(_batch(rng, prior_rows))[0] for _ in range(8)]
    current = _run(_batch(rng, 200))[1]
    return build_load_history_report(
        current_profile=current, prior_runs=history, current_row_count=current_rows
    )


def _spiked(report: dict) -> bool:
    return any("x the prior median" in a for a in report["anomalies"])


def test_volume_spike_starts_above_three_times_the_median():
    # Both loads are over twice the largest prior load; only the ratio differs.
    assert _spiked(_volume_report(3_100)), "3.1x the median must be a spike"
    assert not _spiked(_volume_report(2_900)), "2.9x the median is not"
