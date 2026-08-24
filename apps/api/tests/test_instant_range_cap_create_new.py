"""An instant carrier 68 years wide says so at Map, not at row 900,000.

Create-new from PostgreSQL ``TIMESTAMPTZ`` / Snowflake ``TIMESTAMP_TZ`` onto
MySQL stamps ``TIMESTAMP(6)`` — the only MySQL column that stores an instant,
so polarity and precision are exact and the route needs no Risk Contract. What
that carrier does not keep is the source's *domain*: MySQL ``TIMESTAMP`` holds
1970-01-01 00:00:01 UTC .. 2038-01-19 03:14:07 UTC, while the source spans
4713 BC..294276 AD. Before this, the narrowing was surfaced only by the writer
quarantining rows mid-run; Map showed no risk at all.

The chip is a ``warn`` — a review, not a contract — unless a sampled value is
already outside the window, which is a measured refusal and blocks.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.create_new_risk_stamp import apply_create_new_risk_stamps  # noqa: E402
from services.timezone_policy import (  # noqa: E402
    instant_range_would_cap,
    samples_outside_instant_range,
)
from services.type_system import assess_create_new_type_risk  # noqa: E402


def _kinds(risks: list[dict]) -> set[str]:
    return {str(r.get("kind") or "") for r in risks}


def test_aware_source_into_mysql_timestamp_is_range_capped() -> None:
    for src in ("TIMESTAMPTZ", "TIMESTAMP_TZ", "TIMESTAMP_LTZ", "DATETIMEOFFSET"):
        assert instant_range_would_cap(src, "TIMESTAMP(6)", dest_db="mysql") is True


def test_carriers_that_are_not_epoch_bounded_are_not_capped() -> None:
    # MySQL DATETIME spans 1000..9999 — the range question does not arise.
    assert instant_range_would_cap("TIMESTAMPTZ", "DATETIME(6)", dest_db="mysql") is False
    # PostgreSQL TIMESTAMPTZ has the source's own range.
    assert (
        instant_range_would_cap("TIMESTAMPTZ", "TIMESTAMPTZ", dest_db="postgresql")
        is False
    )
    # A zoneless source is a polarity question, answered elsewhere.
    assert instant_range_would_cap("TIMESTAMP", "TIMESTAMP(6)", dest_db="mysql") is False
    # Non-temporal columns raise no range question.
    assert instant_range_would_cap("VARCHAR(64)", "TIMESTAMP(6)", dest_db="mysql") is False


def test_range_cap_is_a_warn_chip_that_names_the_window_and_the_exit() -> None:
    risks = assess_create_new_type_risk(
        "TIMESTAMPTZ", "TIMESTAMP(6)", destination_db_type="mysql"
    )
    assert _kinds(risks) == {"instant_range_cap"}, risks
    chip = risks[0]
    assert chip["severity"] == "warn"
    assert "2038-01-19 03:14:07 UTC" in chip["message"]
    assert "DATETIME(6)" in chip["message"]


def test_in_window_samples_do_not_clear_the_ceiling() -> None:
    """An eight-row head sample cannot disprove a 2038 ceiling."""
    risks = assess_create_new_type_risk(
        "TIMESTAMPTZ",
        "TIMESTAMP(6)",
        destination_db_type="mysql",
        samples=["2024-01-01T00:00:00Z", "1999-12-31T23:59:59Z"],
    )
    assert _kinds(risks) == {"instant_range_cap"}
    assert risks[0]["severity"] == "warn"


def test_a_sampled_value_outside_the_window_blocks_and_is_named() -> None:
    risks = assess_create_new_type_risk(
        "TIMESTAMPTZ",
        "TIMESTAMP(6)",
        destination_db_type="mysql",
        samples=["2024-01-01T00:00:00Z", "2044-06-01T12:00:00Z"],
    )
    assert _kinds(risks) == {"instant_range_cap"}
    chip = risks[0]
    assert chip["severity"] == "block"
    assert "2044-06-01T12:00:00Z" in chip["message"]
    assert samples_outside_instant_range(["2044-06-01T12:00:00Z"]) == [
        "2044-06-01T12:00:00Z"
    ]
    assert samples_outside_instant_range(["2024-01-01T00:00:00Z", None]) == []


def test_wall_clock_target_keeps_its_polarity_chip_and_no_range_chip() -> None:
    risks = assess_create_new_type_risk(
        "TIMESTAMPTZ", "DATETIME(6)", destination_db_type="mysql"
    )
    assert "instant_range_cap" not in _kinds(risks)


def test_mapping_keeps_the_instant_carrier_and_asks_for_review_not_a_contract() -> None:
    stamped = apply_create_new_risk_stamps(
        [
            {
                "source": "created_at",
                "target": "created_at",
                "source_type": "TIMESTAMPTZ",
                "target_type": "TIMESTAMP(6)",
                "create_new": True,
            }
        ],
        "mysql",
        dest_table_exists=False,
    )
    row = stamped[0]
    assert row["target_type"].upper().startswith("TIMESTAMP(6)")
    assert _kinds(row["create_new_risks"]) == {"instant_range_cap"}
    assert row["requires_review"] is True
    # Range is not fidelity: the instant survives, so no lossy verdict and no
    # Risk Contract is demanded of the operator.
    assert str(row.get("fidelity") or "").lower() in {"", "preserve", "lossless"}
    assert not row.get("requires_risk_contract")
