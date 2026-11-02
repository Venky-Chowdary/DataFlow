"""Unit guards for the round-5 temporal rules (the e2e proof is
``test_r5_bad_date_in_file_e2e.py``): one calendar rule shared by the bind, the
writer's quarantine pass and the bulk-load census; the ISO date reading; and a
file read's date pin that does not outlive the read."""

from __future__ import annotations

import pytest

from connectors.sql_temporal import coerce_sql_temporal, invalid_calendar_reason
from connectors.writer_quarantine_unfit import temporal_cell_unfit_reason
from services.copy_fast_path import text_cell_copy_safe
from services.transform_engine import (
    DATE_LOCALE_ISO,
    apply_transform,
    infer_date_locale,
    reset_active_date_locale,
    set_active_date_locale,
)

UNREAL = ["2024-02-30", "2023-02-29", "0000-00-00", "2024-13-01", "2024-00-10",
          "2024-01-01 25:61:00", "2024-01-01T10:60:00Z", "10:61"]
REAL = ["2024-02-29", "2024-12-31", "2024-01-01 23:59:59", "2024-01-01T10:00:00+05:30",
        "2024-01-01 24:00:00", "0044-03-15 BC", "10000-01-01", "infinity", "not a date", ""]


@pytest.mark.parametrize("value", UNREAL)
def test_unreal_calendar_values_are_named_by_every_layer(value):
    reason = invalid_calendar_reason(value)
    assert reason.startswith(("Invalid date:", "Invalid time:")) and repr(value) in reason
    with pytest.raises(ValueError, match="not a real"):
        coerce_sql_temporal(value, "DATE", engine="postgresql")
    assert "not a real" in temporal_cell_unfit_reason(value, "DATE", dest_db="mysql")


@pytest.mark.parametrize("value", REAL)
def test_real_or_other_rule_values_are_not_this_rules(value):
    assert invalid_calendar_reason(value) == ""


@pytest.mark.parametrize(
    ("value", "logical", "physical", "dest_db", "safe"),
    [
        ("2024-02-30", "date", "DATE", "postgresql", False),
        ("0000-00-00", "date", "DATE", "mysql", False),
        ("2024-13-01", "date", "", "", False),
        ("2024-02-30 10:00:00", "datetime", "timestamp", "postgresql", False),
        ("25:61:00", "time", "TIME", "postgresql", False),
        ("0999-12-31", "date", "DATE", "mysql", False),  # MySQL DATE is 1000..9999
        ("0999-12-31", "date", "date", "postgresql", True),
        ("2024-02-29", "date", "DATE", "mysql", True),
        ("2024-06-01 10:30:00", "datetime", "DATETIME(6)", "mysql", True),
    ],
)
def test_bulk_load_census_grades_the_calendar_not_the_shape(value, logical, physical, dest_db, safe):
    assert text_cell_copy_safe(value, logical, physical=physical, dest_db=dest_db) is safe


def test_iso_evidence_pins_iso_and_mixed_columns_are_ambiguous():
    assert infer_date_locale([{"d": "2024-01-15"}], ["d"]) == DATE_LOCALE_ISO
    # ISO beside slash dates valid under both orders: no order is settled.
    assert infer_date_locale([{"d": "2024-01-15"}, {"d": "01/02/2024"}], ["d"]) == DATE_LOCALE_ISO
    # ISO beside slash dates valid under one order only: that order.
    assert infer_date_locale([{"d": "2024-01-15"}, {"d": "31/12/2024"}], ["d"]) == "DMY"
    assert infer_date_locale(
        [{"d": "03/15/1985"}, {"d": "1987-12-31"}, {"d": "12-31-1990"}], ["d"]
    ) == "MDY"
    # Separate columns keep their own evidence: ISO beside a DMY column is DMY.
    assert infer_date_locale([{"a": "2024-01-15", "b": "31/12/2024"}], ["a", "b"]) == "DMY"
    assert infer_date_locale([{"b": "31/12/2024"}], ["b"]) == "DMY"
    assert infer_date_locale([{"n": "abc"}], ["n"]) == ""

    token = set_active_date_locale(DATE_LOCALE_ISO)
    try:
        for ambiguous in ("01/02/2024", "03-04-2024", "11.12.2024"):
            value, err = apply_transform(ambiguous, "date")
            assert value is None and "ambiguous" in err and ambiguous in err
            with pytest.raises(ValueError, match="ambiguous"):
                coerce_sql_temporal(ambiguous, "DATE", engine="postgresql")
        assert apply_transform("2024-12-31", "date") == ("2024-12-31", None)
        # Valid under one order only: its single day, at every layer.
        for text, day in (("31/12/2024", "2024-12-31"), ("03/15/1985", "1985-03-15"),
                          ("12-31-1990", "1990-12-31"), ("05/05/2024", "2024-05-05")):
            assert apply_transform(text, "date") == (day, None), text
            assert str(coerce_sql_temporal(text, "DATE", engine="postgresql")) == day, text
    finally:
        reset_active_date_locale(token)
    # Auto (no pin) and a declared DMY still read it.
    assert apply_transform("31/12/2024", "date") == ("2024-12-31", None)
    token = set_active_date_locale("DMY")
    try:
        assert apply_transform("31/12/2024", "date") == ("2024-12-31", None)
    finally:
        reset_active_date_locale(token)


def test_a_file_read_does_not_leave_its_date_pin_on_the_thread(tmp_path):
    from services.transform_engine import _active_date_locale, mixed_date_format_reason
    from src.transfer.file_stream import stream_file_to_database

    path = tmp_path / "iso.csv"
    path.write_text("id,d\n1,2024-01-15\n", encoding="utf-8")
    try:
        stream_file_to_database(
            content=path, filename="iso.csv", dest_type="sqlite",
            dest_cfg={"database": str(tmp_path / "out.db")}, dest_table="t",
        )
    except Exception:  # noqa: BLE001 - only the pin after the call matters here
        pass
    assert _active_date_locale() == ""
    assert mixed_date_format_reason("31/12/2024") == ""
