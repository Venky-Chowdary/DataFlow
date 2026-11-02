"""Interval fingerprint canonicalization — PG '1 day 02:00:00' == '26:00:00'.

Regression: the desktop-lab span cells (create_new_array_native /
dest_exists_intarray / dest_exists_native, pg->pg interval column) reported
L4/L5 mismatches because the source side of a reconcile fingerprinted the PG
verbose interval literal while the destination read-back handed the timedelta
render '26:00:00'. Both are the same span — the fingerprint must be one value.
"""
from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.reconciliation import fingerprint_for_reconcile


def test_pg_verbose_interval_equals_hms_render():
    a = fingerprint_for_reconcile("1 day 02:00:00", ddl_type="interval")
    b = fingerprint_for_reconcile("26:00:00", ddl_type="interval")
    assert a == b


def test_timedelta_under_interval_ddl_matches_verbose_text():
    a = fingerprint_for_reconcile("1 day 02:00:00", ddl_type="interval")
    b = fingerprint_for_reconcile(timedelta(days=1, hours=2), ddl_type="interval")
    assert a == b


def test_interval_months_do_not_fold_into_days():
    # PG does NOT equate '1 mon' = '30 days' — months are a distinct field.
    a = fingerprint_for_reconcile("1 mon", ddl_type="interval")
    b = fingerprint_for_reconcile("30 days", ddl_type="interval")
    assert a != b


def test_interval_years_equal_twelve_months():
    a = fingerprint_for_reconcile("1 year", ddl_type="interval")
    b = fingerprint_for_reconcile("12 mons", ddl_type="interval")
    assert a == b


def test_non_interval_text_does_not_canonicalize():
    # A plain string that isn't interval-shaped stays as-is under interval ddl.
    a = fingerprint_for_reconcile("not an interval", ddl_type="interval")
    assert a == "not an interval"


def test_bare_hms_under_time_ddl_unaffected():
    # TIME ddl is a clock value, not an interval — no canonical rewrite.
    a = fingerprint_for_reconcile("26:00:00", ddl_type="time")
    assert a == "26:00:00"
