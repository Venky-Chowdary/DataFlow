"""Gate-8 fingerprint reproduces what the writer bound — not strict refusal.

When ``validation_mode='strict'`` runs without a declared ``number_locale``,
the engine pins ``set_strict_number_reading(True)`` for the whole run. That
pin governs ``apply_transform`` — a locale-ambiguous literal like
``'2.000,50'`` refuses under strict+WIRE. But the write-path bind is not
strict-gated: the writer still landed ``Decimal('2000.50')``.

The source-side fingerprint must mirror the bound value, or Gate-8 reports
the destination cell as tampered (NULL fingerprint vs real value) on a
faithful transfer — the csv→duckdb messy-locale regression.
"""

from decimal import Decimal

from services.reconciliation import fingerprint_for_reconcile
from services.transform_engine import (
    reset_strict_number_reading,
    set_strict_number_reading,
)


def test_fingerprint_mirrors_lenient_writer_bind_under_strict_flag():
    token = set_strict_number_reading(True)
    try:
        src = fingerprint_for_reconcile(
            "2.000,50",
            ddl_type="DECIMAL(38,2)",
            engine="generic_sql",
            transform="decimal",
        )
        dst = fingerprint_for_reconcile(
            Decimal("2000.50"),
            ddl_type="DECIMAL(38,2)",
            engine="generic_sql",
            transform="decimal",
        )
    finally:
        reset_strict_number_reading(token)
    assert src == dst
    assert src != "\x00NULL\x00"


def test_fingerprint_strict_clearing_is_scoped_and_restored():
    token = set_strict_number_reading(True)
    try:
        fingerprint_for_reconcile(
            "2.000,50", ddl_type="DECIMAL(38,2)",
            engine="generic_sql", transform="decimal",
        )
        # Ambient strict pin must survive the call — the flag is run state,
        # not something the fingerprint may consume or leak cleared.
        from services.transform_engine import apply_transform

        _, err = apply_transform("2.000,50", "decimal")
    finally:
        reset_strict_number_reading(token)
    assert err is not None
