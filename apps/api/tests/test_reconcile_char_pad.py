"""CHAR blank-pad vs VARCHAR trailing-space Gate-8 fingerprint honesty."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.reconciliation import fingerprint_for_reconcile, normalize_cell  # noqa: E402


def test_char_pad_rtrimmed_for_reconcile():
    # Oracle/SQL Server CHAR(5) readback pads with spaces — must match unpadded source.
    assert normalize_cell("abc  ", ddl_type="CHAR(5)") == "abc"
    assert normalize_cell("abc", ddl_type="CHAR(5)") == "abc"
    assert fingerprint_for_reconcile("abc", ddl_type="CHAR(5)") == fingerprint_for_reconcile(
        "abc  ", ddl_type="CHAR(5)"
    )


def test_varchar_preserves_trailing_spaces_when_typed():
    # Intentional trailing space on VARCHAR must not be stripped away.
    assert normalize_cell("abc ", ddl_type="VARCHAR(10)") == "abc "
    assert normalize_cell("abc", ddl_type="VARCHAR(10)") == "abc"
    assert fingerprint_for_reconcile("abc ", ddl_type="VARCHAR(10)") != fingerprint_for_reconcile(
        "abc", ddl_type="VARCHAR(10)"
    )


def test_unknown_ddl_keeps_legacy_strip():
    assert normalize_cell("  hello  ") == "hello"
