"""D241: a date-shaped value never matches the phone rule.

The date exemption was all-or-nothing per column: one non-date cell in a DATE
column (a bad row kept for quarantine) and every ``2024-03-10`` in the sample
read as a phone, so every plan warned "d (phone by sample value)".
"""

from __future__ import annotations

import pytest

from services.compliance_guard import detect_pii_fields


def _dates_plus(odd: str) -> list[dict[str, str]]:
    return [{"d": f"2024-03-{(i % 28) + 1:02d}"} for i in range(29)] + [{"d": odd}]


@pytest.mark.parametrize("odd", ["not-a-date", "N/A", "0", "2024-02-30"])
def test_date_column_with_one_bad_cell_is_not_phone(odd: str) -> None:
    report = detect_pii_fields(["d"], _dates_plus(odd))
    assert "phone" not in (report.get("field_risk") or {}).get("d", []), report


@pytest.mark.parametrize("value", ["2024-03-10", "2024/03/10", "2024.03.10", "2024-3-9"])
def test_single_date_value_is_not_phone(value: str) -> None:
    report = detect_pii_fields(["note"], [{"note": value}, {"note": "free text"}])
    assert "phone" not in (report.get("field_risk") or {}).get("note", []), report


@pytest.mark.parametrize("value", ["555-123-4567", "(555) 123-4567", "+1 555 123 4567", "555.123.4567"])
def test_formatted_phones_still_flag(value: str) -> None:
    report = detect_pii_fields(["contact"], [{"contact": value}, {"contact": "n/a"}])
    assert "phone" in (report.get("field_risk") or {}).get("contact", []), report
