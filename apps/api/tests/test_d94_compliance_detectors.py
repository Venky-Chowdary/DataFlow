"""D94 — the compliance (PII) gate must classify email, phone, IP address and
passport data, and must not invent those classes for look-alike values.

Columns here are deliberately neutral (``c``) so the value detectors, not
the name rules, are under test, unless the test is about a name rule.
"""

from __future__ import annotations

import pytest

from services.compliance_guard import detect_pii_fields, score_compliance_risk


def _value_risk(*values: str) -> list[str]:
    report = detect_pii_fields(["c"], [{"c": v} for v in values])
    return report["field_risk"].get("c", [])


@pytest.mark.parametrize(
    "value",
    ["alice@example.com", "Bob.Smith+tag@mail.example.co.uk", "x_y-z@sub.domain.io"],
)
def test_email_values(value):
    assert _value_risk(value) == ["email"]


@pytest.mark.parametrize(
    "value",
    [
        "+14155550132",          # E.164
        "+44 20 7946 0958",
        "(555) 123-4567",        # NANP with area-code parentheses
        "555.123.4567",
        "555-123-4567",
        "+49 (0)30 1234567",
    ],
)
def test_phone_values(value):
    assert _value_risk(value) == ["phone"]


@pytest.mark.parametrize(
    "value",
    ["192.168.10.24", "10.0.0.1", "8.8.8.8", "2001:db8::8a2e:370:7334", "fe80::1"],
)
def test_ip_address_values(value):
    assert _value_risk(value) == ["ip_address"]


# ICAO 9303 TD3 sample passport MRZ (line 1 and line 2 with valid check digits).
MRZ_LINE1 = "P<UTOERIKSSON<<ANNA<MARIA<<<<<<<<<<<<<<<<<<<"
MRZ_LINE2 = "L898902C36UTO7408122F1204159ZE184226B<<<<<10"


@pytest.mark.parametrize("value", [MRZ_LINE1, MRZ_LINE2])
def test_passport_mrz_values(value):
    assert _value_risk(value) == ["passport"]


def test_passport_column_name_is_high_risk():
    report = score_compliance_risk(
        ["id", "passport_number"], [{"id": "1", "passport_number": "K1234567"}]
    )
    assert "passport" in report["field_risk"]["passport_number"]
    assert "passport_number" in report["high_risk_fields"]
    assert report["requires_review"] is True


@pytest.mark.parametrize("name", ["ip_address", "client_ip", "ip", "remote_addr", "ipv6"])
def test_ip_column_names(name):
    report = detect_pii_fields([name], [])
    assert "ip_address" in report["field_risk"].get(name, [])


@pytest.mark.parametrize(
    "value",
    [
        "999.168.10.24",         # octet out of range
        "192.168.010.024",       # leading-zero octets (not a canonical IPv4)
        "1.2.3",                 # semantic version
        "12345.67",              # decimal amount
        "0.0000000150",
        "12:30:45",              # time of day
        "123",                   # too short for a phone
        "1234567890123456789",   # 19 digits: beyond E.164's 15
        "L898902C36UTO7408122F1204159ZE184226B<<<<<11",  # MRZ with bad check digit
        "P<UTOERIKSSON",         # truncated MRZ
        "X12345678",             # SKU-shaped code: passport by name only
        "user at example dot com",
    ],
)
def test_look_alikes_are_not_pii(value):
    assert _value_risk(value) == []


@pytest.mark.parametrize("name", ["zip", "shipping_policy", "description", "tip", "skip_count"])
def test_ip_name_rule_needs_a_whole_token(name):
    report = detect_pii_fields([name], [])
    assert "ip_address" not in report["field_risk"].get(name, [])
