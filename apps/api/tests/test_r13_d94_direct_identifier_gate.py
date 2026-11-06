"""D94 — the PII gate flags email, phone, IP and free text that carries them,
by name and by value, and holds a table made of direct identifiers for review.

QA (Neon ``hq1410_em``: id, email, phone, contact=email -> SQLite strict):
staged ``safe_to_start`` with no PII finding; ``hq1410_p2`` (email_address,
phone_number, ip_address, passport_no, notes with "contact me at x@gmail.com
or 212-555-0001") flagged only date_of_birth / iban; ``card_number`` read as
"account by column name, phone by sample value".

Root cause: ``score_compliance_risk`` averaged per-column weights, so a table
of nothing but 0.18-weight identifiers never reached the 0.45 review floor;
anchored regexes never looked inside prose; and the mapper carried its own
email/phone regexes, so Map could say "email-like (PII)" where the gate saw
nothing. One classifier now serves the gate, the mapper and create_schedule
(which stages through the same proof bundle).
"""

from __future__ import annotations

import pytest

from services.compliance_guard import (
    detect_pii_fields,
    embedded_pii_categories,
    is_email_value,
    is_phone_value,
    pii_value_category,
    score_compliance_risk,
)
from services.preflight_proof_bundle import build_preflight_proof_bundle

_EM_COLS = ["id", "email", "phone", "contact"]
_EM_ROWS = [
    {"id": 1, "email": "user1@example.com", "phone": "+1-555-1001", "contact": "user1@example.com"},
    {"id": 2, "email": "user2@example.com", "phone": "+1-555-1002", "contact": "user2@example.com"},
    {"id": 3, "email": None, "phone": "", "contact": "   "},
    {"id": 4, "email": "łukasz@example.pl", "phone": "+48 22 123 45 67", "contact": "山田@example.jp"},
]

_P2_COLS = ["email_address", "phone_number", "ip_address", "passport_no", "notes"]
_P2_ROWS = [
    {
        "email_address": "a@gmail.com", "phone_number": "(212) 555-0001",
        "ip_address": "10.1.2.3", "passport_no": "K1234567",
        "notes": "contact me at x@gmail.com or 212-555-0001",
    },
    {
        "email_address": "", "phone_number": None, "ip_address": "   ",
        "passport_no": "", "notes": "no identifiers here, see ticket 42",
    },
    {
        "email_address": "b@gmail.com", "phone_number": "(212) 555-0002",
        "ip_address": "2001:db8::1", "passport_no": "X7654321",
        "notes": "reach bob on +44 20 7946 0958 🎉",
    },
]


def _evidence(report: dict, col: str) -> set[tuple[str, str]]:
    return {(e["category"], e["rule"]) for e in report["findings_by_col"][col]}


def _by_col(compliance: dict) -> dict:
    compliance["findings_by_col"] = {f["column"]: f["evidence"] for f in compliance["findings"]}
    return compliance


def test_a_table_of_emails_and_phones_is_held_for_review_by_name_and_by_value():
    out = _by_col(score_compliance_risk(_EM_COLS, _EM_ROWS))
    assert out["requires_review"] is True, out
    assert out["risk_level"] == "high"
    assert out["direct_identifier_columns"] == ["contact", "email", "phone"]
    assert "3 of 4 column(s)" in out["review_reason"]
    assert _evidence(out, "email") == {("email", "column_name"), ("email", "sample_value")}
    assert _evidence(out, "phone") == {("phone", "column_name"), ("phone", "sample_value")}
    # ``contact`` has no PII name: content alone finds it.
    assert _evidence(out, "contact") == {("email", "sample_value")}
    assert "id" not in out["sensitive_fields"]


def test_free_text_carrying_an_email_and_a_phone_is_flagged_and_ip_passport_by_name_and_value():
    out = _by_col(score_compliance_risk(_P2_COLS, _P2_ROWS))
    assert out["requires_review"] is True
    assert _evidence(out, "notes") == {("email", "embedded_value"), ("phone", "embedded_value")}
    assert {("ip_address", "column_name"), ("ip_address", "sample_value")} <= _evidence(
        out, "ip_address"
    )
    assert ("passport", "column_name") in _evidence(out, "passport_no")
    assert {("email", "column_name"), ("email", "sample_value")} <= _evidence(out, "email_address")
    assert _evidence(out, "phone_number") == {("phone", "column_name"), ("phone", "sample_value")}
    assert "passport_no" in out["high_risk_fields"]


def test_card_numbers_are_pci_not_account_or_phone():
    rows = [
        {"card_number": "4111 1111 1111 1111", "ssn": "123-45-6789", "full_name": "Ada Lovelace"},
        {"card_number": "5500-0000-0000-0004", "ssn": "", "full_name": None},
    ]
    out = score_compliance_risk(["card_number", "ssn", "full_name"], rows)
    assert out["field_risk"]["card_number"] == ["pci_pan"], out["field_risk"]
    assert "PCI-DSS" in out["compliance_tags"]
    assert "card_number" in out["high_risk_fields"]


def test_one_customer_email_in_a_wide_business_table_is_a_warning_not_a_hold():
    cols = ["id", "email", "sku", "qty", "price", "created_at", "region", "notes", "status", "tier"]
    rows = [
        {"id": i, "email": f"u{i}@example.com", "sku": f"SKU-{i}", "qty": i, "price": "9.99",
         "created_at": "2024-03-10", "region": "EU", "notes": "reorder soon", "status": "open", "tier": "b"}
        for i in range(5)
    ]
    out = score_compliance_risk(cols, rows)
    assert out["sensitive_fields"] == ["email"]
    assert out["requires_review"] is False
    assert out["direct_identifier_columns"] == ["email"]
    assert out["review_reason"] == ""


def test_a_name_alone_does_not_make_a_boolean_column_a_direct_identifier():
    cols = ["id", "email_opt_in", "phone_verified"]
    rows = [{"id": 1, "email_opt_in": "true", "phone_verified": "false"},
            {"id": 2, "email_opt_in": "false", "phone_verified": "true"}]
    out = score_compliance_risk(cols, rows)
    assert out["direct_identifier_columns"] == []
    assert out["requires_review"] is False
    # Without any sampled value the name is all the evidence there is: it is
    # reported as such, and never holds a table that has shown nothing yet.
    from services.preflight_proof_bundle import _finding_lines

    unsampled = score_compliance_risk(["id", "email", "phone"], [])
    assert unsampled["sensitive_fields"] == ["email", "phone"]
    assert unsampled["direct_identifier_columns"] == []
    assert unsampled["requires_review"] is False
    assert _finding_lines(unsampled) == [
        "email (email by column name, no sample)",
        "phone (phone by column name, no sample)",
    ]
    # A sparse sample (one row, both empty) is the same case.
    sparse = score_compliance_risk(["id", "email", "phone"], [{"id": 1, "email": "", "phone": None}])
    assert sparse["requires_review"] is False
    assert "no sample" in _finding_lines(sparse)[0]
    # Once the content is there the hold is, and the text drops the caveat.
    assert "no sample" not in " ".join(_finding_lines(score_compliance_risk(_EM_COLS, _EM_ROWS)))


@pytest.mark.parametrize("value", [
    "Model 3.5 ships 2024-03-10 10:00:00",
    "order 1234567890 shipped",
    "550e8400-e29b-41d4-a716-446655440000",
    '{"total": 1234.56, "ids": [1, 2, 3]}',
    "   ",
    "",
    None,
    "Łukasz 山田 🎉",
])
def test_free_text_look_alikes_are_not_embedded_pii(value):
    assert embedded_pii_categories(value) == []


@pytest.mark.parametrize("value", ["+tag@example.com", ".dot@example.com", "a.b+c@sub.example.co.uk", "山田@example.jp"])
def test_whole_value_email_classification_is_anchored_not_word_bounded(value):
    # ``\b`` + fullmatch rejected a local part that starts on a non-word
    # character; the classifier uses an anchored pattern (the masker keeps \b).
    assert pii_value_category(value) == "email"
    assert embedded_pii_categories(f"mail me: {value} today") == ["email"]


def test_the_profiler_and_the_sample_quality_check_agree_with_the_gate():
    from services.data_profiler import _infer_pattern
    from services.sample_quality import analyze_column_quality

    values = ["山田@example.jp", "+tag@example.com", "ada@example.com"]
    assert all(is_email_value(v) for v in values)
    # The profiler's dominant pattern and the gate agree on every one of them.
    assert _infer_pattern(values) == "email"
    quality = analyze_column_quality("email", values, inferred_type="TEXT")
    assert not [i for i in (quality.get("issues") or []) if "invalid email" in str(i)], quality


def test_plain_digit_strings_are_not_identifiers_even_when_the_sample_is_one_row():
    """Kafka digit-string keys and fields (test_d190_d194_d197): the second
    incremental run samples one message, so every digit column is 100%
    "phone-shaped" and the density rule held the run. A plain digit run is an
    order id, an epoch or a key: reported at most as a soft phone finding,
    never a direct identifier."""
    from services.preflight_proof_bundle import _finding_lines

    cols = ["_kafka_key", "order_id", "ts_s", "code", "flag", "note"]
    one = [{"_kafka_key": "20240102", "order_id": "20240102", "ts_s": "1700000003",
            "code": "0100", "flag": "f", "note": " "}]
    out = score_compliance_risk(cols, one)
    assert out["direct_identifier_columns"] == [], out["findings"]
    assert out["requires_review"] is False
    assert out["review_reason"] == ""
    for line in _finding_lines(out):
        assert "(bare digits)" in line, line
    three = one + [
        {"_kafka_key": "20240101", "order_id": "20240101", "ts_s": "1700000000", "code": "0012", "flag": "t", "note": ""},
        {"_kafka_key": "1700000000", "order_id": "20231231", "ts_s": "1700000000000", "code": "0007", "flag": "f", "note": None},
    ]
    assert score_compliance_risk(cols, three)["requires_review"] is False
    # A phone shape, or a phone-hinting name, is what makes a phone column.
    shaped = score_compliance_risk(
        ["id", "contact", "mobile", "callback"],
        [{"id": 1, "contact": "+1 415 555 0100", "mobile": "5551234000", "callback": "(212) 555-0199"}],
    )
    assert shaped["direct_identifier_columns"] == ["callback", "contact", "mobile"]
    assert shaped["requires_review"] is True


def test_the_mapper_reads_email_and_phone_through_the_same_classifier():
    from services.mapping_quality import analyze_column_profile

    emails = ["user1@example.com", "user2@example.com", "山田@example.jp"]
    assert all(is_email_value(v) for v in emails)
    assert analyze_column_profile("contact", emails)["likely_email"] is True
    phones = ["+1-555-1001", "(212) 555-0001", "+48 22 123 45 67"]
    assert all(is_phone_value(v) for v in phones)
    assert analyze_column_profile("mobile", phones)["likely_phone"] is True
    # The gate's exclusions are the mapper's too: dates and amounts are neither.
    for v in ("2024-03-10", "12345.67", "1.2.3.4"):
        assert not is_phone_value(v) and pii_value_category(v) != "phone"
    assert analyze_column_profile("c", ["2024-03-10", "2024-03-11"])["likely_phone"] is False


def test_proof_bundle_holds_strict_preflight_until_acknowledged():
    bundle = build_preflight_proof_bundle(
        columns=_EM_COLS, sample_rows=_EM_ROWS,
        mappings=[{"source": c, "target": c, "confidence": 0.99} for c in _EM_COLS],
        validation_mode="strict",
    )
    blockers = bundle["transfer_decision"]["blockers"]
    holds = [b for b in blockers if "PII/compliance review required" in b]
    assert len(holds) == 1, blockers
    assert bundle["transfer_decision"]["decision"] == "review"
    assert "direct identifiers (email/phone/IP) in 3 of 4 column(s)" in holds[0]
    assert "contact (email by sample value)" in holds[0]
    acked = build_preflight_proof_bundle(
        columns=_EM_COLS, sample_rows=_EM_ROWS,
        mappings=[{"source": c, "target": c, "confidence": 0.99} for c in _EM_COLS],
        validation_mode="strict", compliance_acknowledged=True,
        acknowledgment_actor="qa@example.com", acknowledgment_reason="d94",
    )
    assert not [b for b in acked["transfer_decision"]["blockers"] if "PII/compliance" in b]
    assert acked["compliance"]["review_status"] == "acknowledged"


def test_masking_the_identifiers_is_the_remediation_the_hold_asks_for():
    mappings = [{"source": c, "target": c, "confidence": 0.99} for c in _EM_COLS]
    for m in mappings:
        if m["source"] in ("email", "contact"):
            m["transform"] = "mask_pii"
    bundle = build_preflight_proof_bundle(
        columns=_EM_COLS, sample_rows=_EM_ROWS, mappings=mappings, validation_mode="strict",
    )
    assert not [b for b in bundle["transfer_decision"]["blockers"] if "PII/compliance" in b]
    # Still reported: the masked columns are named as protected, phone stays open.
    assert bundle["compliance"]["sensitive_fields"] == ["contact", "email", "phone"]
    assert bundle["compliance"]["review_reason"] == ""


def test_detect_pii_fields_reports_which_columns_had_values():
    report = detect_pii_fields(_EM_COLS, _EM_ROWS)
    assert report["sampled_fields"] == ["contact", "email", "id", "phone"]
