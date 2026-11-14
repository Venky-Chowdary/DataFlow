"""Audit finding 9 (H7) — PII detection sampled the head; ``mask_pii`` kept re-identifying characters.

Detection read ``rows[:100]``: a ``notes`` column whose SSNs and e-mails start
at row 101 (90% of the table) was reported clean. It now reads a sample spread
over every row it is given (head, tail and a seeded reservoir of the middle —
``services.sample_strategy.position_reservoir_sample``) and states what it
sampled.

``mask_pii`` on a value no PII pattern recognises kept a head and a tail:
``'Smith' -> 'Sm*th'``, ``'1987-03-14' -> '19******14'``, ``'Maria Gonzalez' ->
'Maria …alez'`` (10 of 14 characters); 35 of 40 common first names kept a
unique mask. Such a value now keeps its length class only: no source character
survives. Recognised patterns keep their documented shape mask (D355).
"""

from __future__ import annotations

import json

import pytest

from services.compliance_guard import detect_pii_fields, score_compliance_risk
from services.pii_mask_transform import clear_tokens, mask_pii_cell, mask_text
from services.transform_engine import apply_transform

# ── detection reads a sample spread across the rows ─────────────────────────


def _late_pii_rows() -> list[dict]:
    clean = [{"id": i, "notes": "ok"} for i in range(100)]
    dirty = [
        {"id": 100 + i, "notes": f"ssn 123-45-{6000 + i:04d} mail user{i}@example.com"}
        for i in range(900)
    ]
    return clean + dirty


def test_pii_detected_beyond_first_100_rows():
    report = detect_pii_fields(["id", "notes"], _late_pii_rows())

    assert report["sensitive_fields"] == ["notes"]
    assert {"ssn", "email"} <= set(report["field_risk"]["notes"])
    assert report["risk_level"] == "high"
    assert score_compliance_risk(["id", "notes"], _late_pii_rows())["risk_level"] != "low"


def test_detection_states_what_it_sampled():
    report = detect_pii_fields(["id", "notes"], _late_pii_rows())
    sampling = report["sampling"]
    assert sampling["rows_available"] == 1000 and sampling["rows_sampled"] == 100
    assert sampling["strategy"] == "head_tail_reservoir"
    assert "sample" in sampling["note"].lower() and "not" in sampling["note"].lower()
    # Everything it was given, when that fits the budget.
    small = detect_pii_fields(["notes"], _late_pii_rows()[:40])["sampling"]
    assert small == {**small, "rows_available": 40, "rows_sampled": 40, "strategy": "all_rows"}
    assert detect_pii_fields(["notes"], None)["sampling"]["rows_sampled"] == 0


def test_detection_sample_is_deterministic():
    rows = _late_pii_rows()
    assert detect_pii_fields(["id", "notes"], rows) == detect_pii_fields(["id", "notes"], rows)


def test_pii_only_in_the_middle_or_the_tail_is_seen():
    middle = [{"notes": "ok"} for _ in range(450)] + [
        {"notes": "call a.b@example.com"} for _ in range(100)
    ] + [{"notes": "ok"} for _ in range(450)]
    assert detect_pii_fields(["notes"], middle)["sensitive_fields"] == ["notes"]
    tail = [{"notes": "ok"} for _ in range(990)] + [{"notes": "a.b@example.com"}] * 10
    assert detect_pii_fields(["notes"], tail)["sensitive_fields"] == ["notes"]


# ── mask_pii keeps the length class only ────────────────────────────────────

_UNRECOGNISED = [
    "Smith", "Alice", "1987-03-14", "Maria Gonzalez", "12 Main St, Springfield",
    "hunter2", "GB29NWBK60161331926819", "1234567890123456", "Springfield",
    "Jo", "Q", "a much longer free text value that names nobody in particular",
]


@pytest.mark.parametrize("value", _UNRECOGNISED)
def test_mask_pii_reveals_no_source_characters_by_default(value):
    masked = mask_text(value)

    assert masked and set(masked) == {"*"}, masked
    assert len(masked) <= len(value)
    assert apply_transform(value, "mask_pii") == (masked, None)


def test_mask_keeps_only_the_length_class():
    assert mask_text("Smith") == mask_text("Smyth") == mask_text("Jones") == "****"
    assert mask_text("1987-03-14") == mask_text("1990-12-01") == "********"
    assert mask_text("Maria Gonzalez") == "********"
    assert mask_text("abc") == "***"
    assert len(mask_text("x" * 500)) == 32


def test_common_first_names_are_not_re_identifiable_from_their_mask():
    names = [
        "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
        "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph",
        "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Lisa",
        "Daniel", "Nancy", "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra",
        "Donald", "Ashley", "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna",
        "Joshua", "Michelle",
    ]
    by_mask: dict[str, list[str]] = {}
    for name in names:
        by_mask.setdefault(mask_text(name), []).append(name)
    assert len(by_mask) <= 2
    assert not [group for group in by_mask.values() if len(group) == 1]


def test_recognised_patterns_keep_their_shape_mask():
    assert mask_text("a@b.co") == "a*@***.co"
    assert mask_text("maria.garcia@example.com") == "m***********@*******.com"
    assert mask_text("") == ""
    assert mask_text("****") == "****"


def test_a_document_is_masked_value_by_value_with_no_clear_value_left():
    doc = {
        "customer": {"name": "Maria Gonzalez", "dob": "1987-03-14", "email": "m.g@example.com"},
        "address": {"city": "Springfield", "zip": 12345},
        "active": True,
        "note": None,
    }
    masked = json.loads(mask_pii_cell(doc, json.dumps(doc)))

    assert masked["customer"]["name"] == "********"
    assert masked["customer"]["dob"] == "********"
    assert masked["address"] == {"city": "********", "zip": "****"}
    assert masked["active"] is True and masked["note"] is None
    # D355's protection proof: no value the source held in clear is still there.
    assert not (clear_tokens(doc) & clear_tokens(masked))
