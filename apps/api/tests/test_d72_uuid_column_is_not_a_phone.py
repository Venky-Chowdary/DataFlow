"""D72 follow-up: a declared UUID column is never a phone, e-mail or IP address.

On QA's route (PostgreSQL UUID primary key -> Snowflake), UUIDs whose hex
groups happen to be digits (``00000000-0000-0000-1111-222233334444``: nil-
prefixed and sequential UUIDs look like this) were read "phone by embedded
value", and Confirm was held for a PII review on the key column. A column the
source declares ``uuid`` (PostgreSQL ``uuid``, SQL Server ``uniqueidentifier``)
only stores 128-bit UUIDs (https://www.postgresql.org/docs/current/datatype-uuid.html),
so its content cannot be a direct identifier. Its *name* still counts, and the
same text in an undeclared/text column is still judged by its content.
"""

from __future__ import annotations

import uuid

import pytest

from services.compliance_guard import detect_pii_fields

_DIGITY = [str(uuid.UUID(int=i * 0x1111_2222_3333_4444)) for i in range(1, 6)]


@pytest.mark.parametrize("declared", ["UUID", "uuid", "uniqueidentifier"])
def test_declared_uuid_content_is_not_a_direct_identifier(declared):
    report = detect_pii_fields(
        ["u"], [{"u": v} for v in _DIGITY], column_types={"u": declared}
    )
    assert "u" not in report["sensitive_fields"], report["field_evidence"]


def test_the_same_text_in_a_text_column_is_still_judged_by_content():
    report = detect_pii_fields(["u"], [{"u": v} for v in _DIGITY], column_types={"u": "TEXT"})
    assert "u" in report["sensitive_fields"], report


def test_a_uuid_column_named_like_an_identifier_still_counts_by_name():
    report = detect_pii_fields(
        ["email"], [{"email": v} for v in _DIGITY], column_types={"email": "UUID"}
    )
    assert "email" in report["sensitive_fields"]
    assert {e["rule"] for e in report["field_evidence"]["email"]} == {"column_name"}
