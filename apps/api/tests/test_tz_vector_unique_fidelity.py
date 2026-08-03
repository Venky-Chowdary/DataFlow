"""TZ→NTZ / VECTOR(n) write quarantine + computed/FBI unique CI — enterprise SSOT."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    quarantine_unfit_specialty_types,
    quarantine_unfit_temporals,
)
from services.schema_introspect import (  # noqa: E402
    _oracle_fetch_unique_keys,
    _sqlserver_fetch_unique_keys,
)
from services.type_system import (  # noqa: E402
    datetime_timezone_polarity,
    parse_vector_length,
    temporal_value_has_timezone,
    unique_key_forces_casefold,
)


def test_datetime_bare_is_ambiguous_and_offset_detected():
    # Bare DATETIME/TIMESTAMP are wall-clock NTZ — never invent TZ polarity.
    assert datetime_timezone_polarity("DATETIME") == "ntz"
    assert datetime_timezone_polarity("TIMESTAMP") == "ntz"
    assert datetime_timezone_polarity("datetime") == "ntz"
    assert datetime_timezone_polarity("TIMESTAMP_NTZ") == "ntz"
    assert datetime_timezone_polarity("DATETIME(6)") == "ntz"
    assert temporal_value_has_timezone("2024-01-01T12:00:00Z") is True
    assert temporal_value_has_timezone("2024-01-01T12:00:00+00:00") is True
    assert temporal_value_has_timezone("2024-01-01 12:00:00") is False
    assert temporal_value_has_timezone(datetime(2024, 1, 1, tzinfo=timezone.utc)) is True


def test_quarantine_tz_aware_into_ntz():
    details: list[dict] = []
    out = quarantine_unfit_temporals(
        [
            ("2024-01-01T12:00:00Z",),
            ("2024-01-01 12:00:00",),
            ("2024-01-01T12:00:00.123456",),
        ],
        ["ts"],
        ["TIMESTAMP_NTZ(6)"],
        details,
        policy="quarantine",
    )
    assert ("2024-01-01 12:00:00",) in out
    assert ("2024-01-01T12:00:00.123456",) in out
    assert details and "timezone-aware" in details[0]["reason"].lower()


def test_vector_dim_write_quarantine():
    assert parse_vector_length("[1,2,3]") == 3
    details: list[dict] = []
    out = quarantine_unfit_specialty_types(
        [([1.0, 2.0, 3.0],), ([1.0, 2.0],), ("[0,0,0]",)],
        ["emb"],
        ["VECTOR(3)"],
        details,
        policy="quarantine",
    )
    assert out == [([1.0, 2.0, 3.0],), ("[0,0,0]",)]
    assert details and "vector length" in details[0]["reason"].lower()


def test_sqlserver_computed_lower_unique_is_ci():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        ("UQ_email_ci", False, "email_lower", 1, "(lower([email]))", None),
    ]
    meta = _sqlserver_fetch_unique_keys(conn, "dbo", "users")
    uk = meta["unique_keys"][0]
    assert uk["case_insensitive"] is True
    assert "email" in [c.lower() for c in uk["expression_columns"]]
    assert unique_key_forces_casefold(
        "email", ddl_type="VARCHAR(100)", unique_keys=meta["unique_keys"]
    )


def test_oracle_fbi_upper_unique_is_ci():
    conn = MagicMock()
    # constraints empty, then FBI rows
    conn.execute.return_value.fetchall.side_effect = [
        [],
        [("UQ_EMAIL_CI", 'UPPER("EMAIL")', 1)],
    ]
    meta = _oracle_fetch_unique_keys(conn, "APP", "USERS")
    uk = meta["unique_keys"][0]
    assert uk["case_insensitive"] is True
    assert "EMAIL" in uk["expression_columns"] or "email" in [
        c.lower() for c in uk["expression_columns"]
    ]
