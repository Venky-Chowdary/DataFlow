"""M6 — SQLite TIMESTAMP columns store the SQL space form, not ISO 'T'.

Serializers emit RFC 3339 ``2025-01-02T03:04:05``; the row writer's carrier
bind normalizes to the space-separated SQL form, but the COPY text path bound
TIMESTAMP cells via ``_bind_text`` — verbatim — so the same column held both
spellings depending on which writer ran. The text bind now normalizes naive
temporal carriers and declines tz-aware/unparseable cells to the row path.
"""

from __future__ import annotations

import pytest

from services.copy_sqlite_common import sqlite_bind_from_text
from services.copy_fast_path import FastPathUnavailable


@pytest.mark.parametrize("ddl", ["TIMESTAMP", "DATETIME", "TIMESTAMP_NTZ"])
def test_naive_temporal_bind_normalizes_t(ddl):
    bind = sqlite_bind_from_text(ddl)
    assert bind("2025-01-02T03:04:05") == "2025-01-02 03:04:05"
    assert bind("2025-01-02T03:04:05.123456") == "2025-01-02 03:04:05.123456"


def test_naive_temporal_bind_keeps_space_form():
    bind = sqlite_bind_from_text("TIMESTAMP")
    assert bind("2025-01-02 03:04:05") == "2025-01-02 03:04:05"


def test_date_bind_normalizes():
    bind = sqlite_bind_from_text("DATE")
    assert bind("2025-01-02") == "2025-01-02"


@pytest.mark.parametrize(
    "cell",
    [
        "2025-01-02T03:04:05Z",
        "2025-01-02T03:04:05+00:00",
        "2025-01-02T03:04:05-07:30",
        "not-a-date",
    ],
)
def test_tz_aware_and_junk_decline_to_row_path(cell):
    bind = sqlite_bind_from_text("TIMESTAMP")
    with pytest.raises(FastPathUnavailable):
        bind(cell)


def test_null_stays_null():
    assert sqlite_bind_from_text("TIMESTAMP")(None) is None
