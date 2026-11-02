"""Mongo cursor casts must use the write-path boolean and datetime parsers.

Marked ``fake_mongo`` so collection does not skip them when 27017 is down —
these tests never open a socket.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from connectors.mongodb_reader import (
    _align_cursor_to_stored_kind,
    _cast_cursor_value,
)

pytestmark = pytest.mark.fake_mongo


def test_boolean_cursor_binds_canonical_tokens_only():
    assert _cast_cursor_value("true", "BOOLEAN") is True
    assert _cast_cursor_value("t", "BOOL") is True
    assert _cast_cursor_value("1", "BOOLEAN") is True
    assert _cast_cursor_value("false", "BOOLEAN") is False
    assert _cast_cursor_value("0", "BOOLEAN") is False
    # Informal yes/y and non-zero integers are not TRUE.
    assert _cast_cursor_value("yes", "BOOLEAN") == "yes"
    assert _cast_cursor_value("y", "BOOLEAN") == "y"
    assert _cast_cursor_value("2", "BOOLEAN") == "2"


def test_stored_bool_kind_refuses_informal_yes():
    assert _align_cursor_to_stored_kind("true", True, "bool") is True
    assert _align_cursor_to_stored_kind("0", "0", "bool") is False
    with pytest.raises(ValueError, match="canonical boolean"):
        _align_cursor_to_stored_kind("yes", "yes", "bool")
    with pytest.raises(ValueError, match="canonical boolean"):
        _align_cursor_to_stored_kind("2", "2", "bool")


def test_datetime_cursor_binds_write_path_calendars_and_epochs():
    slash = _cast_cursor_value("31/12/2024", "TIMESTAMP")
    assert isinstance(slash, datetime)
    assert slash.date().isoformat() == "2024-12-31"
    epoch = _cast_cursor_value("1704451800", "TIMESTAMP")
    assert isinstance(epoch, datetime)
    millis = _cast_cursor_value("1704451800000", "DATE")
    assert isinstance(millis, datetime)
    assert epoch.timestamp() == millis.timestamp()
    iso = _cast_cursor_value("2025-03-01", "DATETIME")
    assert isinstance(iso, datetime)
    assert iso.date().isoformat() == "2025-03-01"


def test_datetime_cursor_refuses_auto_ambiguous_slash():
    raw = _cast_cursor_value("01/02/2024", "TIMESTAMP")
    assert raw == "01/02/2024"
    with pytest.raises(ValueError, match="not a timestamp"):
        _align_cursor_to_stored_kind("01/02/2024", raw, "date")


def test_inferred_datetime_cursor_uses_write_path_too():
    """No declared type — infer DATETIME from unambiguous slash, then bind."""
    parsed = _cast_cursor_value("31/12/2024", None)
    assert isinstance(parsed, datetime)
    assert parsed.date().isoformat() == "2024-12-31"
    assert _cast_cursor_value("01/02/2024", None) == "01/02/2024"
