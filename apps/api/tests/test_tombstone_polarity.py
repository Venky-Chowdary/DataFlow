"""Conservative tombstone polarity — false positives are silent dest wipes."""

from __future__ import annotations

from services.tombstone import (
    detect_tombstone_column,
    is_row_tombstone,
    is_tombstone_set,
)


def test_liveness_columns_are_not_tombstones():
    assert detect_tombstone_column({}, ["id", "is_active", "name"]) is None
    assert is_row_tombstone({"id": 1, "is_active": 0}) is False
    assert is_row_tombstone({"id": 1, "is_active": 1}) is False


def test_audit_lookalikes_are_not_tombstones():
    assert detect_tombstone_column({}, ["id", "deleted_by", "delete_count"]) is None
    assert is_row_tombstone({"id": 1, "deleted_by": "alice", "delete_count": 3}) is False


def test_boolean_tombstone_fails_closed_on_ambiguity():
    assert is_tombstone_set({"is_deleted": True}, "is_deleted") is True
    assert is_tombstone_set({"is_deleted": False}, "is_deleted") is False
    assert is_tombstone_set({"is_deleted": "true"}, "is_deleted") is True
    assert is_tombstone_set({"is_deleted": "t"}, "is_deleted") is True
    assert is_tombstone_set({"is_deleted": "1"}, "is_deleted") is True
    assert is_tombstone_set({"is_deleted": "0"}, "is_deleted") is False
    assert is_tombstone_set({"is_deleted": "false"}, "is_deleted") is False
    # Informal yes/y/2 are not write-path TRUE — refuse to hard-DELETE.
    assert is_tombstone_set({"is_deleted": "yes"}, "is_deleted") is False
    assert is_tombstone_set({"is_deleted": "y"}, "is_deleted") is False
    assert is_tombstone_set({"is_deleted": "2"}, "is_deleted") is False
    assert is_tombstone_set({"is_deleted": "maybe"}, "is_deleted") is False


def test_timestamp_tombstone_ignores_mysql_zero_date():
    assert is_tombstone_set({"deleted_at": None}, "deleted_at") is False
    assert is_tombstone_set({"deleted_at": ""}, "deleted_at") is False
    assert is_tombstone_set({"deleted_at": "0000-00-00"}, "deleted_at") is False
    assert is_tombstone_set({"deleted_at": "2024-01-15T12:00:00Z"}, "deleted_at") is True


def test_cdc_envelope_deleted_wins_over_business_columns():
    assert is_row_tombstone({"id": 1, "__deleted": True}) is True
    assert is_row_tombstone({"id": 1, "__deleted": False, "deleted_at": "2024-01-01"}) is False
    assert is_row_tombstone({"id": 1, "__op": "d"}) is True
    assert is_row_tombstone({"id": 1, "__op": "u"}) is False
    # Bare business `op` is not a Debezium envelope flag.
    assert is_row_tombstone({"id": 1, "op": "delete"}) is False
