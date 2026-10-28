"""A destination that does not exist is created whole: create_new stamps are not an ADD COLUMN."""

from __future__ import annotations

from types import SimpleNamespace

from src.transfer.engine import _run_backfill_new_fields

_REQ = SimpleNamespace(backfill_new_fields=False, schema_policy="manual_review")
_STAMPED = [{"source": "a", "target": "a", "create_new": True}]


def test_absent_destination_does_not_arm_backfill():
    assert _run_backfill_new_fields(_REQ, _STAMPED, {}, False) is False


def test_existing_destination_lacking_the_column_arms_backfill():
    assert _run_backfill_new_fields(_REQ, _STAMPED, {"id": "INTEGER"}, True) is True


def test_existing_destination_holding_the_column_does_not():
    assert _run_backfill_new_fields(_REQ, _STAMPED, {"a": "TEXT"}, True) is False
