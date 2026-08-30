"""A cursor poll may not silently stand in for log-based CDC.

Query CDC cannot observe a DELETE. Substituting it for logical decoding /
binlog when the server *does* emit a change log (we simply failed to attach:
slot quota, missing REPLICATION grant) leaves deleted rows alive at the
destination with a green run — the exact silent-divergence class this repo
forbids. When the server emits no change log at all, the run may proceed by
cursor but must declare the capture it actually used.
"""

from __future__ import annotations

import pytest

from services.cdc_capability import (
    CAUSE_DRIVER_MISSING,
    CAUSE_PRIVILEGE,
    CAUSE_SERVER_NOT_CONFIGURED,
    CAUSE_SLOT_QUOTA,
    CAUSE_SOURCE_UNREACHABLE,
    CAUSE_UNKNOWN,
    LogCaptureUnavailable,
    classify_log_capture_failure,
)
from src.transfer.cdc_transfer import _query_cdc_downgrade, _refuse_log_capture


class _Reader:
    def __init__(self, reason: object = None) -> None:
        self.unavailable_reason = reason


def test_slot_quota_is_operator_fixable_and_fails_closed() -> None:
    refusal = classify_log_capture_failure(
        "postgresql",
        'all replication slots are in use\nHINT:  Free one or increase max_replication_slots.',
    )
    assert refusal.cause == CAUSE_SLOT_QUOTA
    assert refusal.fail_closed is True
    assert "pg_drop_replication_slot" in refusal.remedy
    with pytest.raises(RuntimeError) as err:
        _query_cdc_downgrade(LogCaptureUnavailable(refusal, "postgresql"), "postgresql")
    msg = str(err.value)
    assert "cannot observe DELETE" in msg
    assert "max_replication_slots" in msg


def test_missing_replication_grant_fails_closed() -> None:
    refusal = classify_log_capture_failure(
        "mysql", "Access denied; you need REPLICATION SLAVE privilege"
    )
    assert refusal.cause == CAUSE_PRIVILEGE
    assert refusal.fail_closed is True
    assert "REPLICATION SLAVE" in refusal.remedy


def test_unreachable_source_fails_closed_and_is_not_called_a_downgrade() -> None:
    refusal = classify_log_capture_failure("postgresql", "could not connect to server")
    assert refusal.cause == CAUSE_SOURCE_UNREACHABLE
    assert refusal.fail_closed is True


def test_unclassified_failure_fails_closed() -> None:
    refusal = classify_log_capture_failure("postgresql", "boom")
    assert refusal.cause == CAUSE_UNKNOWN
    assert refusal.fail_closed is True


@pytest.mark.parametrize(
    ("dialect", "detail"),
    [
        ("postgresql", "wal_level=replica — logical decoding is off on this server"),
        ("mysql", "binlog_format=STATEMENT — ROW format required"),
    ],
)
def test_server_without_change_log_may_degrade_but_must_declare(dialect: str, detail: str) -> None:
    refusal = classify_log_capture_failure(dialect, detail, server_log_enabled=False)
    assert refusal.cause == CAUSE_SERVER_NOT_CONFIGURED
    assert refusal.fail_closed is False
    fields = _query_cdc_downgrade(LogCaptureUnavailable(refusal, dialect), dialect)
    assert fields["cdc_capture_requested"] == "log_based"
    assert fields["cdc_capture_used"] == "query_cursor"
    assert fields["cdc_capture_downgraded"] is True
    assert fields["cdc_delete_capture"] is False
    assert fields["cdc_capture_downgrade_cause"] == CAUSE_SERVER_NOT_CONFIGURED
    assert str(fields["cdc_capture_downgrade_remedy"])


def test_missing_driver_degrades_with_declared_loss() -> None:
    refusal = classify_log_capture_failure("mysql", "ImportError: No module named pymysqlreplication")
    assert refusal.cause == CAUSE_DRIVER_MISSING
    assert refusal.fail_closed is False


def test_reader_reason_is_carried_through_the_refusal() -> None:
    reason = classify_log_capture_failure("postgresql", "all replication slots are in use")
    exc = _refuse_log_capture(_Reader(reason), "postgresql")
    assert exc.refusal is reason
    assert exc.dialect == "postgresql"


def test_reader_without_a_reason_is_not_assumed_degradable() -> None:
    exc = _refuse_log_capture(_Reader(None), "postgresql")
    assert exc.refusal.cause == CAUSE_UNKNOWN
    assert exc.refusal.fail_closed is True
