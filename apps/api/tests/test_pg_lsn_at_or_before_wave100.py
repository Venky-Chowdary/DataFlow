"""Wave 100: PG peek must not drop cross-family incomparable WAL events."""

from __future__ import annotations


def test_lsn_at_or_before_same_family():
    from connectors.postgresql_change_stream import _lsn_at_or_before

    assert _lsn_at_or_before("0/100", "0/200") is True
    assert _lsn_at_or_before("0/200", "0/200") is True
    assert _lsn_at_or_before("0/300", "0/200") is False


def test_lsn_at_or_before_incomparable_is_not_stale():
    from connectors.postgresql_change_stream import _lsn_at_or_before

    # PG WAL vs MySQL-style binlog family — compare_lsn returns 0, but peek
    # must still apply the event (docstring / Debezium stream-wins honesty).
    assert _lsn_at_or_before("mysql-bin.000001:4", "0/200") is False
    assert _lsn_at_or_before("0/200", "mysql-bin.000001:4") is False
