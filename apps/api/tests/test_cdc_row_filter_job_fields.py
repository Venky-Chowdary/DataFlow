"""CDC lag/job field promotion includes SQL Server row filter (no mocks)."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_cdc_lag_fields_promotes_row_filter() -> None:
    from src.transfer.cdc_transfer import _cdc_lag_fields
    from src.transfer.engine import _CDC_JOB_FIELDS

    class _FakeCdc:
        def cdc_metadata(self):
            return {
                "plugin": "sqlserver_native_cdc",
                "cdc_row_filter": "net",
                "delivery": "at-least-once",
            }

    fields = _cdc_lag_fields(_FakeCdc())
    assert fields["cdc_row_filter"] == "net"
    assert fields["cdc_plugin"] == "sqlserver_native_cdc"
    assert "cdc_row_filter" in _CDC_JOB_FIELDS


def test_cdc_lag_fields_reads_row_filter_attr() -> None:
    from src.transfer.cdc_transfer import _cdc_lag_fields

    class _FakeCdc:
        row_filter = "all update old"

        def cdc_metadata(self):
            return {"plugin": "sqlserver_native_cdc"}

    fields = _cdc_lag_fields(_FakeCdc())
    assert fields["cdc_row_filter"] == "all update old"
