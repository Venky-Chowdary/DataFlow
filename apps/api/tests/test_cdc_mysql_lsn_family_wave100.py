"""MySQL snapshot stamps must share the streaming LSN family (wave 100 / C2).

A snapshot chunk stamped with ``gtid:…`` and a later streaming event stamped
with ``binlog.000003:000…123`` are different LSN families. ``compare_lsn``
returns 0 across families, so ``filter_stale_lsn_rows`` treated every later
change as "not newer" and discarded it forever — freezing the row at its
snapshot value. The fix keeps GTID for the peek filter and stamps the row
from binlog ``file:pos``.
"""

from __future__ import annotations

from services.cdc_incremental_runner import _snapshot_low_watermark
from services.cdc_incremental_snapshot import SnapshotSignal
from connectors.writer_common import compare_lsn, extract_cdc_lsn, lsn_family


def _sig(**kwargs) -> SnapshotSignal:
    base = dict(
        id="s1",
        source_key="mysql:db",
        table="t",
        primary_key="id",
        status="running",
    )
    base.update(kwargs)
    return SnapshotSignal(**base)


class TestSnapshotLowWatermarkPrefersBinlog:
    def test_binlog_beats_gtid_when_both_present(self) -> None:
        wm = _snapshot_low_watermark(
            _sig(
                gtid_low="source-uuid:1-10",
                lsn_low="mysql-bin.000003:00000000000000000123",
            )
        )
        assert "file" in wm and "pos" in wm
        assert "gtid" not in wm
        stamped = extract_cdc_lsn(wm)
        assert lsn_family(stamped) == "mysql_binlog"

    def test_gtid_is_the_fallback_when_binlog_unavailable(self) -> None:
        wm = _snapshot_low_watermark(_sig(gtid_low="source-uuid:1-10"))
        assert wm == {"gtid": "source-uuid:1-10"}
        assert lsn_family(extract_cdc_lsn(wm)) == "mysql_gtid"

    def test_pg_lsn_still_works(self) -> None:
        wm = _snapshot_low_watermark(_sig(lsn_low="0/16B72A8"))
        assert wm == {"lsn": "0/16B72A8"}
        assert lsn_family(extract_cdc_lsn(wm)) == "pg_wal"


class TestStreamingCanUpdateSnapshottedRow:
    def test_later_binlog_event_is_newer_than_snapshot_stamp(self) -> None:
        snapshot_stamp = extract_cdc_lsn(
            {"file": "mysql-bin.000003", "pos": 100}
        )
        stream_stamp = extract_cdc_lsn(
            {"file": "mysql-bin.000003", "pos": 500}
        )
        assert lsn_family(snapshot_stamp) == lsn_family(stream_stamp) == "mysql_binlog"
        assert compare_lsn(stream_stamp, snapshot_stamp) == 1

    def test_cross_family_is_still_not_invented_as_newer(self) -> None:
        """Regression: gtid vs binlog must never invent a 'newer' verdict."""
        assert compare_lsn("gtid:source-uuid:1-10", "mysql-bin.000003:00000000000000000100") == 0
