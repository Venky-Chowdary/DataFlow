"""SCD2-F2: a quarantined row's key was closed as "deleted at source".

A complete SCD2 snapshot closes the current version of every key the source
no longer holds (``close_missing``). A row held out to quarantine is still in
the source, but its key was missing from the merged key set, so:

* buffered merge (file source, or a destination without a staging merge):
  run 2 with one unfit value closed that customer's current version, wrote no
  replacement, and Gate-8 passed — the dimension silently said "deleted";
* staged merge (database source -> SQLite / PostgreSQL / MySQL / DuckDB /
  warehouses): the staging load holds out the same row, the close pass reads
  "absent from staging" as a delete, and Gate-8 subtracted the held-out row
  from a source count that already excluded it — every run with one
  quarantined row failed with "expected target 1 vs target 2".

Now the held-out keys keep their current version and are counted as retained
(source = merged + quarantined; current = merged + retained), a key truly
absent from the source still closes, and the staged run's source count is the
rows read. Every route is ``UniversalTransferEngine.execute``.
"""

from __future__ import annotations

import uuid

import pytest

from tests.test_scd2_f1_rename_quarantine_truth import (
    BAD_CLV_MAP,
    _live_dest,
    _recon,
    _run,
    _sqlite_dest,
    _sqlite_rows,
    _sqlite_source,
)
from src.transfer.models import EndpointConfig

CLEAN = [(1, "A", "10"), (2, "B", "20"), (3, "C", "30")]
ONE_UNFIT = [(1, "A", "10"), (2, "B", "bad"), (3, "C", "30")]
UNFIT_AND_DELETED = [(1, "A", "11"), (2, "B", "bad")]


def _assert_green(result, *, read: int, quarantined: int) -> None:
    assert result.success, result.error
    rec = _recon(result)
    assert rec.get("passed") is True, rec.get("message")
    assert rec.get("source_rows") == read, rec
    assert int(rec.get("rejected_rows") or 0) == quarantined, rec
    if quarantined and "retained" in str(rec.get("message")):
        assert "source row was quarantined" in str(rec.get("message")), rec.get("message")


def _csv(rows: list[tuple]) -> bytes:
    return ("id,name,clv\n" + "".join(f"{i},{n},{c}\n" for i, n, c in rows)).encode()


def test_buffered_scd2_quarantined_key_keeps_its_current_version(tmp_path):
    dst_path = tmp_path / "dst.db"
    csv = EndpointConfig(kind="file", format="csv")

    def run(rows):
        return _run(csv, _sqlite_dest(dst_path), mappings=BAD_CLV_MAP,
                    source_filename="cust.csv", source_content=_csv(rows),
                    column_types={"id": "integer", "name": "string", "clv": "string"},
                    skip_preflight=True)

    _assert_green(run(CLEAN), read=3, quarantined=0)
    kept = run(ONE_UNFIT)
    _assert_green(kept, read=3, quarantined=1)
    assert "1 retained key(s) kept current" in str(_recon(kept).get("message"))
    assert _sqlite_rows(dst_path, "SELECT id, clv_value, is_current FROM dim ORDER BY id") == [
        (1, 10, 1), (2, 20, 1), (3, 30, 1),
    ], "key 2 is in the source; quarantine is not a delete"

    # A key the source no longer holds still closes; the held-out one does not.
    _assert_green(run(UNFIT_AND_DELETED), read=2, quarantined=1)
    assert _sqlite_rows(
        dst_path, "SELECT id, clv_value, is_current FROM dim ORDER BY id, valid_from"
    ) == [(1, 10, 0), (1, 11, 1), (2, 20, 1), (3, 30, 0)]


def test_staged_scd2_quarantined_row_reconciles_and_keeps_its_key(tmp_path):
    src_path, dst_path = tmp_path / "src.db", tmp_path / "dst.db"

    def run(rows):
        src = _sqlite_source(src_path, rows, "TEXT")
        result = _run(src, _sqlite_dest(dst_path), mappings=BAD_CLV_MAP, skip_preflight=True)
        if result.success:
            assert any("STAGING" in d for d in result.ddl_executed or []), "staged merge route"
        return result

    # First load with one held-out row: source 3 = merged 2 + quarantined 1.
    _assert_green(run(ONE_UNFIT), read=3, quarantined=1)
    assert _sqlite_rows(dst_path, "SELECT id, clv_value FROM dim ORDER BY id") == [
        (1, 10), (3, 30),
    ]
    _assert_green(run(CLEAN), read=3, quarantined=0)
    _assert_green(run(ONE_UNFIT), read=3, quarantined=1)
    assert _sqlite_rows(
        dst_path, "SELECT id, clv_value, is_current FROM dim ORDER BY id, valid_from"
    ) == [(1, 10, 1), (2, 20, 1), (3, 30, 1)]

    _assert_green(run(UNFIT_AND_DELETED), read=2, quarantined=1)
    assert _sqlite_rows(
        dst_path, "SELECT id, clv_value, is_current FROM dim ORDER BY id, valid_from"
    ) == [(1, 10, 0), (1, 11, 1), (2, 20, 1), (3, 30, 0)]


@pytest.mark.parametrize("kind", ["postgresql", "mysql"])
def test_live_staged_scd2_quarantined_key_keeps_its_current_version(tmp_path, kind):
    from tests import r4_route_harness as h

    table = f"scd2f2_{uuid.uuid4().hex[:8]}"
    dest = _live_dest(kind, table)
    try:
        src_path = tmp_path / "src.db"
        _assert_green(
            _run(_sqlite_source(src_path, CLEAN, "TEXT"), dest, mappings=BAD_CLV_MAP,
                 skip_preflight=True),
            read=3, quarantined=0,
        )
        _assert_green(
            _run(_sqlite_source(src_path, ONE_UNFIT, "TEXT"), dest, mappings=BAD_CLV_MAP,
                 skip_preflight=True),
            read=3, quarantined=1,
        )
        got = h.query(kind, f"SELECT id, clv_value, is_current FROM {h.quote(kind, table)} "  # nosec B608
                            "ORDER BY id")
        assert [(int(i), int(v), bool(c)) for i, v, c in got] == [
            (1, 10, True), (2, 20, True), (3, 30, True),
        ]
    finally:
        h.drop_table(kind, table)
        h.drop_table(kind, f"{table}_df_quarantine")
