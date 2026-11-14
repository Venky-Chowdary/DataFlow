"""Mirror: a quarantined row's key was inferred as deleted (the SCD2-F2 rule).

A mirror removes every destination key the source snapshot no longer
presents. A row held out to quarantine is still in the source, but:

* staged merge (database source): the staging load holds the row out, the
  inferred-delete pass reads "absent from staging" as a delete and tombstones
  (soft) or DELETEs (hard) the key; Gate-8 then counted only the staged rows as
  the source and subtracted the quarantined ones again, failed, and the delete
  stayed;
* buffered merge (file source): the key census holds every source key, so
  the row survived, but Gate-8 graded the kept row as an extra and failed —
  and with a renamed column it failed even a clean run, because the ladder
  compared the in-memory source rows under their source names.

Now a held-out key keeps its live row, is counted as retained (read +
retained == active + held out), and Gate-8 says why; a key truly gone is still
removed. Every route is ``UniversalTransferEngine.execute``.
"""

from __future__ import annotations

import uuid

import pytest

from src.transfer.models import EndpointConfig
from tests.test_scd2_f1_rename_quarantine_truth import (
    BAD_CLV_MAP,
    _live_dest,
    _recon,
    _run,
    _sqlite_dest,
    _sqlite_rows,
    _sqlite_source,
)
from tests.test_scd2_f2_quarantined_key_is_not_a_delete import (
    CLEAN,
    ONE_UNFIT,
    UNFIT_AND_DELETED,
    _csv,
)

MIRROR = "full_refresh_mirror"


def _assert_green(result, *, read: int, quarantined: int, kept: int = 0) -> None:
    assert result.success, result.error
    rec = _recon(result)
    assert rec.get("passed") is True, rec.get("message")
    assert rec.get("source_rows") == read, rec
    assert int(rec.get("rejected_rows") or 0) == quarantined, rec
    accounting = rec.get("row_accounting") or {}
    if accounting:
        assert accounting.get("balanced") is True, accounting
    if kept:
        assert f"{kept} retained key(s) kept current — their source row was quarantined" in str(
            rec.get("message")
        ), rec.get("message")


def _mirror(dest, rows, *, kind: str, tmp_path, contract_extra: dict | None = None):
    extra: dict = {"skip_preflight": True}
    if kind == "csv":
        source = EndpointConfig(kind="file", format="csv")
        extra.update(source_filename="cust.csv", source_content=_csv(rows),
                     column_types={"id": "integer", "name": "string", "clv": "string"})
    else:
        source = _sqlite_source(tmp_path / "src.db", rows, "TEXT")
    if contract_extra:
        extra["stream_contracts"] = [
            {"selected": True, "primary_key": "id", "sync_mode": MIRROR, **contract_extra}
        ]
    result = _run(source, dest, sync=MIRROR, mappings=BAD_CLV_MAP, **extra)
    staged = any("STAGING" in d for d in result.ddl_executed or [])
    if result.success:
        assert staged is (kind == "db"), result.ddl_executed
    return result


@pytest.mark.parametrize("kind", ["db", "csv"], ids=["staged", "file-source"])
def test_mirror_quarantined_key_stays_live_and_reconciles(tmp_path, kind):
    dst = tmp_path / "dst.db"
    dest = _sqlite_dest(dst)

    _assert_green(_mirror(dest, CLEAN, kind=kind, tmp_path=tmp_path), read=3, quarantined=0)
    _assert_green(_mirror(dest, ONE_UNFIT, kind=kind, tmp_path=tmp_path),
                  read=3, quarantined=1, kept=1)
    assert _sqlite_rows(dst, "SELECT id, clv_value, _deleted FROM dim ORDER BY id") == [
        (1, 10, 0), (2, 20, 0), (3, 30, 0),
    ], "key 2 is in the source; quarantine is not a delete"

    # Key 3 is gone from the source: still tombstoned. Key 2 is kept.
    _assert_green(_mirror(dest, UNFIT_AND_DELETED, kind=kind, tmp_path=tmp_path),
                  read=2, quarantined=1, kept=1)
    assert _sqlite_rows(dst, "SELECT id, clv_value, _deleted FROM dim ORDER BY id") == [
        (1, 11, 0), (2, 20, 0), (3, 30, 1),
    ]


def test_staged_hard_delete_mirror_keeps_quarantined_key(tmp_path):
    dst = tmp_path / "dst.db"
    dest = _sqlite_dest(dst)
    hard = {"delete_mode": "hard"}
    _assert_green(_mirror(dest, CLEAN, kind="db", tmp_path=tmp_path, contract_extra=hard),
                  read=3, quarantined=0)
    _assert_green(
        _mirror(dest, UNFIT_AND_DELETED, kind="db", tmp_path=tmp_path, contract_extra=hard),
        read=2, quarantined=1, kept=1,
    )
    assert _sqlite_rows(dst, "SELECT id, clv_value FROM dim ORDER BY id") == [(1, 11), (2, 20)]


@pytest.mark.parametrize("kind", ["postgresql", "mysql"])
def test_live_staged_mirror_quarantined_key_stays_live(tmp_path, kind):
    from tests import r4_route_harness as h

    table = f"mirq_{uuid.uuid4().hex[:8]}"
    dest = _live_dest(kind, table)
    try:
        _assert_green(_mirror(dest, CLEAN, kind="db", tmp_path=tmp_path), read=3, quarantined=0)
        _assert_green(_mirror(dest, UNFIT_AND_DELETED, kind="db", tmp_path=tmp_path),
                      read=2, quarantined=1, kept=1)
        got = h.query(kind, f"SELECT id, clv_value, _deleted FROM {h.quote(kind, table)} "  # nosec B608
                            "ORDER BY id")
        assert [(int(i), int(v), bool(d)) for i, v, d in got] == [
            (1, 11, False), (2, 20, False), (3, 30, True),
        ]
    finally:
        h.drop_table(kind, table)
        h.drop_table(kind, f"{table}_df_quarantine")
