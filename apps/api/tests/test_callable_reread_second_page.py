"""QA 2026-11-06 job 6aee9711: source re-read stuck at 10,000/1,000,000.

The source is a PostgreSQL ``generate_series`` query, spooled to JSONL, not a
table with a primary-key index. The re-read then passed the previous page's
max ``id`` as ``cursor_after``. The spool reader parsed every line on that
path and did not stop early.

These tests use the spool and the seek the re-read now calls. They do not
open a production database.
"""

from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path

from services.cdc_engine import WatermarkType, compare_watermarks
from services.keyset_pagination import KEYSET_SEP
from services.procedure_source import (
    _ResultSpool,
    _cursor_header_index,
    _read_spool_page,
    _row_after_cursor,
    _row_from_spool_line,
)
from services.source_reread import callable_reread_seek
from services.sync_cursor import compare_cursor_values
from src.transfer.reconcile_heartbeat import source_reread_heartbeat


def _spool(path: Path, rows: list[list[str]]) -> _ResultSpool:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    return _ResultSpool(
        path=path,
        headers=["id", "label", "amount", "event_date", "note", "unicode_text", "flag"],
        total=len(rows),
        schema={"id": "BIGINT"},
    )


def _row(i: int) -> list[str]:
    # Same seven carriers as the reported route. id stays decimal text so a
    # value above 2**53 is not rounded through float.
    return [
        str(i),
        f"label-{i}",
        f"{i}.250000",
        "2026-11-06",
        "" if i % 17 == 0 else f"note-{i}",
        f"u-{i}-café",
        "true" if i % 2 == 0 else "false",
    ]


def test_callable_reread_seek_does_not_advance_the_page_max():
    first = callable_reread_seek(page_start=0, seed_watermark=None)
    second = callable_reread_seek(page_start=10_000, seed_watermark=None)
    assert first == {"offset": 0, "cursor_after": None}
    assert second == {"offset": 10_000, "cursor_after": None}
    seeded = callable_reread_seek(page_start=10_000, seed_watermark="500")
    assert seeded["cursor_after"] == "500"
    assert seeded["offset"] == 10_000


def test_second_page_of_a_generate_series_spool_is_the_next_ids(tmp_path: Path):
    rows = [_row(i) for i in range(1, 301)]
    spool = _spool(tmp_path / "gen.jsonl", rows)
    page = 100
    seen: list[str] = []
    for start in (0, page, page * 2):
        seek = callable_reread_seek(page_start=start, seed_watermark=None)
        _headers, got, _total = _read_spool_page(
            spool, offset=seek["offset"], limit=page, cursor_column="id", cursor_after=seek["cursor_after"],
        )
        seen.extend(row[0] for row in got)
    assert seen == [str(i) for i in range(1, 301)]
    assert len(seen) == len(set(seen))


def test_page_max_cursor_parses_the_whole_spool_offset_does_not(tmp_path: Path, monkeypatch):
    """The stuck page: cursor_after = max id of page 1, offset 0.

    That filter parses every JSON line. Offset paging parses one page.
    """
    n = 8_000
    page = 2_000
    spool = _spool(tmp_path / "wide.jsonl", [_row(i) for i in range(1, n + 1)])
    import services.procedure_source as ps

    calls = {"n": 0}
    real = ps.json_loads_exact

    def _count(line):
        calls["n"] += 1
        return real(line)

    monkeypatch.setattr(ps, "json_loads_exact", _count)
    calls["n"] = 0
    t0 = time.perf_counter()
    _h, _rows, filtered_total = _read_spool_page(
        spool, offset=0, limit=page, cursor_column="id", cursor_after=str(page),
    )
    filtered_s = time.perf_counter() - t0
    filtered_parses = calls["n"]
    # The next page of the same filter resumes after the last row it returned
    # and stops at its own end: the filter's total is already known.
    calls["n"] = 0
    _h, next_rows, next_total = _read_spool_page(
        spool, offset=page, limit=page, cursor_column="id", cursor_after=str(page),
    )
    next_parses = calls["n"]
    calls["n"] = 0
    fresh = _spool(tmp_path / "wide2.jsonl", [_row(i) for i in range(1, n + 1)])
    t1 = time.perf_counter()
    _headers, got, _total = _read_spool_page(
        fresh, offset=page, limit=page, cursor_column="id", cursor_after=None,
    )
    offset_s = time.perf_counter() - t1
    # A cursor filter parses the whole spool once: every row's cursor has to be
    # read to know how many rows the filter keeps, and ``total_rows`` is that
    # filtered population on every page (test_procedure_source). Its next page
    # parses only its own rows. Offset paging parses only the page. The
    # re-read no longer uses the filter.
    assert filtered_parses == n
    assert next_parses == page
    assert filtered_total == next_total == n - page
    assert [row[0] for row in next_rows] == [str(i) for i in range(page * 2 + 1, page * 3 + 1)]
    assert calls["n"] == page
    assert [row[0] for row in got] == [str(i) for i in range(page + 1, page * 2 + 1)]
    assert filtered_s >= offset_s


def test_cursor_filter_resume_does_not_skip_or_duplicate(tmp_path: Path):
    rows = [_row(i) for i in range(1, 251)]
    spool = _spool(tmp_path / "flt.jsonl", rows)
    seen: list[str] = []
    for start in (0, 50, 100, 150, 200):
        _headers, got, _matched = _read_spool_page(
            spool, offset=start, limit=50, cursor_column="id", cursor_after="0",
        )
        seen.extend(row[0] for row in got)
    assert seen == [str(i) for i in range(1, 251)]
    assert len(seen) == len(set(seen))


def test_typed_cursors_are_not_lexical_and_null_is_not_a_value():
    big = str(2**53 + 3)
    smaller = str(2**53 + 1)
    assert compare_watermarks(big, smaller, WatermarkType.INTEGER) > 0
    assert compare_watermarks(big, smaller, WatermarkType.INTEGER) != 0
    assert compare_cursor_values(big, smaller) > 0
    assert compare_watermarks("1.250000", "1.250001", WatermarkType.FLOAT) < 0
    assert Decimal("1.250000") != Decimal("1.250001")
    assert compare_watermarks("2026-11-06", "2026-11-07", WatermarkType.DATETIME) < 0
    assert compare_cursor_values("2026-11-06T00:00:00+00:00", "2026-11-06T00:00:01Z") < 0
    assert compare_watermarks("b", "a", WatermarkType.STRING) > 0
    # "999" must not sort above "10000".
    assert compare_cursor_values("999", "10000") < 0
    assert compare_cursor_values(None, "1") < 0
    composite_lo = f"2026-11-06T00:00:00{KEYSET_SEP}9"
    composite_hi = f"2026-11-06T00:00:00{KEYSET_SEP}10"
    assert compare_cursor_values(composite_lo, composite_hi) < 0


def _pinned_prerefix_read_spool_page(
    spool: _ResultSpool,
    *,
    offset: int,
    limit: int,
    cursor_column: str | None = None,
    cursor_after: str | None = None,
):
    """``_read_spool_page`` as of ``f736a768e`` (the commit before the offset re-read).

    Copied verbatim, including the filter path that parses every line and does
    not stop early. Wall-clock cost is not asserted here.
    """
    cursor_col = str(cursor_column or "").strip()
    cursor_idx: int | None = None
    apply_filter = False
    if cursor_col:
        cursor_idx = _cursor_header_index(spool.headers, cursor_col)
        apply_filter = cursor_after not in (None, "")
    rows: list[list[str]] = []
    start = max(0, int(offset))
    end = start + max(0, int(limit))
    matched = 0
    # The pre-fix function looked up json_loads_exact in procedure_source.
    import services.procedure_source as _ps

    with spool.path.open("r", encoding="utf-8") as fh:
        for i, line in enumerate(fh):
            if not apply_filter:
                if i < start:
                    continue
                if i >= end:
                    break
                obj = _ps.json_loads_exact(line)
                rows.append(_row_from_spool_line(obj, spool.headers))
                continue
            obj = _ps.json_loads_exact(line)
            row = _row_from_spool_line(obj, spool.headers)
            if cursor_idx is None or not _row_after_cursor(row, cursor_idx, cursor_after):
                continue
            if start <= matched < end:
                rows.append(row)
            matched += 1
    if apply_filter:
        return list(spool.headers), rows, matched
    if cursor_col:
        return list(spool.headers), rows, spool.total
    return list(spool.headers), rows, spool.total


def test_chunk_5000_reread_parses_each_line_once_not_once_per_page(tmp_path: Path, monkeypatch):
    """Offset paging parses each spool line once. The pinned pre-fix reader does not.

    One filtered page (offset = page, cursor_after = page max) parses every line
    and skips a page of ids. This does not time a 1,000,000-row run.
    """
    n, page = 8_000, 2_000
    path = tmp_path / "chunk.jsonl"
    path.write_text("".join(json.dumps(_row(i)) + "\n" for i in range(1, n + 1)), encoding="utf-8")
    import services.procedure_source as ps

    calls = {"n": 0}
    real = ps.json_loads_exact

    def _count(line):
        calls["n"] += 1
        return real(line)

    monkeypatch.setattr(ps, "json_loads_exact", _count)
    spool = _spool(path, [_row(i) for i in range(1, n + 1)])
    seen: list[str] = []
    for start in range(0, n, page):
        _headers, got, _total = _read_spool_page(
            spool, offset=start, limit=page, cursor_column="id", cursor_after=None,
        )
        seen.extend(row[0] for row in got)
    assert seen == [str(i) for i in range(1, n + 1)]
    assert calls["n"] == n

    calls["n"] = 0
    _headers, old_rows, old_matched = _pinned_prerefix_read_spool_page(
        spool, offset=page, limit=page, cursor_column="id", cursor_after=str(page),
    )
    assert calls["n"] == n
    assert old_matched == n - page
    assert old_rows[0][0] == str(page * 2 + 1)
    assert old_rows[-1][0] == str(page * 3)


def test_a_blocked_reread_page_updates_elapsed_without_inventing_rows():
    class _Store:
        def __init__(self):
            self.fields: list[dict] = []

        def update_job_status(self, _job_id, _status, **kwargs):
            self.fields.append(dict(kwargs))
            return True

        def update_job_fields(self, _job_id, fields, **_kw):
            self.fields.append(dict(fields))
            return True

    store = _Store()
    with source_reread_heartbeat(store, "job", written=1_000_000, expected=1_000_000, interval_s=0.05) as progress:
        progress.rows_hashed = 10_000
        time.sleep(0.2)
    messages = [str(item.get("message") or "") for item in store.fields]
    progressed = [m for m in messages if "10,000 of 1,000,000" in m]
    assert progressed, messages
    assert any("has not returned" in m for m in progressed)
    assert all("20,000" not in m for m in messages)
    pcts = [item.get("reconcile_pct") for item in store.fields if "10,000 of 1,000,000" in str(item.get("message"))]
    assert pcts and pcts[-1] == 1
