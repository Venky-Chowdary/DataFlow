"""Query-source Gate-8 on the engine, not the spool helper.

QA 2026-11-06 job 6aee9711 stopped its visible source re-read at 10,000 of
1,000,000. The re-read used to pass the page max as ``cursor_after`` and the
page start as ``offset`` together. On a sorted spool that skips a page, so
the source digest cannot match a destination that received every row.

This drives ``UniversalTransferEngine`` with a deterministic SQL query over a
SQLite table (the same callable spool the PostgreSQL ``generate_series``
extract uses). It does not open a production database.
"""
from __future__ import annotations

import os
import sqlite3
import uuid

from src.transfer.models import EndpointConfig, TransferRequest

_ROWS = 45_000  # three pages at the 20,000 engine chunk: 1..20000, 20001..40000, 40001..45000
_FIELDS = ("id", "label", "amount", "event_date", "note", "unicode_text", "flag")


def _load(tmp_path, n: int = _ROWS) -> tuple[str, str]:
    src = str(tmp_path / "src.db")
    dst = str(tmp_path / "dst.db")
    conn = sqlite3.connect(src)
    conn.execute(
        "CREATE TABLE series (id INTEGER PRIMARY KEY, label TEXT, amount TEXT, "
        "event_date TEXT, note TEXT, unicode_text TEXT, flag TEXT)"
    )
    conn.executemany(
        "INSERT INTO series VALUES (?,?,?,?,?,?,?)",
        [
            (
                i,
                f"label-{i}",
                f"{i}.250000",
                "2026-11-06",
                None if i % 17 == 0 else f"note-{i}",
                f"u-{i}-café",
                "true" if i % 2 == 0 else "false",
            )
            for i in range(1, n + 1)
        ],
    )
    conn.commit()
    conn.close()
    sqlite3.connect(dst).close()
    return src, dst


def _request(src: str, dst: str) -> TransferRequest:
    query = "SELECT id, label, amount, event_date, note, unicode_text, flag FROM series"
    return TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="sqlite",
            database=src,
            table="series",
            extra={"source_read_mode": "query", "source_query": query},
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=dst, table="series_out",
        ),
        sync_mode="incremental_deduped",
        stream_contracts=[{
            "name": "series",
            "selected": True,
            "sync_mode": "incremental_deduped",
            "primary_key": "id",
            "cursor_field": "id",
        }],
        mappings=[{"source": name, "target": name} for name in _FIELDS],
        skip_preflight=True,
    )


def _run(tmp_path, monkeypatch, *, seek):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    import src.transfer.engine as engine_mod
    import src.transfer.stream as stream_mod
    from services import source_reread
    from src.transfer.engine import UniversalTransferEngine
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    if seek is not None:
        monkeypatch.setattr(source_reread, "callable_reread_seek", seek)
    reads: list[dict] = []
    real = stream_mod._read_batch_impl

    def _spy(*args, **kwargs):
        batch = real(*args, **kwargs)
        rows = getattr(batch, "rows", None) or []
        reads.append({
            "offset": kwargs["offset"] if "offset" in kwargs else (args[4] if len(args) > 4 else None),
            "cursor_after": kwargs.get("cursor_after"),
            "cursor_column": kwargs.get("cursor_column") or "",
            "n": len(rows),
            "first": rows[0][0] if rows else None,
            "last": rows[-1][0] if rows else None,
        })
        return batch

    monkeypatch.setattr(stream_mod, "_read_batch_impl", _spy)
    src, dst = _load(tmp_path)
    job_id = "qrecon" + uuid.uuid4().hex[:10]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(_request(src, dst), job_id)
    with sqlite3.connect(dst) as conn:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        if "series_out" not in tables:
            dest = (0, 0, None, None, tables)
        else:
            count, distinct, lo, hi = conn.execute(
                "SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id) FROM series_out"
            ).fetchone()
            dest = (count, distinct, lo, hi, tables)
    return result, reads, dest


def _reread_pages(reads: list[dict]) -> list[dict]:
    """Pages after the write pass: the second time offset 0 is read with the cursor column."""
    keyed = [r for r in reads if r["cursor_column"] and r["n"]]
    starts = [i for i, r in enumerate(keyed) if r["offset"] == 0]
    assert len(starts) >= 2, keyed
    # The sizing probe and the write pass also start at offset 0. The re-read
    # is the last of those passes.
    return keyed[starts[-1]:]


def test_query_source_engine_reread_pages_by_offset_and_matches(tmp_path, monkeypatch):
    result, reads, dest = _run(tmp_path, monkeypatch, seek=None)
    assert result.success, result.error
    assert result.records_transferred == _ROWS
    assert dest[0] == _ROWS
    assert dest[1] == _ROWS
    assert dest[2] == 1 and dest[3] == _ROWS
    pages = _reread_pages(reads)
    assert len(pages) >= 2
    assert all(page["cursor_after"] in (None, "") for page in pages)
    second = next(page for page in pages if page["offset"])
    assert int(second["first"]) == int(second["offset"]) + 1
    seen = []
    for page in pages:
        # Reconstruct coverage from the edges the engine actually returned.
        seen.append((int(page["first"]), int(page["last"]), page["n"]))
    assert seen[0][0] == 1
    assert seen[-1][1] == _ROWS
    for (prev_last, _n), (first, last, n) in zip(
        [(last, n) for _f, last, n in seen],
        seen[1:],
    ):
        assert first == prev_last + 1
        assert last - first + 1 == n


def test_page_max_cursor_plus_offset_fails_the_engine_checksum(tmp_path, monkeypatch):
    """The pre-fix re-read arguments: offset advances and cursor_after is the page max."""

    def _old_seek(*, page_start: int, seed_watermark: str | None) -> dict:
        start = max(0, int(page_start))
        return {
            "offset": start,
            "cursor_after": str(start) if start else None,
        }

    result, reads, dest = _run(tmp_path, monkeypatch, seek=_old_seek)
    pages = _reread_pages(reads)
    second = next(page for page in pages if page["offset"])
    # offset 20000 and cursor_after "20000" skips 20001..40000 and returns 40001.
    assert int(second["cursor_after"]) == int(second["offset"])
    assert int(second["first"]) == int(second["offset"]) * 2 + 1
    assert result.success is False
    assert "Checksum mismatch" in (result.error or "")
    # A failed proof removes the batch. The rows are not left behind as a match.
    assert dest[0] == 0
