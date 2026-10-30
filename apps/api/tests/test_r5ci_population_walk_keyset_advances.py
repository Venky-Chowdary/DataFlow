"""The pre-write population walk of a cursor-bounded read must page forward.

Round-5 CI (PR #358, run 41058582812):
``test_r4_acc_d199_cancel_resume_cancel_live`` - cancel, resume, cancel on an
800k-row PostgreSQL -> MySQL incremental_upsert - never reached the second
cancel. Once the round-5 watermark merge (a5de9933) made Execute's preflight
find the resumed route's real bookmark, the population-fit walk became a
cursor-bounded read. ``iter_stream_source_column_rows`` advanced an ``offset``
the keyset reader ignores and re-sent the same ``cursor_after``, so it re-read
the first page for ever and the resumed job never left preflight.

The walk now seeks past each page's (cursor, key) high mark - the write loop's
own advance. Peer rows sharing a cursor value straddle the page edges here, and
the walked column holds NULL, '', whitespace and unicode.
"""

from __future__ import annotations

import itertools
import sqlite3

from services.keyset_pagination import encode_keyset_bookmark
from src.transfer.models import EndpointConfig
from src.transfer.source_peek import (
    iter_bounded_table_population_rows,
    iter_stream_source_column_rows,
)

ROWS = 3000


def _value(g: int):
    return {0: None, 1: "", 2: "   ", 3: "naïve 中文 ✓"}.get(g % 7, f"a{g}")


def _source(tmp_path) -> EndpointConfig:
    db = tmp_path / "src.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, u TEXT NOT NULL, a TEXT)")
    # 60 peers per cursor value: page edges (100 rows) fall inside tie groups.
    con.executemany(
        "INSERT INTO t VALUES (?, ?, ?)",
        [(g, f"2026-01-01 00:{g // 60:02d}:00", _value(g)) for g in range(1, ROWS + 1)],
    )
    con.commit()
    con.close()
    return EndpointConfig(kind="database", format="sqlite", database=str(db), table="t")


def test_cursor_bounded_walk_reads_every_row_after_the_mark_once(tmp_path):
    source = _source(tmp_path)
    # Resume point inside a tie group: (00:16:00, id 1000) - ids 960..1019 share it.
    mark = encode_keyset_bookmark(["2026-01-01 00:16:00", "1000"])
    walked = list(itertools.islice(
        iter_stream_source_column_rows(
            source, ["a", "u", "id"], chunk_size=100,
            cursor_column="u", cursor_after=mark, cursor_primary_key="id",
        ),
        ROWS * 3,  # a walk that does not advance would stop here, not hang
    ))
    ids = [int(r["id"]) for r in walked]
    assert ids == list(range(1001, ROWS + 1)), (len(ids), ids[:5], ids[-5:])
    from services.value_serializer import is_reader_null_cell

    for r in walked:
        want = _value(int(r["id"]))
        if want is None:  # SQL NULL travels as the reader's NULL marker
            assert is_reader_null_cell(r["a"]), r
        else:
            assert r["a"] == want, r


def test_population_walk_under_a_bounded_read_scope_terminates(tmp_path, monkeypatch):
    """The route Execute's preflight takes on a resumed incremental run."""
    import src.transfer.stream as stream_mod
    from services.sync_cursor import IncrementalReadScope

    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 100)

    source = _source(tmp_path)
    scope = IncrementalReadScope(
        cursor_column="u",
        primary_key="id",
        watermark=encode_keyset_bookmark(["2026-01-01 00:40:00", "2400"]),
        watermark_cursor_column="u",
        watermark_tiebreak_column="id",
    )
    assert scope.bounded
    rows = iter_bounded_table_population_rows(
        source,
        [{"source": "a", "target": "a", "target_type": "VARCHAR(4)"}],
        column_types={"a": "TEXT"},
        dest_types={"a": "VARCHAR(4)"},
        dest_db="mysql",
        source_kind="database",
        source_format="sqlite",
        read_scope=scope,
        source_columns=["a"],
    )
    walked = list(itertools.islice(rows or [], ROWS * 3))
    assert sorted(int(r["id"]) for r in walked) == list(range(2401, ROWS + 1))

