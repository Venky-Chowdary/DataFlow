"""D359 re-review (0fc432907): stamps folded into source columns, counters, the
commit-window acknowledgement, and the refusal reaching the job record whole.

* A per-run stamp folded into a SOURCE column (``concat_columns(who, [who,
  loaded_at])``) must not be left out of the match: the column still carries
  the row's identity. Left out, a late "Ann" at the watermark matched "Bob"'s
  stored copy and was dropped, Bob was appended again, green. It is refused,
  naming the step.
* ``surrogate_key`` restarts its counter on every run (and every probe fork):
  its column is not matched, so the watermark row is not appended again.
* With the route's audited acknowledgement, a commit-window re-read whose tie
  group has no comparable image appends only that tie group unchecked; rows
  below the watermark are still matched by (cursor, key), the acceptance is
  recorded on the run, and the replay counts are true.
* The refusal keeps its own classification and remedy on the job record.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

pytestmark = pytest.mark.timeout(600)


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Mongo(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields or {})
            return True

    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _request(source, dest, recipe, columns, *, cursor="updated_at", semantics="modification_timestamp"):
    from services.shape_models import ShapeRecipe

    parsed = ShapeRecipe.parse(recipe, source_columns=list(columns))
    targets = list(dict.fromkeys([*columns, *parsed.output_columns]))
    return TransferRequest(
        source=source,
        destination=dest,
        mappings=[{"source": c, "target": c, "approved": True, "confidence": 0.99} for c in targets],
        sync_mode="incremental_append",
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=parsed.recipe_hash,
        stream_contracts=[{
            "name": source.table, "cursor_field": cursor, "cursor_semantics": semantics,
            "sync_mode": "incremental_append", "selected": True,
        }],
    )


def _run(store, request):
    job_id = "d359r" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request={})
    return job_id, UniversalTransferEngine().execute_tracked(request, job_id)


def _sql(path: Path, sql: str, params: tuple = ()) -> list:
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute(sql, params).fetchall()
        conn.commit()
        return rows
    finally:
        conn.close()


def _ep(path: Path, table: str = "t") -> EndpointConfig:
    return EndpointConfig(kind="database", format="sqlite", database=str(path), table=table)


def _ts(i: int) -> str:
    return f"2026-11-01T10:{i:02d}:00"


def test_a_stamp_folded_into_a_source_column_is_refused_and_nothing_is_lost(store, tmp_path):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _sql(src, "CREATE TABLE t (who TEXT, grp TEXT, updated_at TEXT NOT NULL)")
    _sql(src, "INSERT INTO t (rowid, who, grp, updated_at) VALUES (1, 'x', 'g', ?)", (_ts(1),))
    _sql(src, "INSERT INTO t (rowid, who, grp, updated_at) VALUES (2, 'Bob', 'g', ?)", (_ts(2),))
    recipe = {"steps": [
        {"op": "run_timestamp", "options": {"to": "loaded_at"}},
        {"op": "concat_columns", "options": {"to": "who", "columns": ["who", "loaded_at"], "separator": "|"}},
    ]}
    cols = ["who", "grp", "updated_at"]
    _job, first = _run(store, _request(_ep(src), _ep(dst), recipe, cols))
    assert first.success, first.error
    # A late row at the watermark, read before Bob.
    _sql(src, "INSERT INTO t (rowid, who, grp, updated_at) VALUES (0, 'Ann', 'g', ?)", (_ts(2),))

    _job, second = _run(store, _request(_ep(src), _ep(dst), recipe, cols))

    whos = sorted(str(r[0]).split("|")[0] for r in _sql(dst, "SELECT who FROM t"))
    assert not second.success, f"green over a stamp folded into 'who': {whos}"
    assert "Step 2 (concat_columns)" in str(second.error) and "'who'" in str(second.error)
    assert whos == ["Bob", "x"], whos  # nothing appended, nothing lost


def test_surrogate_key_does_not_reappend_the_watermark_row(store, tmp_path):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _sql(src, "CREATE TABLE t (id INTEGER, name TEXT, updated_at TEXT NOT NULL)")
    for i in range(1, 4):
        _sql(src, "INSERT INTO t VALUES (?,?,?)", (i, f"n{i}", _ts(i)))
    recipe = {"steps": [{"op": "surrogate_key", "options": {"to": "sk"}}]}
    cols = ["id", "name", "updated_at"]
    _job, first = _run(store, _request(_ep(src), _ep(dst), recipe, cols))
    assert first.success, first.error
    _sql(src, "INSERT INTO t VALUES (?,?,?)", (4, "n4", _ts(4)))
    _job, second = _run(store, _request(_ep(src), _ep(dst), recipe, cols))
    assert second.success, second.error
    ids = sorted(r[0] for r in _sql(dst, "SELECT id FROM t"))
    assert ids == [1, 2, 3, 4], f"watermark row appended again: {ids}"


def test_the_boundary_refusal_keeps_its_remedy_on_the_job_record(store, tmp_path):
    """The watermark row's unnest yields two rows: no single image, refused by name."""
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _sql(src, "CREATE TABLE t (id INTEGER, tags TEXT, updated_at TEXT NOT NULL)")
    for i in range(1, 4):
        _sql(src, "INSERT INTO t VALUES (?,?,?)", (i, '["a", "b"]' if i == 3 else '["x"]', _ts(i)))
    recipe = {"steps": [{"op": "unnest_json", "column": "tags", "options": {"to": "tag"}}]}
    cols = ["id", "tags", "updated_at"]
    _job, first = _run(store, _request(_ep(src), _ep(dst), recipe, cols))
    assert first.success, first.error
    _sql(src, "INSERT INTO t VALUES (?,?,?)", (4, '["x"]', _ts(4)))

    job_id, second = _run(store, _request(_ep(src), _ep(dst), recipe, cols))

    assert not second.success
    record = store.jobs[job_id]
    stored = str(record.get("error") or record.get("error_message") or "")
    for text in (str(second.error), stored):
        assert "Step 1 (unnest_json)" in text, text
        assert "turns one source row into several written rows" in text, text
        assert "record the route's acknowledgement 'accept possible boundary duplicates'" in text, text
        assert "Open Transform and fix the recipe" not in text, text
        # One period at the end, not "..".
        assert text.rstrip().endswith("'accept possible boundary duplicates'."), text
        assert not text.rstrip().endswith(".."), text
    assert sorted(r[0] for r in _sql(dst, "SELECT id FROM t")) == [1, 2, 3, 3]


def test_an_acknowledged_tie_group_without_an_image_reports_its_count_as_not_measured(
    store, tmp_path, monkeypatch
):
    """Re-review LOW: "0 duplicates" was reported while 2 expanded copies were appended."""
    import src.transfer.stream_boundary_keyed as keyed

    monkeypatch.setattr(keyed, "_accepted", lambda *a, **k: {"actor": "qa", "reason": "ok", "at": "t"})
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _sql(src, "CREATE TABLE t (id INTEGER, tags TEXT, updated_at TEXT NOT NULL)")
    for i in range(1, 4):
        _sql(src, "INSERT INTO t VALUES (?,?,?)", (i, '["a", "b"]' if i == 3 else '["x"]', _ts(i)))
    recipe = {"steps": [{"op": "unnest_json", "column": "tags", "options": {"to": "tag"}}]}
    cols = ["id", "tags", "updated_at"]
    _job, first = _run(store, _request(_ep(src), _ep(dst), recipe, cols))
    assert first.success, first.error
    _sql(src, "INSERT INTO t VALUES (?,?,?)", (4, '["x"]', _ts(4)))

    _job, second = _run(store, _request(_ep(src), _ep(dst), recipe, cols))

    assert second.success, second.error
    # The tie group (row 3, two written rows) went through under the acknowledgement.
    assert sorted(r[0] for r in _sql(dst, "SELECT id FROM t")) == [1, 2, 3, 3, 3, 3, 4]
    check = (second.reconciliation or {}).get("boundary_check") or {}
    assert check.get("status") == "accepted", check
    assert check.get("duplicates") is None, check
    assert "image" in str(check.get("duplicates_not_measured") or ""), check
    assert "Boundary duplicates accepted by qa: not measured (" in str(
        (second.reconciliation or {}).get("message")
    )


def test_commit_window_acknowledgement_appends_only_the_tie_group(store, monkeypatch):
    """Reviewer repro: MySQL -> PG append, unnest_json expands row 10 into 2 rows,
    acknowledgement recorded. Rows below the watermark are not appended again."""
    from tests.c3_live_helpers import mysql_endpoint, mysql_sql, pg_endpoint, pg_sql
    from tests.helpers.live_env import mysql_up, pg_up

    import src.transfer.stream_boundary_keyed as keyed

    if not (pg_up() and mysql_up()):
        pytest.skip("live PostgreSQL and MySQL not available")
    ack = {"actor": "qa", "reason": "unnest route accepts boundary copies", "at": "2026-11-28T00:00:00Z"}
    monkeypatch.setattr(keyed, "_accepted", lambda *a, **k: dict(ack))
    src, dst = f"d359u_{uuid.uuid4().hex[:8]}", f"d359v_{uuid.uuid4().hex[:8]}"
    mysql_sql(f"CREATE TABLE `{src}` (id INT PRIMARY KEY, tags VARCHAR(40), updated_at DATETIME NOT NULL)")
    recipe = {"steps": [{"op": "unnest_json", "column": "tags", "options": {"to": "tag"}}]}
    try:
        for i in range(1, 11):
            tags = '["a", "b"]' if i == 10 else '["x"]'
            mysql_sql(f"INSERT INTO `{src}` VALUES (%s, %s, %s)", (i, tags, _ts(i).replace("T", " ")))

        def go():
            return _run(store, _request(mysql_endpoint(src), pg_endpoint(dst), recipe,
                                        ["id", "tags", "updated_at"], cursor="id",
                                        semantics="monotonic_sequence"))[1]

        first = go()
        assert first.success, first.error
        assert pg_sql(f"SELECT count(*) FROM {dst}", fetch=True)[0][0] == 11
        mysql_sql(f"INSERT INTO `{src}` VALUES (%s, %s, %s)", (11, '["x"]', _ts(11).replace("T", " ")))

        second = go()

        assert second.success, second.error
        ids = sorted(r[0] for r in pg_sql(f"SELECT id FROM {dst}", fetch=True))
        # Only the tie group (row 10, two written rows) is appended unchecked.
        assert ids == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 10, 10, 11], ids
        summary = second.destination_summary or {}
        accepted = (summary.get("incremental_boundary_tie") or {}).get("accepted") or {}
        assert accepted.get("actor") == "qa", summary.get("incremental_boundary_tie")
        # Two expanded copies went through: never reported as "0".
        check = (second.reconciliation or {}).get("boundary_check") or {}
        assert check.get("duplicates") is None and check.get("duplicates_not_measured"), check
        window = summary.get("commit_safe_window") or {}
        assert int(window.get("rows_replayed") or 0) > 0, window
        assert int(window.get("rows_already_landed") or 0) == int(window.get("rows_replayed") or 0), window
    finally:
        mysql_sql(f"DROP TABLE IF EXISTS `{src}`")
        pg_sql(f"DROP TABLE IF EXISTS {dst}")
