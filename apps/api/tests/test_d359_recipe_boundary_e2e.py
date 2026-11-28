"""D359 review round — value-preserving and run-stamping recipes on an incremental append.

Shaping re-read watermark rows before the boundary comparison must not refuse a
recipe that keeps every value (``sort_rows``, ``dedupe_rows`` keep-last), on a
keyless append or on a keyed route whose commit-window re-read replays the
overlap (MySQL source, catalog key, cursor ``id``). And a ``run_timestamp``
column - this run's start on the probe, the earlier run's on the stored copy -
must not make the watermark row look new on every run.
"""

from __future__ import annotations

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

SORT_ROWS = {"steps": [{"op": "sort_rows", "options": {"order_by": "id"}}]}
DEDUPE_LAST = {
    "steps": [{"op": "dedupe_rows", "options": {"keys": ["id"], "keep": "last", "order_by": "updated_at"}}]
}
RUN_STAMP = {"steps": [{"op": "run_timestamp", "options": {"to": "loaded_at"}}]}
RECIPES = {"sort_rows": SORT_ROWS, "dedupe_rows": DEDUPE_LAST}


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Mongo(_FakeMongo):
        # A run_timestamp step settles the run's start on the job record.
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields or {})
            return True

    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _request(source, dest, recipe, columns, *, cursor, semantics, key=None):
    from services.shape_models import ShapeRecipe

    parsed = ShapeRecipe.parse(recipe, source_columns=list(columns))
    targets = list(columns) + [c for c in parsed.output_columns if c not in columns]
    contract = {
        "name": source.table, "cursor_field": cursor, "cursor_semantics": semantics,
        "sync_mode": "incremental_append", "selected": True,
    }
    if key:
        contract["primary_key"] = key
    return TransferRequest(
        source=source,
        destination=dest,
        mappings=[{"source": c, "target": c, "approved": True, "confidence": 0.99} for c in targets],
        sync_mode="incremental_append",
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=parsed.recipe_hash,
        stream_contracts=[contract],
    )


def _run(store, request):
    job_id = "d359b" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _sqlite(path: Path, sql: str, params: tuple = ()) -> list:
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute(sql, params).fetchall()
        conn.commit()
        return rows
    finally:
        conn.close()


def _ts(i: int) -> str:
    return f"2026-11-01T10:{i:02d}:00"


def _sqlite_chain(store, tmp_path: Path, recipe: dict, extra_cols: tuple = ()):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _sqlite(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT NOT NULL)")
    for i in range(1, 4):
        _sqlite(src, "INSERT INTO t VALUES (?,?,?)", (i, f"n{i}", _ts(i)))
    ep = lambda p: EndpointConfig(kind="database", format="sqlite", database=str(p), table="t")  # noqa: E731

    def go():
        return _run(store, _request(ep(src), ep(dst), recipe, ["id", "name", "updated_at"],
                                    cursor="updated_at", semantics="modification_timestamp"))

    results = [go()]
    _sqlite(src, "INSERT INTO t VALUES (?,?,?)", (4, "n4", _ts(4)))
    results.append(go())
    _sqlite(src, "UPDATE t SET name = 'n2b', updated_at = ? WHERE id = 2", (_ts(5),))
    results.append(go())
    ids = sorted(r[0] for r in _sqlite(dst, "SELECT id FROM t"))
    return results, ids


def test_sqlite_keyless_append_with_dedupe_rows_keep_last(store, tmp_path):
    """Reviewer repro (a): refused at run 2 by the first D359 round."""
    results, ids = _sqlite_chain(store, tmp_path, DEDUPE_LAST)
    for n, result in enumerate(results, start=1):
        assert result.success, f"run {n}: {result.error}"
    assert ids == [1, 2, 2, 3, 4], ids


def test_sqlite_keyless_append_with_sort_rows_is_never_refused(store, tmp_path):
    """Reviewer repro (a), sort_rows: refused at run 2 by the first D359 round.

    Only the refusal is asserted. Counts are not: on a4e7d104d too, an
    incremental read whose first page a buffering step holds reads that page
    a second time (the keyset continuation starts with no bookmark), so
    sort_rows writes every row of a run twice. That pre-existing defect is
    reported separately; dedupe_rows hides it by collapsing the copies.
    """
    results, ids = _sqlite_chain(store, tmp_path, SORT_ROWS)
    for n, result in enumerate(results, start=1):
        assert result.success, f"run {n}: {result.error}"
    assert sorted(set(ids)) == [1, 2, 3, 4], ids


def test_run_timestamp_column_does_not_reappend_the_watermark_row(store, tmp_path):
    """Review MED: the probe stamps this run's start; the stored copy the earlier run's."""
    results, ids = _sqlite_chain(store, tmp_path, RUN_STAMP)
    for n, result in enumerate(results, start=1):
        assert result.success, f"run {n}: {result.error}"
    assert ids == [1, 2, 2, 3, 4], f"watermark row appended again: {ids}"


@pytest.mark.parametrize("name", list(RECIPES))
def test_mysql_to_pg_keyed_append_whose_window_replays_the_overlap(store, name):
    """Reviewer repro (b): MySQL source, catalog key, cursor id; base lands 11/11."""
    from tests.c3_live_helpers import mysql_endpoint, mysql_sql, pg_endpoint, pg_sql
    from tests.helpers.live_env import mysql_up, pg_up

    if not (pg_up() and mysql_up()):
        pytest.skip("live PostgreSQL and MySQL not available")
    src, dst = f"d359m_{uuid.uuid4().hex[:8]}", f"d359p_{uuid.uuid4().hex[:8]}"
    mysql_sql(f"CREATE TABLE `{src}` (id INT PRIMARY KEY, name VARCHAR(20), updated_at DATETIME NOT NULL)")
    try:
        for i in range(1, 7):
            mysql_sql(f"INSERT INTO `{src}` VALUES (%s, %s, %s)", (i, f"n{i}", _ts(i).replace("T", " ")))

        def go():
            return _run(store, _request(mysql_endpoint(src), pg_endpoint(dst), RECIPES[name],
                                        ["id", "name", "updated_at"], cursor="id",
                                        semantics="monotonic_sequence"))

        first = go()
        assert first.success, first.error
        for i in range(7, 12):
            mysql_sql(f"INSERT INTO `{src}` VALUES (%s, %s, %s)", (i, f"n{i}", _ts(i).replace("T", " ")))
        second = go()
        assert second.success, second.error
        third = go()
        assert third.success, third.error
        ids = sorted(r[0] for r in pg_sql(f"SELECT id FROM {dst}", fetch=True))
        assert ids == list(range(1, 12)), ids
    finally:
        mysql_sql(f"DROP TABLE IF EXISTS `{src}`")
        pg_sql(f"DROP TABLE IF EXISTS {dst}")
