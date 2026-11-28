"""A recipe that holds rows must not make the stream read the source twice.

Seen while fixing D359 (base a4e7d104d): sort_rows on an incremental_append
from SQLite wrote every source row twice (3 rows -> 6; with several pages, the
last page twice), green.

Root cause (src/transfer/stream.py, the synchronous first-page loop before
``drive_ordered_batches``): the loop keeps fetching while every row of a page
was held or removed on the read, so the DDL page is one that writes. When the
source ended inside that loop the last fetch returned ``None``, and that
``None`` was handed to ``drive_ordered_batches`` as its starting batch;
``fetch_next(None)`` is the request for page one. A keyset-paged read (an
incremental run from a source that does not push the cursor bound into its
read: SQLite, SQL Server, Oracle, ...) then read again from the bookmark it
last advanced to - none after a single page - and the buffering step held
those rows a second time. sort_rows and a global rank_rows wrote the copies;
dedupe_rows and a per-key rank collapse them. PostgreSQL/MySQL incremental
reads re-seek from the advanced cursor and full-refresh OFFSET reads start
past the end, so those runs returned nothing on the extra read; they are
covered here so a change cannot regress them.

Exact counts, more rows than one page (the stream's page size is forced to 4),
SQLite and PostgreSQL, incremental_append and full_refresh_append (keyed full
refresh pages by keyset).
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
import src.transfer.stream as stream_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

pytestmark = pytest.mark.timeout(600)

PAGE = 4
RECIPES = {
    "sort_rows": {"steps": [{"op": "sort_rows", "options": {"order_by": "id", "direction": "desc"}}]},
    "dedupe_rows": {"steps": [{"op": "dedupe_rows", "options": {"keys": ["id"], "keep": "last", "order_by": "id"}}]},
    # A global top-N larger than the table keeps every row once (a per-key
    # limit of 1 would collapse a re-read copy and hide it, as dedupe_rows does).
    "rank_rows": {"steps": [{"op": "rank_rows", "options": {"order_by": "id", "limit": 1000}}]},
    "aggregate": {
        "steps": [{"op": "aggregate", "options": {"group_by": ["id"], "measures": [{"function": "count", "as": "n"}]}}]
    },
}


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", PAGE)
    return fake


def _request(source, dest, recipe, mode):
    from services.shape_models import ShapeRecipe

    cols = ["id", "name"]
    parsed = ShapeRecipe.parse(recipe, source_columns=cols)
    aggregate = any(s.get("op") == "aggregate" for s in recipe["steps"])
    out = list(parsed.output_columns or cols)
    return TransferRequest(
        source=source,
        destination=dest,
        mappings=[{"source": c, "target": c, "approved": True, "confidence": 0.99} for c in out],
        sync_mode=mode,
        validation_mode="strict",
        shape_recipe=recipe,
        approved_shape_recipe_hash=parsed.recipe_hash,
        # Validate's sample has no aggregate image (as tests/test_shape_buffered_ops_stream_e2e).
        skip_preflight=aggregate,
        # Incremental pages by its cursor; a full refresh with a declared key
        # pages by that key (keyset seek, decide_keyset_pagination). aggregate
        # rewrites the key, which a keyed route refuses: it runs keyless (OFFSET).
        stream_contracts=[{
            "name": source.table, "cursor_field": "id", "cursor_semantics": "monotonic_sequence",
            "sync_mode": mode, "selected": True,
        } if mode == "incremental_append" else {
            "name": source.table, "sync_mode": mode, "selected": True,
            **({} if aggregate else {"primary_key": ["id"]}),
        }],
    )


def _run(store, request):
    job_id = "brp" + uuid.uuid4().hex[:12]
    store.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    return result


def _check(rows, recipe_name, expected_ids):
    ids = sorted(int(r[0]) for r in rows)
    assert ids == expected_ids, f"{recipe_name}: {ids}"
    if recipe_name == "aggregate":
        assert {int(r[1]) for r in rows} == {1}, f"aggregate counted rows twice: {rows}"


class _SqliteRoute:
    def __init__(self, tmp_path: Path) -> None:
        self.src, self.dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
        self.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")

    def exec(self, sql: str, params: tuple = ()) -> None:
        conn = sqlite3.connect(self.src)
        conn.execute(sql.replace("%s", "?"), params)
        conn.commit()
        conn.close()

    def endpoints(self, table: str):
        return (
            EndpointConfig(kind="database", format="sqlite", database=str(self.src), table="t"),
            EndpointConfig(kind="database", format="sqlite", database=str(self.dst), table=table),
        )

    def rows(self, table: str, cols: str) -> list:
        conn = sqlite3.connect(self.dst)
        try:
            return conn.execute(f'SELECT {cols} FROM "{table}"').fetchall()  # nosec B608
        finally:
            conn.close()

    def close(self) -> None:
        return None


class _PgRoute:
    def __init__(self) -> None:
        from tests.c3_live_helpers import pg_sql

        self.sql = pg_sql
        self.src = f"brps_{uuid.uuid4().hex[:8]}"
        self.made: list[str] = []
        self.exec("CREATE TABLE {src} (id INT PRIMARY KEY, name TEXT)")

    def exec(self, sql: str, params: tuple = ()) -> None:
        self.sql(sql.replace("{src}", self.src).replace(" t ", f" {self.src} "), params)

    def endpoints(self, table: str):
        from tests.c3_live_helpers import pg_endpoint

        self.made.append(table)
        return pg_endpoint(self.src), pg_endpoint(table)

    def rows(self, table: str, cols: str) -> list:
        return self.sql(f"SELECT {cols} FROM {table}", fetch=True)  # nosec B608

    def close(self) -> None:
        for name in [self.src, *self.made]:
            self.sql(f"DROP TABLE IF EXISTS {name}")


def _route(engine: str, tmp_path: Path):
    if engine == "sqlite":
        return _SqliteRoute(tmp_path)
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not available")
    return _PgRoute()


def _insert(route, ids) -> None:
    for i in ids:
        route.exec("INSERT INTO t VALUES (%s, %s)", (i, f"n{i}"))


@pytest.mark.parametrize("engine", ["sqlite", "postgresql"])
@pytest.mark.parametrize("recipe_name", list(RECIPES))
def test_full_refresh_append_reads_each_row_once(store, tmp_path, engine, recipe_name):
    route = _route(engine, tmp_path)
    try:
        _insert(route, range(1, 11))
        table = f"fr_{recipe_name}_{uuid.uuid4().hex[:6]}"
        src, dst = route.endpoints(table)
        _run(store, _request(src, dst, RECIPES[recipe_name], "full_refresh_append"))
        cols = "id, n" if recipe_name == "aggregate" else "id, name"
        _check(route.rows(table, cols), recipe_name, list(range(1, 11)))
    finally:
        route.close()


@pytest.mark.parametrize("engine", ["sqlite", "postgresql"])
# aggregate rewrites the cursor column an incremental read pages by, which the
# engine refuses before any read; it is covered on full refresh above.
@pytest.mark.parametrize("recipe_name", [n for n in RECIPES if n != "aggregate"])
def test_incremental_append_reads_each_row_once(store, tmp_path, engine, recipe_name):
    route = _route(engine, tmp_path)
    try:
        _insert(route, range(1, 11))
        table = f"ia_{recipe_name}_{uuid.uuid4().hex[:6]}"
        src, dst = route.endpoints(table)
        cols = "id, n" if recipe_name == "aggregate" else "id, name"
        _run(store, _request(src, dst, RECIPES[recipe_name], "incremental_append"))
        _check(route.rows(table, cols), recipe_name, list(range(1, 11)))
        _insert(route, range(11, 14))
        _run(store, _request(src, dst, RECIPES[recipe_name], "incremental_append"))
        _check(route.rows(table, cols), recipe_name, list(range(1, 14)))
    finally:
        route.close()
