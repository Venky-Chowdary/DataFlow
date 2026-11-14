"""An overwrite whose every row is quarantined must fail and keep the destination.

Owner decision: a full-refresh overwrite that would replace the destination
with zero good rows, because every row was quarantined, fails before the
destination is replaced. The existing table stays intact, the quarantine is
saved, and the message is the standard ``all_rows_quarantined`` refusal. A
legitimately empty source (zero rows read, zero quarantined) keeps today's
behaviour.

What it did before, through the real engine:

* file source (file-stream route): every engine — SQLite, DuckDB,
  PostgreSQL, MySQL — ended with an empty table and ``success=True``;
* database source (streaming route): the run failed, but SQLite had already
  published the empty stage and PostgreSQL had committed the held TRUNCATE,
  so the table was empty anyway;
* pre-ingestion staging (``write_via_staging``): the primary table was
  DROPPED before the "no clean rows" check;
* multi-stream overwrite: a MySQL stream whose rows were all quarantined lost
  its table, and the engine "restored" that backup onto the first stream's
  table, discarding the rows that stream had just written.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from typing import Any

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

ALL_BAD = [("1", "bad"), ("2", "worse")]
ONE_BAD = [("1", "5"), ("2", "worse")]
MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 0.95},
    {"source": "age", "target": "age", "confidence": 0.95, "target_type": "integer"},
]
PRIOR = [("9", 99)]


class _Dest:
    """A destination table pre-seeded with one prior row, and a reader for it."""

    def __init__(self, kind: str, tmp_path: Path, table: str) -> None:
        self.kind, self.table = kind, table
        if kind == "sqlite":
            path = tmp_path / f"{table}.db"
            self.endpoint = EndpointConfig(kind="database", format="sqlite", table=table,
                                           connection_string=f"sqlite:///{path}",
                                           database=str(path))
            self._exec = lambda sql: _sqlite_exec(path, sql)
            self._query = lambda sql: sqlite3.connect(path).execute(sql).fetchall()
        elif kind == "duckdb":
            duckdb = pytest.importorskip("duckdb")
            path = tmp_path / f"{table}.duckdb"
            self.endpoint = EndpointConfig(kind="database", format="duckdb", table=table,
                                           connection_string=f"duckdb:///{path}",
                                           database=str(path))

            def _run(sql: str, fetch: bool = False):
                with duckdb.connect(str(path)) as conn:
                    cur = conn.execute(sql)
                    return cur.fetchall() if fetch else None

            self._exec = _run
            self._query = lambda sql: _run(sql, fetch=True)
        else:
            from tests import r4_route_harness as h

            cfg = h.dest_config(kind)  # skips when the service is not configured
            self.endpoint = EndpointConfig(
                kind="database", format=kind, host=cfg["host"], port=int(cfg["port"]),
                database=cfg["database"], username=cfg["username"],
                password=cfg["password"], schema=cfg.get("schema", ""), table=table,
            )
            self._exec = lambda sql: h.execute(kind, sql)
            self._query = lambda sql: h.query(kind, sql)

    def quoted(self, table: str | None = None) -> str:
        name = table or self.table
        return f"`{name}`" if self.kind == "mysql" else f'"{name}"'

    def seed(self, table: str | None = None) -> None:
        self._exec(f"CREATE TABLE {self.quoted(table)} (id varchar(10), age integer)")
        self._exec(f"INSERT INTO {self.quoted(table)} VALUES ('9', 99)")

    def rows(self, table: str | None = None) -> list[tuple]:
        return sorted(
            (str(i), int(a)) for i, a in self._query(f"SELECT id, age FROM {self.quoted(table)}")  # nosec B608
        )

    def table_names(self) -> set[str]:
        if self.kind == "sqlite":
            sql = "SELECT name FROM sqlite_master WHERE type = 'table'"
        elif self.kind == "mysql":
            sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
        elif self.kind == "postgresql":
            sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        else:
            sql = "SELECT table_name FROM information_schema.tables"
        return {str(r[0]) for r in self._query(sql)}

    def drop(self, *tables: str) -> None:
        if self.kind in ("postgresql", "mysql"):
            from tests import r4_route_harness as h

            for t in tables:
                # The pre-ingestion stage is retained when rows were held back.
                for suffix in ("", "_df_quarantine", "_df_staging", "__df_bak"):
                    h.drop_table(self.kind, f"{t}{suffix}")
            # A backup a failed assertion left behind (run-scoped names).
            for name in self.table_names():
                if any(name.startswith(f"{t}__df_bak") for t in tables):
                    h.drop_table(self.kind, name)


def _sqlite_exec(path: Path, sql: str) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(sql)


@pytest.fixture
def dest_factory(tmp_path):
    made: list[_Dest] = []

    def make(kind: str) -> _Dest:
        dest = _Dest(kind, tmp_path, f"owq_{uuid.uuid4().hex[:8]}")
        made.append(dest)
        dest.seed()
        return dest

    yield make
    for dest in made:
        dest.drop(dest.table)


def _source(kind: str, rows: list[tuple], tmp_path: Path) -> tuple[EndpointConfig, dict[str, Any]]:
    if kind == "file":
        body = "id,age\n" + "".join(f"{a},{b}\n" for a, b in rows)
        return EndpointConfig(kind="file", format="csv"), dict(
            source_filename="ages.csv", source_content=body.encode(),
            column_types={"id": "string", "age": "string"},
        )
    path = tmp_path / f"src_{uuid.uuid4().hex[:6]}.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE src (id TEXT, age TEXT)")
        conn.executemany("INSERT INTO src VALUES (?, ?)", rows)
    return EndpointConfig(kind="database", format="sqlite", connection_string=f"sqlite:///{path}",
                          database=str(path), table="src"), {}


def _overwrite(dest: _Dest, rows, *, source: str, tmp_path: Path,
               sync: str = "full_refresh_overwrite", **extra: Any):
    src, src_extra = _source(source, rows, tmp_path)
    return UniversalTransferEngine().execute(
        TransferRequest(
            source=src, destination=dest.endpoint, sync_mode=sync, skip_preflight=True,
            validation_mode="balanced", mappings=[dict(m) for m in MAPPINGS],
            **src_extra, **extra,
        )
    )


def _assert_refused_and_kept(result, dest: _Dest, *, rows: int = 2) -> None:
    from services.error_handling import humanize_transfer_failure
    from services.quarantine_dlq import quarantine_details_from_dlq

    assert result.success is False, "an all-quarantined run is never a success"
    error = str(result.error)
    assert f"All {rows} row(s) were quarantined, so nothing was written" in error, error
    assert humanize_transfer_failure(ValueError(error))["code"] == "all_rows_quarantined"
    assert "already written" not in error and "left in place" not in error, error
    assert dest.rows() == PRIOR, "the destination keeps the rows it held before"
    saved = quarantine_details_from_dlq(result.job_id)
    assert {str((d.get("values") or {}).get("id")) for d in saved} >= {"1", "2"}, saved


@pytest.mark.parametrize("kind", ["sqlite", "duckdb", "postgresql", "mysql"])
@pytest.mark.parametrize("source", ["file", "database"])
def test_overwrite_with_every_row_quarantined_fails_and_keeps_the_table(
    dest_factory, tmp_path, kind, source
):
    dest = dest_factory(kind)
    _assert_refused_and_kept(_overwrite(dest, ALL_BAD, source=source, tmp_path=tmp_path), dest)


@pytest.mark.parametrize("kind", ["sqlite", "postgresql", "mysql"])
def test_pre_ingestion_staging_overwrite_never_drops_the_primary(dest_factory, tmp_path, kind):
    dest = dest_factory(kind)
    result = _overwrite(dest, ALL_BAD, source="file", tmp_path=tmp_path, write_via_staging=True)
    _assert_refused_and_kept(result, dest)


def test_buffered_overwrite_is_refused_before_it_replaces_the_table(dest_factory, tmp_path):
    """``priority_column`` keeps the run on the buffered (in-memory) route."""
    dest = dest_factory("sqlite")
    result = _overwrite(dest, ALL_BAD, source="file", tmp_path=tmp_path, priority_column="id")
    _assert_refused_and_kept(result, dest)


@pytest.mark.parametrize("source", ["file", "database"])
def test_append_with_every_row_quarantined_fails_on_every_route(dest_factory, tmp_path, source):
    dest = dest_factory("sqlite")
    result = _overwrite(dest, ALL_BAD, source=source, tmp_path=tmp_path,
                        sync="full_refresh_append")
    _assert_refused_and_kept(result, dest)


@pytest.mark.parametrize("source", ["file", "database"])
def test_partial_quarantine_still_overwrites(dest_factory, tmp_path, source):
    dest = dest_factory("sqlite")
    result = _overwrite(dest, ONE_BAD, source=source, tmp_path=tmp_path)
    assert result.success, result.error
    assert dest.rows() == [("1", 5)]


def test_empty_source_keeps_its_current_behaviour(dest_factory, tmp_path):
    """Zero rows read, zero quarantined: not this refusal."""
    file_dest = dest_factory("sqlite")
    empty_file = _overwrite(file_dest, [], source="file", tmp_path=tmp_path)
    assert empty_file.success is False and "File contains no records" in str(empty_file.error)
    assert file_dest.rows() == PRIOR

    db_dest = dest_factory("sqlite")
    empty_table = _overwrite(db_dest, [], source="database", tmp_path=tmp_path)
    assert empty_table.success, empty_table.error
    assert db_dest.rows() == [], "an empty source table overwrites to empty, as before"


MULTI_MAPS = [
    {"source": "id", "target": "id", "source_type": "TEXT", "target_type": "VARCHAR(10)"},
    {"source": "age", "target": "age", "source_type": "TEXT", "target_type": "INTEGER"},
]
#: Engines whose overwrite clear keeps a backup until the run's outcome is known.
BACKED_UP = {"mysql", "duckdb"}


def _two_stream_overwrite(tmp_path: Path, kind: str):
    """Stream 1 (good rows) lands; stream 2 (every row unfit) is refused."""
    tag = uuid.uuid4().hex[:6]
    good, bad = f"msg_{tag}", f"msb_{tag}"
    dest = _Dest(kind, tmp_path, good)
    src = tmp_path / f"multi_{tag}.db"
    with sqlite3.connect(src) as conn:
        for table, rows in ((good, [("1", "5"), ("2", "6")]), (bad, ALL_BAD)):
            conn.execute(f"CREATE TABLE {table} (id TEXT, age TEXT)")
            conn.executemany(f"INSERT INTO {table} VALUES (?, ?)", rows)  # noqa: S608
    for table in (good, bad):
        dest.seed(table)
    result = UniversalTransferEngine().execute(TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table=good),
        destination=dest.endpoint, sync_mode="full_refresh_overwrite",
        validation_mode="balanced", skip_preflight=True,
        stream_contracts=[{"name": n, "selected": True, "mappings": MULTI_MAPS} for n in (good, bad)],
    ))
    return dest, good, bad, tag, result


@pytest.mark.parametrize("kind", ["sqlite", "postgresql", "mysql", "duckdb"])
def test_multi_stream_overwrite_keeps_the_refused_streams_table(tmp_path, kind):
    """The failing stream restores its own cleared table, never another's.

    A table the run reached before the failure is undone by the multi-table
    undo (C4): restored from its overwrite backup where the engine keeps one
    until the run's outcome is known; where an overwrite commits for good
    (SQLite stage publish, PostgreSQL held TRUNCATE) it is kept and named.
    """
    dest, good, bad, _tag, result = _two_stream_overwrite(tmp_path, kind)
    try:
        assert result.success is False
        assert "were quarantined, so nothing was written" in str(result.error)
        assert dest.rows(bad) == PRIOR, "the refused stream's table is intact"
        if kind in BACKED_UP:
            assert dest.rows(good) == PRIOR, "the landed stream is undone with the run"
            assert f"{good}: restored from its pre-run backup" in str(result.error)
        else:
            assert dest.rows(good) == [("1", 5), ("2", 6)]
            assert f"{good}: kept as is (held 1 row(s) before" in str(result.error)
    finally:
        dest.drop(good, bad)


@pytest.mark.parametrize("kind", sorted(BACKED_UP))
def test_landed_overwrite_is_restored_when_a_later_stream_fails(tmp_path, kind):
    """Stream 1 overwrites and lands, stream 2 fails: stream 1 is back to its
    pre-run rows, stream 2 is unchanged, and no backup is left behind."""
    dest, good, bad, tag, result = _two_stream_overwrite(tmp_path, kind)
    try:
        assert result.success is False
        assert dest.rows(good) == PRIOR
        assert dest.rows(bad) == PRIOR
        orphans = {t for t in dest.table_names() if tag in t and "__df_bak" in t}
        assert not orphans, orphans
    finally:
        dest.drop(good, bad)


@pytest.mark.parametrize("kind", sorted(BACKED_UP))
def test_multi_stream_overwrite_success_discards_every_backup(tmp_path, kind):
    tag = uuid.uuid4().hex[:6]
    first, second = f"msg_{tag}", f"msh_{tag}"
    dest = _Dest(kind, tmp_path, first)
    src = tmp_path / f"multi_{tag}.db"
    with sqlite3.connect(src) as conn:
        for table in (first, second):
            conn.execute(f"CREATE TABLE {table} (id TEXT, age TEXT)")
            conn.executemany(f"INSERT INTO {table} VALUES (?, ?)", [("1", "5")])  # noqa: S608
    try:
        for table in (first, second):
            dest.seed(table)
        result = UniversalTransferEngine().execute(TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(src), table=first),
            destination=dest.endpoint, sync_mode="full_refresh_overwrite",
            validation_mode="balanced", skip_preflight=True,
            stream_contracts=[
                {"name": n, "selected": True, "mappings": MULTI_MAPS} for n in (first, second)
            ],
        ))
        assert result.success, result.error
        assert dest.rows(first) == dest.rows(second) == [("1", 5)]
        assert not {t for t in dest.table_names() if tag in t and "__df_bak" in t}
    finally:
        dest.drop(first, second)
