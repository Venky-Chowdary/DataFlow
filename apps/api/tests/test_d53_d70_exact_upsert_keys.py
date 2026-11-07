"""D53 / D70 — an upsert key is its exact value.

D53: the dense-upsert guard treated ``''`` (and whitespace) as a NULL key, so a
table with a ``''`` key was refused (``dense upsert refused null/empty conflict
key``). NULL is the only absent key: ``''`` is a value for every database, a
quoted file ``""`` decodes to it, and an unquoted empty file cell is already
NULL by the time it reaches the writer. Oracle stores ``''`` as NULL, so there
it stays refused.

D70: the keyed-delta census normalised keys with ``present_cell_text`` (strip +
drop blanks), so ``'a '`` and ``'a'`` were one key and ``''`` was no key; the
destination then held more rows than the census predicted and a correct upsert
failed Gate-8.

Real SQLite, PostgreSQL and MySQL (dataflow/dataflow on localhost) through the
engine. Case-variant keys follow the destination collation: SQLite and
PostgreSQL keep ``A`` and ``a`` apart; MySQL's default collation is
case-insensitive and folds them, which the run reports instead of passing.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
    except OSError:
        return False
    return True


class _Engine:
    """One database: create/mutate a source table, read the destination back."""

    format = ""

    def endpoint(self, table: str) -> EndpointConfig:
        raise NotImplementedError

    def execute(self, sql: str, rows=None) -> None:
        raise NotImplementedError

    def fetch(self, sql: str) -> list[tuple]:
        raise NotImplementedError

    def drop(self, *tables: str) -> None:
        for t in tables:
            self.execute(f"DROP TABLE IF EXISTS {self.q(t)}")

    def q(self, ident: str) -> str:
        return f'"{ident}"'


class _Sqlite(_Engine):
    format = "sqlite"

    def __init__(self, tmp: Path):
        self.src_path, self.dst_path = tmp / "src.sqlite", tmp / "dst.sqlite"

    def endpoint(self, table):
        path = self.src_path if table.startswith("s_") else self.dst_path
        return EndpointConfig(kind="database", format="sqlite", database=str(path), table=table)

    def execute(self, sql, rows=None, *, dest=False):
        conn = sqlite3.connect(str(self.dst_path if dest else self.src_path))
        try:
            with conn:
                conn.executemany(sql, rows) if rows is not None else conn.execute(sql)
        finally:
            conn.close()

    def fetch(self, sql):
        conn = sqlite3.connect(str(self.dst_path))
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()

    def drop(self, *tables):
        pass


class _Postgres(_Engine):
    format = "postgresql"

    def endpoint(self, table):
        return EndpointConfig(
            kind="database", format="postgresql", host="127.0.0.1", port=5432,
            database="dataflow", schema="public", username="dataflow",
            password="dataflow", table=table,
        )

    def _conn(self):
        import psycopg2

        conn = psycopg2.connect(
            host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
        )
        conn.autocommit = True
        return conn

    def execute(self, sql, rows=None, *, dest=False):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.executemany(sql, rows) if rows is not None else cur.execute(sql)
        finally:
            conn.close()

    def fetch(self, sql):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
        finally:
            conn.close()


class _Mysql(_Engine):
    format = "mysql"

    def endpoint(self, table):
        return EndpointConfig(
            kind="database", format="mysql", host="127.0.0.1", port=3306,
            database="dataflow", username="dataflow", password="dataflow", table=table,
        )

    def _conn(self):
        from connectors.mysql_conn import get_connection

        conn = get_connection(
            host="127.0.0.1", port=3306, database="dataflow",
            username="dataflow", password="dataflow", connection_string="", ssl=False,
        )
        return conn

    def q(self, ident):
        return f"`{ident}`"

    def execute(self, sql, rows=None, *, dest=False):
        conn = self._conn()
        try:
            cur = conn.cursor()
            try:
                sql = sql.replace("?", "%s")
                cur.executemany(sql, rows) if rows is not None else cur.execute(sql)
                conn.commit()
            finally:
                cur.close()
        finally:
            conn.close()

    def fetch(self, sql):
        conn = self._conn()
        try:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                return [tuple(r) for r in cur.fetchall()]
            finally:
                cur.close()
        finally:
            conn.close()


@pytest.fixture(params=["sqlite", "postgresql", "mysql"])
def db(request, tmp_path, monkeypatch):
    kind = request.param
    if kind == "postgresql" and not _up(5432):
        pytest.skip("PostgreSQL not on 127.0.0.1:5432")
    if kind == "mysql" and not _up(3306):
        pytest.skip("MySQL not on 127.0.0.1:3306")
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.data_quality_history as dqh
    import services.sync_cursor as sc

    class _Mongo(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields or {})

    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(dqh, "_profile_collection", lambda: None)
    monkeypatch.setenv("DATAFLOW_QUALITY_PROFILES_DIR", str(tmp_path / "profiles"))
    engine = {"sqlite": lambda: _Sqlite(tmp_path), "postgresql": _Postgres, "mysql": _Mysql}[kind]()
    engine.fake = fake
    suffix = uuid.uuid4().hex[:8]
    engine.src, engine.dst = f"s_{suffix}", f"d_{suffix}"
    yield engine
    if kind != "sqlite":
        engine.drop(engine.src, engine.dst)


def _upsert(db, source=None):
    """Upsert ``source`` (default: a table in ``db`` itself) into ``db.dst``."""
    source = source or db
    job_id = "d53" + uuid.uuid4().hex[:16]
    db.fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(
        TransferRequest(
            source=source.endpoint(source.src),
            destination=db.endpoint(db.dst),
            sync_mode="incremental_upsert",
            stream_contracts=[{
                "name": source.src, "primary_key": ["k"], "cursor_field": "seq",
                "cursor_semantics": "monotonic_sequence",
                "sync_mode": "incremental_upsert", "selected": True,
            }],
            validation_mode="strict",
            skip_preflight=True,
        ),
        job_id,
    )


def _upsert_with_preflight(db, source):
    job_id = "d70" + uuid.uuid4().hex[:16]
    db.fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(
        TransferRequest(
            source=source.endpoint(source.src),
            destination=db.endpoint(db.dst),
            sync_mode="incremental_upsert",
            stream_contracts=[{
                "name": source.src, "primary_key": ["k"], "cursor_field": "seq",
                "cursor_semantics": "monotonic_sequence",
                "sync_mode": "incremental_upsert", "selected": True,
            }],
            validation_mode="strict",
        ),
        job_id,
    )


def _make_source(db, keys):
    db.execute(f"CREATE TABLE {db.q(db.src)} (k VARCHAR(20) NOT NULL PRIMARY KEY, v VARCHAR(20), seq INTEGER NOT NULL)")
    ph = "%s" if db.format in {"postgresql", "mysql"} else "?"
    db.execute(
        f"INSERT INTO {db.q(db.src)} VALUES ({ph}, {ph}, {ph})",
        [(k, f"v1_{i}", i + 1) for i, k in enumerate(keys)],
    )


def _bump_source(db):
    db.execute(f"UPDATE {db.q(db.src)} SET v = CONCAT('v2_', k), seq = seq + 100"
               if db.format == "mysql" else
               f"UPDATE {db.q(db.src)} SET v = 'v2_' || k, seq = seq + 100")


def _dest(db):
    return dict(db.fetch(f"SELECT k, v FROM {db.q(db.dst)}"))


@pytest.mark.parametrize("source_kind", ["same_engine", "sqlite_source"])
def test_empty_and_space_keys_upsert_and_stay_distinct(db, tmp_path, source_kind):
    """D53 + D70: '' is a key; 'a ' and 'a' are two keys; run 2 updates all.

    ``same_engine`` takes each database's own fast path; ``sqlite_source``
    sends the rows through the generic writer guard and the key census."""
    if source_kind == "sqlite_source":
        (tmp_path / "x").mkdir()
        source = _Sqlite(tmp_path / "x")
        source.fake = db.fake
        source.src = db.src
    else:
        source = db
    keys = ["", "a", "a ", "b"]
    if db.format == "mysql" and source_kind == "sqlite_source":
        # A table DataFlow creates on MySQL gets utf8mb4_bin, a PAD SPACE
        # collation where 'a' = 'a ' - covered by the pad-space test below.
        keys.remove("a ")
    _make_source(source, keys)
    first = _upsert(db, source)
    assert first.success, first.error
    assert set(_dest(db)) == set(keys)

    _bump_source(source)
    second = _upsert(db, source)
    assert second.success, second.error
    dest = _dest(db)
    assert dest == {k: f"v2_{k}" for k in keys}


def _case_source(tmp_path):
    src_file = tmp_path / "case_src.sqlite"
    conn = sqlite3.connect(str(src_file))
    with conn:
        conn.execute("CREATE TABLE s_case (k VARCHAR(20) NOT NULL PRIMARY KEY, v VARCHAR(20), seq INTEGER NOT NULL)")
        conn.executemany("INSERT INTO s_case VALUES (?, ?, ?)", [("A", "v1_A", 1), ("a", "v1_a", 2)])
    conn.close()
    return src_file


def _case_upsert(db, src_file, *, skip_preflight=True):
    job_id = "d70" + uuid.uuid4().hex[:16]
    db.fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(
        TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(src_file), table="s_case"),
            destination=db.endpoint(db.dst),
            sync_mode="incremental_upsert",
            stream_contracts=[{
                "name": "s_case", "primary_key": ["k"], "cursor_field": "seq",
                "cursor_semantics": "monotonic_sequence",
                "sync_mode": "incremental_upsert", "selected": True,
            }],
            validation_mode="strict",
            skip_preflight=skip_preflight,
        ),
        job_id,
    )


def test_case_variant_keys_stay_distinct_in_a_table_dataflow_creates(db, tmp_path):
    """A SQLite source holds 'A' and 'a'. DataFlow-created key columns compare
    exactly on every engine (MySQL gets utf8mb4_bin), so both rows land."""
    result = _case_upsert(db, _case_source(tmp_path))
    assert result.success, result.error
    assert set(_dest(db)) == {"A", "a"}


def test_case_folding_destination_is_graded_before_any_row_is_written(db, tmp_path):
    """An existing MySQL table keeps its default case-insensitive collation:
    'A' and 'a' are one key there. Validate must block the upsert (the sample
    identity check applies the destination collation) instead of letting the
    engine merge two source rows into one."""
    if db.format != "mysql":
        pytest.skip("only MySQL's default collation is case-insensitive")
    db.execute(
        f"CREATE TABLE {db.q(db.dst)} (k VARCHAR(20) NOT NULL PRIMARY KEY, v VARCHAR(20), seq BIGINT NOT NULL)"
    )
    result = _case_upsert(db, _case_source(tmp_path), skip_preflight=False)
    assert not result.success
    assert "Duplicate identity keys" in str(result.error), result.error
    assert _dest(db) == {}


def test_pad_space_destination_never_merges_trailing_space_keys_silently(db, tmp_path):
    """MySQL's utf8mb4_bin (what DataFlow stamps on a created key column) is a
    PAD SPACE collation: 'a' = 'a '. Validate blocks the pair as a duplicate
    identity before any write; with preflight skipped the run still fails its
    row-count identity rather than reporting a clean 2-row upsert."""
    if db.format != "mysql":
        pytest.skip("PAD SPACE key collation is MySQL / SQL Server behaviour")
    (tmp_path / "x").mkdir()
    source = _Sqlite(tmp_path / "x")
    source.fake, source.src = db.fake, db.src
    _make_source(source, ["a", "a "])
    result = _upsert_with_preflight(db, source)
    assert not result.success
    assert "Duplicate identity keys" in str(result.error), result.error
