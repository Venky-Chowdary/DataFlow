"""Repro: D115 (NULL text -> S3 CSV fails + file deleted) and
D116 (S3 CSV append fails on updated rows, batch left behind).
Run on d1cef840 to observe production behavior; keep on the hotfix
branch as regression tests.
"""
from __future__ import annotations

import uuid

import psycopg2
import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

PG_CONN = dict(
    host="localhost", port=5432, database="dataflow",
    user="dataflow", password="dataflow",
)
PG_CFG = dict(
    host="localhost", port=5432, database="dataflow",
    username="dataflow", password="dataflow",
)


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            out, self._pos = self._data[self._pos:], len(self._data)
            return out
        out = self._data[self._pos:self._pos + size]
        self._pos += len(out)
        return out

    def iter_chunks(self, chunk_size: int = 1024):
        yield self._data


def _no_such_key(key: str):
    from botocore.exceptions import ClientError

    return ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "not found"},
         "ResponseMetadata": {"HTTPStatusCode": 404}},
        "GetObject",
    )


class FakeS3:
    """In-memory S3 with a real version bump per put (spill cache freshness)."""

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.version = 0

    def head_bucket(self, Bucket):  # noqa: N803
        return {}

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.store:
            raise _no_such_key(Key)
        return {"ContentLength": len(self.store[Key]), "ETag": f'"{self.version}"', "VersionId": str(self.version)}

    def put_object(self, Bucket, Key, Body, **kw):  # noqa: N803
        self.version += 1
        self.store[Key] = Body if isinstance(Body, bytes) else bytes(Body)
        return {"VersionId": str(self.version)}

    def get_object(self, Bucket, Key, **kw):  # noqa: N803
        if Key not in self.store:
            raise _no_such_key(Key)
        return {"Body": _Body(self.store[Key])}

    def copy_object(self, **kwargs):
        self.version += 1
        self.store[kwargs["Key"]] = self.store[kwargs["CopySource"]["Key"]]

    def delete_object(self, Bucket, Key, **kw):  # noqa: N803
        self.store.pop(Key, None)

    def upload_file(self, Filename, Bucket, Key, **kw):  # noqa: N803
        self.version += 1
        with open(Filename, "rb") as f:
            self.store[Key] = f.read()

    def upload_fileobj(self, Fileobj, Bucket, Key, **kw):  # noqa: N803
        self.version += 1
        self.store[Key] = Fileobj.read()

    def get_paginator(self, name):
        fake = self

        class _Paginator:
            def paginate(self, Bucket, Prefix="", **kw):  # noqa: N803
                yield {
                    "Contents": [
                        {
                            "Key": k,
                            "Size": len(v),
                            "ETag": f'"{fake.version}"',
                            "LastModified": "2026-01-01T00:00:00Z",
                        }
                        for k, v in fake.store.items()
                        if k.startswith(Prefix)
                    ]
                }

        return _Paginator()


# Every module that binds ``boto3_client`` by name at import time. They must
# all be imported BEFORE any of them is patched: a module first imported while
# ``aws_common.boto3_client`` is the fake copies the fake into its own
# namespace, monkeypatch then records the fake as that module's "original",
# and teardown "restores" the fake. Later S3 tests in the same process (any
# order under pytest-randomly) then talk to this in-memory bucket.
_BOTO3_CLIENT_BINDERS = (
    "connectors.aws_common",
    "connectors.s3",
    "connectors.s3_reader",
    "connectors.s3_writer",
    "services.copy_s3_common",
)


@pytest.fixture
def fake_s3(monkeypatch):
    import importlib

    from services.object_streaming import clear_spill_cache

    modules = [importlib.import_module(name) for name in _BOTO3_CLIENT_BINDERS]
    fake = FakeS3()
    for mod in modules:
        monkeypatch.setattr(mod, "boto3_client", lambda *a, **k: fake)
    import connectors.s3_writer as s3w

    monkeypatch.setattr(s3w, "_ensure_bucket", lambda *a, **k: None)
    clear_spill_cache()
    yield fake
    # The object spill cache is keyed by bucket/key/version; this fake's
    # version counter restarts per test, so its entries must not outlive it.
    clear_spill_cache()


def _seed(name: str, rows: list[tuple], *, with_seq: bool = False) -> None:
    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = True
    with conn.cursor() as cur:
        if with_seq:
            cur.execute(
                f'CREATE TABLE "{name}" (id int PRIMARY KEY, seq int, name text)'
            )
            cur.executemany(
                f'INSERT INTO "{name}" (id, seq, name) VALUES (%s,%s,%s)', rows
            )
        else:
            cur.execute(f'CREATE TABLE "{name}" (id int PRIMARY KEY, name text)')
            cur.executemany(f'INSERT INTO "{name}" (id, name) VALUES (%s,%s)', rows)
    conn.close()


def _drop(*tables: str) -> None:
    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = True
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f'DROP TABLE IF EXISTS "{t}"')
    conn.close()


def _req(src_table: str, key: str, sync_mode: str, validation_mode: str,
         cursor: str | None = None) -> TransferRequest:
    contract = {
        "name": "t", "sync_mode": sync_mode,
        "primary_key": "id", "selected": True,
    }
    if cursor:
        contract["cursor_column"] = cursor
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", schema="public",
            table=src_table, **PG_CFG,
        ),
        destination=EndpointConfig(
            kind="database", format="s3", host="localhost", port=0,
            database="fake-bucket", table=key,
            username="", password="",
        ),
        sync_mode=sync_mode,
        validation_mode=validation_mode,
        stream_contracts=[contract],
        skip_preflight=True,
    )


def _csv_rows(fake: FakeS3, key: str) -> list[list]:
    from services.csv_codec import iter_csv_records
    import io
    text = fake.store[key].decode()
    it = iter_csv_records(io.StringIO(text, newline=""), ",")
    next(it)  # header
    return list(it)


@pytest.mark.parametrize("vmode", ["strict", "balanced"])
def test_d115_null_and_empty_and_literal_text_to_s3_csv(fake_s3, vmode):
    sfx = uuid.uuid4().hex[:8]
    src, key = f"d115_s_{sfx}", f"d115/{sfx}.csv"
    _seed(src, [
        (1, "ok"),
        (2, None),                # NULL
        (3, ""),                  # real empty string
        (4, "__DF_SQL_NULL__"),   # literal colliding with the sentinel
    ])
    try:
        result = UniversalTransferEngine().execute_tracked(
            _req(src, key, "full_refresh_overwrite", vmode), uuid.uuid4().hex[:24]
        )
        assert result.success is True, f"D115 fail: {result.error}"
        rows = {int(r[0]): r[1] for r in _csv_rows(fake_s3, key)}
        assert rows[2] is None, f"NULL read back as {rows[2]!r}"
        assert rows[3] == "", f"'' read back as {rows[3]!r}"
        assert rows[4] == "__DF_SQL_NULL__", f"literal read back as {rows[4]!r}"
    finally:
        _drop(src)


def test_d116_append_with_updated_row_reports_ok_and_leaves_correct_file(fake_s3):
    sfx = uuid.uuid4().hex[:8]
    src, key = f"d116_s_{sfx}", f"d116/{sfx}.csv"
    _seed(src, [(i, i, f"n{i}") for i in range(1, 41)], with_seq=True)
    try:
        eng = UniversalTransferEngine()
        r1 = eng.execute_tracked(
            _req(src, key, "incremental_append", "strict", cursor="seq"),
            uuid.uuid4().hex[:24],
        )
        assert r1.success is True, f"first load failed: {r1.error}"

        # Second delta: updated id=1 re-emits (seq bumped past watermark) plus
        # 8 new rows — the production job had a 8-row batch incl. updated id.
        conn = psycopg2.connect(**PG_CONN)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'UPDATE "{src}" SET name=%s, seq=41 WHERE id=1', ("UPD1",))
            cur.executemany(
                f'INSERT INTO "{src}" (id, seq, name) VALUES (%s,%s,%s)',
                [(i, i + 1, f"n{i}") for i in range(41, 49)],
            )
        conn.close()

        r2 = eng.execute_tracked(
            _req(src, key, "incremental_append", "strict", cursor="seq"),
            uuid.uuid4().hex[:24],
        )
        # D116: reported FAILED (read-back vs stale copy) but rows stayed,
        # so a retry would duplicate them.
        assert r2.success is True, f"D116 fail: {r2.error}"
        rows = _csv_rows(fake_s3, key)
        # Append semantics legitimately keep both copies of the updated key:
        # 40 originals + 9-row delta (updated id=1 + 8 inserts) = 49 lines.
        assert len(rows) == 49, f"expected 49 rows, got {len(rows)}"
    finally:
        _drop(src)


# ---------------------------------------------------------------------------
# D113 matrix: NULL in EVERY non-text column type, per engine, per mode.
# Each fails on d1cef840 (pre-fix): mid-wire re-escape turns the wire NULL
# into literal '__DF_ESC____DF_SQL_NULL__' text that typed casts refuse
# ("Invalid decimal", prod jobs 6adfcecd/6adfcecf/6adfced0/6adfced2).
# ---------------------------------------------------------------------------

WIDE_ROWS = [
    (1, 10, 100, 10.5, "2026-01-01", "2026-01-01 01:00:00",
     "2026-01-01 01:00:00+00", True,
     "550e8400-e29b-41d4-a716-446655440000", '{"a":1}', "ok"),
    (2, None, None, None, None, None, None, None, None, None, None),
    (3, 30, 300, 30.0, "2026-03-03", "2026-03-03 03:00:00",
     "2026-03-03 03:00:00+00", False,
     "550e8400-e29b-41d4-a716-446655440001", '{"c":3}', "NULL"),
    # A stored literal colliding with the wire sentinel takes the row set
    # through the richer mapped-bundle path — the route prod hit when a
    # typed NULL still failed the job (D113).
    (4, 40, 400, 40.0, "2026-04-04", "2026-04-04 04:00:00",
     "2026-04-04 04:00:00+00", True,
     "550e8400-e29b-41d4-a716-446655440002", '{"d":4}', "__DF_SQL_NULL__"),
]

WIDE_COLS = (
    "i4 int, i8 bigint, amt numeric(14,4), d date, ts timestamp, "
    "tstz timestamptz, b bool, u uuid, j jsonb, note text"
)


def _seed_wide(name: str) -> None:
    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            f'CREATE TABLE "{name}" (id int PRIMARY KEY, {WIDE_COLS})'
        )
        cur.executemany(
            f'INSERT INTO "{name}" VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            WIDE_ROWS,
        )
    conn.close()


def _req_pg(src: str, dst: str, sync_mode: str, vmode: str) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", schema="public",
            table=src, **PG_CFG,
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql", schema="public",
            table=dst, **PG_CFG,
        ),
        sync_mode=sync_mode,
        validation_mode=vmode,
        stream_contracts=[{
            "name": "t", "sync_mode": sync_mode,
            "primary_key": "id", "selected": True,
        }],
        skip_preflight=True,
    )


@pytest.mark.parametrize("vmode", ["strict", "balanced"])
@pytest.mark.parametrize("sync_mode", ["upsert", "full_refresh_overwrite"])
def test_d113_pg_null_typed_matrix(sync_mode, vmode):
    """NULL in int/bigint/numeric/date/timestamp/timestamptz/bool/uuid/jsonb
    must load as NULL PG->PG (prod jobs 6adfcecd int, 6adfcecf date,
    6adfced0 boolean, 6adfced2 timestamptz all FAILED on d1cef840)."""
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"d113m_s_{sfx}", f"d113m_d_{sfx}"
    _seed_wide(src)
    try:
        r = UniversalTransferEngine().execute_tracked(
            _req_pg(src, dst, sync_mode, vmode), uuid.uuid4().hex[:24]
        )
        assert r.success is True, f"D113({sync_mode}/{vmode}): {r.error}"
        conn = psycopg2.connect(**PG_CONN)
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT i4,i8,amt,d,ts,tstz,b,u,j,note FROM "{dst}" WHERE id=2'
            )
            row = cur.fetchone()
        conn.close()
        assert row is not None, "id=2 missing on dest"
        assert all(v is None for v in row), \
            f"typed NULLs did not land NULL: {row!r}"
        conn = psycopg2.connect(**PG_CONN)
        with conn.cursor() as cur:
            cur.execute(f'SELECT note FROM "{dst}" WHERE id=4')
            literal = cur.fetchone()[0]
        conn.close()
        assert literal == "__DF_SQL_NULL__", \
            f"literal note landed as {literal!r}"
    finally:
        _drop(src, dst)


@pytest.mark.parametrize("vmode", ["strict", "balanced"])
def test_d113_s3_csv_null_typed_matrix(fake_s3, vmode):
    """Same typed NULLs PG->S3 CSV. Pre-fix: job fails on the cast and the
    file is deleted; text NULLs read back as '' (D115)."""
    sfx = uuid.uuid4().hex[:8]
    src, key = f"d113c_s_{sfx}", f"d113c/{sfx}.csv"
    _seed_wide(src)
    try:
        r = UniversalTransferEngine().execute_tracked(
            _req(src, key, "full_refresh_overwrite", vmode), uuid.uuid4().hex[:24]
        )
        assert r.success is True, f"D113+115 s3({vmode}): {r.error}"
        rows = {int(r_[0]): r_[1:] for r_ in _csv_rows(fake_s3, key)}
        # every typed cell on id=2 reads back NULL (None), not ''
        assert all(v is None for v in rows[2]), \
            f"typed NULL cells read back as {rows[2]!r}"
        assert rows[1][0] == "10" and rows[1][9] == "ok"
        assert rows[3][9] == "NULL"
        assert rows[4][9] == "__DF_SQL_NULL__"
    finally:
        _drop(src)


def test_d113_snowflake_shaped_typed_null_bind():
    """Snowflake-carrier binds: a wire NULL binds SQL NULL on every typed
    carrier — never the '__DF_ESC__' literal text that prod job 6adfcfc8
    failed on."""
    from connectors.writer_common import bind_sql_mapped_rows_with_quarantine
    from services.transform_engine import apply_transform
    from services.value_serializer import (
        SQL_NULL_SENTINEL, is_reader_null_cell,
    )
    try:
        from services.value_serializer import WireLiteral  # noqa: F401
    except ImportError:
        pytest.fail("sentinel-layer fix absent (pre-fix tree)")

    cols = ["i4", "i8", "amt", "d", "ts", "tstz", "b", "j"]
    ddl = ["INTEGER", "BIGINT", "DECIMAL(14,4)", "DATE", "TIMESTAMP_NTZ",
           "TIMESTAMP_TZ", "BOOLEAN", "VARIANT"]
    rejected: list = []
    bound = bind_sql_mapped_rows_with_quarantine(
        [tuple([1] + [SQL_NULL_SENTINEL] * len(cols))],
        ["id"] + cols, ["INTEGER"] + ddl, rejected, "fail",
        engine="snowflake", dialect_label="Snowflake",
        mappings=[{"source": c, "target": c} for c in ["id"] + cols],
    )
    assert not rejected and len(bound) == 1
    for col, cell in zip(cols, bound[0][1:]):
        assert is_reader_null_cell(cell), \
            f"{col}: NULL did not bind as NULL ({cell!r})"
        assert "__DF_ESC__" not in str(cell)
    # and the typed cast path itself never sees escaped text for a NULL
    for transform in ("decimal", "integer", "date", "datetime", "boolean"):
        conv, err = apply_transform(SQL_NULL_SENTINEL, transform)
        assert conv is None and err is None, \
            f"{transform}: NULL produced {conv!r}/{err!r}"


def test_d113_sqlite_null_typed_control():
    """Control leg: SQLite was unaffected by D113 (NULLs stayed NULL on
    c6c16128 AND d1cef840) — guards against a future regression. PG->SQLite."""
    import os
    import tempfile

    sfx = uuid.uuid4().hex[:8]
    src = f"d113q_s_{sfx}"
    db = os.path.join(tempfile.gettempdir(), f"d113q_{sfx}.db")
    _seed_wide(src)
    try:
        req = TransferRequest(
            source=EndpointConfig(
                kind="database", format="postgresql", schema="public",
                table=src, **PG_CFG,
            ),
            destination=EndpointConfig(
                kind="database", format="sqlite", database=db, table="dst",
            ),
            sync_mode="full_refresh_overwrite",
            validation_mode="strict",
            stream_contracts=[{
                "name": "t", "sync_mode": "full_refresh_overwrite",
                "primary_key": "id", "selected": True,
            }],
            skip_preflight=True,
        )
        r = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
        assert r.success is True, f"sqlite leg: {r.error}"
        import sqlite3
        row = sqlite3.connect(db).execute(
            "SELECT i4,i8,amt,d,ts,tstz,b,u,j,note FROM dst WHERE id=2"
        ).fetchone()
        assert row is not None and all(v is None for v in row), row
    finally:
        _drop(src)
        if os.path.exists(db):
            os.remove(db)
