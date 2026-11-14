"""An overwrite that quarantines every row fails and keeps the previous object.

Owner rule: no path may destroy a destination when nothing valid would
replace it. Snowflake, BigQuery and Elasticsearch overwrites dropped the live
table or index before any row was judged, an S3 overwrite replaced the live
object with a header-only export, and a ClickHouse overwrite emptied the live
table without a copy (or could not overwrite an existing table at all). The
run then failed with ``all_rows_quarantined`` and the previous data was gone.

Each case seeds a destination with three rows, runs a full-refresh overwrite
whose every source row fails an integer transform, and checks that the run
fails, that the previous rows are still under the live name, and that no
backup is left behind. A second case proves a good overwrite still replaces
the rows and drops the backup.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path
from typing import Any

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

PRIOR = [(101, "kept-a", 1), (102, "kept-b", 2), (103, "kept-c", 3)]


@pytest.fixture()
def fake_mongo(monkeypatch, tmp_path):
    from tests.test_currency_to_sqlite import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _source(tmp_path: Path, *, valid: bool) -> str:
    """Five rows. ``valid=False`` makes every ``qty`` fail the integer transform."""
    path = tmp_path / f"src_{uuid.uuid4().hex[:6]}.db"
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, qty TEXT)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?, ?)",
            [(i, f"new {i}", str(i * 10) if valid else f"oops{i}") for i in range(1, 6)],
        )
    conn.close()
    return str(path)


_MAPPINGS = [
    {"source": "id", "target": "id", "target_type": "INTEGER"},
    {"source": "name", "target": "name", "target_type": "TEXT"},
    {"source": "qty", "target": "qty", "target_type": "INTEGER", "transform": "integer"},
]


def _run(fake_mongo, tmp_path: Path, destination: EndpointConfig, *, valid: bool):
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=_source(tmp_path, valid=valid), table="t"
        ),
        destination=destination,
        mappings=[dict(m) for m in _MAPPINGS],
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="balanced",
    )
    job_id = "owq" + uuid.uuid4().hex[:16]
    fake_mongo.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _assert_quarantine_failure(result: Any) -> None:
    assert result.success is False
    error = str(result.error or "")
    assert "were quarantined, so nothing was written" in error, error


# ---------------------------------------------------------------- Snowflake


@pytest.fixture()
def snowflake_env(tmp_path, monkeypatch):
    pytest.importorskip("fakesnow")
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))


def _sf_conn():
    from connectors.snowflake_conn import get_connection

    return get_connection(
        account="localhost", username="test", password="test",
        database="dataflow", schema="PUBLIC", warehouse="", connection_string="",
    )


def _sf_exec(sql: str, params: Any = None) -> list[tuple]:
    conn = _sf_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params) if params is not None else cur.execute(sql)
            if cur.description is None:  # DDL / DML: no result set
                return []
            return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _sf_tables() -> set[str]:
    return {
        str(r[0]).upper()
        for r in _sf_exec(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'PUBLIC'"
        )
    }


def _sf_seed(table: str) -> None:
    _sf_exec(f'CREATE TABLE "{table}" ("ID" INTEGER, "NAME" VARCHAR, "QTY" INTEGER)')
    for row in PRIOR:
        _sf_exec(f'INSERT INTO "{table}" VALUES (%s, %s, %s)', row)


def _sf_rows(table: str) -> list[tuple]:
    return sorted(_sf_exec(f'SELECT "ID", "NAME", "QTY" FROM "{table}"'))


def _sf_destination(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="snowflake", host="localhost", port=443,
        database="dataflow", username="test", password="test",
        schema="public", table=table,
    )


def test_snowflake_all_quarantined_overwrite_keeps_table(snowflake_env, fake_mongo, tmp_path):
    table = "OWQ_" + uuid.uuid4().hex[:8].upper()
    _sf_seed(table)

    result = _run(fake_mongo, tmp_path, _sf_destination(table), valid=False)

    _assert_quarantine_failure(result)
    assert _sf_rows(table) == sorted(PRIOR)
    assert f"{table}__DF_BAK" not in _sf_tables()


def test_snowflake_good_overwrite_replaces_and_drops_backup(snowflake_env, fake_mongo, tmp_path):
    table = "OWQ_" + uuid.uuid4().hex[:8].upper()
    _sf_seed(table)

    result = _run(fake_mongo, tmp_path, _sf_destination(table), valid=True)

    assert result.success is True, result.error
    tables = _sf_tables()
    assert f"{table}__DF_BAK" not in tables
    live = next(t for t in tables if t == table)
    assert [r[0] for r in _sf_rows(live)] == [1, 2, 3, 4, 5]


# ----------------------------------------------------------------------- S3


def _aws_client(service: str, endpoint: str) -> Any:
    import boto3

    return boto3.client(
        service, endpoint_url=endpoint, aws_access_key_id="test",
        aws_secret_access_key="test", region_name="us-east-1",
    )


def _aws_destination(fmt: str, endpoint: str, *, database: str, table: str) -> EndpointConfig:
    from urllib.parse import urlparse

    parsed = urlparse(endpoint)
    return EndpointConfig(
        kind="database", format=fmt, host=parsed.hostname or "127.0.0.1",
        port=parsed.port or 443, database=database, username="test",
        password="test", connection_string=endpoint, table=table,
    )


def _s3_keys(client: Any, bucket: str, prefix: str) -> list[str]:
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    return sorted(item["Key"] for page in pages for item in page.get("Contents") or [])


_S3_PRIOR = b'[{"id": 101, "name": "kept-a", "qty": 1}, {"id": 102, "name": "kept-b", "qty": 2}]'


@pytest.mark.parametrize("valid", [False, True], ids=["all-quarantined", "good"])
def test_s3_overwrite_keeps_object_unless_replaced(local_object_store, fake_mongo, tmp_path, valid):
    if not local_object_store:
        pytest.skip("no local object store endpoint (install moto or set DATAFLOW_TEST_S3_ENDPOINT)")
    from tests.conftest import LOCAL_OBJECT_STORE_BUCKET as bucket

    stem = f"owq/{uuid.uuid4().hex[:8]}"
    key = f"{stem}/orders.json"
    client = _aws_client("s3", local_object_store)
    client.put_object(Bucket=bucket, Key=key, Body=_S3_PRIOR)

    result = _run(
        fake_mongo, tmp_path,
        _aws_destination("s3", local_object_store, database=bucket, table=key),
        valid=valid,
    )

    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    if valid:
        assert result.success is True, result.error
        assert body != _S3_PRIOR and b"new 5" in body
    else:
        _assert_quarantine_failure(result)
        assert body == _S3_PRIOR
    # Neither outcome leaves the backup copy behind.
    assert _s3_keys(client, bucket, stem) == [key]


# ----------------------------------------------------------------- DynamoDB


def test_dynamodb_all_quarantined_overwrite_keeps_items(local_object_store, fake_mongo, tmp_path):
    """DynamoDB never clears on overwrite (each item is a PutItem): nothing to lose."""
    if not local_object_store:
        pytest.skip("no local AWS endpoint (install moto or set DATAFLOW_TEST_S3_ENDPOINT)")
    table = "owq_" + uuid.uuid4().hex[:8]
    client = _aws_client("dynamodb", local_object_store)
    client.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for rid, name, qty in PRIOR:
        client.put_item(
            TableName=table,
            Item={"id": {"N": str(rid)}, "name": {"S": name}, "qty": {"N": str(qty)}},
        )

    result = _run(
        fake_mongo, tmp_path,
        _aws_destination("dynamodb", local_object_store, database=table, table=table),
        valid=False,
    )

    _assert_quarantine_failure(result)
    items = client.scan(TableName=table)["Items"]
    assert sorted(
        (int(i["id"]["N"]), i["name"]["S"], int(i["qty"]["N"])) for i in items
    ) == sorted(PRIOR)


def test_s3_all_quarantined_overwrite_keeps_a_multi_part_export(local_object_store, fake_mongo, tmp_path):
    """A part-file export survives too: the overwrite's stale-part purge used to remove it."""
    if not local_object_store:
        pytest.skip("no local object store endpoint (install moto or set DATAFLOW_TEST_S3_ENDPOINT)")
    from tests.conftest import LOCAL_OBJECT_STORE_BUCKET as bucket

    stem = f"owq/{uuid.uuid4().hex[:8]}"
    key = f"{stem}/orders.json"
    parts = {
        f"{stem}/orders/part-00001.json": b'[{"id": 101, "name": "kept-a", "qty": 1}]',
        f"{stem}/orders/part-00002.json": b'[{"id": 102, "name": "kept-b", "qty": 2}]',
    }
    client = _aws_client("s3", local_object_store)
    for part_key, body in parts.items():
        client.put_object(Bucket=bucket, Key=part_key, Body=body)

    result = _run(
        fake_mongo, tmp_path,
        _aws_destination("s3", local_object_store, database=bucket, table=key),
        valid=False,
    )

    _assert_quarantine_failure(result)
    assert _s3_keys(client, bucket, stem) == sorted(parts)
    for part_key, body in parts.items():
        assert client.get_object(Bucket=bucket, Key=part_key)["Body"].read() == body


# ------------------------------------------------- emulated destinations
#
# No Elasticsearch, ClickHouse or BigQuery server runs here. These cases run
# the whole engine: the real clear, the real stream and the real failure path.
# The destination store is emulated, and the engine's writer is replaced by
# one that quarantines every row, as the real writer does for these values.


def _quarantining_writer(on_write: Any = None) -> Any:
    from connectors.writer_common import WriteResult

    def write_mapped_rows(**kwargs: Any) -> WriteResult:
        rows = list(kwargs.get("data_rows") or kwargs.get("records") or [])
        if on_write is not None:
            on_write(kwargs)
        return WriteResult(
            ok=True,
            rows_written=0,
            table_name=str(kwargs.get("table_name") or ""),
            target_schema=str(kwargs.get("schema") or ""),
            checksum="",
            chunks_completed=1,
            rejected_rows=len(rows),
            rejected_details=[
                {"row": i + 1, "column": "qty", "reason": "Invalid integer"}
                for i in range(len(rows))
            ],
        )

    return write_mapped_rows


class _FakeIndices:
    def __init__(self, store: dict[str, dict[str, Any]]) -> None:
        self.store = store

    def exists(self, index: str) -> bool:
        return index in self.store

    def put_settings(self, index: str, settings: dict[str, Any]) -> dict:
        idx = self.store[index]
        for name, value in settings.items():
            if value is None:
                idx["settings"].pop(name, None)
            else:
                idx["settings"][name] = value
        return {"acknowledged": True}

    def clone(self, index: str, target: str, **_: Any) -> dict:
        # Elasticsearch's own preconditions for _clone.
        if self.store[index]["settings"].get("index.blocks.write") is not True:
            raise RuntimeError(f"index {index} must be read-only to clone it")
        if target in self.store:
            raise RuntimeError(f"index {target} already exists")
        src = self.store[index]
        self.store[target] = {"docs": list(src["docs"]), "settings": dict(src["settings"])}
        return {"acknowledged": True}

    def delete(self, index: str, ignore_unavailable: bool = False) -> dict:
        if index not in self.store and not ignore_unavailable:
            raise RuntimeError(f"no such index [{index}]")
        self.store.pop(index, None)
        return {"acknowledged": True}

    def refresh(self, index: str = "", **_: Any) -> dict:
        return {}


class _FakeElasticsearch:
    def __init__(self, store: dict[str, dict[str, Any]]) -> None:
        self.indices = _FakeIndices(store)
        self.cluster = self
        self.store = store

    def health(self, index: str = "", **_: Any) -> dict:
        return {"status": "green" if index in self.store else "red"}

    def count(self, index: str, **_: Any) -> dict:
        return {"count": len(self.store[index]["docs"])}

    def close(self) -> None:
        pass


def test_elasticsearch_all_quarantined_overwrite_keeps_index(fake_mongo, tmp_path, monkeypatch):
    import connectors.elasticsearch_reader as es_reader
    import connectors.elasticsearch_writer as es_writer

    index = "owq_" + uuid.uuid4().hex[:8]
    prior_docs = [{"id": r[0], "name": r[1], "qty": r[2]} for r in PRIOR]
    store: dict[str, dict[str, Any]] = {index: {"docs": list(prior_docs), "settings": {}}}
    monkeypatch.setattr(es_reader, "_client", lambda cfg: _FakeElasticsearch(store))

    def _recreate(kwargs: dict) -> None:
        # The real writer creates the index it loads into.
        store.setdefault(str(kwargs["table_name"]), {"docs": [], "settings": {}})

    monkeypatch.setattr(es_writer, "write_mapped_rows", _quarantining_writer(_recreate))

    result = _run(
        fake_mongo, tmp_path,
        EndpointConfig(kind="database", format="elasticsearch", host="127.0.0.1",
                       port=9200, table=index),
        valid=False,
    )

    _assert_quarantine_failure(result)
    assert sorted(store) == [index]
    assert store[index]["docs"] == prior_docs
    # The restored index takes writes again.
    assert "index.blocks.write" not in store[index]["settings"]


class _ChdbResult:
    def __init__(self, text: str) -> None:
        import csv
        import io

        rows = list(csv.reader(io.StringIO(text))) if text.strip() else []
        self._keys = rows[0] if rows else []
        self._rows = rows[1:]

    def keys(self) -> list[str]:
        return list(self._keys)

    def scalar(self) -> Any:
        return self._rows[0][0] if self._rows and self._rows[0] else None

    def fetchall(self) -> list[list[str]]:
        return list(self._rows)


class _ChdbConnection:
    """The SQLAlchemy connection surface the overwrite clear uses, on chdb."""

    def __init__(self, session: Any) -> None:
        self.session = session

    def execute(self, statement: Any, *_: Any) -> _ChdbResult:
        return _ChdbResult(str(self.session.query(str(statement), "CSVWithNames")))

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def __enter__(self) -> "_ChdbConnection":
        return self

    def __exit__(self, *_: Any) -> None:
        pass


class _ChdbEngine:
    class dialect:  # noqa: N801 - mirrors ``Engine.dialect.name``
        name = "clickhouse"

    def __init__(self, session: Any) -> None:
        self.session = session

    def connect(self) -> _ChdbConnection:
        return _ChdbConnection(self.session)

    def dispose(self) -> None:
        pass


_CHDB_SESSION: Any = None


@pytest.fixture(params=["MergeTree", "None"], ids=["default-engine", "no-default-engine"])
def clickhouse(request, monkeypatch):
    """A chdb (embedded ClickHouse) database ``owq`` holding ``orders`` with PRIOR.

    ``no-default-engine`` is a server where ``CREATE TABLE AS SELECT`` needs an
    explicit ENGINE (the default before ``default_table_engine``).
    """
    chdb_session = pytest.importorskip("chdb.session")
    import connectors.generic_sql as generic_sql

    # chdb is one embedded server per process: share one session, reset the database.
    global _CHDB_SESSION
    if _CHDB_SESSION is None:
        _CHDB_SESSION = chdb_session.Session()
    session = _CHDB_SESSION
    session.query(f"SET default_table_engine = '{request.param}'")
    session.query("DROP DATABASE IF EXISTS owq")
    session.query("CREATE DATABASE owq")
    session.query(
        "CREATE TABLE owq.orders (id Int64, name String, qty Int64) "
        "ENGINE = MergeTree ORDER BY id"
    )
    session.query("INSERT INTO owq.orders VALUES " + ", ".join(
        f"({i}, '{n}', {q})" for i, n, q in PRIOR
    ))
    monkeypatch.setattr(generic_sql, "get_sqlalchemy_engine", lambda cfg: _ChdbEngine(session))
    monkeypatch.setattr(generic_sql, "write_mapped_rows", _quarantining_writer())
    yield session
    session.query("SET default_table_engine = 'MergeTree'")


def _assert_clickhouse_prior(session: Any) -> None:
    rows = str(session.query("SELECT id, name, qty FROM owq.orders ORDER BY id", "CSV"))
    assert rows.split() == [f'{i},"{n}",{q}' for i, n, q in PRIOR]
    assert str(session.query("SHOW TABLES FROM owq", "CSV")).split() == ['"orders"']
    # The live table is the operator's object, not a copy: its sorting key stays.
    assert "ORDER BY id" in str(session.query("SHOW CREATE TABLE owq.orders", "CSV"))


def test_clickhouse_generic_sql_all_quarantined_overwrite_keeps_rows(clickhouse, fake_mongo, tmp_path):
    """ClickHouse through the generic SQL driver: the row copy, on real ClickHouse SQL.

    Before, ``CREATE TABLE AS SELECT`` failed without a default ENGINE and the
    overwrite emptied the table with no copy; with one, the copy was made but
    ``DELETE FROM t`` without WHERE is refused, so no overwrite could run.
    """
    result = _run(
        fake_mongo, tmp_path,
        EndpointConfig(kind="database", format="generic_sql",
                       connection_string="clickhouse+http://default:@127.0.0.1:8123/owq",
                       database="owq", schema="owq", table="orders"),
        valid=False,
    )

    _assert_quarantine_failure(result)
    _assert_clickhouse_prior(clickhouse)


def test_clickhouse_connector_overwrite_is_refused_before_the_clear(clickhouse, fake_mongo, tmp_path):
    """The ClickHouse catalog connector is Planned: the run is refused with the table untouched."""
    result = _run(
        fake_mongo, tmp_path,
        EndpointConfig(kind="database", format="clickhouse", host="127.0.0.1", port=8123,
                       database="owq", schema="owq", table="orders"),
        valid=False,
    )

    assert result.success is False
    assert "not available for production transfer" in str(result.error or ""), result.error
    _assert_clickhouse_prior(clickhouse)


# ----------------------------------------------------------------- BigQuery


class _NotFound(Exception):
    pass


_NotFound.__name__ = "NotFound"


class _FakeBigQuery:
    """Tables as row lists. ``copy_jobs=False`` is a server without copy jobs (CTAS only)."""

    def __init__(self, tables: dict[str, list[tuple]], *, copy_jobs: bool = True) -> None:
        self.tables = tables
        self.copy_jobs = copy_jobs

    def get_table(self, table_id: str, **_: Any) -> Any:
        if table_id not in self.tables:
            raise _NotFound(f"Not found: Table {table_id}")
        return table_id

    def delete_table(self, table_id: str, not_found_ok: bool = False, **_: Any) -> None:
        if table_id not in self.tables and not not_found_ok:
            raise _NotFound(table_id)
        self.tables.pop(table_id, None)

    def copy_table(self, src: str, dst: str, job_config: Any = None, **_: Any) -> Any:
        if not self.copy_jobs:
            raise RuntimeError("copy jobs are not supported")
        assert getattr(job_config, "write_disposition", "") == "WRITE_EMPTY"
        if dst in self.tables:
            raise RuntimeError(f"Already Exists: {dst}")
        self.tables[dst] = list(self.tables[src])
        return self

    def query(self, sql: str, **_: Any) -> Any:
        import re

        match = re.fullmatch(r"CREATE TABLE `([^`]+)` AS SELECT \* FROM `([^`]+)`", sql)
        assert match, sql
        dst, src = match.groups()
        if dst in self.tables:
            raise RuntimeError(f"Already Exists: {dst}")
        self.tables[dst] = list(self.tables[src])
        return self

    def result(self, **_: Any) -> list:
        return []

    def close(self) -> None:
        pass


@pytest.mark.parametrize("copy_jobs", [True, False], ids=["copy-job", "ctas"])
def test_bigquery_all_quarantined_overwrite_keeps_table(fake_mongo, tmp_path, monkeypatch, copy_jobs):
    import connectors.bigquery_conn as bq_conn
    import connectors.bigquery_writer as bq_writer

    table = "owq_" + uuid.uuid4().hex[:8]
    live_id = f"dataflow-test.dataflow.{table}"
    tables: dict[str, list[tuple]] = {live_id: list(PRIOR)}
    monkeypatch.setattr(
        bq_conn, "get_client", lambda **_: _FakeBigQuery(tables, copy_jobs=copy_jobs)
    )

    def _recreate(kwargs: dict) -> None:
        # The real writer creates the table it streams into.
        tables.setdefault(f"dataflow-test.dataflow.{kwargs['table_name']}", [])

    monkeypatch.setattr(bq_writer, "write_mapped_rows", _quarantining_writer(_recreate))

    result = _run(
        fake_mongo, tmp_path,
        EndpointConfig(kind="database", format="bigquery", database="dataflow-test",
                       schema="dataflow", table=table),
        valid=False,
    )

    _assert_quarantine_failure(result)
    assert sorted(tables) == [live_id]
    assert tables[live_id] == list(PRIOR)


def test_bigquery_emulator_all_quarantined_overwrite_keeps_table(fake_mongo, tmp_path):
    """The same run against goccy/bigquery-emulator (CI starts it on 9050)."""
    import socket

    try:
        with socket.create_connection(("127.0.0.1", 9050), timeout=1):
            pass
    except OSError:
        pytest.skip("BigQuery emulator 9050 not reachable")
    from connectors.bigquery_conn import get_client
    from services.dest_precount import _bigquery_run_job

    table = "owq_" + uuid.uuid4().hex[:8]
    ref = f"`dataflow-test`.`dataflow`.`{table}`"
    client = get_client(
        project_id="dataflow-test", host="127.0.0.1", port=9050,
        connection_string="http://127.0.0.1:9050",
    )
    try:
        _bigquery_run_job(client, f"CREATE TABLE {ref} (id INT64, name STRING, qty INT64)")
        _bigquery_run_job(client, f"INSERT INTO {ref} VALUES " + ", ".join(
            f"({i}, '{n}', {q})" for i, n, q in PRIOR
        ))

        result = _run(
            fake_mongo, tmp_path,
            EndpointConfig(kind="database", format="bigquery", host="127.0.0.1", port=9050,
                           database="dataflow-test", schema="dataflow", table=table,
                           connection_string="http://127.0.0.1:9050"),
            valid=False,
        )

        _assert_quarantine_failure(result)
        rows = list(_bigquery_run_job(client, f"SELECT id, name, qty FROM {ref} ORDER BY id"))
        assert [tuple(r.values()) for r in rows] == list(PRIOR)
        names = {t.table_id for t in client.list_tables("dataflow-test.dataflow")}
        assert f"{table}__df_bak" not in names
    finally:
        try:
            client.delete_table(f"dataflow-test.dataflow.{table}", not_found_ok=True)
            client.delete_table(f"dataflow-test.dataflow.{table}__df_bak", not_found_ok=True)
        finally:
            client.close()


# -------------------------------------------------------------- multi-stream


def _multi_stream_source(tmp_path: Path, *, orders_valid: bool) -> str:
    path = tmp_path / f"ms_{uuid.uuid4().hex[:6]}.db"
    conn = sqlite3.connect(str(path))
    with conn:
        for name in ("people", "orders"):
            valid = orders_valid or name == "people"
            conn.execute(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, name TEXT, qty TEXT)")  # noqa: S608
            conn.executemany(
                f"INSERT INTO {name} VALUES (?, ?, ?)",  # noqa: S608
                [(i, f"{name} {i}", str(i) if valid else f"oops{i}") for i in range(1, 4)],
            )
    conn.close()
    return str(path)


def _multi_stream_run(fake_mongo, tmp_path: Path, *, orders_valid: bool):
    """Streams ``people`` and ``orders`` into Snowflake tables PEOPLE and ORDERS."""
    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", table="people",
            database=_multi_stream_source(tmp_path, orders_valid=orders_valid),
        ),
        destination=_sf_destination(""),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[
            {"name": n, "selected": True, "mappings": [dict(m) for m in _MAPPINGS]}
            for n in ("people", "orders")
        ],
        skip_preflight=True,
        validation_mode="balanced",
    )
    job_id = "owqm" + uuid.uuid4().hex[:16]
    fake_mongo.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def test_multi_stream_overwrite_settles_each_streams_backup(snowflake_env, fake_mongo, tmp_path):
    """Every stream's backup is settled: a rerun is not refused, a held-out stream keeps its table."""
    first = _multi_stream_run(fake_mongo, tmp_path, orders_valid=True)
    assert first.success is True, first.error
    assert {"PEOPLE", "ORDERS"} <= _sf_tables()

    # Before, the first stream's backup was never dropped, so this rerun's
    # PEOPLE backup outlived it and the next overwrite refused.
    second = _multi_stream_run(fake_mongo, tmp_path, orders_valid=True)
    assert second.success is True, second.error
    assert not [t for t in _sf_tables() if t.endswith("__DF_BAK")]

    third = _multi_stream_run(fake_mongo, tmp_path, orders_valid=False)

    _assert_quarantine_failure(third)
    assert sorted(_sf_exec('SELECT "ID", "NAME", "QTY" FROM "ORDERS"')) == [
        (i, f"orders {i}", i) for i in range(1, 4)
    ]
    assert not [t for t in _sf_tables() if t.endswith("__DF_BAK")]
