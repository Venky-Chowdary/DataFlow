"""The set-aside backup's lifecycle: discard, crash recovery, refusal.

``tests/test_overwrite_all_quarantined_keeps_set_aside_destination.py`` runs
the engine; these pin the edges of :mod:`connectors.overwrite_set_aside` and
of the ClickHouse row copy directly:

- a committed overwrite drops the backup;
- a backup left by a run that died after setting the live object aside, with
  no live object, is the only copy of the pre-run data: it becomes this run's
  backup, so a failure puts it back;
- a backup next to a live object cannot be judged (failed restore or failed
  discard) and the overwrite is refused with both kept;
- a copy that cannot be made refuses the overwrite before anything is removed.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from connectors.overwrite_set_aside import (
    discard_set_aside,
    restore_set_aside,
    retire_set_aside,
)
from connectors.table_manager import TableDropError
from tests.test_overwrite_all_quarantined_keeps_set_aside_destination import (
    PRIOR,
    _aws_client,
    _ChdbEngine,
    _FakeBigQuery,
    _FakeElasticsearch,
    _s3_keys,
    _sf_exec,
    _sf_seed,
    _sf_tables,
)

# ----------------------------------------------------------------------- S3


@pytest.fixture()
def s3(local_object_store):
    if not local_object_store:
        pytest.skip("no local object store endpoint (install moto or set DATAFLOW_TEST_S3_ENDPOINT)")
    from tests.conftest import LOCAL_OBJECT_STORE_BUCKET as bucket

    cfg = {
        "database": bucket, "connection_string": local_object_store,
        "username": "test", "password": "test", "schema": "",
    }
    stem = f"owb/{uuid.uuid4().hex[:8]}"
    return cfg, _aws_client("s3", local_object_store), bucket, stem


def test_s3_committed_overwrite_drops_the_backup(s3):
    cfg, client, bucket, stem = s3
    key = f"{stem}/orders.json"
    client.put_object(Bucket=bucket, Key=key, Body=b"[1]")

    backup = retire_set_aside("s3", cfg, key)
    assert backup == f"{key}.__df_bak/"
    # The writer replaces the live object; the backup holds the previous one.
    client.put_object(Bucket=bucket, Key=key, Body=b"[2]")
    assert client.get_object(Bucket=bucket, Key=backup + key)["Body"].read() == b"[1]"

    discard_set_aside("s3", cfg, key, backup)
    assert _s3_keys(client, bucket, stem) == [key]
    assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == b"[2]"


def test_s3_restore_removes_parts_the_run_wrote(s3):
    cfg, client, bucket, stem = s3
    key = f"{stem}/orders.json"
    client.put_object(Bucket=bucket, Key=key, Body=b"[1]")
    backup = retire_set_aside("s3", cfg, key)
    client.put_object(Bucket=bucket, Key=f"{stem}/orders/part-00001.json", Body=b"[9]")

    restore_set_aside("s3", cfg, key, backup)

    assert _s3_keys(client, bucket, stem) == [key]
    assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == b"[1]"


def test_s3_backup_without_live_object_becomes_this_runs_backup(s3):
    cfg, client, bucket, stem = s3
    key = f"{stem}/orders.json"
    client.put_object(Bucket=bucket, Key=f"{key}.__df_bak/{key}", Body=b"[1]")

    backup = retire_set_aside("s3", cfg, key)
    assert backup == f"{key}.__df_bak/"
    restore_set_aside("s3", cfg, key, backup)

    assert _s3_keys(client, bucket, stem) == [key]
    assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == b"[1]"


def test_s3_backup_next_to_live_object_refuses(s3):
    cfg, client, bucket, stem = s3
    key = f"{stem}/orders.json"
    client.put_object(Bucket=bucket, Key=key, Body=b"[2]")
    client.put_object(Bucket=bucket, Key=f"{key}.__df_bak/{key}", Body=b"[1]")

    with pytest.raises(TableDropError, match="still present"):
        retire_set_aside("s3", cfg, key)

    assert len(_s3_keys(client, bucket, stem)) == 2


def test_s3_absent_export_needs_no_backup(s3):
    cfg, client, bucket, stem = s3
    assert retire_set_aside("s3", cfg, f"{stem}/orders.json") is None


# ---------------------------------------------------------------- Snowflake


@pytest.fixture()
def snowflake(tmp_path, monkeypatch):
    pytest.importorskip("fakesnow")
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    cfg = {
        "host": "localhost", "username": "test", "password": "test",
        "database": "dataflow", "schema": "PUBLIC", "warehouse": "",
    }
    return cfg, "OWB_" + uuid.uuid4().hex[:8].upper()


def test_snowflake_committed_overwrite_drops_the_backup(snowflake):
    cfg, table = snowflake
    _sf_seed(table)

    backup = retire_set_aside("snowflake", cfg, table, "PUBLIC")
    assert backup == f"{table}__DF_BAK"
    assert table not in _sf_tables()
    discard_set_aside("snowflake", cfg, table, backup, "PUBLIC")

    assert backup not in _sf_tables()


def test_snowflake_backup_without_live_table_becomes_this_runs_backup(snowflake):
    cfg, table = snowflake
    _sf_seed(table)
    _sf_exec(f'ALTER TABLE "{table}" RENAME TO "{table}__DF_BAK"')

    backup = retire_set_aside("snowflake", cfg, table, "PUBLIC")
    restore_set_aside("snowflake", cfg, table, backup, "PUBLIC")

    assert sorted(_sf_exec(f'SELECT "ID", "NAME", "QTY" FROM "{table}"')) == sorted(PRIOR)
    assert backup not in _sf_tables()


def test_snowflake_backup_next_to_live_table_refuses(snowflake):
    cfg, table = snowflake
    _sf_seed(table)
    _sf_exec(f'CREATE TABLE "{table}__DF_BAK" ("ID" INTEGER)')

    with pytest.raises(TableDropError, match="still present"):
        retire_set_aside("snowflake", cfg, table, "PUBLIC")

    assert {table, f"{table}__DF_BAK"} <= _sf_tables()


# ------------------------------------------------------------ Elasticsearch


@pytest.fixture()
def es(monkeypatch):
    import connectors.elasticsearch_reader as es_reader

    store: dict[str, dict[str, Any]] = {"orders": {"docs": [1, 2, 3], "settings": {}}}
    client = _FakeElasticsearch(store)
    monkeypatch.setattr(es_reader, "_client", lambda cfg: client)
    return client, store


def test_elasticsearch_committed_overwrite_drops_the_backup(es):
    client, store = es
    backup = retire_set_aside("elasticsearch", {}, "orders")
    # The backup stays read-only while it is the only copy.
    assert store[backup]["settings"] == {"index.blocks.write": True}
    store["orders"] = {"docs": [4, 5], "settings": {}}

    discard_set_aside("elasticsearch", {}, "orders", backup)

    assert store == {"orders": {"docs": [4, 5], "settings": {}}}


def test_elasticsearch_failed_clone_lifts_the_write_block(es, monkeypatch):
    client, store = es

    def _refuse(**_: Any) -> None:
        raise RuntimeError("clone refused")

    monkeypatch.setattr(client.indices, "clone", _refuse)

    with pytest.raises(TableDropError, match="clone refused"):
        retire_set_aside("elasticsearch", {}, "orders")

    assert store == {"orders": {"docs": [1, 2, 3], "settings": {}}}


def test_elasticsearch_backup_next_to_live_index_refuses(es):
    client, store = es
    store["orders__df_bak"] = {"docs": [0], "settings": {"index.blocks.write": True}}

    with pytest.raises(TableDropError, match="still present"):
        retire_set_aside("elasticsearch", {}, "orders")

    assert sorted(store) == ["orders", "orders__df_bak"]


def test_elasticsearch_backup_without_live_index_becomes_this_runs_backup(es):
    client, store = es
    store["orders__df_bak"] = store.pop("orders")
    store["orders__df_bak"]["settings"]["index.blocks.write"] = True

    backup = retire_set_aside("elasticsearch", {}, "orders")
    restore_set_aside("elasticsearch", {}, "orders", backup)

    assert store == {"orders": {"docs": [1, 2, 3], "settings": {}}}


# ----------------------------------------------------------------- BigQuery

_BQ_CFG = {"database": "p", "schema": "d"}


def test_bigquery_copy_failure_refuses_before_deleting(monkeypatch):
    import connectors.bigquery_conn as bq_conn

    tables = {"p.d.orders": list(PRIOR)}
    fake = _FakeBigQuery(tables, copy_jobs=False)

    def _no_ctas(sql: str, **_: Any) -> None:
        raise RuntimeError("CTAS refused")

    monkeypatch.setattr(fake, "query", _no_ctas)
    monkeypatch.setattr(bq_conn, "get_client", lambda **_: fake)

    with pytest.raises(TableDropError, match="CTAS refused"):
        retire_set_aside("bigquery", _BQ_CFG, "orders", "d")

    assert tables == {"p.d.orders": list(PRIOR)}


def test_bigquery_copy_job_that_finished_despite_an_error_is_kept(monkeypatch):
    """A copy job is atomic: an error after it finished must not trigger a second copy."""
    import connectors.bigquery_conn as bq_conn

    tables = {"p.d.orders": list(PRIOR)}
    fake = _FakeBigQuery(tables)
    copy = fake.copy_table

    def _copy_then_fail(*args: Any, **kwargs: Any) -> None:
        copy(*args, **kwargs)
        raise RuntimeError("connection reset while polling the job")

    def _no_ctas(sql: str, **_: Any) -> None:
        raise AssertionError("CTAS must not run after a finished copy")

    monkeypatch.setattr(fake, "copy_table", _copy_then_fail)
    monkeypatch.setattr(fake, "query", _no_ctas)
    monkeypatch.setattr(bq_conn, "get_client", lambda **_: fake)

    backup = retire_set_aside("bigquery", _BQ_CFG, "orders", "d")

    assert tables == {f"p.d.{backup}": list(PRIOR)}


def test_bigquery_backup_next_to_live_table_refuses(monkeypatch):
    import connectors.bigquery_conn as bq_conn

    tables = {"p.d.orders": list(PRIOR), "p.d.orders__df_bak": []}
    monkeypatch.setattr(bq_conn, "get_client", lambda **_: _FakeBigQuery(tables))

    with pytest.raises(TableDropError, match="still present"):
        retire_set_aside("bigquery", _BQ_CFG, "orders", "d")

    assert sorted(tables) == ["p.d.orders", "p.d.orders__df_bak"]


def test_bigquery_committed_overwrite_drops_the_backup(monkeypatch):
    import connectors.bigquery_conn as bq_conn

    tables = {"p.d.orders": list(PRIOR)}
    monkeypatch.setattr(bq_conn, "get_client", lambda **_: _FakeBigQuery(tables))

    backup = retire_set_aside("bigquery", _BQ_CFG, "orders", "d")
    tables["p.d.orders"] = [(1, "new", 10)]
    discard_set_aside("bigquery", _BQ_CFG, "orders", backup, "d")

    assert tables == {"p.d.orders": [(1, "new", 10)]}


# --------------------------------------------------------------- ClickHouse


def test_clickhouse_committed_overwrite_drops_the_row_copy(clickhouse):
    from connectors.table_manager import (
        discard_generic_sql_overwrite,
        retire_generic_sql_overwrite,
    )

    cfg = {"type": "clickhouse", "schema": "owq"}
    state, backup = retire_generic_sql_overwrite("generic_sql", cfg, "orders", "owq")
    assert state == "emptied" and backup
    assert str(clickhouse.query("SELECT count() FROM owq.orders", "CSV")).strip() == "0"

    discard_generic_sql_overwrite("generic_sql", cfg, backup, "owq")

    assert str(clickhouse.query("SHOW TABLES FROM owq", "CSV")).split() == ['"orders"']


def test_clickhouse_absent_table_needs_no_copy(clickhouse):
    from connectors.table_manager import retire_generic_sql_overwrite

    clickhouse.query("DROP TABLE owq.orders")
    cfg = {"type": "clickhouse", "schema": "owq"}
    assert retire_generic_sql_overwrite("generic_sql", cfg, "orders", "owq") == ("absent", None)


@pytest.fixture()
def clickhouse(monkeypatch):
    """Reuse the engine test's chdb database without its writer patch."""
    from tests import test_overwrite_all_quarantined_keeps_set_aside_destination as mod

    chdb_session = pytest.importorskip("chdb.session")
    import connectors.generic_sql as generic_sql

    if mod._CHDB_SESSION is None:
        mod._CHDB_SESSION = chdb_session.Session()
    session = mod._CHDB_SESSION
    session.query("DROP DATABASE IF EXISTS owq")
    session.query("CREATE DATABASE owq")
    session.query(
        "CREATE TABLE owq.orders (id Int64, name String, qty Int64) "
        "ENGINE = MergeTree ORDER BY id"
    )
    session.query("INSERT INTO owq.orders VALUES (1, 'a', 1)")
    monkeypatch.setattr(generic_sql, "get_sqlalchemy_engine", lambda cfg: _ChdbEngine(session))
    return session
