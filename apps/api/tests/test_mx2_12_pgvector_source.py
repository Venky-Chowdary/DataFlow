"""QA MX2-12 — a pgvector connector could not be a transfer source.

pgvector is PostgreSQL plus the ``vector`` type. Query mode built
``pgvector://`` for SQLAlchemy ("SQLAlchemy dialect/driver for 'pgvector' is
not available (tried 'pgvector')") and table mode raised "Database source
'pgvector' read not implemented". Reads must ride the PostgreSQL dialect.

Live cases use a pgvector/pgvector:pg16 container on ``localhost:5434``
(``DATAFLOW_TEST_PGVECTOR_PORT``) with dataflow/dataflow credentials.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from src.transfer.models import EndpointConfig

_PORT = int(os.environ.get("DATAFLOW_TEST_PGVECTOR_PORT", "5434"))


def _reachable() -> bool:
    try:
        with socket.create_connection(("localhost", _PORT), timeout=1):
            return True
    except OSError:
        return False


live = pytest.mark.skipif(not _reachable(), reason=f"pgvector not reachable on localhost:{_PORT}")


def _endpoint(**extra) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="pgvector",
        host="localhost",
        port=_PORT,
        database="dataflow",
        username="dataflow",
        password="dataflow",  # nosec B106 - local test container
        schema="public",
        **extra,
    )


@pytest.fixture()
def products_table():
    import psycopg2

    table = f"mx212_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(
        host="localhost", port=_PORT, dbname="dataflow", user="dataflow", password="dataflow"  # nosec B106
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT, embedding vector(3))")
        cur.execute(
            f"INSERT INTO {table} VALUES (1, 'a', '[1,2,3]'), (2, 'b', '[4,5,6]'), (3, 'c', NULL)"
        )
    try:
        yield table
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()


def test_generic_sql_url_for_pgvector_is_the_postgresql_dialect():
    from connectors.generic_sql import _build_url

    url = _build_url({"type": "pgvector", "host": "h", "port": 5432, "database": "d"})
    assert str(url.drivername).startswith("postgresql"), url.drivername


def test_pgvector_is_allowed_as_a_transfer_source():
    from src.transfer.connector_capabilities import endpoint_allowed_for_role, get_capabilities

    ok, reason = endpoint_allowed_for_role("pgvector", "source")
    assert ok, reason
    assert endpoint_allowed_for_role("pgvector", "destination")[0]
    assert get_capabilities("pgvector").get("read") is True


def test_pgvector_streams_as_a_source():
    from src.transfer.stream import supports_streaming

    sqlite = EndpointConfig(kind="database", format="sqlite")
    assert supports_streaming(EndpointConfig(kind="database", format="pgvector"), sqlite)


@live
def test_table_mode_read_returns_rows(products_table):
    from src.transfer.adapters import read_source_database

    records, headers, _schema = read_source_database(_endpoint(table=products_table))
    # Readers emit wire strings; the typed schema rides alongside.
    assert sorted(str(r["id"]) for r in records) == ["1", "2", "3"]
    assert headers == ["id", "name", "embedding"]
    by_id = {str(r["id"]): r for r in records}
    assert str(by_id["1"]["embedding"]).replace(" ", "") == "[1,2,3]"
    from services.value_serializer import SQL_NULL_SENTINEL

    assert by_id["3"]["embedding"] in (None, SQL_NULL_SENTINEL)


@live
def test_query_mode_read_returns_rows(products_table):
    from src.transfer.adapters import read_source_database

    ep = _endpoint(
        extra={"source_read_mode": "query", "source_query": f"SELECT id, name FROM {products_table}"}
    )
    records, headers, _schema = read_source_database(ep)
    assert sorted(str(r["id"]) for r in records) == ["1", "2", "3"]
    assert headers == ["id", "name"]


@live
def test_batch_reader_pages_a_pgvector_source(products_table):
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.batch_readers import _read_batch_impl

    cfg = resolve_connector_config(_endpoint(table=products_table))
    batch = _read_batch_impl("pgvector", cfg, products_table, None, 0, 2)
    assert len(batch.rows) == 2


def test_pgvector_reconciles_as_the_postgresql_family():
    from services.source_reread import engine_family

    assert engine_family("pgvector") == "postgresql"
