"""Phase F3 — bulk export capability + PostgreSQL COPY reader."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_bulk_capability_matrix():
    from connectors.bulk_export import (
        bulk_export_implemented,
        bulk_export_supported,
        read_bigquery_storage_batch,
        read_snowflake_unload_batch,
    )

    assert bulk_export_supported("postgresql")
    assert bulk_export_supported("snowflake")
    assert bulk_export_supported("bigquery")
    assert bulk_export_implemented("postgresql")
    assert not bulk_export_implemented("snowflake")
    assert not bulk_export_implemented("bigquery")
    with pytest.raises(NotImplementedError, match="Snowflake"):
        read_snowflake_unload_batch()
    with pytest.raises(NotImplementedError, match="BigQuery"):
        read_bigquery_storage_batch()


def test_bulk_export_default_off(monkeypatch):
    monkeypatch.delenv("DATAFLOW_BULK_EXPORT", raising=False)
    monkeypatch.delenv("DATAWRAP_BULK_EXPORT", raising=False)
    from connectors.bulk_export import bulk_export_enabled

    assert bulk_export_enabled() is False


def test_postgresql_copy_batches_roundtrip():
    psycopg2 = pytest.importorskip("psycopg2")
    import os

    host = os.environ.get("PGHOST") or os.environ.get("DATAFLOW_PG_HOST")
    if not host:
        pytest.skip("PostgreSQL not configured (PGHOST)")

    from connectors.bulk_export import iter_postgresql_copy_batches
    from connectors.postgresql_conn import get_connection

    port = int(os.environ.get("PGPORT", "5432"))
    database = os.environ.get("PGDATABASE", "dataflow")
    username = os.environ.get("PGUSER", "dataflow")
    password = os.environ.get("PGPASSWORD", "dataflow")

    conn = get_connection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string="",
        ssl=False,
    )
    table = "df_bulk_export_f3"
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS public.{table}")
            cur.execute(
                f"CREATE TABLE public.{table} (id BIGINT PRIMARY KEY, label TEXT)"
            )
            cur.executemany(
                f"INSERT INTO public.{table} VALUES (%s, %s)",
                [(i, f"r{i}") for i in range(1, 26)],
            )
            conn.commit()
    finally:
        conn.close()

    pages = list(
        iter_postgresql_copy_batches(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema="public",
            connection_string="",
            ssl=False,
            table=table,
            columns=["id", "label"],
            batch_rows=10,
        )
    )
    assert len(pages) >= 3
    rows = [r for p in pages for r in p.rows]
    assert len(rows) == 25
    assert rows[0][0] == "1"
    assert rows[-1][1] == "r25"
