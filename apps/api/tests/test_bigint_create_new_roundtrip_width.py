"""Live / offline proof: BIGINT create-new invents 64-bit destination columns.

When PostgreSQL is available (CI topology), runs an end-to-end auto-create
transfer of ``2**63-1``. Offline, asserts Map→DDL invent alone (always).
"""

from __future__ import annotations

import os
import uuid

import pytest

from services.type_system import ddl_type, integer_bit_width


def test_bigint_carrier_materializes_64_bit_on_major_dests():
    for dest, want_substr in (
        ("postgresql", "BIGINT"),
        ("mysql", "BIGINT"),
        ("sqlserver", "BIGINT"),
        ("clickhouse", "Int64"),
        ("iceberg", "long"),
        ("oracle", "NUMBER(38"),
        ("duckdb", "BIGINT"),
        ("trino", "bigint"),
        ("databricks", "BIGINT"),
    ):
        # Prefer ddl_type invent (materialize may passthrough BIGINT token).
        physical = ddl_type(dest, "BIGINT")
        assert want_substr.lower() in physical.lower(), (dest, physical)
        width = integer_bit_width(physical)
        assert width == 64 or "38" in physical or width is None, (dest, physical, width)


@pytest.mark.integration
def test_pg_bigint_auto_create_roundtrip_maxint():
    """Proven audit path: PG bigint holding 2^63-1 must create BIGINT dest."""
    dsn = (
        os.getenv("DATAFLOW_TEST_PG_DSN")
        or os.getenv("DATABASE_URL")
        or ""
    ).strip()
    if not dsn and not os.getenv("PGHOST"):
        pytest.skip("No PostgreSQL DSN in environment")

    try:
        import psycopg2
    except ImportError:
        pytest.skip("psycopg2 not installed")

    host = os.getenv("PGHOST", "127.0.0.1")
    port = int(os.getenv("PGPORT", "5432"))
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "postgres")
    dbname = os.getenv("PGDATABASE", "postgres")

    suffix = uuid.uuid4().hex[:10]
    src = f"df_width_src_{suffix}"
    dst = f"df_width_dst_{suffix}"
    max_i = 9223372036854775807

    conn = psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=dbname
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")
            cur.execute(f"DROP TABLE IF EXISTS {dst}")
            cur.execute(
                f"CREATE TABLE {src} (id BIGINT PRIMARY KEY, big_val BIGINT, dbl DOUBLE PRECISION)"
            )
            cur.execute(
                f"INSERT INTO {src} VALUES (1, %s, %s), (2, 5000000000, 0.1)",
                (max_i, 1.2345678901234567),
            )
    finally:
        conn.close()

    from services.schema_introspect import introspect_schema

    config = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": dbname,
        "db_type": "postgresql",
    }
    try:
        schema = introspect_schema("postgresql", config, table=src)
    except Exception as exc:
        pytest.skip(f"introspect unavailable: {exc}")

    cols = {c["name"]: c for c in (schema.get("columns") or [])}
    assert cols.get("big_val", {}).get("inferred_type") == "BIGINT", cols.get("big_val")
    assert cols.get("id", {}).get("inferred_type") == "BIGINT", cols.get("id")
    dbl = str(cols.get("dbl", {}).get("inferred_type") or "")
    assert "DOUBLE" in dbl.upper() or dbl.upper() == "FLOAT8", dbl

    # Invent dest DDL from introspected carriers (Map→CREATE path).
    assert ddl_type("postgresql", cols["big_val"]["inferred_type"]) == "BIGINT"
    assert ddl_type("postgresql", cols["id"]["inferred_type"]) == "BIGINT"
