"""Live source duplicate-key probe for MySQL and PostgreSQL.

Runs against the docker-compose services on localhost; skips cleanly when the
ports are not reachable.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest


@pytest.fixture
def unique_table() -> str:
    return f"dupes_{uuid.uuid4().hex[:8]}"


def _seed_mysql(table: str) -> None:
    pymysql = pytest.importorskip("pymysql")
    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(f"CREATE TABLE {table} (id VARCHAR(50), name VARCHAR(50))")
            cur.executemany(
                f"INSERT INTO {table} VALUES (%s, %s)",
                [("a", "A"), ("b", "B"), ("a", "A2"), ("c", "C"), ("b", "B2")],
            )
            conn.commit()
    finally:
        conn.close()


def _seed_postgresql(table: str) -> None:
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="dataflow",
        password="dataflow",
        dbname="dataflow",
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE TABLE {table} (id TEXT, name TEXT)")
        cur.executemany(
            f"INSERT INTO {table} VALUES (%s, %s)",
            [("a", "A"), ("b", "B"), ("a", "A2"), ("c", "C"), ("b", "B2")],
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


@pytest.mark.parametrize(
    "db_type,seed_fn,config",
    [
        (
            "mysql",
            _seed_mysql,
            {
                "name": "MySQL Dupes Live",
                "type": "mysql",
                "role": "source",
                "host": "127.0.0.1",
                "port": 3306,
                "database": "dataflow",
                "username": "dataflow",
                "password": "dataflow",
                "ssl": False,
            },
        ),
        (
            "postgresql",
            _seed_postgresql,
            {
                "name": "PG Dupes Live",
                "type": "postgresql",
                "role": "source",
                "host": "localhost",
                "port": 5432,
                "database": "dataflow",
                "username": "dataflow",
                "password": "dataflow",
                "ssl": False,
            },
        ),
    ],
)
def test_live_duplicate_key_probe(
    db_type: str,
    seed_fn: Any,
    config: dict[str, Any],
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    unique_table: str,
) -> None:
    """Duplicate keys in a live SQL source are detected before the transfer runs."""
    from tests.typed_fidelity_helpers import require_ports

    port = 3306 if db_type == "mysql" else 5432
    require_ports(port)

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    store_path = tmp_path / "connectors.json"
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store_path))

    seed_fn(unique_table)

    from services.connector_store import create_connector
    from services.source_duplicate_probe import probe_source_duplicate_keys

    saved = create_connector(config)
    result = probe_source_duplicate_keys(
        source_connector_id=saved.id,
        source_table=unique_table,
        primary_key="id",
    )
    assert len(result) == 2
    values = {r["value"]: r["count"] for r in result}
    assert values.get("a") == 2
    assert values.get("b") == 2


def test_live_pg_qualified_table_name_does_not_double_prefix(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    unique_table: str,
) -> None:
    """Studio ``public.t`` + connector schema=public probes ``public.t``, not public.public.t."""
    from tests.typed_fidelity_helpers import require_ports

    require_ports(5432)
    _seed_postgresql(unique_table)

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))

    from services.connector_store import create_connector
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    saved = create_connector(
        {
            "name": "PG Qualified Dupes",
            "type": "postgresql",
            "role": "source",
            "host": "localhost",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "schema": "public",
            "ssl": False,
        }
    )
    result = probe_source_duplicate_keys_result(
        source_connector_id=saved.id,
        source_table=f"public.{unique_table}",
        primary_key="id",
    )
    assert result.status == "ran", result.message
    assert "does not exist" not in (result.message or "").lower()
    values = {r["value"]: r["count"] for r in result.findings}
    assert values.get("a") == 2
    assert values.get("b") == 2

    bare = probe_source_duplicate_keys_result(
        source_connector_id=saved.id,
        source_table=unique_table,
        primary_key="id",
    )
    assert bare.status == "ran", bare.message
    assert {r["value"]: r["count"] for r in bare.findings} == values


def test_preflight_blocks_inline_mysql_duplicate_keys(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    unique_table: str,
) -> None:
    """An inline MySQL source config (no saved connector_id) still triggers G9."""
    from tests.typed_fidelity_helpers import require_ports

    require_ports(3306)
    _seed_mysql(unique_table)

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    store_path = tmp_path / "connectors.json"
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store_path))

    from services.preflight_service import run_file_preflight

    source_config = {
        "kind": "database",
        "format": "mysql",
        "host": "127.0.0.1",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "ssl": False,
        "table": unique_table,
    }
    result = run_file_preflight(
        columns=["id", "name"],
        column_types={"id": "VARCHAR", "name": "VARCHAR"},
        row_count=5,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99, "transform": None},
            {"source": "name", "target": "name", "confidence": 0.99, "transform": None},
        ],
        destination_connected=True,
        destination_can_create=True,
        source_connected=True,
        source_kind="database",
        source_format="mysql",
        sync_mode="upsert",
        sample_rows=[{"id": "c", "name": "C"}],
        destination_db_type="postgresql",
        source_config=source_config,
        source_table=unique_table,
        destination_table="jobs",
        destination_table_exists=True,
        destination_pk_columns=["id"],
        validation_mode="strict",
    )
    gate_status = {g["id"]: g for g in result["gates"]}
    assert gate_status["g9_data_integrity"]["status"] == "block"
    g9_issues = gate_status["g9_data_integrity"].get("details", {}).get("issues", [])
    assert any("duplicate key values from source probe" in str(i) for i in g9_issues)
