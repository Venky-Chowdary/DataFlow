"""D91 / D92 / D140 — key probes and samples must address the object named.

* D91: with a saved connector the duplicate-key probe used the connector's
  default schema (``public``), not the schema the run reads, so a key-based
  run from another schema probed a different table or none.
* D92: ``sample_connector_object("sales.orders")`` resolved to ``orders`` in
  the default schema by bare-name match — a decoy.
* D140: a pgvector source was "not implemented" for the probe, so strict
  key-based runs could never pass preflight.

D91/D140 run against live PostgreSQL.
"""

from __future__ import annotations

import socket
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from services.source_duplicate_probe import probe_source_duplicate_keys_result
from src.ai.copilot.query_tools import resolve_table_name

PG = dict(host="127.0.0.1", port=5432, database="dataflow", username="dataflow", password="dataflow")


def _reachable() -> bool:
    try:
        with socket.create_connection((PG["host"], PG["port"]), timeout=1.5):
            return True
    except OSError:
        return False


def _pg(sql: str) -> None:
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"], port=PG["port"], dbname=PG["database"],
        user=PG["username"], password=PG["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()


@pytest.fixture()
def schema_with_dupes():
    if not _reachable():
        pytest.skip("PostgreSQL not reachable")
    schema = f"d91_{uuid.uuid4().hex[:8]}"
    # A same-named, duplicate-free decoy in public is what the bug probed.
    _pg(
        f"CREATE SCHEMA {schema}; "
        f"CREATE TABLE {schema}.orders (id int, v text); "
        f"INSERT INTO {schema}.orders VALUES (1, 'a'), (1, 'b'), (2, 'c'); "
        f"CREATE TABLE public.orders_{schema} (id int)"
    )
    yield schema
    _pg(f"DROP SCHEMA {schema} CASCADE; DROP TABLE IF EXISTS public.orders_{schema}")


def test_saved_connector_probe_reads_the_runs_schema(schema_with_dupes):
    saved_cfg = {**PG, "type": "postgresql", "schema": ""}
    with (
        patch("services.connector_store.get_connector",
              return_value=SimpleNamespace(type="postgresql")),
        patch("services.connector_probe.probe_cfg_from_saved", return_value=saved_cfg),
    ):
        result = probe_source_duplicate_keys_result(
            source_connector_id="c1",
            source_config={"type": "postgresql", "schema": schema_with_dupes},
            source_table="orders",
            primary_key="id",
        )
    assert result.status == "ran", result.message
    assert [f["value"] for f in result.findings] == ["1"]


def test_pgvector_source_uses_the_postgres_probe(schema_with_dupes):
    result = probe_source_duplicate_keys_result(
        source_config={**PG, "type": "pgvector", "schema": schema_with_dupes},
        source_table="orders",
        primary_key="id",
    )
    assert result.status == "ran", result.message
    assert [f["count"] for f in result.findings] == [2]


def test_schema_qualified_sample_never_resolves_to_a_decoy():
    inventory = SimpleNamespace(
        success=True, output={"objects": [{"name": "orders"}, {"name": "customers"}]}
    )
    conn = {"id": "c1", "name": "pg", "type": "postgresql"}
    with patch("src.ai.copilot.query_tools.list_connector_objects", return_value=inventory):
        resolved, note, _names = resolve_table_name(conn, "sales.orders")
        bare, _n, _c = resolve_table_name(conn, "ORDERS")
    assert resolved == "sales.orders"
    assert note is None
    # Unqualified names keep their case-insensitive resolve.
    assert bare == "orders"
