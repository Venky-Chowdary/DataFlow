"""Live leftover MERGE on warehouse / object-store emulators.

Airbyte S3 overwrite leaves leftovers across failed generations and other
connections (airbytehq/airbyte#61522). Fivetran warehouse leftover is a
``_fivetran_deleted`` soft-flag so COUNT(*) does not drop. This file measures
DataFlow dest-engine identity:

    dest {1,2,3,99} vs S {1,2,3} → DELETE 99
    dest COUNT 4→3 (GET streams / native COUNT(*), never catalog stats)
    incremental leftover MERGE is a hard no-op

Emulators are not a customer-tenant certificate. Snowflake/BQ customer-tenant
PRODUCTION_SKU is not claimed. Skip when a port is closed — never invent green.
"""

from __future__ import annotations

import socket
import uuid
from urllib.parse import urlparse

import pytest

from services.dest_precount import (
    EXTRA_KEYS_KEY,
    MISSING_KEYS_KEY,
    destination_keyset_census,
    destination_row_count,
)
from services.row_conservation import apply_inferred_leftover_deletes

AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)
MAPPINGS = [
    {"source": "id", "target": "id", "transform": "direct"},
    {"source": "v", "target": "v", "transform": "direct"},
]
GHOST_ROWS = [["1", "a"], ["2", "b"], ["3", "c"], ["99", "ghost"]]
SOURCE_KEYS = [("1",), ("2",), ("3",)]


def _port_up(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, port), timeout=1.5).close()
        return True
    except OSError:
        return False


def _assert_leftover_merge(db_type: str, cfg: dict, *, schema: str, table: str) -> None:
    before = destination_row_count(db_type, cfg, schema=schema, table_name=table)
    assert before == 4, f"{db_type} dest COUNT before leftover MERGE: {before}"
    census_before = destination_keyset_census(
        db_type, cfg, schema=schema, table_name=table, key_columns=["id"], keys=SOURCE_KEYS
    )
    assert census_before is not None, f"{db_type} dest keyset census unmeasured"
    assert census_before["dest_count"] == 4
    assert census_before[EXTRA_KEYS_KEY] == 1
    assert census_before[MISSING_KEYS_KEY] == 0

    refused = apply_inferred_leftover_deletes(
        db_type=db_type,
        cfg=cfg,
        schema=schema,
        table_name=table,
        key_columns=["id"],
        keys=SOURCE_KEYS,
        complete_snapshot=False,
    )
    assert refused is None
    assert destination_row_count(db_type, cfg, schema=schema, table_name=table) == 4

    deleted = apply_inferred_leftover_deletes(
        db_type=db_type,
        cfg=cfg,
        schema=schema,
        table_name=table,
        key_columns=["id"],
        keys=SOURCE_KEYS,
        complete_snapshot=True,
    )
    assert deleted == 1, f"{db_type} leftover MERGE deleted {deleted}, expected 1"
    after = destination_keyset_census(
        db_type, cfg, schema=schema, table_name=table, key_columns=["id"], keys=SOURCE_KEYS
    )
    assert after is not None
    assert after["dest_count"] == 3
    assert after[EXTRA_KEYS_KEY] == 0
    assert after[MISSING_KEYS_KEY] == 0
    assert destination_row_count(db_type, cfg, schema=schema, table_name=table) == 3

    second = apply_inferred_leftover_deletes(
        db_type=db_type,
        cfg=cfg,
        schema=schema,
        table_name=table,
        key_columns=["id"],
        keys=SOURCE_KEYS,
        complete_snapshot=True,
    )
    assert second == 0


def _write_object_store(kind: str, **kwargs):
    if kind == "s3":
        from connectors.s3_writer import write_mapped_rows
    elif kind == "gcs":
        from connectors.gcs_writer import write_mapped_rows
    else:
        from connectors.adls_writer import write_mapped_rows
    written = write_mapped_rows(**kwargs)
    assert written.ok, written.error
    return written


def test_object_store_leftover_merge_on_moto(local_object_store: str) -> None:
    """In-process moto S3: dest-engine leftover MERGE 4→3. Not a cloud tenant."""
    if not local_object_store:
        pytest.skip("moto / DATAFLOW_TEST_S3_ENDPOINT unavailable")
    from tests.conftest import LOCAL_OBJECT_STORE_BUCKET

    parsed = urlparse(local_object_store)
    table = f"leftover/orders_moto_{uuid.uuid4().hex[:8]}.json"
    cfg = {
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 443,
        "database": LOCAL_OBJECT_STORE_BUCKET,
        "username": "test",
        "password": "test",
        "connection_string": local_object_store,
        "path_style": True,
    }
    _write_object_store(
        "s3",
        host=cfg["host"],
        port=int(cfg["port"]),
        database=cfg["database"],
        username="test",
        password="test",
        schema="",
        connection_string=local_object_store,
        ssl=False,
        table_name=table,
        headers=["id", "v"],
        data_rows=GHOST_ROWS,
        mappings=MAPPINGS,
        column_types={"id": "INTEGER", "v": "STRING"},
        path_style=True,
    )
    _assert_leftover_merge("s3", cfg, schema="", table=table)


def test_minio_leftover_merge_when_reachable() -> None:
    if not _port_up("127.0.0.1", 9000):
        pytest.skip("MinIO not listening on 9000")
    table = f"leftover/orders_minio_{uuid.uuid4().hex[:8]}.json"
    cfg = {
        "host": "127.0.0.1",
        "port": 9000,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflowsecret",
        "connection_string": "http://127.0.0.1:9000",
        "path_style": True,
    }
    _write_object_store(
        "s3",
        host="127.0.0.1",
        port=9000,
        database="dataflow",
        username="dataflow",
        password="dataflowsecret",
        schema="",
        connection_string="http://127.0.0.1:9000",
        ssl=False,
        table_name=table,
        headers=["id", "v"],
        data_rows=GHOST_ROWS,
        mappings=MAPPINGS,
        column_types={"id": "INTEGER", "v": "STRING"},
        path_style=True,
    )
    _assert_leftover_merge("s3", cfg, schema="", table=table)


def test_gcs_leftover_merge_when_reachable() -> None:
    if not _port_up("127.0.0.1", 4443):
        pytest.skip("fake-gcs-server not listening on 4443")
    table = f"leftover/orders_gcs_{uuid.uuid4().hex[:8]}.json"
    cfg = {
        "host": "localhost",
        "port": 4443,
        "database": "dataflow-test",
        "connection_string": "http://localhost:4443",
    }
    _write_object_store(
        "gcs",
        host="localhost",
        port=4443,
        database="dataflow-test",
        username="",
        password="",
        schema="",
        connection_string="http://localhost:4443",
        ssl=False,
        table_name=table,
        headers=["id", "v"],
        data_rows=GHOST_ROWS,
        mappings=MAPPINGS,
        column_types={"id": "INTEGER", "v": "STRING"},
    )
    _assert_leftover_merge("gcs", cfg, schema="", table=table)


def test_adls_leftover_merge_when_reachable() -> None:
    if not _port_up("127.0.0.1", 10000):
        pytest.skip("Azurite not listening on 10000")
    table = f"leftover/orders_adls_{uuid.uuid4().hex[:8]}.json"
    cfg = {
        "host": "127.0.0.1",
        "port": 10000,
        "database": "test",
        "username": "devstoreaccount1",
        "password": AZURITE_KEY,
    }
    _write_object_store(
        "adls",
        host="127.0.0.1",
        port=10000,
        database="test",
        username="devstoreaccount1",
        password=AZURITE_KEY,
        schema="",
        connection_string="",
        ssl=False,
        table_name=table,
        headers=["id", "v"],
        data_rows=GHOST_ROWS,
        mappings=MAPPINGS,
        column_types={"id": "INTEGER", "v": "STRING"},
    )
    _assert_leftover_merge("adls", cfg, schema="", table=table)


def test_bigquery_emulator_leftover_merge_when_reachable() -> None:
    if not _port_up("127.0.0.1", 9050):
        pytest.skip("BigQuery emulator not listening on 9050")
    from connectors.bigquery_writer import write_mapped_rows

    table = f"leftover_orders_{uuid.uuid4().hex[:8]}"
    cfg = {
        "type": "bigquery",
        "host": "127.0.0.1",
        "port": 9050,
        "database": "dataflow-test",
        "schema": "dataflow",
        "connection_string": "http://127.0.0.1:9050",
    }
    written = write_mapped_rows(
        host="127.0.0.1",
        port=9050,
        database="dataflow-test",
        username="",
        password="",
        schema="dataflow",
        connection_string="http://127.0.0.1:9050",
        ssl=False,
        warehouse="",
        table_name=table,
        headers=["id", "v"],
        data_rows=GHOST_ROWS,
        mappings=MAPPINGS,
        column_types={"id": "INTEGER", "v": "STRING"},
        create_table=True,
    )
    assert written.ok, written.error
    _assert_leftover_merge("bigquery", cfg, schema="dataflow", table=table)


def test_fakesnow_leftover_merge() -> None:
    """In-process fakesnow: dest-engine leftover MERGE 4→3. Not a Snowflake tenant."""
    pytest.importorskip("fakesnow")
    from connectors.snowflake_writer import write_mapped_rows

    table = f"leftover_orders_{uuid.uuid4().hex[:8]}"
    cfg = {
        "type": "snowflake",
        "host": "localhost",
        "database": "dataflow",
        "username": "test",
        "password": "test",
        "schema": "public",
        "warehouse": "",
    }
    written = write_mapped_rows(
        host="localhost",
        port=443,
        database="dataflow",
        username="test",
        password="test",
        schema="public",
        connection_string="",
        ssl=False,
        warehouse="",
        table_name=table,
        headers=["id", "v"],
        data_rows=GHOST_ROWS,
        mappings=MAPPINGS,
        column_types={"id": "INTEGER", "v": "STRING"},
        create_table=True,
    )
    assert written.ok, written.error
    _assert_leftover_merge("snowflake", cfg, schema="public", table=table)


def test_object_store_leftover_merge_composite_pk_on_moto(local_object_store: str) -> None:
    if not local_object_store:
        pytest.skip("moto / DATAFLOW_TEST_S3_ENDPOINT unavailable")
    from tests.conftest import LOCAL_OBJECT_STORE_BUCKET

    parsed = urlparse(local_object_store)
    table = f"leftover/orders_comp_{uuid.uuid4().hex[:8]}.json"
    cfg = {
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 443,
        "database": LOCAL_OBJECT_STORE_BUCKET,
        "username": "test",
        "password": "test",
        "connection_string": local_object_store,
        "path_style": True,
    }
    mappings = [
        {"source": "order_id", "target": "order_id", "transform": "direct"},
        {"source": "line_id", "target": "line_id", "transform": "direct"},
        {"source": "v", "target": "v", "transform": "direct"},
    ]
    _write_object_store(
        "s3",
        host=cfg["host"],
        port=int(cfg["port"]),
        database=cfg["database"],
        username="test",
        password="test",
        schema="",
        connection_string=local_object_store,
        ssl=False,
        table_name=table,
        headers=["order_id", "line_id", "v"],
        data_rows=[["1", "1", "a"], ["1", "2", "b"], ["9", "9", "ghost"]],
        mappings=mappings,
        column_types={"order_id": "INTEGER", "line_id": "INTEGER", "v": "STRING"},
        path_style=True,
    )
    assert destination_row_count("s3", cfg, schema="", table_name=table) == 3
    refused = apply_inferred_leftover_deletes(
        db_type="s3",
        cfg=cfg,
        schema="",
        table_name=table,
        key_columns=["order_id", "line_id"],
        keys=[("1", "1"), ("1", "2")],
        complete_snapshot=False,
    )
    assert refused is None
    deleted = apply_inferred_leftover_deletes(
        db_type="s3",
        cfg=cfg,
        schema="",
        table_name=table,
        key_columns=["order_id", "line_id"],
        keys=[("1", "1"), ("1", "2")],
        complete_snapshot=True,
    )
    assert deleted == 1
    assert destination_row_count("s3", cfg, schema="", table_name=table) == 2
