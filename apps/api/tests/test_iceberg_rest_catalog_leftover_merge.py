"""Warehouse leftover MERGE through a live Iceberg REST catalog.

SqlCatalog leftover MERGE is already measured. This file proves the same
dest-engine identity on RestCatalog:

    dest {1,2,3,99} vs S {1,2,3} → delete 99
    dest COUNT is file footers, never scan().count() / to_arrow()
    incremental leftover MERGE is a hard no-op

Glue / Nessie servers are not on this host — those stay Planned.
Not leftover MERGE certified for every warehouse catalog.
"""

from __future__ import annotations

import os
import socket
import uuid
from urllib.error import URLError
from urllib.request import urlopen

import pytest

from services.dest_precount import (
    EXTRA_KEYS_KEY,
    MISSING_KEYS_KEY,
    destination_keyset_census,
    destination_row_count,
)
from services.row_conservation import apply_inferred_leftover_deletes

REST_URI = os.environ.get("DATAFLOW_ICEBERG_REST_URI", "http://127.0.0.1:8181").rstrip("/")
REST_WAREHOUSE = os.environ.get(
    "DATAFLOW_ICEBERG_REST_WAREHOUSE", "file:///tmp/iceberg-rest-wh"
)


def _rest_reachable() -> bool:
    try:
        host = REST_URI.split("://", 1)[-1].split("/", 1)[0]
        hostname, _, port_s = host.partition(":")
        port = int(port_s or "8181")
        with socket.create_connection((hostname, port), timeout=1.5):
            pass
        with urlopen(f"{REST_URI}/v1/config", timeout=2) as resp:
            return int(getattr(resp, "status", 0) or 0) == 200
    except (OSError, URLError, ValueError):
        return False


requires_rest = pytest.mark.skipif(
    not _rest_reachable(),
    reason=f"Iceberg REST catalog not reachable at {REST_URI}",
)


def _rest_cfg(table: str) -> dict:
    return {
        "type": "iceberg",
        "connection_string": REST_URI,
        "warehouse": REST_WAREHOUSE,
        "table": table,
        "schema": "default",
        "extra": {"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
    }


def _write_orders(table: str, rows: list[list[str]]) -> None:
    from connectors.iceberg_writer import write_mapped_rows

    written = write_mapped_rows(
        connection_string=REST_URI,
        warehouse=REST_WAREHOUSE,
        table_name=f"default.{table}",
        headers=["id", "v"],
        data_rows=rows,
        mappings=[
            {"source": "id", "target": "id", "transform": "direct"},
            {"source": "v", "target": "v", "transform": "direct"},
        ],
        write_mode="append",
        create_table=True,
        extra={"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
    )
    assert written.ok, written.error


def test_iceberg_dest_layout_hadoop_is_catalog_not_filesystem():
    """Hadoop intent must not invent a local warehouse tree for leftover COUNT."""
    from services.dest_precount import _iceberg_dest_layout

    layout = _iceberg_dest_layout(
        {
            "connection_string": "/tmp/iceberg-hadoop-wh",
            "table": "orders",
            "schema": "default",
            "extra": {"catalog_type": "hadoop"},
        }
    )
    assert layout == "catalog"


def test_iceberg_hadoop_catalog_refuses_sql_fallback():
    """pyiceberg 0.11 has no HadoopCatalog — leftover MERGE must not invent SQL."""
    from connectors.iceberg_catalog import load_catalog

    with pytest.raises(RuntimeError, match="Hadoop catalog is not available|SqlCatalog fallback"):
        load_catalog(
            {
                "connection_string": "/tmp/iceberg-hadoop-wh",
                "table": "orders",
                "schema": "default",
                "extra": {"catalog_type": "hadoop"},
            }
        )


@requires_rest
def test_iceberg_rest_catalog_leftover_merge_deletes_extra_and_count_is_snapshot_len(
    monkeypatch: pytest.MonkeyPatch,
):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from pyiceberg.table import DataScan
    from services.dest_precount import _iceberg_snapshot_rows

    table = f"rest_orders_{uuid.uuid4().hex[:8]}"
    cfg = _rest_cfg(table)
    _write_orders(table, [["1", "a"], ["2", "b"], ["3", "c"], ["99", "ghost"]])

    def _no_count(self):
        raise AssertionError("REST leftover MERGE must not scan().count()")

    def _no_arrow(self):
        raise AssertionError("REST leftover MERGE must not to_arrow the table")

    monkeypatch.setattr(DataScan, "count", _no_count)
    monkeypatch.setattr(DataScan, "to_arrow", _no_arrow)

    snapshot = _iceberg_snapshot_rows(cfg, schema="default", table_name=table, cols=("id",))
    assert snapshot is not None
    assert destination_row_count("iceberg", cfg, schema="default", table_name=table) == len(
        snapshot
    )
    before = destination_keyset_census(
        "iceberg",
        cfg,
        schema="default",
        table_name=table,
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
    )
    assert before is not None
    assert before["dest_count"] == 4
    assert before[EXTRA_KEYS_KEY] == 1
    assert before[MISSING_KEYS_KEY] == 0

    refused = apply_inferred_leftover_deletes(
        db_type="iceberg",
        cfg=cfg,
        schema="default",
        table_name=table,
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
        complete_snapshot=False,
    )
    assert refused is None
    assert destination_row_count("iceberg", cfg, schema="default", table_name=table) == 4

    deleted = apply_inferred_leftover_deletes(
        db_type="iceberg",
        cfg=cfg,
        schema="default",
        table_name=table,
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
        complete_snapshot=True,
    )
    assert deleted == 1
    after = destination_keyset_census(
        "iceberg",
        cfg,
        schema="default",
        table_name=table,
        key_columns=["id"],
        keys=[("1",), ("2",), ("3",)],
    )
    assert after is not None
    assert after["dest_count"] == 3
    assert after[EXTRA_KEYS_KEY] == 0
    assert after[MISSING_KEYS_KEY] == 0
    remaining = _iceberg_snapshot_rows(cfg, schema="default", table_name=table, cols=("id",))
    assert remaining is not None
    assert {str(row.get("id")) for row in remaining} == {"1", "2", "3"}
    assert destination_row_count("iceberg", cfg, schema="default", table_name=table) == len(
        remaining
    )


@requires_rest
def test_iceberg_rest_catalog_leftover_merge_composite_pk(
    monkeypatch: pytest.MonkeyPatch,
):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from connectors.iceberg_writer import write_mapped_rows
    from pyiceberg.table import DataScan
    from services.dest_precount import _iceberg_snapshot_rows

    table = f"rest_lines_{uuid.uuid4().hex[:8]}"
    cfg = _rest_cfg(table)
    written = write_mapped_rows(
        connection_string=REST_URI,
        warehouse=REST_WAREHOUSE,
        table_name=f"default.{table}",
        headers=["order_id", "line_id", "note"],
        data_rows=[["1", "1", "a"], ["1", "2", "b"], ["9", "9", "ghost"]],
        mappings=[
            {"source": "order_id", "target": "order_id", "transform": "direct"},
            {"source": "line_id", "target": "line_id", "transform": "direct"},
            {"source": "note", "target": "note", "transform": "direct"},
        ],
        column_types={
            "order_id": "integer",
            "line_id": "integer",
            "note": "string",
        },
        write_mode="append",
        create_table=True,
        extra={"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
    )
    assert written.ok, written.error

    def _no_count(self):
        raise AssertionError("REST leftover MERGE must not scan().count()")

    def _no_arrow(self):
        raise AssertionError("REST leftover MERGE must not to_arrow the table")

    monkeypatch.setattr(DataScan, "count", _no_count)
    monkeypatch.setattr(DataScan, "to_arrow", _no_arrow)

    refused = apply_inferred_leftover_deletes(
        db_type="iceberg",
        cfg=cfg,
        schema="default",
        table_name=table,
        key_columns=["order_id", "line_id"],
        keys=[("1", "1"), ("1", "2")],
        complete_snapshot=False,
    )
    assert refused is None
    assert destination_row_count("iceberg", cfg, schema="default", table_name=table) == 3

    deleted = apply_inferred_leftover_deletes(
        db_type="iceberg",
        cfg=cfg,
        schema="default",
        table_name=table,
        key_columns=["order_id", "line_id"],
        keys=[("1", "1"), ("1", "2")],
        complete_snapshot=True,
    )
    assert deleted == 1
    remaining = _iceberg_snapshot_rows(
        cfg, schema="default", table_name=table, cols=("order_id", "line_id")
    )
    assert remaining is not None
    assert {(str(r.get("order_id")), str(r.get("line_id"))) for r in remaining} == {
        ("1", "1"),
        ("1", "2"),
    }
    assert destination_row_count("iceberg", cfg, schema="default", table_name=table) == 2
