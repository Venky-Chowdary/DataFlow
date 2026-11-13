"""G11: each writer names mapped targets by its own destination's rules.

Before, every writer used SQL identifier sanitizing cut at PostgreSQL's 63:

* Elasticsearch renamed ``@timestamp`` to ``_timestamp`` and ``host.name`` to
  ``host_name`` (an object path the index mapping expects);
* BigQuery and local file exports (csv/tsv/jsonl/parquet/xlsx) lowercased
  every column (``CustomerName`` -> ``customername``);
* a 64-character MySQL column, a 100-character SQL Server column and a long
  SQLite column were cut to 63 characters, silently.

Now: search stores keep the name exactly; BigQuery and file exports keep the
case; SQL writers cut only at the engine's real limit (and Validate/Execute
refuse a name past it - ``test_g11_target_name_gates``).
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
import sys
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


# -- Elasticsearch keeps the name exactly ------------------------------------


def test_elasticsearch_keeps_at_timestamp_and_dotted_paths():
    from connectors.elasticsearch_writer import write_mapped_rows

    client = MagicMock()
    client.indices.exists.return_value = True
    captured: list[dict] = []

    def fake_bulk(_client, actions, raise_on_error=False):
        items = list(actions)
        captured.extend(items)
        return (len(items), [])

    with patch("connectors.elasticsearch_writer._client", return_value=client), patch(
        "elasticsearch.helpers.bulk", side_effect=fake_bulk
    ):
        result = write_mapped_rows(
            host="localhost", port=9200, database="", username="", password="", schema="",
            connection_string="", ssl=False, table_name="logs",
            headers=["id", "ts", "host", "Level"],
            data_rows=[["e-1", "2026-01-02T03:04:05Z", "web-1", "WARN"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "ts", "target": "@timestamp"},
                {"source": "host", "target": "host.name"},
                {"source": "Level", "target": "Level"},
            ],
            column_types={"id": "TEXT", "ts": "TEXT", "host": "TEXT", "Level": "TEXT"},
            create_table=False, write_mode="insert", error_policy="fail",
            conflict_columns=["id"],
        )
    assert result.ok is True, result.error
    source = captured[0]["_source"]
    assert set(source) == {"id", "@timestamp", "host.name", "Level"}, source
    assert source["@timestamp"] == "2026-01-02T03:04:05Z"
    assert source["host.name"] == "web-1"


# -- BigQuery keeps case ---------------------------------------------------------


def test_bigquery_writer_target_columns_keep_case():
    from connectors.writer_common import resolve_target_columns

    mappings = [
        {"source": "id", "target": "CustomerID"},
        {"source": "nm", "target": "CustomerName"},
        {"source": "ts", "target": "createdAt"},
    ]
    # The exact call the BigQuery writer makes.
    cols, _ = resolve_target_columns(
        mappings, {"id": "INTEGER", "nm": "STRING", "ts": "STRING"},
        table_exists=False, dest_db="bigquery",
    )
    assert cols == ["CustomerID", "CustomerName", "createdAt"]


def test_bigquery_create_table_schema_keeps_case():
    from connectors.bigquery_writer import write_mapped_rows

    client = MagicMock()
    client.get_table.side_effect = Exception("404 Not found: Table")
    created: list = []
    client.create_table.side_effect = lambda table, **_k: created.append(table) or table

    with patch("connectors.bigquery_writer.stub_writes_allowed", return_value=False), patch(
        "connectors.bigquery_conn.get_client", return_value=client
    ), patch("connectors.bigquery_conn._is_local_endpoint", return_value=(False, "")):
        write_mapped_rows(
            host="proj", port=443, database="proj", username="", password="", schema="ds",
            connection_string="/tmp/sa.json", ssl=True, table_name="customers",
            headers=["id", "nm"], data_rows=[["1", "Ada"]],
            mappings=[
                {"source": "id", "target": "CustomerID", "target_type": "STRING"},
                {"source": "nm", "target": "CustomerName", "target_type": "STRING"},
            ],
            column_types={"id": "STRING", "nm": "STRING"},
            destination_column_types={"CustomerID": "STRING", "CustomerName": "STRING"},
            create_table=True, service_account="/tmp/sa.json", warehouse="",
        )
    assert created, "BigQuery writer did not reach CREATE TABLE"
    names = [f.name for f in created[0].schema]
    assert names == ["CustomerID", "CustomerName"], names


# -- local file exports keep case -------------------------------------------------

_EXPORT_MAPPINGS = [
    {"source": "id", "target": "CustomerID"},
    {"source": "name", "target": "CustomerName"},
    {"source": "created", "target": "createdAt"},
]
_EXPORT_HEADERS = ["CustomerID", "CustomerName", "createdAt"]


def _export(fmt: str) -> bytes:
    from src.transfer.adapters_file_write import write_destination_file
    from src.transfer.models import EndpointConfig

    data, _name, _meta = write_destination_file(
        EndpointConfig(kind="file_export", format=fmt),
        [{"id": "1", "name": "Ada", "created": "2026-01-02"}],
        ["id", "name", "created"],
        source_format="csv",
        mappings=[dict(m) for m in _EXPORT_MAPPINGS],
        column_types={"id": "VARCHAR", "name": "VARCHAR", "created": "VARCHAR"},
    )
    return data


@pytest.mark.parametrize("fmt", ["csv", "tsv"])
def test_delimited_export_keeps_header_case(fmt: str):
    text = _export(fmt).decode("utf-8-sig")
    rows = list(csv.reader(io.StringIO(text), delimiter="\t" if fmt == "tsv" else ","))
    assert rows[0] == _EXPORT_HEADERS, rows[0]


def test_jsonl_export_keeps_key_case():
    line = _export("jsonl").decode("utf-8").splitlines()[0]
    assert list(json.loads(line)) == _EXPORT_HEADERS


def test_parquet_export_keeps_column_case():
    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(_export("parquet")))
    assert table.column_names == _EXPORT_HEADERS


def test_excel_export_keeps_header_case():
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(_export("excel")), read_only=True)
    first = next(wb.active.iter_rows(min_row=1, max_row=1, values_only=True))
    assert list(first) == _EXPORT_HEADERS


# -- SQL writers cut only at the engine's own limit ---------------------------

LONG_64 = "customer_lifetime_value_after_discounts_and_refunds_in_eur_cents"
assert len(LONG_64) == 64


@pytest.mark.parametrize(
    ("dest", "length", "kept"),
    [
        ("postgresql", 63, 63),
        ("mysql", 64, 64),
        ("sqlserver", 100, 100),
        ("oracle", 128, 128),
        ("snowflake", 200, 200),
        ("bigquery", 300, 300),
        ("sqlite", 200, 200),
    ],
)
def test_writer_target_columns_are_not_cut_below_the_engine_limit(dest, length, kept):
    from connectors.writer_common import resolve_target_columns

    name = ("n" * length)
    cols, _ = resolve_target_columns(
        [{"source": "x", "target": name}], {"x": "VARCHAR"},
        preserve_case=True, dest_db=dest,
    )
    assert len(cols[0]) == kept


def test_generic_sql_writer_names_targets_for_its_engine():
    """SQL Server / Oracle write through ``generic_sql``: it must say which."""
    src = (_API_ROOT / "connectors" / "generic_sql.py").read_text(encoding="utf-8")
    call = src[src.index("target_cols, logical_types = resolve_target_columns("):][:200]
    assert "dest_db=" in call, call


def _mysql_up() -> bool:
    import socket

    try:
        socket.create_connection(("localhost", 3306), timeout=1).close()
        import pymysql

        pymysql.connect(host="localhost", port=3306, user="dataflow", password="dataflow",
                        database="dataflow").close()
    except Exception:  # noqa: BLE001 - any failure means "not available"
        return False
    return True


@pytest.mark.skipif(not _mysql_up(), reason="no MySQL on :3306")
def test_mysql_64_character_column_lands_uncut(tmp_path: Path):
    import pymysql

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.typed_fidelity_helpers import drop_mysql_table, mysql_endpoint

    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(src))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, 10), (2, 20)])
    conn.close()
    table = f"g11_{uuid.uuid4().hex[:10]}"
    try:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
                destination=mysql_endpoint(table),
                mappings=[
                    {"source": "id", "target": "id", "confidence": 1.0},
                    {"source": "v", "target": LONG_64, "confidence": 1.0},
                ],
                sync_mode="full_refresh_overwrite",
                skip_preflight=True,
            ),
            "g11" + uuid.uuid4().hex[:16],
        )
        assert result.success, result.error
        db = pymysql.connect(host="localhost", port=3306, user="dataflow", password="dataflow",
                             database="dataflow")
        try:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'dataflow' AND table_name = %s ORDER BY ordinal_position",
                    (table,),
                )
                cols = [r[0] for r in cur.fetchall()]
        finally:
            db.close()
        assert cols == ["id", LONG_64], cols
        assert (result.reconciliation or {}).get("passed") is True, result.reconciliation
    finally:
        drop_mysql_table(table)


def test_sqlite_long_column_lands_uncut(tmp_path: Path):
    from connectors.sqlite_writer import write_mapped_rows

    db = tmp_path / "dest.sqlite"
    name = "c" * 120
    res = write_mapped_rows(
        host="", port=0, database=str(db), username="", password="", schema="",
        connection_string="", ssl=False, table_name="t",
        headers=["id", "v"], data_rows=[["1", "x"]],
        mappings=[{"source": "id", "target": "id"}, {"source": "v", "target": name}],
        column_types={"id": "INTEGER", "v": "TEXT"}, create_table=True,
    )
    assert res.ok, res.error
    conn = sqlite3.connect(str(db))
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(t)")]
    finally:
        conn.close()
    assert cols == ["id", name], cols
