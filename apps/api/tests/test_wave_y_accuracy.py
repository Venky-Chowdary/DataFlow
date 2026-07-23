"""Wave Y accuracy: Mongo keyset CDC, Couchbase/Iceberg/Snowflake honesty."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def test_iceberg_empty_dir_without_metadata_deny_create(tmp_path: Path):
    from connectors.iceberg_writer import write_mapped_rows

    table_dir = tmp_path / "wh" / "ns" / "events"
    table_dir.mkdir(parents=True)
    result = write_mapped_rows(
        host="",
        port=0,
        database=str(tmp_path / "wh"),
        username="",
        password="",
        schema="ns",
        connection_string="",
        ssl=False,
        table_name="events",
        headers=["id"],
        data_rows=[["1"]],
        mappings=[{"source": "id", "target": "id"}],
        column_types={"id": "INTEGER"},
        create_table=False,
    )
    assert result.ok is False
    assert "metadata is missing" in (result.error or "").lower()
    assert not (table_dir / "metadata").exists()


def test_iceberg_parquet_type_failure_refuses_jsonl_downgrade(tmp_path: Path):
    pytest.importorskip("pyarrow")
    from connectors.iceberg_writer import _write_data_file

    data_dir = tmp_path / "tbl" / "data"
    with pytest.raises(ValueError, match="refusing JSONL type downgrade"):
        _write_data_file(
            data_dir,
            ["id", "amt"],
            [{"id": "1", "amt": "not-a-decimal"}],
            column_types={"id": "TEXT", "amt": "DECIMAL(5,2)"},
        )
    assert not list(tmp_path.rglob("*.jsonl"))


def test_snowflake_deny_create_skips_database_and_schema_ddl():
    from connectors.snowflake_writer import write_mapped_rows

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    executed: list[str] = []

    def _exec(sql, *a, **k):
        executed.append(str(sql))

    cur.execute.side_effect = _exec

    with patch("connectors.snowflake_writer.get_connection", return_value=conn), patch(
        "connectors.snowflake_conn.resolve_snowflake_table_name", return_value=None
    ):
        result = write_mapped_rows(
            host="x",
            port=443,
            database="APP_DB",
            username="u",
            password="p",
            schema="PUBLIC",
            connection_string="",
            ssl=True,
            warehouse="WH",
            table_name="ORDERS",
            headers=["id"],
            data_rows=[["1"]],
            mappings=[{"source": "id", "target": "id"}],
            column_types={"id": "INTEGER"},
            create_table=False,
        )

    assert result.ok is False
    assert "create_table is disabled" in (result.error or "")
    joined = "\n".join(executed).upper()
    assert "CREATE DATABASE" not in joined
    assert "CREATE SCHEMA" not in joined
    assert "CREATE TABLE" not in joined


def test_couchbase_full_page_does_not_claim_total():
    from connectors.couchbase import read_object

    def fake_n1ql(_url, _user, _password, statement):
        return {
            "results": [
                {"travel-sample": {"id": "1"}},
                {"travel-sample": {"id": "2"}},
            ]
        }

    with patch("connectors.couchbase._n1ql", side_effect=fake_n1ql):
        batch = read_object(
            cfg={"host": "localhost", "username": "u", "password": "p"},
            object="travel-sample",
            limit=2,
            offset=0,
        )
    assert len(batch.rows) == 2
    assert batch.total_rows is None
