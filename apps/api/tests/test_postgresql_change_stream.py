"""Tests for the PostgreSQL logical decoding CDC parser and integration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from connectors.postgresql_change_stream import (
    PostgreSqlChangeStreamCdc,
    _parse_change_line,
    _parse_columns,
    _slot_name,
)
from services.cdc_engine import ChangeBatch


def test_slot_name_is_lowercase_and_truncated() -> None:
    name = _slot_name("MyDB", "Orders", "mongo:test:orders→sql:test:dst:stream")
    assert name.islower()
    assert len(name) <= 63
    assert name.startswith("df_mydb_orders_")


def test_parse_columns_with_quoted_spaces() -> None:
    payload = "id[int4]:2 name[text]:'hello world' active[bool]:true"
    result = _parse_columns(payload)
    assert result == {"id": "2", "name": "hello world", "active": "true"}


def test_parse_change_line_insert() -> None:
    line = "table public.orders: INSERT: id[int4]:1 amount[numeric]:100.50"
    op, old_key, new_tuple = _parse_change_line(line, "public", "orders")  # type: ignore[misc]
    assert op == "insert"
    assert old_key is None
    assert new_tuple == {"id": "1", "amount": "100.50"}


def test_parse_change_line_delete() -> None:
    line = "table public.orders: DELETE: id[int4]:1"
    op, old_key, new_tuple = _parse_change_line(line, "public", "orders")  # type: ignore[misc]
    assert op == "delete"
    assert old_key == {"id": "1"}
    assert new_tuple is None


def test_parse_change_line_update() -> None:
    line = "table public.orders: UPDATE: old-key: id[int4]:1 new-tuple: id[int4]:1 amount[numeric]:200.00"
    op, old_key, new_tuple = _parse_change_line(line, "public", "orders")  # type: ignore[misc]
    assert op == "update"
    assert old_key == {"id": "1"}
    assert new_tuple == {"id": "1", "amount": "200.00"}


def test_poll_parses_slot_changes() -> None:
    cfg = {
        "host": "localhost",
        "port": 5432,
        "database": "test",
        "username": "",
        "password": "",
        "connection_string": "",
        "ssl": False,
        "schema": "public",
    }
    cdc = PostgreSqlChangeStreamCdc(
        cfg,
        table="orders",
        primary_key="id",
        cursor_key="pg:test:orders→sql:test:dst:stream",
        resume_token="df_test_orders_12345678",
    )

    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = [
        ("BEGIN",),
        ("table public.orders: INSERT: id[int4]:1 amount[numeric]:100.00",),
        ("table public.orders: UPDATE: old-key: id[int4]:2 new-tuple: id[int4]:2 amount[numeric]:200.00",),
        ("table public.orders: DELETE: id[int4]:3",),
        ("COMMIT",),
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("connectors.postgresql_change_stream.get_connection", return_value=conn), \
         patch.object(cdc, "_ensure_slot"):
        changes = list(cdc.poll())

    assert len(changes) == 1
    batch = changes[0]
    assert len(batch.inserts) == 1
    assert len(batch.updates) == 1
    assert batch.deletes == ["3"]
    assert batch.resume_token == "df_test_orders_12345678"


def test_run_cdc_database_transfer_uses_postgresql_logical_decoding():
    """Exercise the PostgreSQL logical-decoding branch of the CDC runner."""
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    source = EndpointConfig(kind="database", format="postgresql", database="test", table="orders")
    destination = EndpointConfig(kind="database", format="generic_sql", database="test", table="dst")

    mock_write = MagicMock(return_value=(2, "abc", {}))
    mock_delete = MagicMock(return_value=0)

    class FakePgCdc:
        def __init__(self, *args, **kwargs):
            pass

        def is_available(self):
            return True

        def snapshot(self):
            yield ChangeBatch(inserts=[{"id": "1", "amount": "100.00"}, {"id": "2", "amount": "200.00"}])

        def poll(self):
            return iter([])

    with (
        patch("src.transfer.cdc_transfer.PostgreSqlChangeStreamCdc", FakePgCdc),
        patch("src.transfer.cdc_transfer._write_batch", mock_write),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", mock_delete),
        patch("src.transfer.cdc_transfer.get_watermark", return_value=None),
    ):
        rows_written, ddl_log, dest_summary, columns = run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
            schema={"id": "integer", "amount": "decimal"},
            stream_contracts=[{"sync_mode": "cdc", "primary_key": "id"}],
            job_id="cdc-pg-test",
        )

    assert rows_written == 2
    assert dest_summary["cdc"]["inserts"] == 2
    assert any("logical_decoding" in line for line in ddl_log)
