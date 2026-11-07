"""Stream path must forward Studio live DDL (destination_column_types)."""

from __future__ import annotations

from unittest.mock import patch

from src.transfer.models import EndpointConfig


def test_stream_write_batch_forwards_destination_column_types():
    """Execute stream must not drop Studio schema_types (invent cliff amplifier)."""
    from src.transfer import stream as stream_mod

    dest = EndpointConfig(
        kind="database",
        format="mysql",
        host="localhost",
        port=3306,
        database="app",
        username="u",
        password="p",
        extra={
            "schema_types": {"lat": "DECIMAL(10,6)", "id": "INT"},
            "schema_nullability": {"id": False},
        },
    )
    cfg = {
        "host": "localhost",
        "port": 3306,
        "database": "app",
        "username": "u",
        "password": "p",
        "schema": "",
        "connection_string": "",
        "ssl": False,
        "auth_source": "",
    }
    captured: dict = {}

    class _Ok:
        ok = True
        rows_written = 1
        checksum = "abc"
        table_name = "t"
        target_schema = "app"
        driver = "pymysql"
        rejected_details = []
        warnings = []
        rejected_rows = 0
        coerced_null_rows = 0

    def _fake_write(**kwargs):
        captured.update(kwargs)
        return _Ok()

    with patch("connectors.mysql_writer.write_mapped_rows", _fake_write):
        with patch("connectors.write_resilience.build_write_batch_key", return_value="k"):
            rows, _checksum, _summary = stream_mod._write_batch(
                "mysql",
                dest,
                cfg,
                "t",
                ["id", "lat"],
                [["1", "1.5"]],
                [
                    {"source": "id", "target": "id"},
                    {"source": "lat", "target": "lat"},
                ],
                {"id": "INT", "lat": "DECIMAL"},
                True,
                None,
                0,
                1,
                0,
            )

    assert rows == 1
    assert captured.get("destination_column_types") == {
        "lat": "DECIMAL(10,6)",
        "id": "INT",
    }
    assert captured.get("destination_column_nullability") == {"id": False}
