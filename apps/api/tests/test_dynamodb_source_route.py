"""DynamoDB as a transfer *source* — the direction that never ran.

Every DynamoDB source route in the matrix was skipped because the transfer
raised before moving a row. Three separate defects were in the way, each of
which also affects any other source that cannot cheaply count itself (Kafka, a
search index), so they are pinned here rather than in a DynamoDB-only corner.
"""

from __future__ import annotations

import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


# ── column typing ────────────────────────────────────────────────────────────


def test_column_type_widens_instead_of_taking_the_majority():
    """One decimal among many integers must widen the column, not lose the vote.

    Majority voting typed a column of 999 integers and a single ``2000.50`` as
    INTEGER, and the write then failed that row with "Invalid integer". The
    larger the table, the more certain the minority value was mistyped.
    """
    from connectors.dynamodb_reader import widen_logical_votes

    assert widen_logical_votes({"INTEGER": 999, "DECIMAL": 1}) == "DECIMAL"
    assert widen_logical_votes({"BOOLEAN": 5, "INTEGER": 1}) == "INTEGER"
    assert widen_logical_votes({"INTEGER": 3}) == "INTEGER"
    # Families that do not unify land on text, which holds any serialization.
    assert widen_logical_votes({"VARCHAR": 3, "INTEGER": 1}) == "TEXT"
    assert widen_logical_votes({"JSON": 1, "ARRAY": 2}) == "JSON"
    # No votes at all (null-only attribute) must not invent a numeric type.
    assert widen_logical_votes({}) == "VARCHAR"


def test_scaled_decimal_is_not_reported_as_an_integer():
    """``Decimal("1000.00")`` is integral in value but carries a scale."""
    from connectors.dynamodb_reader import infer_logical_from_native

    assert infer_logical_from_native(Decimal("1000.00")) == "DECIMAL"
    assert infer_logical_from_native(Decimal("2000.50")) == "DECIMAL"
    assert infer_logical_from_native(Decimal("1000")) == "INTEGER"
    assert infer_logical_from_native(5) == "INTEGER"


# ── unknown cardinality ──────────────────────────────────────────────────────


def test_preflight_accepts_an_unknown_row_count():
    """``row_count=None`` is honest for a Scan; it used to crash before gate 1."""
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["id"],
        column_types={"id": "INTEGER"},
        row_count=None,
        mappings=[{"source": "id", "target": "id", "confidence": 0.99}],
        sample_rows=[{"id": "1"}, {"id": "2"}],
        destination_connected=True,
        destination_can_create=True,
    )
    assert isinstance(result, dict)
    assert result.get("gates")


def test_row_count_label_does_not_format_none():
    from services.batch_progress import row_count_label

    assert row_count_label(1234) == "1,234"
    assert row_count_label(0) == "0"
    assert "unknown" in row_count_label(None)


# ── emptiness is observed, not reported ──────────────────────────────────────


def test_stale_item_count_does_not_prove_an_empty_table():
    """``ItemCount`` reading zero is not evidence the table is empty.

    AWS refreshes that metric roughly every six hours, so a table loaded minutes
    ago reports zero while full. Treating it as proof skipped the guards that
    stop untyped Map carriers binding against live data — the empty→NULL invent
    they exist to prevent.
    """
    from unittest.mock import MagicMock, patch

    from connectors.dynamodb_writer import write_mapped_rows

    client = MagicMock()
    # The catalog claims empty; a scan sees an item. The scan is the evidence.
    client.describe_table.return_value = {
        "Table": {
            "ItemCount": 0,
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
        }
    }
    with (
        patch("connectors.dynamodb_writer.boto3_client", return_value=client),
        patch(
            "connectors.dynamodb_writer._table_key_types", return_value={"id": "S"}
        ),
        patch(
            "connectors.dynamodb_writer._fetch_dynamo_physical_types",
            # Key typed, non-key column unresolved, and one item actually seen.
            return_value=({"id": "VARCHAR"}, True, 1, True),
        ),
    ):
        result = write_mapped_rows(
            host="",
            port=0,
            database="test",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id", "amount"],
            data_rows=[["1", "10.50"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "amount", "target": "amount"},
            ],
            column_types={"id": "VARCHAR", "amount": "VARCHAR"},
            error_policy="quarantine",
        )
    assert result.ok is False, "a populated table must not accept untyped carriers"
    assert result.rows_written == 0


def test_a_scan_that_saw_nothing_still_allows_map_only_create_new():
    """Proven empty is the one case that may skip the coverage guard.

    Otherwise a genuinely empty table with only key AttributeDefinitions would
    refuse every non-key column an operator mapped.
    """
    from unittest.mock import MagicMock, patch

    from connectors.dynamodb_writer import write_mapped_rows

    client = MagicMock()
    client.describe_table.return_value = {
        "Table": {
            "ItemCount": 0,
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
        }
    }
    client.batch_write_item.return_value = {"UnprocessedItems": {}}
    with (
        patch("connectors.dynamodb_writer.boto3_client", return_value=client),
        patch(
            "connectors.dynamodb_writer._table_key_types", return_value={"id": "S"}
        ),
        patch(
            "connectors.dynamodb_writer._fetch_dynamo_physical_types",
            # Scan ran and came back empty: emptiness is proven.
            return_value=({"id": "VARCHAR"}, True, 0, True),
        ),
    ):
        result = write_mapped_rows(
            host="",
            port=0,
            database="test",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id", "amount"],
            data_rows=[["1", "10.50"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "amount", "target": "amount"},
            ],
            column_types={"id": "VARCHAR", "amount": "VARCHAR"},
            error_policy="quarantine",
        )
    assert result.ok is True, result.error


def test_stale_positive_item_count_does_not_outrank_an_empty_scan():
    """ItemCount lags by hours in both directions — the Scan is the observation.

    A table emptied minutes ago still reports a positive ItemCount. Letting that
    stand as proof of population forced live-DDL coverage on a table with no
    rows, refusing a Map-only first load of exactly the non-key columns the
    operator asked for.
    """
    from unittest.mock import MagicMock, patch

    from connectors.dynamodb_writer import write_mapped_rows

    client = MagicMock()
    client.describe_table.return_value = {
        "Table": {
            # Stale: the table was emptied since AWS last refreshed this.
            "ItemCount": 5,
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
        }
    }
    client.batch_write_item.return_value = {"UnprocessedItems": {}}
    with (
        patch("connectors.dynamodb_writer.boto3_client", return_value=client),
        patch(
            "connectors.dynamodb_writer._table_key_types", return_value={"id": "S"}
        ),
        patch(
            "connectors.dynamodb_writer._fetch_dynamo_physical_types",
            # Scan ran, saw nothing: the table is empty whatever ItemCount says.
            return_value=({"id": "VARCHAR"}, True, 0, True),
        ),
    ):
        result = write_mapped_rows(
            host="",
            port=0,
            database="test",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id", "amount"],
            data_rows=[["1", "10.50"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "amount", "target": "amount"},
            ],
            column_types={"id": "VARCHAR", "amount": "VARCHAR"},
            error_policy="quarantine",
        )
    assert result.ok is True, result.error


def test_no_scan_at_all_does_not_prove_emptiness():
    """No Scan runs when every mapped column is a declared key attribute.

    Zero items seen without a Scan is an absence of evidence, so a positive
    ItemCount still stands and the coverage guard must run.
    """
    from unittest.mock import MagicMock, patch

    from connectors.dynamodb_writer import write_mapped_rows

    client = MagicMock()
    client.describe_table.return_value = {
        "Table": {
            "ItemCount": 5,
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
        }
    }
    with (
        patch("connectors.dynamodb_writer.boto3_client", return_value=client),
        patch(
            "connectors.dynamodb_writer._table_key_types", return_value={"id": "S"}
        ),
        patch(
            "connectors.dynamodb_writer._fetch_dynamo_physical_types",
            return_value=({"id": "VARCHAR"}, True, 0, False),
        ),
    ):
        result = write_mapped_rows(
            host="",
            port=0,
            database="test",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id", "amount"],
            data_rows=[["1", "10.50"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "amount", "target": "amount"},
            ],
            column_types={"id": "VARCHAR", "amount": "VARCHAR"},
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "amount" in (result.error or "")


def test_a_failed_scan_leaves_emptiness_unproven():
    """Scan failure is unknown, not empty, so the guard must still refuse."""
    from unittest.mock import MagicMock, patch

    from connectors.dynamodb_writer import write_mapped_rows

    client = MagicMock()
    client.describe_table.return_value = {
        "Table": {
            "ItemCount": 0,
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
        }
    }
    with (
        patch("connectors.dynamodb_writer.boto3_client", return_value=client),
        patch(
            "connectors.dynamodb_writer._table_key_types", return_value={"id": "S"}
        ),
        patch(
            "connectors.dynamodb_writer._fetch_dynamo_physical_types",
            return_value=({"id": "VARCHAR"}, False, 0, True),
        ),
    ):
        result = write_mapped_rows(
            host="",
            port=0,
            database="test",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name="orders",
            headers=["id", "amount"],
            data_rows=[["1", "10.50"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "amount", "target": "amount"},
            ],
            column_types={"id": "VARCHAR", "amount": "VARCHAR"},
            error_policy="quarantine",
        )
    assert result.ok is False
    assert result.rows_written == 0


# ── uniqueness ───────────────────────────────────────────────────────────────


def _moto():
    pytest.importorskip("moto")
    pytest.importorskip("boto3")
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    return server, f"http://{host}:{port}"


def _table(endpoint: str, *, key_type: str = "N") -> tuple[str, dict]:
    import boto3

    name = f"ddb_{uuid.uuid4().hex[:10]}"
    boto3.client(
        "dynamodb",
        endpoint_url=endpoint,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    ).create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": key_type}],
        BillingMode="PAY_PER_REQUEST",
    )
    cfg = {
        "type": "dynamodb",
        "host": "us-east-1",
        "port": 443,
        "database": "us-east-1",
        "username": "test",
        "password": "test",
        "connection_string": endpoint,
    }
    return name, cfg


def test_table_key_uniqueness_is_structural_not_skipped():
    """DynamoDB cannot hold two items with one primary key.

    The probe used to answer ``skipped_unsupported`` for every DynamoDB source,
    which fails a uniqueness-required sync closed. The key schema is stronger
    evidence than any scan, so it is reported as having run.
    """
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    server, endpoint = _moto()
    try:
        table, cfg = _table(endpoint)
        probe = probe_source_duplicate_keys_result(
            source_config=cfg, source_table=table, primary_key="id"
        )
        assert probe.status == "ran", probe.message
        assert probe.findings == []
        assert "key" in probe.message.lower()
    finally:
        server.stop()


def test_non_key_identity_is_scanned_and_finds_duplicates():
    """A non-key attribute carries no uniqueness guarantee, so it is counted."""
    from src.transfer.adapters import write_destination_database
    from src.transfer.models import EndpointConfig
    from services.source_duplicate_probe import probe_source_duplicate_keys_result

    server, endpoint = _moto()
    try:
        table, cfg = _table(endpoint)
        source = EndpointConfig(
            kind="database",
            format="dynamodb",
            host="us-east-1",
            port=443,
            database="us-east-1",
            table=table,
            username="test",
            password="test",
            connection_string=endpoint,
        )
        write_destination_database(
            source,
            [
                {"id": "1", "code": "A"},
                {"id": "2", "code": "B"},
                {"id": "3", "code": "A"},
            ],
            ["id", "code"],
            {"id": "INTEGER", "code": "VARCHAR"},
            [{"source": "id", "target": "id"}, {"source": "code", "target": "code"}],
        )
        probe = probe_source_duplicate_keys_result(
            source_config=cfg, source_table=table, primary_key="code"
        )
        assert probe.status == "ran", probe.message
        assert [f["value"] for f in probe.findings] == ["A"]
        assert probe.findings[0]["count"] == 2
    finally:
        server.stop()


# ── the route itself ─────────────────────────────────────────────────────────


def test_dynamodb_source_moves_rows_with_exact_decimals(tmp_path):
    import sqlite3

    from src.transfer.adapters import write_destination_database
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    server, endpoint = _moto()
    try:
        table, _cfg = _table(endpoint)
        source = EndpointConfig(
            kind="database",
            format="dynamodb",
            host="us-east-1",
            port=443,
            database="us-east-1",
            table=table,
            username="test",
            password="test",
            connection_string=endpoint,
        )
        mappings = [
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "amount", "target": "amount", "confidence": 0.99},
        ]
        write_destination_database(
            source,
            [{"id": "1", "amount": "1000.00"}, {"id": "2", "amount": "2000.50"}],
            ["id", "amount"],
            {"id": "INTEGER", "amount": "DECIMAL"},
            [{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
        )
        db_path = str(tmp_path / "out.db")
        destination = EndpointConfig(
            kind="database", format="sqlite", database=db_path, table="out"
        )
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=source,
                destination=destination,
                sync_mode="full_refresh_overwrite",
                skip_preflight=False,
                validation_mode="strict",
                mappings=mappings,
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success is True, result.error
        assert result.records_transferred == 2
        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute("SELECT id, amount FROM out ORDER BY id").fetchall()
        finally:
            conn.close()
        # The fractional digits must survive: a mistyped INTEGER column was the
        # original failure, and rounding to 2000 would be silent corruption.
        assert [r[1] for r in rows] == ["1000.00", "2000.50"]
    finally:
        server.stop()
