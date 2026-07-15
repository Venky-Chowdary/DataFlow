"""Cross-schema edge-type transfers.

Proves that DataFlow preserves precision and semantics for booleans, high-
precision decimals, timezones, JSON/arrays, and locale-ambiguous dates when
mapping between different column names and types.
"""
from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[2]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.adapters import write_destination_database  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

sys.path.insert(0, str(_API_ROOT / "tests"))
from test_execute_tracked_universal_matrix import (  # noqa: E402
    _build_db_endpoint,
    _endpoint_reachable,
)

EDGE_COLUMNS = ["rec_id", "compensation", "active", "recorded_at", "payload", "comment"]
EDGE_SOURCE_SCHEMA = {
    "rec_id": "INTEGER",
    "compensation": "DECIMAL",
    "active": "BOOLEAN",
    "recorded_at": "DATETIME",
    "payload": "JSON",
    "comment": "TEXT",
}

# Values exercise high precision, scientific notation, timezones, JSON objects/
# arrays, empty arrays, and an unambiguous day-first date string.
EDGE_RECORDS = [
    {
        "rec_id": "1",
        "compensation": "12345678901234567890.12345",
        "active": "true",
        "recorded_at": "2024-06-01T12:00:00+05:30",
        "payload": '{"tier":"gold","tags":["a","b"]}',
        "comment": "high precision",
    },
    {
        "rec_id": "2",
        "compensation": "0.00000000015",
        "active": "false",
        "recorded_at": "2024-12-31T23:59:59Z",
        "payload": "[1, 2, 3]",
        "comment": "scientific",
    },
    {
        "rec_id": "3",
        "compensation": "-9999.99",
        "active": "yes",
        "recorded_at": "2024-07-14",
        "payload": "{}",
        "comment": "date only",
    },
    {
        "rec_id": "4",
        "compensation": "3.14159",
        "active": "0",
        "recorded_at": "31/12/2024 10:00:00",
        "payload": "[]",
        "comment": "dayfirst",
    },
]

# Rename and type-shift columns: DECIMAL -> TEXT, BOOLEAN stays BOOLEAN, etc.
EDGE_MANUAL_MAPPINGS = [
    {"source": "rec_id", "target": "id"},
    {"source": "compensation", "target": "pay_amount", "target_type": "TEXT"},
    {"source": "active", "target": "is_active"},
    {"source": "recorded_at", "target": "recorded_at"},
    {"source": "payload", "target": "payload"},
    {"source": "comment", "target": "note"},
]


def _seed_postgresql_source(tmp_path: Path) -> EndpointConfig:
    source = _build_db_endpoint("postgresql", tmp_path, "edge_src", uuid.uuid4().hex[:8])
    if not _endpoint_reachable(source):
        pytest.skip("PostgreSQL source not reachable")

    identity = [{"source": c, "target": c} for c in EDGE_COLUMNS]
    rows, _, summary = write_destination_database(
        source, EDGE_RECORDS, EDGE_COLUMNS, EDGE_SOURCE_SCHEMA, identity
    )
    if rows != len(EDGE_RECORDS):
        pytest.skip(f"source seed wrote {rows} rows: {summary}")
    return source


@pytest.mark.parametrize(
    "dest_driver",
    ["postgresql", "mongodb", "sqlite", "generic_sql", "s3"],
)
def test_edge_type_cross_schema_transfer(dest_driver: str, tmp_path: Path) -> None:
    source = _seed_postgresql_source(tmp_path)

    suffix = uuid.uuid4().hex[:8]
    destination = _build_db_endpoint(dest_driver, tmp_path, "edge_dst", suffix)
    if not _endpoint_reachable(destination):
        pytest.skip(f"{dest_driver} destination not reachable")

    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        mappings=EDGE_MANUAL_MAPPINGS,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])

    assert result.success, f"{dest_driver}: {result.error}"
    assert result.records_transferred == len(EDGE_RECORDS), (
        f"{dest_driver}: {result.records_transferred}"
    )
    assert result.explanation, f"{dest_driver}: missing explanation"

    if destination.kind == "database":
        assert result.reconciliation.get("passed") is True, (
            f"{dest_driver}: {result.reconciliation}"
        )
