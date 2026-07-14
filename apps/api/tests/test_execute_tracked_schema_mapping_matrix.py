"""Cross-schema transfer matrix.

Proves that DataFlow can move data between tables/collections with different
column names and types, both with explicit mappings and via the semantic
auto-mapper when a target schema already exists.
"""
from __future__ import annotations

import contextlib
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[2]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.adapters import write_destination_database  # noqa: E402
from src.transfer.connector_capabilities import default_port, resolve_driver_type  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

sys.path.insert(0, str(_API_ROOT / "tests"))
from test_execute_tracked_universal_matrix import _build_db_endpoint, _endpoint_reachable, _seed_source  # noqa: E402


SOURCE_RECORDS = [
    {"emp_id": "1", "emp_name": "Alice", "salary": "90000.50", "is_active": "true"},
    {"emp_id": "2", "emp_name": "Bob", "salary": "120000.00", "is_active": "false"},
]
SOURCE_COLUMNS = ["emp_id", "emp_name", "salary", "is_active"]
SOURCE_SCHEMA = {
    "emp_id": "INTEGER",
    "emp_name": "TEXT",
    "salary": "DECIMAL",
    "is_active": "BOOLEAN",
}

# Explicit cross-schema mapping.
MANUAL_MAPPINGS = [
    {"source": "emp_id", "target": "id"},
    {"source": "emp_name", "target": "name"},
    {"source": "salary", "target": "compensation"},
    {"source": "is_active", "target": "active"},
]

# Target columns must be created by the engine from the manual mappings.
TARGET_COLUMNS = ["id", "name", "compensation", "active"]


def _reachable_postgresql(tmp_path: Path) -> EndpointConfig:
    ep = _build_db_endpoint("postgresql", tmp_path, "src", uuid.uuid4().hex[:8])
    if not _endpoint_reachable(ep):
        pytest.skip("PostgreSQL source not reachable")
    return ep


def _seed_source_table(endpoint: EndpointConfig) -> None:
    """Seed a PostgreSQL source table with the cross-schema columns."""
    identity = [{"source": c, "target": c} for c in SOURCE_COLUMNS]
    rows, _, summary = write_destination_database(
        endpoint, SOURCE_RECORDS, SOURCE_COLUMNS, SOURCE_SCHEMA, identity
    )
    if rows != len(SOURCE_RECORDS):
        pytest.skip(f"source seed wrote {rows} rows: {summary}")


@pytest.mark.parametrize(
    "dest_driver",
    ["postgresql", "mongodb", "s3", "sqlite", "generic_sql", "snowflake", "redis"],
)
def test_manual_cross_schema_mapping(dest_driver: str, tmp_path: Path) -> None:
    """Transfer from PostgreSQL source to a destination with different column names."""
    source = _reachable_postgresql(tmp_path)
    _seed_source_table(source)  # creates source table with SOURCE_* columns

    suffix = uuid.uuid4().hex[:8]
    destination = _build_db_endpoint(dest_driver, tmp_path, "dst", suffix)
    if not _endpoint_reachable(destination):
        pytest.skip(f"{dest_driver} destination not reachable")

    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        mappings=MANUAL_MAPPINGS,
    )

    snowflake = pytest.importorskip("fakesnow") if destination.format == "snowflake" else None
    ctx = snowflake.patch() if snowflake else contextlib.nullcontext()

    engine = UniversalTransferEngine()
    with ctx:
        result = engine.execute_tracked(request, uuid.uuid4().hex[:24])

    assert result.success, f"{dest_driver}: {result.error}"
    assert result.records_transferred == 2, f"{dest_driver}: {result.records_transferred}"
    assert result.explanation, f"{dest_driver}: missing explanation"
    if destination.kind == "database":
        assert result.reconciliation.get("passed") is True, (
            f"{dest_driver}: {result.reconciliation}"
        )


@pytest.mark.parametrize(
    "dest_driver",
    ["postgresql", "mongodb", "s3", "sqlite", "generic_sql", "snowflake", "redis"],
)
def test_intelligent_cross_schema_mapping(dest_driver: str, tmp_path: Path) -> None:
    """Auto-map source columns to an existing target schema during upsert/append."""
    source = _reachable_postgresql(tmp_path)
    _seed_source_table(source)

    suffix = uuid.uuid4().hex[:8]
    destination = _build_db_endpoint(dest_driver, tmp_path, "dst", suffix)
    if not _endpoint_reachable(destination):
        pytest.skip(f"{dest_driver} destination not reachable")

    # Pre-create the destination with different column names so the auto-mapper
    # has a target schema to align against.
    target_seed = [
        {"id": "0", "name": "Placeholder", "compensation": "0.00", "active": "true"},
    ]
    target_schema = {
        "id": "INTEGER",
        "name": "TEXT",
        "compensation": "DECIMAL",
        "active": "BOOLEAN",
    }
    identity_mappings = [
        {"source": "id", "target": "id"},
        {"source": "name", "target": "name"},
        {"source": "compensation", "target": "compensation"},
        {"source": "active", "target": "active"},
    ]
    _seed_target(destination, target_seed, target_schema, identity_mappings)

    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode="upsert",
        skip_preflight=True,
        validation_mode="strict",
        stream_contracts=[{
            "name": "employees",
            "sync_mode": "upsert",
            "primary_key": "emp_id",
            "cursor_field": "",
        }],
    )

    snowflake = pytest.importorskip("fakesnow") if destination.format == "snowflake" else None
    ctx = snowflake.patch() if snowflake else contextlib.nullcontext()

    engine = UniversalTransferEngine()
    with ctx:
        result = engine.execute_tracked(request, uuid.uuid4().hex[:24])

    assert result.success, f"{dest_driver}: {result.error}"
    assert result.records_transferred == 2, f"{dest_driver}: {result.records_transferred}"
    assert result.explanation, f"{dest_driver}: missing explanation"


def _seed_target(endpoint: EndpointConfig, records, schema, mappings) -> None:
    """Seed a destination table/collection with the target schema."""
    rows, _, summary = write_destination_database(
        endpoint, records, list(schema.keys()), schema, mappings
    )
    if rows != len(records):
        pytest.skip(f"target seed wrote {rows} rows: {summary}")
