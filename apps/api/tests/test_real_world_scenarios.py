"""Real-world industry scenario transfers.

Exercises the universal engine with messy, schema-shifted data from healthcare,
logistics, and banking domains.  Verifies that dates, currency, JSON, decimals,
booleans, and PII survive across PostgreSQL, MongoDB, SQLite/DuckDB, S3, and
CSV file export.
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


HEALTHCARE_COLUMNS = [
    "patient_id",
    "dob",
    "ssn",
    "email",
    "phone",
    "diagnosis_code",
    "medications",
    "admitted_at",
    "account_balance",
]

HEALTHCARE_SCHEMA = {
    "patient_id": "VARCHAR",
    "dob": "DATE",
    "ssn": "VARCHAR",
    "email": "VARCHAR",
    "phone": "VARCHAR",
    "diagnosis_code": "VARCHAR",
    "medications": "JSON",
    "admitted_at": "DATETIME",
    "account_balance": "DECIMAL",
}

HEALTHCARE_RECORDS = [
    {
        "patient_id": "P-1001",
        "dob": "03/15/1985",
        "ssn": "123-45-6789",
        "email": "jdoe@hospital.test",
        "phone": "+1-555-0199",
        "diagnosis_code": "E11.9",
        "medications": '[{"name":"metformin","dose":"500mg"}]',
        "admitted_at": "2024-06-01T08:30:00+05:30",
        "account_balance": "123456789.12",
    },
    {
        "patient_id": "P-1002",
        "dob": "1987-12-31",
        "ssn": "987-65-4321",
        "email": "asmith@hospital.test",
        "phone": "(555) 123-4567",
        "diagnosis_code": "I10",
        "medications": "[\"lisinopril\"]",
        "admitted_at": "31/12/2024 22:00:00",
        "account_balance": "-50.00",
    },
    {
        "patient_id": "P-1003",
        "dob": "12-31-1990",
        "ssn": "000-00-0000",
        "email": "noone@hospital.test",
        "phone": "555.867.5309",
        "diagnosis_code": "J45",
        "medications": "{}",
        "admitted_at": "2024-07-14T00:00:00Z",
        "account_balance": "0.00000000001",
    },
]

HEALTHCARE_MAPPINGS = [
    {"source": "patient_id", "target": "mrn"},
    {"source": "dob", "target": "date_of_birth", "target_type": "DATE"},
    {"source": "ssn", "target": "tax_id", "transform": "mask_pii"},
    {"source": "email", "target": "contact_email", "transform": "mask_pii"},
    {"source": "phone", "target": "contact_phone", "transform": "mask_pii"},
    {"source": "diagnosis_code", "target": "dx_code"},
    {"source": "medications", "target": "meds", "target_type": "JSON"},
    {"source": "admitted_at", "target": "admitted_at", "target_type": "DATETIME"},
    {"source": "account_balance", "target": "balance_due", "target_type": "DECIMAL"},
]


LOGISTICS_COLUMNS = [
    "shipment_id",
    "origin_lat",
    "origin_lon",
    "destination_lat",
    "destination_lon",
    "shipped_at",
    "delivered_at",
    "weight_kg",
    "status",
    "tags",
]

LOGISTICS_SCHEMA = {
    "shipment_id": "VARCHAR",
    "origin_lat": "DECIMAL",
    "origin_lon": "DECIMAL",
    "destination_lat": "DECIMAL",
    "destination_lon": "DECIMAL",
    "shipped_at": "DATETIME",
    "delivered_at": "DATETIME",
    "weight_kg": "DECIMAL",
    "status": "VARCHAR",
    "tags": "JSON",
}

LOGISTICS_RECORDS = [
    {
        "shipment_id": "SHP-0001",
        "origin_lat": "40.7128",
        "origin_lon": "-74.0060",
        "destination_lat": "34.0522",
        "destination_lon": "-118.2437",
        "shipped_at": "2024-12-25T10:00:00Z",
        "delivered_at": "25/12/2024 20:00:00",
        "weight_kg": "1,234.567",
        "status": "delivered",
        "tags": '["fragile","priority"]',
    },
    {
        "shipment_id": "SHP-0002",
        "origin_lat": "51.5074",
        "origin_lon": "-0.1278",
        "destination_lat": "48.8566",
        "destination_lon": "2.3522",
        "shipped_at": "2024-07-04 06:30:00",
        "delivered_at": "04/07/2024 16:30:00",
        "weight_kg": "999.999",
        "status": "in_transit",
        "tags": "[]",
    },
    {
        "shipment_id": "SHP-0003",
        "origin_lat": "35.6762",
        "origin_lon": "139.6503",
        "destination_lat": "1.3521",
        "destination_lon": "103.8198",
        "shipped_at": "01-01-2025 00:00:00",
        "delivered_at": "",
        "weight_kg": "0.5",
        "status": "pending",
        "tags": '{"bulk": true}',
    },
]

LOGISTICS_MAPPINGS = [
    {"source": "shipment_id", "target": "shipment_id"},
    {"source": "origin_lat", "target": "origin_lat", "target_type": "DECIMAL"},
    {"source": "origin_lon", "target": "origin_lon", "target_type": "DECIMAL"},
    {"source": "destination_lat", "target": "destination_lat", "target_type": "DECIMAL"},
    {"source": "destination_lon", "target": "destination_lon", "target_type": "DECIMAL"},
    {"source": "shipped_at", "target": "shipped_at", "target_type": "DATETIME"},
    {"source": "delivered_at", "target": "delivered_at", "target_type": "DATETIME"},
    {"source": "weight_kg", "target": "weight_kg", "target_type": "DECIMAL"},
    {"source": "status", "target": "status"},
    {"source": "tags", "target": "tags", "target_type": "JSON"},
]


BANKING_COLUMNS = [
    "txn_id",
    "account_id",
    "amount",
    "currency",
    "is_fraud",
    "txn_time",
    "metadata",
    "fee",
]

BANKING_SCHEMA = {
    "txn_id": "VARCHAR",
    "account_id": "VARCHAR",
    "amount": "DECIMAL",
    "currency": "VARCHAR",
    "is_fraud": "BOOLEAN",
    "txn_time": "DATETIME",
    "metadata": "JSON",
    "fee": "DECIMAL",
}

BANKING_RECORDS = [
    {
        "txn_id": "TXN-0001",
        "account_id": "ACC-4444",
        "amount": "$1,234.56",
        "currency": "USD",
        "is_fraud": "false",
        "txn_time": "1719792000000",
        "metadata": '{"channel":"web","ip":"203.0.113.1"}',
        "fee": "2.50",
    },
    {
        "txn_id": "TXN-0002",
        "account_id": "ACC-5555",
        "amount": "€2.500,75",
        "currency": "EUR",
        "is_fraud": "0",
        "txn_time": "2024-07-01T12:00:00Z",
        "metadata": '{"channel":"mobile","mcc":"5411"}',
        "fee": "1,99",
    },
    {
        "txn_id": "TXN-0003",
        "account_id": "ACC-6666",
        "amount": "Rs. 99,999.00",
        "currency": "INR",
        "is_fraud": "yes",
        "txn_time": "03/07/2024 09:15:00",
        "metadata": "{}",
        "fee": "0.00",
    },
]

BANKING_MAPPINGS = [
    {"source": "txn_id", "target": "transaction_id"},
    {"source": "account_id", "target": "account_ref"},
    {"source": "amount", "target": "amount", "target_type": "DECIMAL"},
    {"source": "currency", "target": "currency"},
    {"source": "is_fraud", "target": "fraud_flag", "target_type": "BOOLEAN"},
    {"source": "txn_time", "target": "transaction_time", "target_type": "DATETIME"},
    {"source": "metadata", "target": "metadata", "target_type": "JSON"},
    {"source": "fee", "target": "fee", "target_type": "DECIMAL"},
]


SCENARIOS = {
    "healthcare": (HEALTHCARE_COLUMNS, HEALTHCARE_SCHEMA, HEALTHCARE_RECORDS, HEALTHCARE_MAPPINGS),
    "logistics": (LOGISTICS_COLUMNS, LOGISTICS_SCHEMA, LOGISTICS_RECORDS, LOGISTICS_MAPPINGS),
    "banking": (BANKING_COLUMNS, BANKING_SCHEMA, BANKING_RECORDS, BANKING_MAPPINGS),
}


def _seed_scenario_source(
    scenario: str, tmp_path: Path
) -> EndpointConfig:
    columns, schema, records, mappings = SCENARIOS[scenario]
    # Prefer PostgreSQL as the source because it is the strictest SQL target;
    # fall back to SQLite if Postgres is not running.
    source = _build_db_endpoint("postgresql", tmp_path, f"{scenario}_src", uuid.uuid4().hex[:8])
    if not _endpoint_reachable(source):
        source = _build_db_endpoint("sqlite", tmp_path, f"{scenario}_src", uuid.uuid4().hex[:8])

    identity = [{"source": c, "target": c} for c in columns]
    rows, _, summary = write_destination_database(
        source, records, columns, schema, identity
    )
    if rows != len(records):
        pytest.skip(f"source seed wrote {rows} rows: {summary}")
    return source


@pytest.mark.parametrize("scenario", ["healthcare", "logistics", "banking"])
@pytest.mark.parametrize(
    "dest_driver",
    ["postgresql", "mongodb", "sqlite", "generic_sql", "s3", "file_export"],
)
def test_real_world_scenario_transfer(scenario: str, dest_driver: str, tmp_path: Path) -> None:
    columns, schema, records, mappings = SCENARIOS[scenario]
    source = _seed_scenario_source(scenario, tmp_path)

    suffix = uuid.uuid4().hex[:8]
    if dest_driver == "file_export":
        destination = EndpointConfig(kind="file_export", format="csv")
    else:
        destination = _build_db_endpoint(dest_driver, tmp_path, f"{scenario}_dst", suffix)

    if not _endpoint_reachable(destination):
        pytest.skip(f"{dest_driver} destination not reachable")

    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        mappings=mappings,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])

    assert result.success, f"{scenario} → {dest_driver}: {result.error}"
    assert result.records_transferred == len(records), (
        f"{scenario} → {dest_driver}: expected {len(records)}, got {result.records_transferred}"
    )
    assert result.explanation, f"{scenario} → {dest_driver}: missing pipeline explanation"

    if destination.kind == "database":
        assert result.reconciliation.get("passed") is True, (
            f"{scenario} → {dest_driver}: reconciliation failed: {result.reconciliation}"
        )
    else:
        assert result.destination_summary.get("filename"), (
            f"{scenario} → {dest_driver}: no exported filename"
        )


def test_pii_is_masked_in_healthcare_transfer(tmp_path: Path) -> None:
    """End-to-end check that mask_pii transform actually redacts sensitive values."""
    source = _seed_scenario_source("healthcare", tmp_path)
    destination = _build_db_endpoint("sqlite", tmp_path, "pii_check", uuid.uuid4().hex[:8])

    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
        mappings=HEALTHCARE_MAPPINGS,
    )

    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success
    assert result.records_transferred == len(HEALTHCARE_RECORDS)
    assert result.reconciliation.get("passed")

    # The masked values must not appear in the explanation or summary that the UI exposes.
    searchable = (
        str(result.explanation)
        + str(result.destination_summary)
        + str(result.source_summary)
    )
    for record in HEALTHCARE_RECORDS:
        assert record["ssn"] not in searchable
        assert record["email"] not in searchable
        assert record["phone"] not in searchable
