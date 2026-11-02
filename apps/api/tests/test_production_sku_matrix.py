"""Production SKU transfer matrix.

Runs the 5 × 5 source × destination pairs we explicitly support and verifies
each route with two reference rows, full reconciliation, and a pipeline
explanation. Routes whose source or destination is not reachable on the
machine are skipped so the same test runs in CI and locally.

Enterprise proof bar: preflight stays ON (never skip_preflight). Rows moving
without Validate is not a PRODUCTION_SKU claim.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import TransferRequest
from src.transfer.registry import PRODUCTION_SKU

try:
    from test_execute_tracked_universal_matrix import (
        _NO_INDEPENDENT_VERIFIER,
        DESTINATION_NUMERIC_SOURCE_OVERRIDES,
        _build_destination,
        _build_source,
        _endpoint_reachable,
        _seed_source,
        _uses_snowflake,
    )
    from typed_fidelity_helpers import assert_preflight_ran
except Exception:  # pragma: no cover - fallback if import path differs
    from tests.test_execute_tracked_universal_matrix import (
        _NO_INDEPENDENT_VERIFIER,
        DESTINATION_NUMERIC_SOURCE_OVERRIDES,
        _build_destination,
        _build_source,
        _endpoint_reachable,
        _seed_source,
        _uses_snowflake,
    )
    from tests.typed_fidelity_helpers import assert_preflight_ran

# Studio-grade mappings: explicit confidence so G4/G5 match operator-approved maps.
# Bare {source,target} pairs score 0% and dishonestly fail Validate.
SKU_MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 0.99},
    {"source": "amount", "target": "amount", "confidence": 0.99},
]
# Mongo is key-addressed: Map must bind an identity. Business ``id`` → ``_id``
# (server ObjectId invent is insert-only; Validate still requires a Map PK).
SKU_MAPPINGS_MONGODB = [
    {"source": "id", "target": "_id", "confidence": 0.99},
    {"source": "amount", "target": "amount", "confidence": 0.99},
]
_FULL_RANGE_NUMERIC_CASES = {
    "salesforce": {
        "source_format": "snowflake",
        "records": [
            {"id": "1", "amount": "1000.1234567890"},
            {"id": "2", "amount": "2000.5000000000"},
        ],
        "approved_records": [
            {"id": "1", "amount": "1000.12"},
            {"id": "2", "amount": "2000.50"},
        ],
        "source_schema": {"id": "DECIMAL(38,0)", "amount": "DECIMAL(38,10)"},
        "target_types": {"id": "BIGINT", "amount": "DECIMAL(18,2)"},
        "risk_columns": {
            "id": ("DECIMAL(38,0)", "BIGINT"),
            "amount": ("DECIMAL(38,10)", "DECIMAL(18,2)"),
        },
    },
    "hubspot": {
        "source_format": "bigquery",
        "records": [
            {"id": "1", "amount": "1000.123456789012345678901234567890"},
            {"id": "2", "amount": "2000.500000000000000000000000000000"},
        ],
        "approved_records": [
            {"id": "1", "amount": "1000.12"},
            {"id": "2", "amount": "2000.50"},
        ],
        "source_schema": {"id": "INT64", "amount": "BIGNUMERIC"},
        "target_types": {"amount": "DECIMAL(38,2)"},
        "risk_columns": {
            "amount": ("BIGNUMERIC", "DECIMAL(38,2)"),
        },
    },
}
# Mongo reads back its own storage key. G13 accounts for every source field, so
# the ObjectId is a recorded omission rather than a column the run drops quietly.
_MONGO_OBJECT_ID_OMISSION = {
    "source": "_id",
    "target": "",
    "confidence": 0.0,
    "intentional_omit": True,
}
# Local SaaS stub returns extra catalog fields. G13: omit, never drop silently.
_STUB_SOURCE_OMITS = {
    "stripe": (
        {"source": "object", "target": "", "confidence": 1.0, "intentional_omit": True},
        {"source": "email", "target": "", "confidence": 1.0, "intentional_omit": True},
    ),
    "salesforce": (
        {"source": "Name", "target": "", "confidence": 1.0, "intentional_omit": True},
        {"source": "email", "target": "", "confidence": 1.0, "intentional_omit": True},
    ),
    "hubspot": (
        {"source": "Name", "target": "", "confidence": 1.0, "intentional_omit": True},
        {"source": "email", "target": "", "confidence": 1.0, "intentional_omit": True},
    ),
}


@pytest.mark.parametrize(
    "route",
    PRODUCTION_SKU,
    ids=lambda r: f"{r[0]}_{r[1]}_to_{r[2]}_{r[3]}",
)
def test_production_sku_transfer(
    route: tuple[str, str, str, str],
    tmp_path: Path,
    local_object_store: str,
    local_sftp: Any,
) -> None:
    src_kind, src_fmt, dst_kind, dst_fmt = route
    suffix = uuid.uuid4().hex[:12]

    source, source_content, source_filename = _build_source(
        src_kind,
        src_fmt,
        tmp_path,
        suffix,
        object_store=local_object_store,
        sftp_server=local_sftp,
    )
    destination = _build_destination(
        dst_kind,
        dst_fmt,
        tmp_path,
        suffix,
        object_store=local_object_store,
        sftp_server=local_sftp,
    )

    if not _endpoint_reachable(source):
        pytest.skip(f"source {src_kind}/{src_fmt} not reachable")
    if not _endpoint_reachable(destination):
        pytest.skip(f"destination {dst_kind}/{dst_fmt} not reachable")

    # Object-store SKU routes need a working pyexpat (botocore XML). Skip with
    # an environment reason — never claim product failure for a broken host XML.
    if src_fmt in {"s3", "gcs", "adls"} or dst_fmt in {"s3", "gcs", "adls"}:
        from services.runtime_checks import (
            python_xml_runtime_ok,
            python_xml_runtime_skip_reason,
        )

        if not python_xml_runtime_ok():
            pytest.skip(python_xml_runtime_skip_reason())

    validation_mode = "balanced" if dst_fmt in _NO_INDEPENDENT_VERIFIER else "strict"
    mappings = list(SKU_MAPPINGS_MONGODB if dst_fmt == "mongodb" else SKU_MAPPINGS)
    if src_fmt == "mongodb":
        mappings.append(dict(_MONGO_OBJECT_ID_OMISSION))
    for omit in _STUB_SOURCE_OMITS.get(src_fmt, ()):
        mappings.append(dict(omit))
    request = TransferRequest(
        source=source,
        destination=destination,
        source_content=source_content,
        source_filename=source_filename,
        # A Kafka topic is append-only: published messages are final, so the
        # destination refuses overwrite/dedupe/merge by design.
        sync_mode="full_refresh_append" if dst_fmt == "kafka" else "full_refresh_overwrite",
        # Preflight ON — PRODUCTION_SKU proves Validate + transfer, not write-only.
        skip_preflight=False,
        validation_mode=validation_mode,
        mappings=mappings,
    )

    if _uses_snowflake(source, destination):
        pytest.importorskip("fakesnow")

    engine = UniversalTransferEngine()
    saas_sources = {"salesforce", "hubspot", "stripe", "rest_api"}
    if source.kind == "database" and src_fmt not in saas_sources:
        numeric_override = (
            DESTINATION_NUMERIC_SOURCE_OVERRIDES.get(dst_fmt)
            if (src_fmt, dst_fmt) in {
                ("snowflake", "salesforce"),
                ("bigquery", "hubspot"),
            }
            else None
        )
        if numeric_override:
            _seed_source(source, **numeric_override)
        else:
            _seed_source(source)
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])

    err = result.error or ""
    assert result.success, f"{route}: {err}"
    assert_preflight_ran(result)
    assert result.records_transferred == 2, (
        f"{route}: expected 2 records, got {result.records_transferred}"
    )
    assert result.explanation, f"{route}: missing pipeline explanation"

    if destination.kind == "database" and dst_fmt not in _NO_INDEPENDENT_VERIFIER:
        assert result.reconciliation.get("passed") is True, (
            f"{route}: reconciliation failed: {result.reconciliation}"
        )
    elif destination.kind != "database":
        assert result.destination_summary.get("filename"), (
            f"{route}: no exported filename in destination_summary"
        )


def _numeric_risk_route(
    destination_format: str,
    tmp_path: Path,
    local_object_store: str,
    local_sftp: Any,
    *,
    with_contract: bool,
):
    from services.migration_risk_contract import create_migration_risk_contract

    case = _FULL_RANGE_NUMERIC_CASES[destination_format]
    source_format = case["source_format"]
    route = ("database", source_format, "database", destination_format)
    suffix = uuid.uuid4().hex[:12]
    source, source_content, source_filename = _build_source(
        "database",
        source_format,
        tmp_path,
        suffix,
        object_store=local_object_store,
        sftp_server=local_sftp,
    )
    destination = _build_destination(
        "database",
        destination_format,
        tmp_path,
        suffix,
        object_store=local_object_store,
        sftp_server=local_sftp,
    )
    if not _endpoint_reachable(source):
        pytest.skip(f"{route}: source emulator is unreachable")
    if not _endpoint_reachable(destination):
        pytest.skip(f"{route}: destination is unreachable")
    if _uses_snowflake(source, destination):
        __import__("fakesnow")
    records = case["approved_records"] if with_contract else case["records"]

    _seed_source(
        source,
        records=records,
        schema=case["source_schema"],
    )
    mappings = []
    for column in ("id", "amount"):
        mapping = {
            "source": column,
            "target": column,
            "confidence": 0.99,
        }
        if column in case["target_types"]:
            mapping["target_type"] = case["target_types"][column]
        if column in case["risk_columns"]:
            mapping["transform"] = "decimal"
        if with_contract and column in case["risk_columns"]:
            source_type, destination_type = case["risk_columns"][column]
            approved_by = "risk-approver@example.com"
            contract = create_migration_risk_contract(
                column=column,
                source_type=source_type,
                destination_type=destination_type,
                approved_by=approved_by,
                reason=f"Approve {source_type} to {destination_type} fidelity risk",
                execution_policy="CAST_AND_CONTINUE",
                expected_precision_loss=True,
                transform="decimal",
            ).to_dict()
            mapping.update(risk_acknowledged=True, risk_contract=contract)
        mappings.append(mapping)

    request = TransferRequest(
        source=source,
        destination=destination,
        source_content=source_content,
        source_filename=source_filename,
        sync_mode="full_refresh_overwrite",
        skip_preflight=False,
        validation_mode="balanced",
        mappings=mappings,
    )
    if with_contract:
        request.triggered_by = "risk-approver@example.com"
    return request


@pytest.mark.parametrize(
    "destination_format",
    ["salesforce", "hubspot"],
    ids=["snowflake_to_salesforce", "bigquery_to_hubspot"],
)
def test_full_range_numeric_routes_block_without_risk_contract(
    destination_format: str,
    tmp_path: Path,
    local_object_store: str,
    local_sftp: Any,
) -> None:
    request = _numeric_risk_route(
        destination_format,
        tmp_path,
        local_object_store,
        local_sftp,
        with_contract=False,
    )
    result = UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])

    assert not result.success, result.error
    gates = result.validation_plan.get("gates", [])
    blocked = {gate["id"] for gate in gates if gate.get("status") == "block"}
    assert {
        "g3_schema_contract",
        "g6_target_ddl",
        "g9_data_integrity",
        "schema_drift",
    } <= blocked
    blocked_columns = {
        issue.get("column")
        for gate in gates
        if gate.get("status") == "block"
        for issue in (gate.get("details") or {}).get("issues_detail", [])
    }
    expected_columns = set(_FULL_RANGE_NUMERIC_CASES[destination_format]["risk_columns"])
    assert expected_columns <= blocked_columns


@pytest.mark.parametrize(
    "destination_format",
    ["salesforce", "hubspot"],
    ids=["snowflake_to_salesforce", "bigquery_to_hubspot"],
)
def test_full_range_numeric_routes_require_signed_risk_contract_approval(
    destination_format: str,
    tmp_path: Path,
    local_object_store: str,
    local_sftp: Any,
) -> None:
    request = _numeric_risk_route(
        destination_format,
        tmp_path,
        local_object_store,
        local_sftp,
        with_contract=True,
    )
    job_id = uuid.uuid4().hex[:24]
    result = UniversalTransferEngine().execute_tracked(request, job_id)

    assert result.success, result.error
    assert result.records_transferred == 2
    from services.mongodb_service import get_mongodb_service

    job = get_mongodb_service().get_job(job_id)
    accepted = (job or {}).get("accepted_risks") or []
    expected_columns = set(_FULL_RANGE_NUMERIC_CASES[destination_format]["risk_columns"])
    assert {risk["column"] for risk in accepted} == expected_columns
    assert all(risk["approved_by"] == "risk-approver@example.com" for risk in accepted)
    assert all(risk["risk_id"] for risk in accepted)
