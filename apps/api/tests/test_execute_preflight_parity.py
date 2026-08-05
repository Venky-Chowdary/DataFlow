"""Execute preflight kwargs must match Validate-grade privilege / FK / identity."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from src.transfer.engine import (
    _execute_preflight_parity_kwargs,
    _preflight_sample_rows,
)
from src.transfer.models import EndpointConfig, TransferRequest


def _request(**kwargs: Any) -> TransferRequest:
    src = EndpointConfig(
        kind="database",
        format="mysql",
        connector_id="src-1",
        table="users",
    )
    dst = EndpointConfig(
        kind="database",
        format="postgresql",
        connector_id="dst-1",
        table="users",
        schema="public",
    )
    base = dict(
        source=src,
        destination=dst,
        mappings=[{"source": "id", "target": "id", "confidence": 0.99}],
        sync_mode="full_refresh_append",
        stream_contracts=[
            {"name": "users", "selected": True, "primary_key": "id"},
            {"name": "orders", "selected": True, "primary_key": "order_id"},
        ],
        compliance_acknowledged=True,
        acknowledgment_actor="admin@dataflow.app",
        acknowledgment_reason="PII approved for pilot",
    )
    base.update(kwargs)
    return TransferRequest(**base)


def test_parity_kwargs_prefer_stream_matched_contract_pk() -> None:
    meta = {
        "connected": True,
        "table_exists": False,
        "can_create_table": False,
        "can_write": True,
        "primary_key_columns": [],
        "unique_keys": [],
        "foreign_keys": [{"columns": ["org_id"], "ref_table": "orgs"}],
        "privilege_probe": {
            "can_create_table": False,
            "can_write": True,
            "detail": "INSERT yes, CREATE no",
        },
    }
    with patch(
        "services.preflight_service.inspect_destination_for_preflight",
        return_value=meta,
    ):
        kw = _execute_preflight_parity_kwargs(_request(), destination_connected=True)
    assert kw["contract_primary_key"] == "id"
    assert kw["destination_can_create"] is False
    assert kw["destination_can_write"] is True
    assert kw["privilege_probe"]["can_create_table"] is False
    assert kw["destination_foreign_keys"][0]["ref_table"] == "orgs"
    assert kw["compliance_acknowledged"] is True
    assert kw["acknowledgment_actor"] == "admin@dataflow.app"
    # Must not invent create from connectivity when privilege says no.
    assert kw["destination_can_create"] is not True or meta["can_create_table"] is True


def test_parity_kwargs_never_invent_create_when_privilege_denies() -> None:
    meta = {
        "connected": True,
        "table_exists": False,
        "can_create_table": False,
        "can_write": False,
        "primary_key_columns": ["id"],
        "unique_keys": [],
        "foreign_keys": [],
        "privilege_probe": {"can_create_table": False, "can_write": False},
    }
    with patch(
        "services.preflight_service.inspect_destination_for_preflight",
        return_value=meta,
    ):
        kw = _execute_preflight_parity_kwargs(_request(), destination_connected=True)
    assert kw["destination_can_create"] is False
    assert kw["destination_can_write"] is False


def test_preflight_sample_rows_match_validate_cap() -> None:
    from services.coercion_probe import PREFLIGHT_SAMPLE_LIMIT

    rows = [{"id": i} for i in range(PREFLIGHT_SAMPLE_LIMIT + 200)]
    sample = _preflight_sample_rows(rows)
    assert len(sample) == PREFLIGHT_SAMPLE_LIMIT
    # Legacy Execute hard-cap of 100 must not remain.
    assert len(sample) > 100
