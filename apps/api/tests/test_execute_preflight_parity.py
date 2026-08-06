"""Execute preflight kwargs must match Validate-grade privilege / FK / identity."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from src.transfer.engine import (
    _execute_policy_gates_for_request,
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


def test_parity_kwargs_preserves_composite_stream_contract_pk() -> None:
    meta = {
        "connected": True,
        "table_exists": True,
        "can_create_table": True,
        "can_write": True,
        "primary_key_columns": [],
        "unique_keys": [],
        "foreign_keys": [],
        "privilege_probe": {},
    }
    req = _request(
        stream_contracts=[
            {
                "name": "users",
                "selected": True,
                "primary_key": ["org_id", "code"],
            }
        ]
    )
    with patch(
        "services.preflight_service.inspect_destination_for_preflight",
        return_value=meta,
    ):
        kw = _execute_preflight_parity_kwargs(req, destination_connected=True)
    assert kw["contract_primary_key"] == "org_id,code"
    assert kw["stream_contracts"][0]["primary_key"] == ["org_id", "code"]


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


def test_parity_kwargs_never_invent_create_when_inspect_empty() -> None:
    """Failed/empty inspect must not invent can_create from connectivity alone."""
    with patch(
        "services.preflight_service.inspect_destination_for_preflight",
        return_value={"connected": True},
    ):
        kw = _execute_preflight_parity_kwargs(_request(), destination_connected=True)
    assert kw["destination_can_create"] is False


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


def test_parity_owns_destination_table_exists_no_duplicate_kwarg() -> None:
    """Regression: Execute crashed with multiple values for destination_table_exists."""
    meta = {
        "connected": True,
        "table_exists": False,
        "can_create_table": True,
        "can_write": True,
        "primary_key_columns": [],
        "unique_keys": [],
        "foreign_keys": [],
        "privilege_probe": {},
    }
    with patch(
        "services.preflight_service.inspect_destination_for_preflight",
        return_value=meta,
    ):
        parity = _execute_preflight_parity_kwargs(
            _request(),
            destination_connected=True,
            destination_table_exists_fallback=True,
        )
    assert "destination_table_exists" in parity
    # Inspect wins over fallback when known.
    assert parity["destination_table_exists"] is False

    # Simulate engine merge: explicit call-site kwargs + **parity must not collide.
    call_site = {
        "columns": ["id"],
        "column_types": {"id": "VARCHAR"},
        "row_count": 1,
        "mappings": [{"source": "id", "target": "id"}],
        "destination_connected": True,
        "destination_db_type": "postgresql",
    }
    overlap = set(call_site) & set(parity)
    assert not overlap, f"Execute call-site duplicates parity keys: {overlap}"

    def _accept(**kwargs: Any) -> dict[str, Any]:
        return kwargs

    merged = _accept(**call_site, **parity)
    assert merged["destination_table_exists"] is False


def test_execute_policy_gates_pass_source_kind_and_dest_type() -> None:
    """CDC must not false-block as file source after Validate approved database."""
    gates = _execute_policy_gates_for_request(
        _request(
            sync_mode="cdc",
            stream_contracts=[
                {
                    "name": "users",
                    "selected": True,
                    "primary_key": "id",
                    "cursor_field": "updated_at",
                }
            ],
        ),
        source_columns=["id", "updated_at"],
    )
    g9 = next(g for g in gates if g["id"] == "g9_sync_contract")
    assert g9["status"] != "block" or "database source" not in str(g9.get("details") or "").lower()
    # Explicit: source_kind database from request.source.kind
    assert not any(
        "CDC requires a database source" in str(g.get("details") or g.get("message") or "")
        for g in gates
    )


def test_execute_policy_gates_scd2_honors_postgres_dest() -> None:
    gates = _execute_policy_gates_for_request(
        _request(
            sync_mode="scd2",
            stream_contracts=[
                {"name": "users", "selected": True, "primary_key": "id"}
            ],
        ),
        source_columns=["id"],
    )
    g9 = next(g for g in gates if g["id"] == "g9_sync_contract")
    assert "SQL table destination" not in str(g9.get("details") or "")


def test_parity_falls_back_table_exists_when_inspect_omits() -> None:
    meta = {
        "connected": True,
        "can_create_table": True,
        "can_write": True,
        "primary_key_columns": [],
        "unique_keys": [],
        "foreign_keys": [],
    }
    with patch(
        "services.preflight_service.inspect_destination_for_preflight",
        return_value=meta,
    ):
        parity = _execute_preflight_parity_kwargs(
            _request(),
            destination_connected=True,
            destination_table_exists_fallback=False,
        )
    assert parity["destination_table_exists"] is False
