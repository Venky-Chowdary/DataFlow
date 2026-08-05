"""Mapping proof must not invent create-new from mapping stamps alone."""

from __future__ import annotations

from src.transfer.engine import _mapping_proof_for_request
from src.transfer.models import EndpointConfig, TransferRequest


def _request(*, table_exists) -> TransferRequest:
    extra: dict = {}
    if table_exists is not None:
        extra["table_exists"] = table_exists
    dest = EndpointConfig(
        kind="database",
        format="postgresql",
        connector_id="dst-1",
        table="users",
        schema="railway",
        extra=extra,
    )
    src = EndpointConfig(
        kind="database",
        format="mysql",
        connector_id="src-1",
        table="users",
    )
    return TransferRequest(
        source=src,
        destination=dest,
        mappings=[
            {
                "source": "id",
                "target": "id",
                "confidence": 0.93,
                "create_new": True,
                "assignment_strategy": "identity_passthrough",
            }
        ],
        sync_mode="full_refresh_append",
    )


def test_mapping_proof_unknown_existence_is_schema_pending_not_create_new() -> None:
    """Missing extra.table_exists must not forge create_new (railway.users case)."""
    proof = _mapping_proof_for_request(_request(table_exists=None))
    assert proof.get("dest_mode") == "schema_pending"
    assert proof.get("dest_mode") != "create_new"


def test_mapping_proof_honors_live_table_exists_true() -> None:
    proof = _mapping_proof_for_request(_request(table_exists=True))
    assert proof.get("dest_mode") == "match_existing"


def test_mapping_proof_honors_confirmed_missing_table() -> None:
    proof = _mapping_proof_for_request(_request(table_exists=False))
    assert proof.get("dest_mode") == "create_new"
