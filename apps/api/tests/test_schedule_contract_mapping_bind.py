"""Signed-contract mapping fingerprint must match the schedule beat."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from services.data_contract import ContractStatus, DataContract
from services.schema_fingerprint import fingerprint_mappings
from services.schedule_store import (
    assert_schedule_mapping_matches_contract,
    assert_schedule_run_allowed,
)


MAP_A = [{"source": "id", "target": "id", "confidence": 1.0, "transform": None}]
MAP_B = [
    {"source": "id", "target": "id", "confidence": 1.0, "transform": None},
    {"source": "email", "target": "email", "confidence": 0.9, "transform": None},
]


def test_mapping_bind_passes_when_fingerprints_match(monkeypatch):
    from services.contract_store import InMemoryContractStore, reset_contract_store

    reset_contract_store()
    store = InMemoryContractStore()
    monkeypatch.setattr("services.contract_store.get_contract_store", lambda: store)
    contract = DataContract(
        name="bind-ok",
        status=ContractStatus.SIGNED,
        mappings=list(MAP_A),
    )
    store.save_contract(contract)
    sched = SimpleNamespace(
        contract_id=contract.id,
        require_signed_contract=True,
        mappings=list(MAP_A),
    )
    assert_schedule_mapping_matches_contract(sched)
    preview = assert_schedule_run_allowed(sched)
    assert preview["contract_id"] == contract.id
    assert fingerprint_mappings(MAP_A) == fingerprint_mappings(contract.mappings)


def test_confidence_and_passthrough_spelling_do_not_drift_the_contract():
    """A re-plan must not mint a new contract hash for the same columns."""
    signed = [
        {"source": "id", "target": "id", "transform": None, "confidence": 0.91},
        {"source_column": "region", "target_column": "region", "transform": "none", "confidence": 1},
    ]
    replanned = [
        {"source": "id", "target": "id", "transform": "identity", "confidence": 0.5},
        {"source": "region", "target": "region", "transform": "", "confidence": 0.2},
    ]
    assert fingerprint_mappings(signed) == fingerprint_mappings(replanned)


def test_signed_contract_rows_are_what_the_schedule_stores(monkeypatch):
    from services.contract_store import InMemoryContractStore, reset_contract_store
    from services.schedule_store import mappings_bound_to_signed_contract

    reset_contract_store()
    store = InMemoryContractStore()
    monkeypatch.setattr("services.contract_store.get_contract_store", lambda: store)
    signed = [
        {"source": "id", "target": "id", "transform": "none", "confidence": 0.88},
        {"source": "region", "target": "region", "transform": None, "confidence": 0.7},
    ]
    contract = DataContract(name="bind-same-columns", status=ContractStatus.SIGNED, mappings=signed)
    store.save_contract(contract)
    planned = [
        {"source": "id", "target": "id", "transform": "", "confidence": 1.0},
        {"source": "region", "target": "region", "transform": "identity", "confidence": 0.4},
    ]
    stored = mappings_bound_to_signed_contract(contract.id, planned)
    assert stored == signed
    sched = SimpleNamespace(
        contract_id=contract.id,
        require_signed_contract=True,
        mappings=stored,
    )
    assert_schedule_mapping_matches_contract(sched)


def test_a_different_column_binding_is_refused_before_the_schedule_is_stored(monkeypatch):
    from services.contract_store import InMemoryContractStore, reset_contract_store
    from services.schedule_store import mappings_bound_to_signed_contract

    reset_contract_store()
    store = InMemoryContractStore()
    monkeypatch.setattr("services.contract_store.get_contract_store", lambda: store)
    contract = DataContract(
        name="bind-drift-columns",
        status=ContractStatus.SIGNED,
        mappings=list(MAP_A),
    )
    store.save_contract(contract)
    with pytest.raises(ValueError, match="do not match signed contract"):
        mappings_bound_to_signed_contract(contract.id, list(MAP_B))


def test_mapping_bind_refuses_drifted_schedule(monkeypatch):
    from services.contract_store import InMemoryContractStore, reset_contract_store

    reset_contract_store()
    store = InMemoryContractStore()
    monkeypatch.setattr("services.contract_store.get_contract_store", lambda: store)
    contract = DataContract(
        name="bind-drift",
        status=ContractStatus.SIGNED,
        mappings=list(MAP_A),
    )
    store.save_contract(contract)
    with pytest.raises(ValueError, match="do not match signed contract"):
        assert_schedule_run_allowed(
            SimpleNamespace(
                contract_id=contract.id,
                require_signed_contract=True,
                mappings=list(MAP_B),
            )
        )


def test_mapping_bind_skips_when_contract_has_no_mappings(monkeypatch):
    from services.contract_store import InMemoryContractStore, reset_contract_store

    reset_contract_store()
    store = InMemoryContractStore()
    monkeypatch.setattr("services.contract_store.get_contract_store", lambda: store)
    contract = DataContract(name="bind-empty", status=ContractStatus.SIGNED, mappings=[])
    store.save_contract(contract)
    assert_schedule_mapping_matches_contract(
        SimpleNamespace(contract_id=contract.id, mappings=list(MAP_B))
    )
