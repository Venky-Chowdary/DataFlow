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
