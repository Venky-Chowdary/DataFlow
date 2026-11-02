"""A workspace brief must count every contract, not the first page of 50."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from services.contract_store import InMemoryContractStore
from services.data_contract import ContractStatus, DataContract

_BRIEFING = (
    Path(__file__).resolve().parents[1] / "src" / "ai" / "copilot" / "workspace_briefing.py"
)


def _briefing():
    spec = importlib.util.spec_from_file_location("workspace_briefing_census", _BRIEFING)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_memory_store_counts_past_the_list_page() -> None:
    store = InMemoryContractStore()
    for i in range(60):
        store.save_contract(
            DataContract(
                id=f"c{i}",
                name=f"c{i}",
                status=ContractStatus.DRAFT if i < 7 else ContractStatus.SIGNED,
            )
        )
    assert len(store.list_contracts(limit=50)) == 50
    counts = store.count_contracts_by_status()
    assert sum(counts.values()) == 60
    assert counts["draft"] == 7
    assert counts["signed"] == 53


def test_briefing_counts_past_the_list_page(monkeypatch) -> None:
    store = InMemoryContractStore()
    for i in range(80):
        store.save_contract(
            DataContract(
                id=f"c{i}",
                name=f"c{i}",
                status=ContractStatus.SIGNED if i % 2 == 0 else ContractStatus.DRAFT,
            )
        )
    monkeypatch.setattr("services.contract_store.get_contract_store", lambda: store)
    facts = _briefing()._contract_facts("")
    assert facts["contract_count"] == 80
    assert facts["contracts_unsigned"] == 40
    assert facts["contracts_by_status"] == {"signed": 40, "draft": 40}


def test_short_population_is_counted_exactly(monkeypatch) -> None:
    store = InMemoryContractStore()
    store.save_contract(DataContract(id="d", name="d", status=ContractStatus.DRAFT))
    store.save_contract(DataContract(id="s", name="s", status=ContractStatus.SIGNED))
    monkeypatch.setattr("services.contract_store.get_contract_store", lambda: store)
    facts = _briefing()._contract_facts("")
    assert (facts["contract_count"], facts["contracts_unsigned"]) == (2, 1)
