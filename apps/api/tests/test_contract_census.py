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


def test_full_page_uses_the_store_census(monkeypatch) -> None:
    module = _briefing()
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
    page = [{"status": "signed"} for _ in range(module._CONTRACT_PAGE)]
    total, unsigned = module._contract_census("", page)
    assert total == 80
    assert unsigned == 40


def test_short_page_is_the_population() -> None:
    module = _briefing()
    total, unsigned = module._contract_census(
        "",
        [{"status": "draft"}, {"status": "signed"}],
    )
    assert (total, unsigned) == (2, 1)
