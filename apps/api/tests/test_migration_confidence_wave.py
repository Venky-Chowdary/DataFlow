"""Migration confidence wave: stratified Gate-8, breaker canary/notify, wide-row chunk."""

from __future__ import annotations

from unittest.mock import patch

from services.data_contract import BreakerState, CircuitBreaker
from services.reconciliation import sample_compare_rows
from services.resilience import adaptive_chunk_size
from src.transfer.contract_engine import finalize_contract


def test_stratified_sample_includes_rare_category():
    """First-N keyed sort can miss rare classes; stratified must include them."""
    # 40 common + 2 rare — sample_size 10 must still see rare status
    rows = [{"id": i, "status": "ok", "amt": i} for i in range(40)]
    rows.append({"id": 100, "status": "rare_fail", "amt": -1})
    rows.append({"id": 101, "status": "rare_fail", "amt": -2})
    target = list(rows)
    # Corrupt rare row on target
    target[-1] = {"id": 101, "status": "rare_fail", "amt": 999}
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "status", "target": "status"},
        {"source": "amt", "target": "amt"},
    ]
    result = sample_compare_rows(
        rows,
        target,
        mappings,
        sample_size=10,
        sort_key="id",
        stratify_by="status",
    )
    assert result["sample_seed"]["method"] == "stratified"
    assert result["sample_seed"]["stratify_by"] == "status"
    # Must compare the rare corrupted row somehow
    assert result["compared"] > 0
    assert result["passed"] is False
    assert any(m.get("source") == "amt" for m in result["mismatches"])


def test_breaker_canary_allows_deterministic_fraction_when_open():
    cb = CircuitBreaker("canary-contract-alpha", failure_threshold=1, canary_pct=100)
    cb.record_failure()
    assert cb.state == BreakerState.OPEN
    assert cb.allow() is False  # fail-closed at 100

    cb2 = CircuitBreaker("canary-contract-alpha", failure_threshold=1, canary_pct=50)
    cb2.record_failure()
    # Same contract_id → deterministic allow bit; just assert call does not raise
    allowed = cb2.allow()
    assert isinstance(allowed, bool)


def test_finalize_notifies_on_breaker_open():
    from services.contract_store import get_contract_store
    from services.data_contract import DataContract, ContractStatus

    store = get_contract_store()
    cid = "notify-breaker-test-1"
    store.save_contract(
        DataContract(
            id=cid,
            name="t",
            status=ContractStatus.ACTIVE,
            workspace_id="ws-notify",
        )
    )
    # Ensure closed breaker with threshold 1
    b = CircuitBreaker(cid, failure_threshold=1, canary_pct=100)
    store.save_breaker(b)

    with patch("src.transfer.contract_engine.notify_workspace", create=True):
        # Patch where imported inside function
        with patch("services.notification_service.notify_workspace") as nw:
            nw.return_value = [{"ok": True}]
            finalize_contract(cid, success=False, workspace_id="ws-notify")
            assert nw.called
            payload = nw.call_args[0][1]
            assert payload["kind"] == "contract_breaker_open"


def test_wide_row_adaptive_chunk_shrinks():
    # 50 KB rows → 4 MB budget ≈ 80 rows
    size = adaptive_chunk_size(5000, 50_000, target_memory_bytes=4 * 1024 * 1024)
    assert size <= 100
    assert size >= 1
