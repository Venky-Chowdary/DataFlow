"""DataContract, CircuitBreaker, and ContractEnforcer unit tests."""

from __future__ import annotations

import pytest

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.services.data_contract import (
    CircuitBreaker,
    BreakerState,
    ContractEnforcer,
    ContractStatus,
    ContractViolation,
    DataContract,
    ColumnRule,
    build_contract_from_preflight,
)
from src.services.contract_store import InMemoryContractStore, get_contract_store, reset_contract_store
from src.transfer.models import EndpointConfig, TransferRequest


@pytest.fixture(autouse=True)
def _reset_store():
    reset_contract_store()
    yield


def test_circuit_breaker_opens_after_failures():
    cb = CircuitBreaker("c1", failure_threshold=2, recovery_timeout_seconds=60.0)
    assert cb.allow() is True
    cb.record_failure()
    assert cb.state == BreakerState.CLOSED
    assert cb.allow() is True
    cb.record_failure()
    assert cb.state == BreakerState.OPEN
    assert cb.allow() is False


def test_circuit_breaker_half_open_then_closes():
    cb = CircuitBreaker("c2", failure_threshold=1, recovery_timeout_seconds=0.0, half_open_max=1)
    cb.record_failure()
    assert cb.state == BreakerState.OPEN
    assert cb.allow() is True
    assert cb.state == BreakerState.HALF_OPEN
    cb.record_success()
    assert cb.state == BreakerState.CLOSED


def test_circuit_breaker_half_open_then_reopens():
    cb = CircuitBreaker("c3", failure_threshold=1, recovery_timeout_seconds=0.0, half_open_max=1)
    cb.record_failure()
    assert cb.allow() is True
    cb.record_failure()
    assert cb.state == BreakerState.OPEN


def test_contract_enforcer_blocks_missing_required_column():
    contract = DataContract(
        columns=[ColumnRule(source_name="id", target_name="id", source_type="INTEGER", target_type="INTEGER", nullable=False)],
    )
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="postgresql"),
    )
    enforcer = ContractEnforcer(contract)
    with pytest.raises(ContractViolation) as exc:
        enforcer.enforce(request, sample_schema={"amount": "DECIMAL"})
    assert exc.value.violations[0]["rule"] == "required_column"


def test_contract_enforcer_blocks_type_change():
    contract = DataContract(
        columns=[ColumnRule(source_name="id", target_name="id", source_type="INTEGER", target_type="INTEGER")],
    )
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="postgresql"),
    )
    enforcer = ContractEnforcer(contract)
    with pytest.raises(ContractViolation) as exc:
        enforcer.enforce(request, sample_schema={"id": "TEXT"})
    assert exc.value.violations[0]["rule"] == "source_type_change"


def test_contract_enforcer_allows_superset_columns():
    contract = DataContract(
        columns=[ColumnRule(source_name="id", target_name="id", source_type="INTEGER", target_type="INTEGER", nullable=True)],
    )
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="postgresql"),
    )
    enforcer = ContractEnforcer(contract)
    enforcer.enforce(request, sample_schema={"id": "INTEGER", "extra": "TEXT"})


def test_build_contract_from_preflight_creates_columns_and_mappings():
    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="postgresql"),
    )
    schema = {"id": "INTEGER", "name": "VARCHAR"}
    mappings = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]
    pf = {
        "gates": [
            {"id": "g1", "status": "pass", "message": "ok"},
        ],
        "readiness_score": 90,
    }
    contract = build_contract_from_preflight(request, pf, schema=schema, mappings=mappings)
    assert contract.id
    assert len(contract.columns) == 2
    assert contract.columns[0].source_name == "id"
    assert contract.metadata["readiness_score"] == 90


def test_in_memory_store_roundtrip():
    store = InMemoryContractStore()
    contract = DataContract(name="test")
    store.save_contract(contract)
    loaded = store.get_contract(contract.id)
    assert loaded.name == "test"
    breaker = store.get_breaker(contract.id)
    breaker.record_failure()
    store.save_breaker(breaker)
    reloaded = store.get_breaker(contract.id)
    assert reloaded.failure_count == 1


def test_get_contract_store_default_is_mongo_with_fallback():
    store = get_contract_store()
    # Without a real MongoDB, the store falls back to in-memory for get/save.
    contract = DataContract(name="default-test")
    store.save_contract(contract)
    assert store.get_contract(contract.id).name == "default-test"
