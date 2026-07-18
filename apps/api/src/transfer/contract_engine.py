"""Contract lifecycle integration for the transfer engine.

Orchestrates loading, enforcing, creating, and finalizing DataContracts for
each transfer run. The contract layer converts the preflight gates into a
versioned, reusable agreement that can break the pipeline if violated.
"""

from __future__ import annotations

from typing import Any

try:
    from services.contract_store import get_contract_store
    from services.data_contract import (
        ContractEnforcer,
        ContractStatus,
        ContractViolation,
        build_contract_from_preflight,
    )
except ImportError:  # pragma: no cover - compatibility for tests
    from src.services.contract_store import get_contract_store
    from src.services.data_contract import (
        ContractEnforcer,
        ContractStatus,
        ContractViolation,
        build_contract_from_preflight,
    )


def enforce_or_create_contract(
    request: Any,
    schema: dict[str, str] | None,
    mappings: list[dict[str, Any]] | None,
    preflight: dict[str, Any] | None,
) -> str:
    """Return the active contract id for this transfer.

    If the request supplies a contract_id and enforce_contract is True, the
    stored contract is loaded and enforced against the current schema/mappings.
    The associated circuit breaker is also consulted: an OPEN breaker halts
    the transfer until the contract is re-signed or the recovery timeout elapses.
    Otherwise a new contract is generated from the preflight result and saved.
    """
    store = get_contract_store()
    if request.contract_id and getattr(request, "enforce_contract", True):
        contract = store.get_contract(request.contract_id)
        if contract is None:
            raise ContractViolation(
                f"Contract {request.contract_id} not found",
                violations=[{"rule": "contract_not_found", "contract_id": request.contract_id}],
            )
        breaker = store.get_breaker(contract.id)
        if not breaker.allow():
            raise ContractViolation(
                f"Circuit breaker for contract {contract.id} is OPEN; transfer blocked until recovery",
                violations=[{"rule": "circuit_breaker_open", "contract_id": contract.id, "state": breaker.state.value}],
            )
        enforcer = ContractEnforcer(contract)
        enforcer.enforce(request, sample_schema=schema or request.column_types or {})
        return contract.id

    if not getattr(request, "enforce_contract", True):
        return request.contract_id

    contract = build_contract_from_preflight(request, preflight, schema=schema, mappings=mappings)
    store.save_contract(contract)
    return contract.id


def finalize_contract(contract_id: str, success: bool) -> None:
    """Update the circuit breaker and optionally mark contract as broken."""
    if not contract_id:
        return
    store = get_contract_store()
    breaker = store.get_breaker(contract_id)
    if success:
        breaker.record_success()
    else:
        breaker.record_failure()
    store.save_breaker(breaker)

    contract = store.get_contract(contract_id)
    if contract and not success and breaker.state.value == "open":
        contract.status = ContractStatus.BROKEN
        store.save_contract(contract)
