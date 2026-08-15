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


def resolve_bound_contract(
    *,
    explicit_id: str = "",
    explicit_require: bool | None = None,
    policies: dict[str, Any] | None = None,
) -> tuple[str, bool]:
    """Resolve opt-in contract bind. Explicit request fields win; else plan policies.

    An empty explicit id does **not** mean "clear the bind" when a plan carries
    ``contract_id`` — SDK / ``plan_id`` Execute often omit the form fields.
    Selecting a contract defaults require-signed, matching Studio and schedules.
    """
    cid = str(explicit_id or "").strip()
    if cid:
        require = True if explicit_require is None else bool(explicit_require)
        return cid, require
    plan = policies or {}
    plan_cid = str(plan.get("contract_id") or "").strip()
    if plan_cid:
        if "require_signed_contract" in plan:
            return plan_cid, bool(plan.get("require_signed_contract"))
        return plan_cid, True
    return "", bool(explicit_require)


def stamp_request_contract(
    request: Any,
    *,
    explicit_id: str = "",
    explicit_require: bool = False,
    policies: dict[str, Any] | None = None,
) -> None:
    """Stamp Execute/replay bind. Explicit id wins; else plan policies.

    ``require_signed`` without an id still fail-closes when the plan is unbound
    (same as schedules). Form defaults of false do not wipe a stored plan bind.
    """
    cid = str(explicit_id or "").strip()
    resolved_id, require = resolve_bound_contract(
        explicit_id=cid,
        explicit_require=bool(explicit_require) if cid else None,
        policies=policies,
    )
    if not resolved_id and explicit_require and not cid:
        require = True
    stamp_bound_contract(request, contract_id=resolved_id, require_signed=require)


def stamp_bound_contract(
    request: Any,
    *,
    contract_id: str = "",
    require_signed: bool = False,
) -> None:
    """Bind an operator-selected contract before enqueue. Fail-closed if SIGNED is required."""
    cid = str(contract_id or "").strip()
    require = bool(require_signed)
    if cid or require:
        from services.schedule_store import assert_signed_contract

        assert_signed_contract(cid, require_signed=require)
    if cid:
        request.contract_id = cid
        request.enforce_contract = True
        request.require_signed_contract = require


def enforce_bound_contract(
    request: Any,
    schema: dict[str, str] | None = None,
    mappings: list[dict[str, Any]] | None = None,
) -> str:
    """Enforce a bound contract. Never auto-create from missing preflight.

    Quarantine replay and other skip_preflight writers must call this instead
    of ``enforce_or_create_contract`` so an unbound job does not invent a draft.
    """
    cid = str(getattr(request, "contract_id", "") or "").strip()
    if not cid or not getattr(request, "enforce_contract", True):
        return cid
    return enforce_or_create_contract(request, schema, mappings, preflight=None)


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
        require_signed = bool(getattr(request, "require_signed_contract", False))
        enforcer = ContractEnforcer(contract)
        enforcer.enforce(
            request,
            sample_schema=schema or request.column_types or {},
            require_signed=require_signed,
        )
        return contract.id

    if not getattr(request, "enforce_contract", True):
        return request.contract_id

    contract = build_contract_from_preflight(request, preflight, schema=schema, mappings=mappings)
    store.save_contract(contract)
    return contract.id


def finalize_contract(contract_id: str, success: bool, *, workspace_id: str = "") -> None:
    """Update the circuit breaker and optionally mark contract as broken.

    When the breaker transitions to OPEN, notify the workspace (fail-open on
    notify errors — transfer finalization must not crash on Slack/email).
    """
    if not contract_id:
        return
    store = get_contract_store()
    breaker = store.get_breaker(contract_id)
    prior = breaker.state.value
    if success:
        breaker.record_success()
    else:
        breaker.record_failure()
    store.save_breaker(breaker)

    contract = store.get_contract(contract_id)
    opened = prior != "open" and breaker.state.value == "open"
    if contract and not success and breaker.state.value == "open":
        contract.status = ContractStatus.BROKEN
        store.save_contract(contract)

    if opened:
        _notify_breaker_open(
            contract_id,
            workspace_id=workspace_id or getattr(contract, "workspace_id", "") or "",
            failure_count=breaker.failure_count,
            canary_pct=getattr(breaker, "canary_pct", 100),
        )


def _notify_breaker_open(
    contract_id: str,
    *,
    workspace_id: str,
    failure_count: int,
    canary_pct: int,
) -> None:
    try:
        from services.notification_service import notify_workspace
    except Exception:
        return
    if not workspace_id:
        return
    try:
        notify_workspace(
            workspace_id,
            {
                "kind": "contract_breaker_open",
                "title": f"Contract breaker OPEN · {contract_id[:12]}",
                "text": (
                    f"Circuit breaker opened after {failure_count} failure(s). "
                    f"Transfers enforcing this contract are blocked "
                    f"(canary_pct={canary_pct}; 100=fail-closed). "
                    "Reset the breaker after remediating, or re-sign the contract."
                ),
                "contract_id": contract_id,
                "urgency": "2",
            },
        )
    except Exception:
        # Notify must never break finalize.
        return
