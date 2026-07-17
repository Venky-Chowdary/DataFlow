"""Data contract management endpoints.

Contracts are the enforceable agreements between a source schema, a destination
schema, and the mapping/transformation that links them. This router exposes
contract lifecycle management and circuit-breaker status.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from ..services.contract_store import get_contract_store
from ..services.data_contract import (
    BreakerState,
    ContractEnforcer,
    ContractStatus,
    ContractViolation,
    DataContract,
)

router = APIRouter(prefix="/contracts", tags=["contracts"])


class _ContractResponse(BaseModel):
    id: str
    name: str
    version: int
    status: str
    source: dict[str, Any]
    destination: dict[str, Any]
    columns: list[dict[str, Any]]
    mappings: list[dict[str, Any]]
    quality_rules: list[dict[str, Any]]
    strict: bool
    created_at: str
    updated_at: str
    metadata: dict[str, Any]


class _ContractListResponse(BaseModel):
    contracts: list[_ContractResponse]


class _BreakerResponse(BaseModel):
    contract_id: str
    state: str
    failure_count: int
    success_count: int
    failure_threshold: int
    recovery_timeout_seconds: float


class _SignRequest(BaseModel):
    strict: bool = True


class _ContractTestRequest(BaseModel):
    contract_id: str
    source: dict[str, Any] = Field(default_factory=dict)
    destination: dict[str, Any] = Field(default_factory=dict)
    column_types: dict[str, str] = Field(default_factory=dict)


class _ContractTestResponse(BaseModel):
    valid: bool
    violations: list[dict[str, Any]]


def _contract_to_response(contract: DataContract) -> _ContractResponse:
    return _ContractResponse(**contract.to_dict())


@router.get("", response_model=_ContractListResponse)
def list_contracts(request: Request):
    store = get_contract_store()
    # In-memory fallback does not support list; Mongo list is supported.
    try:
        db = store._get_db() if hasattr(store, "_get_db") else None
    except Exception:
        db = None
    if db is None:
        return _ContractListResponse(contracts=[])
    docs = list(db["contracts"].find().sort("updated_at", -1).limit(200))
    contracts = []
    for doc in docs:
        doc.pop("_id", None)
        contracts.append(_ContractResponse(**DataContract.from_dict(doc).to_dict()))
    return _ContractListResponse(contracts=contracts)


@router.get("/{contract_id}", response_model=_ContractResponse)
def get_contract(contract_id: str):
    store = get_contract_store()
    contract = store.get_contract(contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    return _contract_to_response(contract)


@router.post("/{contract_id}/sign", response_model=_ContractResponse)
def sign_contract(contract_id: str, body: _SignRequest):
    store = get_contract_store()
    contract = store.get_contract(contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    contract.status = ContractStatus.SIGNED
    contract.strict = body.strict
    store.save_contract(contract)
    return _contract_to_response(contract)


@router.post("/{contract_id}/deprecate", response_model=_ContractResponse)
def deprecate_contract(contract_id: str):
    store = get_contract_store()
    contract = store.get_contract(contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    contract.status = ContractStatus.DEPRECATED
    store.save_contract(contract)
    return _contract_to_response(contract)


@router.get("/{contract_id}/breaker", response_model=_BreakerResponse)
def get_breaker(contract_id: str):
    store = get_contract_store()
    breaker = store.get_breaker(contract_id)
    return _BreakerResponse(**breaker.to_dict())


@router.post("/{contract_id}/breaker/reset", response_model=_BreakerResponse)
def reset_breaker(contract_id: str):
    store = get_contract_store()
    contract = store.get_contract(contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    breaker = store.get_breaker(contract_id)
    breaker.state = BreakerState.CLOSED
    breaker.failure_count = 0
    breaker.success_count = 0
    breaker.last_failure_time = None
    if contract.status == ContractStatus.BROKEN:
        contract.status = ContractStatus.SIGNED
    store.save_breaker(breaker)
    store.save_contract(contract)
    return _BreakerResponse(**breaker.to_dict())


@router.post("/test", response_model=_ContractTestResponse)
def test_contract(body: _ContractTestRequest):
    store = get_contract_store()
    contract = store.get_contract(body.contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    from ..transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(**body.source),
        destination=EndpointConfig(**body.destination),
    )
    enforcer = ContractEnforcer(contract)
    try:
        enforcer.enforce(request, sample_schema=body.column_types)
    except ContractViolation as cv:
        return _ContractTestResponse(valid=False, violations=cv.violations)
    return _ContractTestResponse(valid=True, violations=[])
