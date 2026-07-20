"""Data contract management endpoints.

Contracts are the enforceable agreements between a source schema, a destination
schema, and the mapping/transformation that links them. This router exposes
contract lifecycle management and circuit-breaker status.
"""

from __future__ import annotations

from typing import Any, Literal

import yaml
from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from ..services.contract_store import get_contract_store
from ..services.data_contract import (
    BreakerState,
    ColumnRule,
    ContractEnforcer,
    ContractStatus,
    ContractViolation,
    DataContract,
    QualityRule,
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
    preflight_gates: list[dict[str, Any]] = Field(default_factory=list)


class _CreateFromTransferRequest(BaseModel):
    """Create a draft contract from Transfer Studio mapping + validate gates."""

    name: str = Field(..., min_length=1, max_length=200)
    source: dict[str, Any] = Field(default_factory=dict)
    destination: dict[str, Any] = Field(default_factory=dict)
    columns: list[dict[str, Any]] = Field(default_factory=list)
    mappings: list[dict[str, Any]] = Field(default_factory=list)
    quality_rules: list[dict[str, Any]] = Field(default_factory=list)
    preflight_gates: list[dict[str, Any]] = Field(default_factory=list)
    column_types: dict[str, str] = Field(default_factory=dict)
    strict: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


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
    contracts = [_contract_to_response(c) for c in store.list_contracts(limit=200)]
    return _ContractListResponse(contracts=contracts)


@router.post("", response_model=_ContractResponse)
@router.post("/from-transfer", response_model=_ContractResponse)
def create_contract_from_transfer(body: _CreateFromTransferRequest):
    """Persist a draft contract from Transfer Studio validate/mapping state."""
    store = get_contract_store()
    mappings = list(body.mappings or [])
    columns: list[ColumnRule] = []
    if body.columns:
        for c in body.columns:
            columns.append(
                ColumnRule(
                    source_name=str(c.get("source_name") or c.get("source") or ""),
                    target_name=str(c.get("target_name") or c.get("target") or ""),
                    source_type=str(c.get("source_type") or body.column_types.get(str(c.get("source_name") or c.get("source") or ""), "VARCHAR")),
                    target_type=str(c.get("target_type") or c.get("source_type") or "VARCHAR"),
                    transform=c.get("transform"),
                    nullable=bool(c.get("nullable", True)),
                    primary_key=bool(c.get("primary_key", False)),
                )
            )
    else:
        for m in mappings:
            src = str(m.get("source") or m.get("source_column") or "")
            tgt = str(m.get("target") or m.get("target_column") or src)
            columns.append(
                ColumnRule(
                    source_name=src,
                    target_name=tgt,
                    source_type=str(body.column_types.get(src) or m.get("source_type") or "VARCHAR"),
                    target_type=str(m.get("target_type") or body.column_types.get(src) or "VARCHAR"),
                    transform=m.get("transform"),
                    nullable=True,
                    primary_key=src.lower() in {"id", "_id"} or tgt.lower() in {"id", "_id"},
                )
            )

    quality_rules = [
        QualityRule(
            name=str(q.get("name") or q.get("id") or "rule"),
            expectation=str(q.get("expectation") or q.get("message") or ""),
            threshold=q.get("threshold"),
            severity=str(q.get("severity") or "warning"),
        )
        for q in (body.quality_rules or [])
    ]
    # Promote blocking preflight gates into quality rules when none were sent.
    if not quality_rules:
        for g in body.preflight_gates or []:
            status = str(g.get("status") or "").lower()
            if status in {"block", "fail", "failed"}:
                quality_rules.append(
                    QualityRule(
                        name=str(g.get("id") or g.get("name") or "gate"),
                        expectation=str(g.get("message") or ""),
                        severity="block",
                    )
                )

    contract = DataContract(
        name=body.name.strip(),
        status=ContractStatus.DRAFT,
        source=body.source or {},
        destination=body.destination or {},
        columns=columns,
        mappings=mappings,
        quality_rules=quality_rules,
        preflight_gates=list(body.preflight_gates or []),
        strict=body.strict,
        metadata=dict(body.metadata or {}),
    )
    store.save_contract(contract)
    return _contract_to_response(contract)


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


@router.get("/{contract_id}/export")
def export_contract(contract_id: str, format: Literal["yaml", "json"] = "yaml"):
    """Export a contract as ``dataflow-contract.yaml`` (kind + metadata + spec)."""
    from services.gitops_manifest import contract_artifact

    store = get_contract_store()
    contract = store.get_contract(contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    artifact = contract_artifact(contract)
    if format == "yaml":
        return Response(
            content=yaml.safe_dump(artifact, sort_keys=False, default_flow_style=False),
            media_type="application/x-yaml",
            headers={"Content-Disposition": f"attachment; filename=dataflow-contract-{contract_id}.yaml"},
        )
    return artifact


@router.post("/import", response_model=_ContractResponse)
def import_contract(payload: dict[str, Any]):
    """Import a contract from YAML/JSON (raw or DataContract kind wrapper).

    Imported contracts are saved as DRAFT — sign before enforcing on schedules.
    """
    from services.gitops_manifest import apply_manifest

    # Accept bare contract dicts and kind-wrapped artifacts.
    if payload.get("kind") == "DataContract" or payload.get("kind") == "DataFlowManifest":
        result = apply_manifest(payload, dry_run=False)
        rows = [r for r in (result.get("results") or []) if r.get("kind") == "DataContract" and r.get("ok")]
        if not rows:
            err = next((r.get("error") for r in (result.get("results") or []) if r.get("error")), None)
            raise HTTPException(status_code=422, detail=err or "Invalid contract payload")
        store = get_contract_store()
        contract = store.get_contract(str(rows[0].get("id") or ""))
        if not contract:
            raise HTTPException(status_code=500, detail="Contract imported but not readable")
        return _contract_to_response(contract)

    store = get_contract_store()
    try:
        contract = DataContract.from_dict(payload.get("spec") if isinstance(payload.get("spec"), dict) else payload)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Invalid contract payload: {exc}") from exc
    contract.status = ContractStatus.DRAFT
    store.save_contract(contract)
    return _contract_to_response(contract)
