"""Compatibility shim: canonical implementation now lives in services.data_contract."""
from __future__ import annotations

from services.data_contract import (
    BreakerState,
    CircuitBreaker,
    ColumnRule,
    ContractEnforcer,
    ContractStatus,
    ContractViolation,
    DataContract,
    QualityRule,
    build_contract_from_preflight,
)

__all__ = ['ContractStatus', 'BreakerState', 'ColumnRule', 'QualityRule', 'DataContract', 'ContractViolation', 'CircuitBreaker', 'ContractEnforcer', 'build_contract_from_preflight']
