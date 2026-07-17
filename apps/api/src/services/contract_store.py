"""Compatibility shim: canonical implementation now lives in services.contract_store."""
from __future__ import annotations

from services.contract_store import (
    ContractStore,
    InMemoryContractStore,
    MongoContractStore,
    get_contract_store,
    reset_contract_store,
)

__all__ = ['ContractStore', 'InMemoryContractStore', 'MongoContractStore', 'get_contract_store', 'reset_contract_store']
