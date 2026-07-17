"""Persistence for DataContracts and CircuitBreakers.

Falls back to an in-memory store when MongoDB is unavailable, so contract logic
works in unit tests and local runs without a real database.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from services.data_contract import CircuitBreaker, DataContract


class ContractStore(ABC):
    @abstractmethod
    def save_contract(self, contract: DataContract) -> DataContract:
        raise NotImplementedError

    @abstractmethod
    def get_contract(self, contract_id: str) -> DataContract | None:
        raise NotImplementedError

    @abstractmethod
    def save_breaker(self, breaker: CircuitBreaker) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_breaker(self, contract_id: str) -> CircuitBreaker:
        raise NotImplementedError


class InMemoryContractStore(ContractStore):
    """Thread-unsafe in-memory store for tests and local fallback."""

    def __init__(self):
        self._contracts: dict[str, DataContract] = {}
        self._breakers: dict[str, CircuitBreaker] = {}

    def save_contract(self, contract: DataContract) -> DataContract:
        self._contracts[contract.id] = contract
        return contract

    def get_contract(self, contract_id: str) -> DataContract | None:
        return self._contracts.get(contract_id)

    def save_breaker(self, breaker: CircuitBreaker) -> None:
        self._breakers[breaker.contract_id] = breaker

    def get_breaker(self, contract_id: str) -> CircuitBreaker:
        if contract_id not in self._breakers:
            self._breakers[contract_id] = CircuitBreaker(contract_id)
        return self._breakers[contract_id]


class MongoContractStore(ContractStore):
    """MongoDB-backed contract store."""

    def __init__(self, mongo_service: Any | None = None):
        self.mongo = mongo_service
        self._fallback = InMemoryContractStore()

    def _get_db(self):
        try:
            if self.mongo is None:
                from services.mongodb_service import get_mongodb_service

                self.mongo = get_mongodb_service()
            return self.mongo.get_database()
        except Exception:
            return None

    def save_contract(self, contract: DataContract) -> DataContract:
        db = self._get_db()
        if db is None:
            return self._fallback.save_contract(contract)
        db["contracts"].update_one(
            {"id": contract.id},
            {"$set": contract.to_dict()},
            upsert=True,
        )
        return contract

    def get_contract(self, contract_id: str) -> DataContract | None:
        db = self._get_db()
        if db is None:
            return self._fallback.get_contract(contract_id)
        doc = db["contracts"].find_one({"id": contract_id})
        if not doc:
            return None
        doc.pop("_id", None)
        return DataContract.from_dict(doc)

    def save_breaker(self, breaker: CircuitBreaker) -> None:
        db = self._get_db()
        if db is None:
            self._fallback.save_breaker(breaker)
            return
        db["contract_breakers"].update_one(
            {"contract_id": breaker.contract_id},
            {"$set": breaker.to_dict()},
            upsert=True,
        )

    def get_breaker(self, contract_id: str) -> CircuitBreaker:
        db = self._get_db()
        if db is None:
            return self._fallback.get_breaker(contract_id)
        doc = db["contract_breakers"].find_one({"contract_id": contract_id})
        if doc:
            doc.pop("_id", None)
            return CircuitBreaker.from_dict(doc)
        return CircuitBreaker(contract_id)


_store_instance: ContractStore | None = None


def get_contract_store(mongo_service: Any | None = None) -> ContractStore:
    """Return a singleton contract store."""
    global _store_instance
    if _store_instance is None:
        _store_instance = MongoContractStore(mongo_service)
    return _store_instance


def reset_contract_store() -> None:
    """Reset the singleton store (useful in tests)."""
    global _store_instance
    _store_instance = None
