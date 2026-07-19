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
    def list_contracts(self, limit: int = 200) -> list[DataContract]:
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

    def list_contracts(self, limit: int = 200) -> list[DataContract]:
        items = sorted(
            self._contracts.values(),
            key=lambda c: c.updated_at or c.created_at or "",
            reverse=True,
        )
        return items[:limit]

    def save_breaker(self, breaker: CircuitBreaker) -> None:
        self._breakers[breaker.contract_id] = breaker

    def get_breaker(self, contract_id: str) -> CircuitBreaker:
        if contract_id not in self._breakers:
            self._breakers[contract_id] = CircuitBreaker(contract_id)
        return self._breakers[contract_id]


class MongoContractStore(ContractStore):
    """MongoDB-backed contract store with durable file fallback."""

    def __init__(self, mongo_service: Any | None = None):
        self.mongo = mongo_service
        from services.contract_file_store import FileContractStore

        self._fallback = FileContractStore()

    def _get_db(self):
        try:
            if self.mongo is None:
                from services.mongodb_service import get_mongodb_service

                self.mongo = get_mongodb_service()
            # The in-memory fallback returns an empty dict, not a real MongoDB
            # database, so fall back to the durable file store instead.
            if type(self.mongo).__name__ == "MemoryMongoDBService":
                return None
            return self.mongo.get_database()
        except Exception:
            return None

    def save_contract(self, contract: DataContract) -> DataContract:
        db = self._get_db()
        # Always mirror to file so Contracts page survives process restarts /
        # MemoryMongoDBService / ephemeral Mongo.
        self._fallback.save_contract(contract)
        if db is None:
            return contract
        try:
            db["contracts"].update_one(
                {"id": contract.id},
                {"$set": contract.to_dict()},
                upsert=True,
            )
        except Exception:
            pass
        return contract

    def get_contract(self, contract_id: str) -> DataContract | None:
        db = self._get_db()
        if db is not None:
            try:
                doc = db["contracts"].find_one({"id": contract_id})
                if doc:
                    doc.pop("_id", None)
                    return DataContract.from_dict(doc)
            except Exception:
                pass
        return self._fallback.get_contract(contract_id)

    def list_contracts(self, limit: int = 200) -> list[DataContract]:
        by_id: dict[str, DataContract] = {}
        for c in self._fallback.list_contracts(limit=limit):
            by_id[c.id] = c
        db = self._get_db()
        if db is not None:
            try:
                docs = list(db["contracts"].find().sort("updated_at", -1).limit(limit))
                for doc in docs:
                    doc.pop("_id", None)
                    c = DataContract.from_dict(doc)
                    by_id[c.id] = c
            except Exception:
                pass
        items = sorted(
            by_id.values(),
            key=lambda c: c.updated_at or c.created_at or "",
            reverse=True,
        )
        return items[:limit]

    def save_breaker(self, breaker: CircuitBreaker) -> None:
        self._fallback.save_breaker(breaker)
        db = self._get_db()
        if db is None:
            return
        try:
            db["contract_breakers"].update_one(
                {"contract_id": breaker.contract_id},
                {"$set": breaker.to_dict()},
                upsert=True,
            )
        except Exception:
            pass

    def get_breaker(self, contract_id: str) -> CircuitBreaker:
        db = self._get_db()
        if db is not None:
            try:
                doc = db["contract_breakers"].find_one({"contract_id": contract_id})
                if doc:
                    doc.pop("_id", None)
                    return CircuitBreaker.from_dict(doc)
            except Exception:
                pass
        return self._fallback.get_breaker(contract_id)


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
