"""Durable file-backed contract store — survives restarts when Mongo is memory/unavailable."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

from services.data_contract import CircuitBreaker, DataContract


def _default_path() -> Path:
    override = os.environ.get("DATAFLOW_CONTRACTS_PATH", "").strip()
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[1] / "data" / "contracts.json"


class FileContractStore:
    """JSON file persistence for contracts + breakers."""

    def __init__(self, path: Path | None = None):
        self.path = path or _default_path()
        self._lock = threading.RLock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write({"contracts": {}, "breakers": {}})

    def _read(self) -> dict[str, Any]:
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"contracts": {}, "breakers": {}}

    def _write(self, payload: dict[str, Any]) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        tmp.replace(self.path)

    def save_contract(self, contract: DataContract) -> DataContract:
        with self._lock:
            data = self._read()
            contracts = data.setdefault("contracts", {})
            contracts[contract.id] = contract.to_dict()
            self._write(data)
        return contract

    def get_contract(self, contract_id: str) -> DataContract | None:
        with self._lock:
            doc = self._read().get("contracts", {}).get(contract_id)
        if not doc:
            return None
        return DataContract.from_dict(doc)

    def list_contracts(self, limit: int = 200) -> list[DataContract]:
        with self._lock:
            docs = list(self._read().get("contracts", {}).values())
        items = [DataContract.from_dict(d) for d in docs]
        items.sort(key=lambda c: c.updated_at or c.created_at or "", reverse=True)
        return items[:limit]

    def save_breaker(self, breaker: CircuitBreaker) -> None:
        with self._lock:
            data = self._read()
            breakers = data.setdefault("breakers", {})
            breakers[breaker.contract_id] = breaker.to_dict()
            self._write(data)

    def get_breaker(self, contract_id: str) -> CircuitBreaker:
        with self._lock:
            doc = self._read().get("breakers", {}).get(contract_id)
        if doc:
            return CircuitBreaker.from_dict(doc)
        return CircuitBreaker(contract_id)
