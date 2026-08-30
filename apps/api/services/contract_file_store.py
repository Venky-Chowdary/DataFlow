"""Durable file-backed contract store — survives restarts when Mongo is memory/unavailable.

One file per record. A single JSON blob made every save a read-modify-write of
the whole store, so cost grew with the number of contracts (a 10k-contract store
spent seconds re-encoding 54 MB on each transfer), and two workers saving
different contracts at once clobbered each other's write.
"""

from __future__ import annotations

import json
import os
from services.brand_env import getenv_brand
import threading
from pathlib import Path
from typing import Any

from services.data_contract import CircuitBreaker, DataContract


def _default_path() -> Path:
    override = getenv_brand("CONTRACTS_PATH", "").strip()
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[1] / "data" / "contracts.json"


def _safe_stem(record_id: str) -> str:
    """File name for a record id — never escape the store directory."""
    keep = [c if (c.isalnum() or c in "-_.") else "_" for c in str(record_id)]
    stem = "".join(keep).strip(".") or "unnamed"
    return stem[:180]


class FileContractStore:
    """Directory-backed JSON persistence for contracts + breakers."""

    def __init__(self, path: Path | None = None):
        self.path = path or _default_path()
        self._lock = threading.RLock()
        self.root = self.path.parent / f"{self.path.stem}.d"
        self._contracts_dir = self.root / "contracts"
        self._breakers_dir = self.root / "breakers"
        self._contracts_dir.mkdir(parents=True, exist_ok=True)
        self._breakers_dir.mkdir(parents=True, exist_ok=True)
        self._migrate_legacy_blob()

    def _migrate_legacy_blob(self) -> None:
        """Split a pre-existing single-file store into per-record files once."""
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        for record_id, doc in (payload.get("contracts") or {}).items():
            self._write_record(self._contracts_dir, record_id, doc, overwrite=False)
        for record_id, doc in (payload.get("breakers") or {}).items():
            self._write_record(self._breakers_dir, record_id, doc, overwrite=False)
        try:
            self.path.replace(self.path.with_suffix(f"{self.path.suffix}.migrated"))
        except OSError:
            pass

    def _record_path(self, directory: Path, record_id: str) -> Path:
        return directory / f"{_safe_stem(record_id)}.json"

    def _write_record(
        self,
        directory: Path,
        record_id: str,
        doc: dict[str, Any],
        *,
        overwrite: bool = True,
    ) -> None:
        target = self._record_path(directory, record_id)
        if not overwrite and target.exists():
            return
        # Process/thread-unique temp file so concurrent writers never steal
        # each other's source before os.replace.
        tmp = target.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
        directory.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
        tmp.replace(target)

    def _read_record(self, directory: Path, record_id: str) -> dict[str, Any] | None:
        try:
            return json.loads(
                self._record_path(directory, record_id).read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError):
            return None

    def _read_newest(self, directory: Path, want: int) -> list[dict[str, Any]]:
        """Read the most recently written records, newest file first.

        A record's file is rewritten on every save, so modification time orders
        the store the same way ``updated_at`` does without parsing every file.
        A margin over the requested count absorbs records saved in one batch
        (a legacy migration writes them all at the same instant).
        """
        files = sorted(
            directory.glob("*.json"),
            key=lambda f: (f.stat().st_mtime if f.exists() else 0.0),
            reverse=True,
        )[: max(int(want), 0) * 5 or None]
        docs: list[dict[str, Any]] = []
        for file in files:
            try:
                docs.append(json.loads(file.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError):
                continue
        return docs

    def save_contract(self, contract: DataContract) -> DataContract:
        with self._lock:
            self._write_record(self._contracts_dir, contract.id, contract.to_dict())
        return contract

    def get_contract(self, contract_id: str) -> DataContract | None:
        with self._lock:
            doc = self._read_record(self._contracts_dir, contract_id)
        if not doc:
            return None
        return DataContract.from_dict(doc)

    def list_contracts(self, limit: int = 200) -> list[DataContract]:
        with self._lock:
            docs = self._read_newest(self._contracts_dir, limit)
        items = [DataContract.from_dict(d) for d in docs]
        items.sort(key=lambda c: c.updated_at or c.created_at or "", reverse=True)
        return items[:limit]

    def save_breaker(self, breaker: CircuitBreaker) -> None:
        with self._lock:
            self._write_record(
                self._breakers_dir, breaker.contract_id, breaker.to_dict()
            )

    def get_breaker(self, contract_id: str) -> CircuitBreaker:
        with self._lock:
            doc = self._read_record(self._breakers_dir, contract_id)
        if doc:
            return CircuitBreaker.from_dict(doc)
        return CircuitBreaker(contract_id)
