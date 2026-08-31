"""Live MongoDB contract persistence carries a `Decimal` instead of losing it.

Constraint and profiling values come from real column data, so a contract
document holds `Decimal`. BSON has no wire for it, so the write used to raise
`InvalidDocument: cannot encode object: Decimal('1')` and the contract survived
only in the file mirror — the control plane's own store had nothing. This reads
the stored document back off a client the product never touched and asserts the
exact digits arrived as `Decimal128`.

Skips when the compose MongoDB is not reachable.
"""

from __future__ import annotations

import os
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.contract_store import MongoContractStore  # noqa: E402
from services.data_contract import (  # noqa: E402
    ColumnRule,
    ContractStatus,
    DataContract,
)

MONGO_URI = (
    os.environ.get("P2_MONGO_URI")
    or os.environ.get("MONGO_URI")
    or "mongodb://127.0.0.1:27017"
)
DB_NAME = os.environ.get("MONGO_DATABASE") or "dataflow_contract_live"


def _client():
    import pymongo

    return pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)


def _mongo_up() -> bool:
    try:
        client = _client()
        try:
            client.admin.command("ping")
            return True
        finally:
            client.close()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _mongo_up(), reason=f"MongoDB not reachable at {MONGO_URI}"
)


class _LiveMongoService:
    """Minimal stand-in for the app's Mongo service, on a real server."""

    def __init__(self) -> None:
        self._client = _client()

    def get_database(self):
        return self._client[DB_NAME]

    def close(self) -> None:
        self._client.close()


def test_a_contract_carrying_decimals_lands_in_mongo() -> None:
    service = _LiveMongoService()
    store = MongoContractStore(service)
    contract_id = "dfc-live-" + uuid.uuid4().hex[:12]
    contract = DataContract(
        id=contract_id,
        name="live decimal contract",
        status=ContractStatus.SIGNED,
        columns=[
            ColumnRule(
                source_name="amount",
                target_name="amount",
                source_type="DECIMAL(12,2)",
                target_type="DECIMAL(12,2)",
            )
        ],
        metadata={
            # The shapes real profiling produces: a scalar, a nested map, a list.
            "min": Decimal("1"),
            "max": Decimal("99999999.99"),
            "profile": {"mean": Decimal("12.3456789012345678901234567890")},
            "samples": [Decimal("0.01"), Decimal("-7.5")],
        },
    )
    try:
        store.save_contract(contract)

        # Independent client: the product's own store is not asked to confirm
        # its own write.
        client = _client()
        try:
            doc = client[DB_NAME]["contracts"].find_one({"id": contract_id})
        finally:
            client.close()
        assert doc is not None, "contract never reached MongoDB"

        from bson.decimal128 import Decimal128

        meta = doc["metadata"]
        assert isinstance(meta["min"], Decimal128)
        assert meta["min"].to_decimal() == Decimal("1")
        assert meta["max"].to_decimal() == Decimal("99999999.99")
        assert meta["profile"]["mean"].to_decimal() == Decimal(
            "12.3456789012345678901234567890"
        )
        assert [v.to_decimal() for v in meta["samples"]] == [
            Decimal("0.01"),
            Decimal("-7.5"),
        ]

        # And the store reads its own contract back from the server.
        loaded = store.get_contract(contract_id)
        assert loaded is not None
        assert loaded.name == "live decimal contract"
        assert loaded.columns[0].target_type == "DECIMAL(12,2)"
    finally:
        client = _client()
        try:
            client[DB_NAME]["contracts"].delete_many({"id": contract_id})
        finally:
            client.close()
        service.close()


def test_a_decimal_wider_than_the_bson_wire_keeps_its_digits() -> None:
    """Decimal128 holds 34 significant digits; a wider one stays exact as text."""
    service = _LiveMongoService()
    store = MongoContractStore(service)
    contract_id = "dfc-live-wide-" + uuid.uuid4().hex[:12]
    wide = Decimal("1." + "9" * 60)
    contract = DataContract(
        id=contract_id, name="wide decimal", metadata={"max": wide}
    )
    try:
        store.save_contract(contract)
        client = _client()
        try:
            doc = client[DB_NAME]["contracts"].find_one({"id": contract_id})
        finally:
            client.close()
        assert doc is not None
        stored = doc["metadata"]["max"]
        from bson.decimal128 import Decimal128

        if isinstance(stored, Decimal128):
            assert stored.to_decimal() == wide
        else:
            # Rounding into the wire would be a quiet loss; the digits are kept.
            assert stored == str(wide)
    finally:
        client = _client()
        try:
            client[DB_NAME]["contracts"].delete_many({"id": contract_id})
        finally:
            client.close()
        service.close()
