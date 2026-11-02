"""One contract status/version read model: Sign, list, drawer, briefing and run agree.

QA (demo videos, production 557f556c): Sign returned 200 with status=signed and
a bumped version, the API persisted it, yet the Contracts list and drawer kept
showing "Draft v1 (Signed 0)", the Columns tab showed col_0..col_7, and the
Pilot briefing said contracts were "all signed" while the page said Draft.

Everything here goes through the REST routes the web app calls
(``/api/v1/contracts`` …) and the Pilot route (``/api/v1/copilot/chat``), on the
live MongoDB contract store (skips when MongoDB is not reachable). The web half
(the page must not let a stale client copy override the server row) is
``apps/web/src/lib/contractReadModel.test.tsx``.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

MONGO_URI = (
    os.environ.get("P2_MONGO_URI")
    or os.environ.get("MONGO_URI")
    or "mongodb://127.0.0.1:27017"
)


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


pytestmark = pytest.mark.skipif(not _mongo_up(), reason=f"MongoDB not reachable at {MONGO_URI}")


class _LiveMongo:
    def __init__(self, db_name: str) -> None:
        self._client = _client()
        self.db_name = db_name

    def get_database(self):
        return self._client[self.db_name]

    def drop(self) -> None:
        self._client.drop_database(self.db_name)
        self._client.close()


# Eight real columns, including unicode, so a col_0..col_7 fallback is visible.
_COLUMNS = {
    "order_id": "INTEGER",
    "customer_name": "VARCHAR(120)",
    "région": "VARCHAR(40)",
    "amount": "NUMERIC(12,2)",
    "status": "VARCHAR(20)",
    "created_at": "TIMESTAMP",
    "notes": "TEXT",
    "is_paid": "BOOLEAN",
}


def _contract_body(name: str) -> dict:
    return {
        "name": name,
        "source": {"kind": "database", "format": "postgresql", "table": "orders"},
        "destination": {"kind": "database", "format": "postgresql", "table": "orders_copy"},
        "mappings": [
            {"source": col, "target": col, "confidence": 0.99} for col in _COLUMNS
        ],
        "column_types": dict(_COLUMNS),
        "preflight_gates": [{"id": "mapping", "status": "pass", "message": "ok"}],
        "strict": True,
    }


@pytest.fixture
def live_store(tmp_path, monkeypatch):
    from src.services.contract_store import get_contract_store, reset_contract_store

    monkeypatch.setenv("DATAFLOW_CONTRACTS_PATH", str(tmp_path / "contracts.json"))
    mongo = _LiveMongo(f"r7_contract_rm_{uuid.uuid4().hex[:10]}")
    reset_contract_store()
    store = get_contract_store(mongo)
    assert type(store).__name__ == "MongoContractStore"
    yield store, mongo
    reset_contract_store()
    mongo.drop()


def _brief(client: TestClient) -> str:
    response = client.post("/api/v1/copilot/chat", json={"message": "give me a briefing"})
    assert response.status_code == 200, response.text
    body = response.json()
    assert "brief_workspace" in [t.get("name") for t in body.get("tools_used") or []], body
    return body.get("answer") or ""


def test_sign_list_drawer_briefing_and_run_agree_on_signed_version(live_store, monkeypatch):
    store, mongo = live_store
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.main import app

    name = f"orders → orders_copy {uuid.uuid4().hex[:6]}"
    with TestClient(app) as client:
        created = client.post("/api/v1/contracts/from-transfer", json=_contract_body(name))
        assert created.status_code == 200, created.text
        cid = created.json()["id"]
        assert created.json()["status"] == "draft"

        first = client.post(f"/api/v1/contracts/{cid}/sign", json={"strict": True})
        assert first.status_code == 200, first.text
        # Re-sign bumps the agreement version (QA saw v3 after repeated Signs).
        second = client.post(f"/api/v1/contracts/{cid}/sign", json={"strict": True})
        assert second.status_code == 200, second.text
        signed = second.json()
        assert (signed["status"], signed["version"]) == ("signed", 2)

        # The live Mongo document is what every later read must reflect.
        doc = mongo.get_database()["contracts"].find_one({"id": cid})
        assert (doc["status"], doc["version"]) == ("signed", 2)

        listed = client.get("/api/v1/contracts")
        assert listed.status_code == 200
        row = next(c for c in listed.json()["contracts"] if c["id"] == cid)
        drawer = client.get(f"/api/v1/contracts/{cid}")
        assert drawer.status_code == 200
        detail = drawer.json()

        # One read model: Sign's response, the list row and the drawer agree.
        for view in (signed, row, detail):
            assert view["status"] == "signed"
            assert view["version"] == 2
            assert view["status_label"] == "Signed v2"
            assert view["signed_version"] == 2
            assert view["signed_at"]
        # Sign is a write: updated_at moves, so the newest-first list and the
        # drawer's "Updated" stop showing the draft's creation time.
        assert row["updated_at"] > created.json()["updated_at"]
        assert detail["updated_at"] == row["updated_at"] == signed["updated_at"]

        # Columns come from the bound schema, not col_0..col_7.
        for view in (row, detail):
            names = [c.get("name") for c in view["columns"]]
            assert names == list(_COLUMNS), names
            assert {c["name"]: c["type"] for c in view["columns"]} == _COLUMNS

        # The Pilot briefing reads the same model and says the same thing.
        answer = _brief(client)
        assert "Signed v2" in answer, answer
        assert name in answer, answer
        assert "all signed or active" not in answer

    # The run gate reads the same status: a signed contract stamps, a draft refuses.
    from src.transfer.contract_engine import stamp_bound_contract
    from src.transfer.models import EndpointConfig, TransferRequest

    req = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table="orders"),
        destination=EndpointConfig(kind="database", format="postgresql", table="orders_copy"),
    )
    stamp_bound_contract(req, contract_id=cid, require_signed=True)
    assert req.contract_id == cid

    from src.services.data_contract import DataContract

    draft = DataContract(name="draft-only")
    store.save_contract(draft)
    with pytest.raises(ValueError, match="current status: draft"):
        stamp_bound_contract(req, contract_id=draft.id, require_signed=True)


def test_briefing_counts_the_same_population_the_contracts_page_lists(live_store, monkeypatch):
    """A workspace whose newest 50 contracts are signed still has its older Draft.

    The briefing read a 50-row page and, inside a workspace, called that page the
    population — "all signed" — while the Contracts page (200 rows) showed Draft.
    """
    store, _mongo = live_store
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.workspace_briefing import collect_workspace_briefing
    from src.main import app
    from src.services.data_contract import ContractStatus, DataContract

    ws = f"ws-r7-{uuid.uuid4().hex[:8]}"
    draft = DataContract(
        name="saved draft",
        status=ContractStatus.DRAFT,
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:00+00:00",
        metadata={"workspace_id": ws},
    )
    store.save_contract(draft)
    for i in range(55):
        store.save_contract(
            DataContract(
                name=f"auto-{i}",
                status=ContractStatus.SIGNED,
                metadata={"workspace_id": ws},
            )
        )

    with TestClient(app) as client:
        listed = client.get("/api/v1/contracts", headers={"X-Workspace-Id": ws})
        assert listed.status_code == 200
        rows = [c for c in listed.json()["contracts"] if c.get("metadata", {}).get("workspace_id") == ws]
        assert len(rows) == 56
        page_drafts = [c for c in rows if c["status"] == "draft"]
        assert [c["id"] for c in page_drafts] == [draft.id]

    facts = collect_workspace_briefing(workspace_id=ws)
    assert facts["contract_count"] == 56
    assert facts["contracts_unsigned"] == 1
    assert facts["contracts_by_status"] == {"signed": 55, "draft": 1}
