"""X-24: the live probe path must resolve every id ``list_connectors`` shows.

Production defect: ``test_connector`` resolved the connector id (X-23) but
``probe_saved_connector`` reported ``Connector '<uuid>' not found`` while
``list_connector_objects`` on the same id reached the driver. The probe's
``get_connector(id, workspace_id=...)`` could miss a doc the id already
addressed (scope mismatch, store/fallback divergence).

Fix: ``probe_saved_connector`` retries unscoped when the scoped lookup misses,
then falls back to the mongodb_service raw-doc store the list path can see.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import services.connector_probe as probe_mod  # noqa: E402
import services.connector_store as store  # noqa: E402
from test_connector_id_lookup import _FakeColl  # noqa: E402


@pytest.fixture
def fake_mongo_store(monkeypatch):
    coll = _FakeColl()
    monkeypatch.setattr(store, "_use_mongo", lambda: True)
    monkeypatch.setattr(store, "_mongo_collection", lambda: coll)
    return coll


def _trial_doc(public_id: str, **over) -> dict:
    doc = {
        "_id": public_id,
        "name": "Snowflake Trial UA81716",
        "type": "postgresql",
        "host": "db.example.com",
        "port": 5432,
        "database": "DATAFLOW_QA",
        "username": "u",
        "role": "source",
        "created_at": "2026-10-19T00:00:00",
    }
    doc.update(over)
    return doc


def _patch_driver(monkeypatch) -> list[str]:
    probed: list[str] = []
    monkeypatch.setattr(
        "src.transfer.connector_registry.run_probe",
        lambda ctype, cfg: (probed.append(ctype) or (True, "reachable")),
    )
    return probed


def test_probe_scoped_miss_heals_same_scope_doc(fake_mongo_store, monkeypatch):
    """Doc stored unscoped (''); caller claims ws-prod scope. The unscoped
    retry heals the store miss — the doc's scope does not contradict the
    caller's claim."""
    public_id = str(uuid.uuid4())
    fake_mongo_store.insert_one(_trial_doc(public_id, workspace_id=""))
    probed = _patch_driver(monkeypatch)

    ok, message, cfg = probe_mod.probe_saved_connector(
        public_id, workspace_id="ws-prod"
    )
    assert ok is True, message
    assert probed == ["postgresql"], "driver was never reached — conn stayed None"
    assert cfg


def test_probe_refuses_cross_workspace_doc(fake_mongo_store, monkeypatch):
    """Tenancy (X-24 P2): doc stored under ws-prod; caller claims ws-other.
    The unscoped retry AND the raw-doc service fallback must not hand a
    cross-workspace doc to the driver — callers pass the resolved doc's
    own workspace_id, never drop scope."""
    doc = _trial_doc(str(uuid.uuid4()), workspace_id="ws-prod")
    public_id = doc["_id"]
    fake_mongo_store.insert_one(doc)

    class _FakeSvc:
        def get_connector(self, cid):
            return doc if cid == public_id else None

    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service", lambda: _FakeSvc()
    )
    probed = _patch_driver(monkeypatch)

    ok, message, _ = probe_mod.probe_saved_connector(
        public_id, workspace_id="ws-other"
    )
    assert ok is False
    assert probed == [], "cross-workspace doc reached the driver — tenancy leak"


def test_probe_same_workspace_still_resolves(fake_mongo_store, monkeypatch):
    public_id = str(uuid.uuid4())
    fake_mongo_store.insert_one(_trial_doc(public_id, workspace_id="ws-prod"))
    probed = _patch_driver(monkeypatch)

    ok, message, _ = probe_mod.probe_saved_connector(
        public_id, workspace_id="ws-prod"
    )
    assert ok is True, message
    assert probed == ["postgresql"]


def test_probe_falls_back_to_mongodb_service_doc(fake_mongo_store, monkeypatch):
    """Store layer misses entirely; the raw-doc service still holds the id —
    the probe resolves it exactly like list_connector_objects does."""
    public_id = str(uuid.uuid4())
    raw_doc = _trial_doc(public_id)

    class _StoreMiss:
        def find_one(self, query):
            return None

    class _FakeSvc:
        def get_connector(self, cid):
            return raw_doc if cid == public_id else None

    monkeypatch.setattr(store, "_mongo_collection", lambda: _StoreMiss())
    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service", lambda: _FakeSvc()
    )
    probed = _patch_driver(monkeypatch)

    ok, message, cfg = probe_mod.probe_saved_connector(public_id, workspace_id=None)
    assert ok is True, message
    assert probed == ["postgresql"]
    assert cfg


def test_probe_malformed_doc_reports_unreadable_not_missing(
    fake_mongo_store, monkeypatch
):
    """Prod shape: the raw doc resolves for the list path, but its record
    cannot be materialized (e.g. a credential field that fails decryption in
    from_dict). The probe must say 'exists but unreadable' — never 'not
    found' for a doc the workspace can see."""
    public_id = str(uuid.uuid4())
    raw_doc = {"_id": public_id}  # no 'type'/'name' — from_dict cannot parse

    class _StoreMiss:
        def find_one(self, query):
            return None

    class _FakeSvc:
        def get_connector(self, cid):
            return raw_doc if cid == public_id else None

    monkeypatch.setattr(store, "_mongo_collection", lambda: _StoreMiss())
    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service", lambda: _FakeSvc()
    )
    probed = _patch_driver(monkeypatch)

    ok, message, _ = probe_mod.probe_saved_connector(public_id, workspace_id=None)
    assert ok is False
    assert "not found" not in message
    assert "could not be read" in message
    assert probed == []


def test_probe_unknown_id_still_reports_not_found(fake_mongo_store, monkeypatch):
    """Fail-closed: a genuinely unknown id must never reach the driver."""
    probed = _patch_driver(monkeypatch)

    class _FakeSvc:
        def get_connector(self, cid):
            return None

    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service", lambda: _FakeSvc()
    )
    ok, message, _ = probe_mod.probe_saved_connector(
        str(uuid.uuid4()), workspace_id=None
    )
    assert ok is False
    assert "not found" in message
    assert probed == []
