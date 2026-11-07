"""X-23: copilot connector briefs must resolve ``_id``-only docs and refuse
empty ids.

Production defect: ``_connector_brief`` resolved ``id``/``connector_id`` but
not ``_id``. A doc whose public id lives only in ``_id`` produced
``connector_id: ""`` — ``test_connector`` then probed and ``mark_tested`` an
empty id, and staged ``update_connector`` / ``delete_connector`` /
``prepare_cdc_source`` payloads carried ``""``.

Fix: the brief uses the same ``_id``-or-``id`` resolution the store's
``_connector_id_query`` uses, and probe/stage paths fail closed on empty.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import services.connector_store as store  # noqa: E402
from test_connector_id_lookup import _FakeColl  # noqa: E402

from src.ai.copilot import lifecycle_tools as lt  # noqa: E402


@pytest.fixture
def fake_mongo_store(monkeypatch):
    coll = _FakeColl()
    monkeypatch.setattr(store, "_use_mongo", lambda: True)
    monkeypatch.setattr(store, "_mongo_collection", lambda: coll)
    return coll


def _id_only_doc(public_id: str, **over) -> dict:
    doc = {
        "_id": public_id,  # public id lives ONLY in _id; no "id" field
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


def test_brief_resolves_id_from_raw_id_only_doc():
    """The exact prod shape: a raw doc dict carrying the id in ``_id`` only
    (mongodb_service path returns raw dicts, not normalized models)."""
    public_id = str(uuid.uuid4())
    brief = lt._connector_brief(
        {"_id": public_id, "name": "Snowflake Trial UA81716", "type": "snowflake"}
    )
    assert brief["connector_id"] == public_id


def test_test_connector_resolves_id_only_doc_and_marks_it(fake_mongo_store, monkeypatch):
    public_id = str(uuid.uuid4())
    fake_mongo_store.insert_one(_id_only_doc(public_id))

    called: list[str] = []
    monkeypatch.setattr(
        "services.connector_probe.probe_saved_connector",
        lambda cid, workspace_id=None: (called.append(cid) or (True, "reachable", {})),
    )

    result = lt.test_connector(connector_id=public_id)
    assert result.success is True
    assert result.output["connector_id"] == public_id
    assert called == [public_id], "probe ran on a wrong/empty connector id"
    assert store.get_connector(public_id).last_test_ok is True


def test_staged_connector_payloads_carry_the_resolved_id(fake_mongo_store):
    public_id = str(uuid.uuid4())
    fake_mongo_store.insert_one(_id_only_doc(public_id))

    from src.ai.copilot.ack_ledger import get_ack_ledger

    for tool_call in (
        lambda: lt.delete_connector(connector_id=public_id),
        lambda: lt.update_connector(connector_id=public_id, host="new.example.com"),
        lambda: lt.prepare_cdc_source(connector_id=public_id),
    ):
        result = tool_call()
        assert result.success is True, result.error
        assert result.output["preview"]["connector_id"] == public_id
        payload, err = get_ack_ledger().get_pending_payload(result.output["ack_id"])
        assert not err
        assert payload["connector_id"] == public_id, (
            "staged payload must carry the doc's _id, never ''"
        )


def test_empty_resolved_id_is_refused_everywhere(fake_mongo_store):
    """A doc with no resolvable id field must never probe or stage."""
    # Bypass insert_one's _id generation: file-store/export docs can carry
    # no id fields at all, and the guard must still fail closed.
    fake_mongo_store.docs["k1"] = {
        "id": "",
        "name": "Idless Saved Connector",
        "type": "postgresql",
    }

    results = [
        lt.test_connector(name="Idless Saved Connector"),
        lt.delete_connector(name="Idless Saved Connector"),
        lt.update_connector(name="Idless Saved Connector", host="x"),
        lt.prepare_cdc_source(name="Idless Saved Connector"),
    ]
    for result in results:
        assert result.success is False
        assert "no resolvable id" in result.error
