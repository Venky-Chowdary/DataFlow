"""Pilot ground truth — working memory is sealed at rest (review 2, M1).

The result store was sealed in round 1, but the working memory beside it —
the focus a follow-up edits and the clarification it holds — kept a query's
WHERE literals in clear: ``"where": "email is person2@example.com"`` in
``pilot_memory.json`` and in the shared Mongo ``pilot_memory`` collection.
Focus, pending slots and proposals are now sealed with the Pilot state key;
with no local key the WHERE text is not persisted at all.
"""

from __future__ import annotations

import base64
import json
import os

import pytest

CELLS = ["person2@example.com", "123-45-6002"]
WHERE = "email is person2@example.com"


@pytest.fixture
def keyed(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", base64.urlsafe_b64encode(os.urandom(32)).decode())


def _remember(memory):
    from src.ai.copilot.working_memory import PendingSlot, PilotFocus, PilotProposal

    memory.remember_focus("s1", PilotFocus(connector_name="CRM SQLite", table="customers", where=WHERE))
    memory.remember_pending("s1", PendingSlot(
        tool="aggregate_data", args={"table": "customers", "where": "ssn = '123-45-6002'"}, missing="connector_name",
    ))
    memory.remember_proposal("s1", PilotProposal(tool="sample_connector_object", args={"where": WHERE}))


def _leaked(text: str) -> list[str]:
    return [cell for cell in CELLS if cell in text]


def test_file_memory_is_sealed_at_rest_and_reads_back(keyed, tmp_path):
    from src.ai.copilot.working_memory import PilotWorkingMemory

    path = tmp_path / "pilot_memory.json"
    _remember(PilotWorkingMemory(path=path))
    assert _leaked(path.read_text()) == [], "WHERE literals are plaintext in pilot_memory.json"

    again = PilotWorkingMemory(path=path)  # a restarted API reads its own state back
    assert again.get_focus("s1").where == WHERE
    assert again.get_pending("s1").args["where"] == "ssn = '123-45-6002'"
    assert again.get_proposal("s1").args == {"where": WHERE}


def test_legacy_plaintext_memory_is_read_and_resealed(keyed, tmp_path):
    import time

    from src.ai.copilot.working_memory import PilotWorkingMemory

    path = tmp_path / "pilot_memory.json"
    path.write_text(json.dumps({"focus": {"s1": {"table": "customers", "where": WHERE, "updated_at": time.time()}}}))
    memory = PilotWorkingMemory(path=path)
    assert memory.get_focus("s1").where == WHERE
    assert _leaked(path.read_text()) == [], "legacy plaintext stays on disk after load"


class _FakeCollection:
    """The few pymongo calls the shared working memory makes."""

    def __init__(self):
        self.docs: dict[str, dict] = {}

    def find_one(self, query):
        row = self.docs.get(query["_id"])
        return json.loads(json.dumps(row)) if row else None

    def replace_one(self, query, doc, upsert=False):
        self.docs[query["_id"]] = json.loads(json.dumps(doc))

    def find_one_and_delete(self, query):
        return self.docs.pop(query["_id"], None)

    def delete_many(self, query):
        for key in [k for k, v in self.docs.items() if v.get("kind") == query.get("kind")]:
            if "stamp" not in query or self.docs[key]["stamp"] <= query["stamp"]["$lte"]:
                self.docs.pop(key)


def test_shared_mongo_memory_is_sealed_at_rest(keyed):
    from src.ai.copilot.working_memory import MongoWorkingMemory

    coll = _FakeCollection()
    memory = MongoWorkingMemory(collection=coll)
    _remember(memory)
    assert _leaked(json.dumps(coll.docs)) == [], "WHERE literals are plaintext in the pilot_memory collection"
    assert memory.get_focus("s1").where == WHERE
    assert memory.get_pending("s1").args["where"] == "ssn = '123-45-6002'"


def test_without_a_local_key_the_where_text_is_not_persisted(monkeypatch, tmp_path):
    from src.ai.copilot import sealed_state
    from src.ai.copilot.working_memory import PilotWorkingMemory

    monkeypatch.setattr(sealed_state, "seal", lambda value: None)  # no key to seal with
    path = tmp_path / "pilot_memory.json"
    memory = PilotWorkingMemory(path=path)
    _remember(memory)
    assert _leaked(path.read_text()) == []
    assert memory.get_focus("s1").where == WHERE, "the live process keeps its focus in memory"
