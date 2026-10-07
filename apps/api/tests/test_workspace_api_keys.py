"""Workspace API keys persist across a new read and expire like a PAT."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services import integrations_store


class _Result:
    def __init__(self, deleted: int) -> None:
        self.deleted_count = deleted


class _Keys:
    def __init__(self) -> None:
        self.docs: dict[str, dict] = {}

    def find(self, _query=None):
        return [dict(doc) for doc in self.docs.values()]

    def update_one(self, filt, update, upsert=False):
        key_id = filt["id"]
        if upsert or key_id in self.docs:
            self.docs[key_id] = dict(update["$set"])
        return _Result(1)

    def delete_one(self, filt):
        removed = self.docs.pop(filt["id"], None)
        return _Result(1 if removed else 0)


@pytest.fixture
def key_store(tmp_path, monkeypatch):
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    coll = _Keys()
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: coll)
    return coll


def test_minted_key_is_still_there_on_the_next_read(key_store):
    created = integrations_store.create_api_key(
        "Production ETL", "admin@example.com", role="editor", expires_in="90d"
    )
    listed = integrations_store.list_api_keys()
    assert listed[0]["id"] == created["id"]
    assert listed[0]["expired"] is False
    assert listed[0]["lifetime"] == "90d"
    assert listed[0]["expires_at"]
    assert "key_hash" not in listed[0]
    assert integrations_store.verify_workspace_api_key(created["key"])["id"] == created["id"]
    assert key_store.docs[created["id"]]["key_hash"]


def test_key_survives_when_the_local_file_is_empty(key_store):
    created = integrations_store.create_api_key(
        "MCP", "admin@example.com", expires_in="30d"
    )
    integrations_store._save_file_keys([])
    listed = integrations_store.list_api_keys()
    assert [row["id"] for row in listed] == [created["id"]]
    assert integrations_store.verify_workspace_api_key(created["key"])["role"] == "editor"


def test_file_key_is_healed_into_mongo(key_store, monkeypatch):
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    created = integrations_store.create_api_key(
        "Legacy file", "admin@example.com", expires_in="never"
    )
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: key_store)
    listed = integrations_store.list_api_keys()
    assert listed[0]["id"] == created["id"]
    assert listed[0]["expires_at"] is None
    assert created["id"] in key_store.docs
    assert integrations_store.verify_workspace_api_key(created["key"]) is not None


def test_expired_key_no_longer_authenticates_and_stays_listed(key_store):
    created = integrations_store.create_api_key(
        "Short", "admin@example.com", expires_in="7d"
    )
    record = key_store.docs[created["id"]]
    record["expires_at"] = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    integrations_store._upsert_api_key_record(record)
    assert integrations_store.verify_workspace_api_key(created["key"]) is None
    listed = integrations_store.list_api_keys()
    assert listed[0]["expired"] is True


def test_legacy_key_without_an_expiry_still_authenticates(key_store, monkeypatch):
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    raw = "dfk_legacy-token-value"
    integrations_store._save_file_keys(
        [
            {
                "id": "legacy-1",
                "name": "Old key",
                "prefix": raw[:12],
                "role": "editor",
                "key_hash": integrations_store._hash_api_key(raw),
                "created_at": "2024-01-01T00:00:00+00:00",
                "created_by": "admin@example.com",
                "last_used_at": None,
            }
        ]
    )
    verified = integrations_store.verify_workspace_api_key(raw)
    assert verified is not None
    assert verified["id"] == "legacy-1"


def test_unknown_lifetime_is_refused():
    with pytest.raises(ValueError, match="expires_in"):
        integrations_store.parse_api_key_lifetime("2h")


def test_revoke_removes_the_key_from_both_stores(key_store):
    created = integrations_store.create_api_key("Drop me", "admin@example.com", expires_in="90d")
    assert integrations_store.revoke_api_key(created["id"]) is True
    assert integrations_store.list_api_keys() == []
    assert created["id"] not in key_store.docs
    assert integrations_store.verify_workspace_api_key(created["key"]) is None
    assert integrations_store.revoke_api_key(created["id"]) is False
