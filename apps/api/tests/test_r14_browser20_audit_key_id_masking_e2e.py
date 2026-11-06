"""Browser 20 (QA r14): API-key ids in audit reads and exports, by reader role.

QA (Oct 30): the Audit page masked key ids, but ``GET /audit/events`` and every
export format still returned the raw key UUID (``actor`` = ``api_key:<uuid>``,
``actor_id``, and the ``id`` in key-management details) to any reader holding
``audit.read`` — a viewer included.

Secure default (owner decision): key ids are handles for revocation and
forensics, so only a workspace admin (older builds' ``owner``) or a platform
admin reads them whole; viewers, operators and editors see ``…`` + the last
four characters, in the list and in CSV / JSON / NDJSON / CEF. The stored
record is never rewritten; a masked read says ``key_ids_masked``.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r8_security_world import PASSWORD, world  # noqa: E402,F401

HOOK = {"kind": "webhook", "label": "b20-hook", "config": {"url": "https://example.com/hook"}}


def _key_activity(world) -> dict:
    """An admin key that acts (so records carry ``api_key:<id>``) and is named in details."""
    key = world.mint_key("admin", name="b20 deploy key")
    acted = world.as_key(key).post("/api/v1/workspace/notifications", json=HOOK)
    assert acted.status_code == 200, acted.text
    return key


def _raw_ids_in(text: str, key_id: str) -> bool:
    return key_id.lower() in text.lower()


@pytest.mark.parametrize("role", ["viewer", "editor"])
def test_non_admin_readers_get_masked_key_ids_in_list_and_every_export(world, role):
    key = _key_activity(world)
    world.member(f"{role}-b20@acme.test", role)
    readers = {
        "session": world.login(f"{role}-b20@acme.test", PASSWORD),
        "api_key": world.as_key(world.mint_key(role)),
    }
    for kind, reader in readers.items():
        listed = reader.get("/api/v1/audit/events?limit=500")
        assert listed.status_code == 200, (kind, listed.text)
        body = listed.json()
        assert body["key_ids_masked"] is True
        assert body["events"], "the reader sees its workspace's events"
        assert not _raw_ids_in(listed.text, key["id"]), (kind, "raw key id in /audit/events")
        assert f"…{key['id'][-4:]}" in listed.text
        for fmt in ("csv", "json", "ndjson", "cef"):
            exported = reader.get(f"/api/v1/audit/export?format={fmt}&limit=500")
            assert exported.status_code == 200, (kind, fmt, exported.text)
            assert exported.headers.get("X-Datawrap-Key-Ids-Masked") == "1", (kind, fmt)
            assert not _raw_ids_in(exported.text, key["id"]), (kind, fmt, "raw key id exported")
        doc = reader.get("/api/v1/audit/export?format=json&limit=500").json()
        assert doc["key_ids_masked"] is True
        assert doc["honesty"]["key_ids_masked"] is True


def test_workspace_admin_and_platform_admin_keep_full_key_ids(world):
    key = _key_activity(world)
    admin_key = world.as_key(world.mint_key("admin"))
    for reader in (world.owner, admin_key, world.root):
        listed = reader.get("/api/v1/audit/events?limit=500")
        assert listed.status_code == 200, listed.text
        assert listed.json()["key_ids_masked"] is False
        assert _raw_ids_in(listed.text, key["id"])
        exported = reader.get("/api/v1/audit/export?format=ndjson&limit=500")
        assert exported.status_code == 200, exported.text
        assert "X-Datawrap-Key-Ids-Masked" not in exported.headers
        rows = [json.loads(line) for line in exported.text.splitlines() if line]
        assert any(str(r.get("actor_id") or "") == key["id"] for r in rows) or any(
            key["id"] in json.dumps(r) for r in rows
        )


def test_masking_leaves_other_ids_and_the_stored_record_exact(world):
    key = _key_activity(world)
    world.member("viewer-b20x@acme.test", "viewer")
    viewer = world.login("viewer-b20x@acme.test", PASSWORD)
    events = viewer.get("/api/v1/audit/events?limit=500").json()["events"]
    # The record's own id, hash and workspace stay exact (UUID-shaped or not).
    uuid = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
    assert all(e.get("workspace_id") == world.workspace_id for e in events)
    assert any(uuid.fullmatch(str(e.get("id") or "")) for e in events) or all(
        e.get("event_hash") for e in events
    )
    # The audit store itself still holds the exact actor.
    stored = Path(world.tmp / "audit.jsonl").read_text(encoding="utf-8")
    assert key["id"] in stored


def test_mask_helper_masks_only_issued_key_ids():
    from services.audit_actor_display import mask_key_ids

    key_id = "276d4753-0000-4000-8000-000000000001"
    job_id = "6ae69b70-1111-4111-8111-111111111111"
    events = [
        {
            "id": "e1",
            "actor": f"api_key:{key_id}",
            "actor_type": "api_key",
            "actor_id": key_id,
            "resource": f"/jobs/{job_id}",
            "details": {"principal": {"api_key_id": key_id}, "job_id": job_id},
            "event_hash": "h",
        }
    ]
    masked = mask_key_ids(events)[0]
    assert masked["actor"] == "api_key:…0001"
    assert masked["actor_id"] == "…0001"
    assert masked["details"]["principal"]["api_key_id"] == "…0001"
    assert masked["resource"] == f"/jobs/{job_id}"
    assert masked["details"]["job_id"] == job_id
    assert events[0]["actor_id"] == key_id  # input untouched
