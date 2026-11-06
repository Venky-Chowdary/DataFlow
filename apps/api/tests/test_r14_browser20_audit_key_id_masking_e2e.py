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


def _oracle(reader, prefix_template: str) -> str:
    """The reviewer's attack: extend q one hex character at a time while count > 0."""
    recovered = ""
    for _ in range(36):
        for c in "0123456789abcdef-":
            q = prefix_template + recovered + c
            body = reader.get("/api/v1/audit/events", params={"limit": 500, "q": q}).json()
            if body["count"]:
                recovered += c
                break
        else:
            break
    return recovered


@pytest.mark.parametrize("reader_kind", ["viewer_session", "operator_key"])
def test_masked_reader_search_never_matches_hidden_key_id_characters(world, reader_kind):
    """Search matches the masked projection: no prefix-extension oracle (review finding)."""
    key = _key_activity(world)
    if reader_kind == "viewer_session":
        world.member("viewer-b20q@acme.test", "viewer")
        reader = world.login("viewer-b20q@acme.test", PASSWORD)
    else:
        reader = world.as_key(world.mint_key("operator"))
    # The machine actor is searchable as a kind, and by the last four it is shown.
    assert reader.get("/api/v1/audit/events", params={"q": "api_key:"}).json()["count"] > 0
    assert reader.get("/api/v1/audit/events", params={"q": key["id"][-4:]}).json()["count"] > 0
    # Any hidden leading character matches nothing, in the actor and in resources.
    for n in (1, 8, 32, 36):
        for q in (f"api_key:{key['id'][:n]}",) + ((key["id"][:n],) if n >= 8 else ()):
            listed = reader.get("/api/v1/audit/events", params={"q": q, "limit": 500})
            assert listed.status_code == 200, listed.text
            assert listed.json()["count"] == 0, (reader_kind, q)
    assert _oracle(reader, "api_key:") == ""


def test_admin_search_still_matches_the_full_key_id(world):
    key = _key_activity(world)
    found = world.owner.get("/api/v1/audit/events", params={"q": f"api_key:{key['id'][:8]}"}).json()
    assert found["count"] > 0 and found["key_ids_masked"] is False


def test_operator_key_reader_gets_masked_ids_in_list_and_export(world):
    key = _key_activity(world)
    operator = world.as_key(world.mint_key("operator"))
    listed = operator.get("/api/v1/audit/events?limit=500")
    assert listed.status_code == 200 and listed.json()["key_ids_masked"] is True
    assert not _raw_ids_in(listed.text, key["id"])
    exported = operator.get("/api/v1/audit/export?format=ndjson&limit=500")
    assert exported.status_code == 200
    assert not _raw_ids_in(exported.text, key["id"])


def test_prefixed_key_reference_is_masked_even_for_a_key_missing_from_the_store():
    from services.audit_actor_display import mask_key_ids

    gone = "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d"
    masked = mask_key_ids([{"id": "e", "resource": f"service_account:{gone}", "actor": "x@y"}])[0]
    assert masked["resource"] == "service_account:…cb6d"


def test_verify_pack_masks_a_key_actor_for_a_non_admin_reader(world):
    """S8 (second review): chain_record.actor carried the exporting key's full id."""
    from services.signed_proof_pack import export_proof_pack_for_job

    key = world.mint_key("admin", name="b20 pack exporter")
    pack = export_proof_pack_for_job(
        {"_id": "job-b20-pack", "workspace_id": world.workspace_id,
         "reconciliation": {"passed": True, "phase": "sample_verified"}},
        actor=f"api_key:{key['id']}",
    )
    editor = world.as_key(world.mint_key("editor"))
    masked = editor.post("/api/v1/audit/verify-pack", json=pack)
    assert masked.status_code == 200, masked.text
    record = masked.json()["chain_record"]
    assert record is not None
    assert key["id"] not in masked.text
    assert record["actor"] == f"api_key:…{key['id'][-4:]}"
    # Whole only for a key manager of the RECORD's workspace (review 3): this
    # anchor is filed outside any request, so it carries no workspace and only
    # a platform admin reads the id whole.
    full = world.root.post("/api/v1/audit/verify-pack", json=pack)
    assert full.json()["chain_record"]["actor"] == f"api_key:{key['id']}"
    owner = world.owner.post("/api/v1/audit/verify-pack", json=pack).json()
    assert owner["chain_record_withheld"] or key["id"] not in str(owner)


def test_verify_pack_judges_by_the_records_workspace_not_the_readers_header(world):
    """Review 3: A's admin posting a pack from B's key saw B's key id unmasked."""
    from services import team_store
    from services.signed_proof_pack import export_proof_pack_for_job

    other = team_store.create_workspace(name="B", created_by="boss@b.test")
    key_b = "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d"
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter

    with bound_workspace_filter(WorkspaceFilter.only(other.id), stamp=other.id):
        pack = export_proof_pack_for_job(
            {"_id": "job-b-pack", "workspace_id": other.id, "reconciliation": {"passed": True}},
            actor=f"api_key:{key_b}",
        )
    seen = world.owner.post("/api/v1/audit/verify-pack", json=pack)  # admin of A, header A
    assert seen.status_code == 200, seen.text
    assert key_b not in seen.text
    body = seen.json()
    assert body["chain_record_found"] is True
    if body["chain_record_withheld"]:
        assert body["chain_record"] is None
    else:
        assert body["chain_record"]["actor"] == "api_key:…cb6d"


def test_key_store_unreadable_masks_every_uuid(monkeypatch):
    from services import audit_actor_display

    def broken(*, strict=False):
        raise RuntimeError("key store down")

    monkeypatch.setattr(audit_actor_display, "_key_names", broken)
    job = "6ae69b70-1111-4111-8111-111111111111"
    out = audit_actor_display.mask_key_ids([{"id": "e", "resource": f"/jobs/{job}"}])[0]
    assert job not in out["resource"] and out["resource"].endswith("…1111")


def test_prefixed_reference_after_an_underscore_is_masked():
    from services.audit_actor_display import mask_key_ids

    kid = "276d4753-0000-4000-8000-000000000001"
    out = mask_key_ids([{"id": "e", "details": {"h": f"x_api_key:{kid}"}}])[0]
    assert kid not in str(out)
