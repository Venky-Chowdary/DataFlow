"""A1 (QA handover, build 9c75ada4): audit verify said ``verified:true`` over nothing.

QA ran Settings -> Audit Logs -> Verify chain as the workspace owner on
production and read ``verified: true, checked: 0``; the response also named the
``DATAFLOW_AUDIT_HMAC_KEY`` server variable.

Root cause (services/evidence_chain.py on 9c75ada4): ``read_chain`` read the
OLDEST ``limit`` records of the whole store (``find({}).sort(...).limit``) and
the workspace walk then filtered them in Python, so a workspace whose records
were not among the store's oldest N walked nothing; ``"verified": not found``
and the empty-store branch then reported true over zero records. A Mongo read
error silently re-read the local JSONL file instead, and ``describe()`` and
three finding texts named environment variables.

Contract checked here through ``GET /api/v1/audit/verify`` on the live MongoDB
audit chain (``r8_ent_harness``), plus QA's fake-Mongo repro
(scratchpad/qa9s/audit_repro.py) as a service-level test:

* the walk reads the addressed workspace's own records from the store, newest
  ``limit`` of them, and says when older ones were left out
  (``truncated_by_limit``) — their absence is not a broken link;
* nothing walked is never "verified": ``verified`` is null and ``status`` is
  ``nothing_checked``;
* the report names the store it walked;
* a Mongo read failure is reported (``status: store_unavailable``), never
  answered from the local file;
* no server variable name appears anywhere in the report.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from r8_ent_harness import ent  # noqa: F401


def _seed(n: int, workspace_id: str, action: str) -> None:
    from services.audit_log import append_audit_event

    for i in range(n):
        append_audit_event(
            action=action, resource=f"/r11/{i}", actor="seed@example.com",
            workspace_id=workspace_id, details={"i": i, "note": "naïve ✓"},
        )


def _no_env_names(report: dict) -> None:
    text = json.dumps(report)
    assert "DATAFLOW_" not in text and "DATAWRAP_" not in text, text[:2000]
    assert "env" not in (report.get("signing") or {}), report.get("signing")


def _admin_of(ent, ws: str, email: str):
    return ent.member(email=email, workspace_id=ws, role="admin")


def test_workspace_verify_walks_its_own_records_beyond_the_store_oldest_window(ent):
    other = ent.workspace("R11 other tenant")
    mine = ent.workspace("R11 my workspace")
    owner = _admin_of(ent, mine, "owner-a1-r11@example.com")
    _seed(60, other, "r11.other")
    _seed(15, mine, "r11.mine")
    in_store = ent.db["audit_events"].count_documents({"workspace_id": mine})
    assert in_store >= 15

    res = owner.get("/api/v1/audit/verify", params={"limit": 50})
    assert res.status_code == 200, res.text
    body = res.json()
    # 9c75ada4 walked the store's oldest 50 records (mostly the other tenant's)
    # and so checked only the handful of this workspace's records among them.
    assert body["scope"] == "workspace"
    assert body["checked"] == in_store, body
    assert body["verified"] is True, body["findings"][:3]
    assert body["status"] == "verified"
    assert body["store"] == "mongo"
    assert body["truncated_by_limit"] is False
    assert body["records_in_scope"] == in_store
    _no_env_names(body)


def test_newest_window_is_reported_and_its_missing_prefix_is_not_a_broken_link(ent):
    mine = ent.workspace("R11 window")
    owner = _admin_of(ent, mine, "owner-win-r11@example.com")
    _seed(12, mine, "r11.window")
    in_store = ent.db["audit_events"].count_documents({"workspace_id": mine})

    body = owner.get("/api/v1/audit/verify", params={"limit": 5}).json()
    assert body["checked"] == 5
    assert body["truncated_by_limit"] is True
    assert body["records_in_scope"] == in_store
    # The newest 5: the window's oldest record links to one outside it, which
    # is the limit, not a deletion.
    assert body["findings"] == [], body["findings"]
    assert body["verified"] is True
    assert body["status"] == "verified"
    assert "newest 5" in body["honesty"]
    head = ent.db["audit_events"].find_one({"event_hash": body["chain_head"]})
    assert head is not None and head["workspace_id"] == mine


def test_nothing_checked_is_never_verified(ent):
    empty = ent.workspace("R11 empty")
    owner = _admin_of(ent, empty, "owner-empty-r11@example.com")
    # Records the workspace setup itself wrote are removed so the workspace has
    # none at all — the state QA's workspace was in for this walk.
    ent.db["audit_events"].delete_many({"workspace_id": empty})

    res = owner.get("/api/v1/audit/verify")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["checked"] == 0
    assert body["verified"] is None, body
    assert body["status"] == "nothing_checked"
    assert body["store"] == "mongo"
    assert "nothing" in body["honesty"].lower()
    _no_env_names(body)


def test_platform_walk_reports_store_and_window(ent):
    ws = ent.workspace("R11 platform")
    _seed(8, ws, "r11.platform")
    body = ent.admin.get("/api/v1/audit/verify", params={"limit": 3}).json()
    assert body["scope"] == "platform"
    assert body["checked"] == 3
    assert body["truncated_by_limit"] is True
    assert body["store"] == "mongo"
    assert not [f for f in body["findings"] if f["kind"] == "unexplained_prefix"], body["findings"]
    _no_env_names(body)


def test_mongo_read_failure_is_reported_not_answered_from_the_local_file(ent, monkeypatch):
    from pymongo.errors import ServerSelectionTimeoutError

    from services import audit_log

    mine = ent.workspace("R11 mongo down")
    owner = _admin_of(ent, mine, "owner-down-r11@example.com")
    # The local JSONL holds a valid-looking chain for this workspace (what the
    # Mongo-insert fallback leaves behind). 9c75ada4 verified THAT and called it
    # the workspace's chain when the Mongo read failed.
    real = audit_log._mongo_collection
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    _seed(4, mine, "r11.file_only")
    monkeypatch.setattr(audit_log, "_mongo_collection", real)

    class _Down:
        def __init__(self, coll):
            self._coll = coll

        def __getattr__(self, name):
            return getattr(self._coll, name)

        def find(self, *a, **k):
            raise ServerSelectionTimeoutError("r11: mongo unreachable")

        def count_documents(self, *a, **k):
            raise ServerSelectionTimeoutError("r11: mongo unreachable")

    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: _Down(real()))
    res = owner.get("/api/v1/audit/verify")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["store"] == "mongo"
    assert body["status"] == "store_unavailable", body
    assert body["verified"] is None
    assert body["checked"] == 0
    assert body["store_error"]
    assert "r11: mongo unreachable" not in json.dumps(body)  # no raw driver text
    _no_env_names(body)


def test_key_mismatch_and_retired_key_texts_name_no_server_variable(monkeypatch, tmp_path):
    from services import audit_log
    from services.evidence_chain import verify_chain

    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_KEY", "r11-key-one-0123456789abcdef")
    _seed(3, "ws-keys", "r11.keys")
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_KEY", "r11-key-two-0123456789abcdef")
    report = verify_chain(workspace_id="ws-keys")
    assert report["store"] == "file"
    assert {f["kind"] for f in report["findings"]} == {"key_mismatch"}
    _no_env_names(report)


# ── QA's fake-Mongo repro (scratchpad/qa9s/audit_repro.py) as a test ─────────


class _Cur:
    def __init__(self, docs):
        self.docs = docs

    def sort(self, order):
        for k, d in reversed(order):
            self.docs.sort(
                key=lambda x: (x.get(k) is not None, x.get(k) if x.get(k) is not None else 0),
                reverse=(d < 0),
            )
        return self

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    def batch_size(self, _n):
        return self

    def __iter__(self):
        return iter([dict(d) for d in self.docs])


class _Coll:
    name = "audit_events"

    class database:  # noqa: N801 - mimics pymongo's attribute
        name = "fake"

    def __init__(self):
        self.rows = {}

    def create_index(self, *a, **k):
        pass

    def insert_one(self, doc):
        assert doc["_id"] not in self.rows
        self.rows[doc["_id"]] = dict(doc)

    def _match(self, q):
        return [d for d in self.rows.values() if all(d.get(k) == v for k, v in (q or {}).items())]

    def find(self, q=None, *a, **k):
        return _Cur(self._match(q))

    def count_documents(self, q=None, *a, **k):
        return len(self._match(q))


@pytest.mark.parametrize("limit", [50, 5000])
def test_qa9s_repro_workspace_records_outside_the_oldest_window(monkeypatch, tmp_path, limit):
    from services import audit_log
    from services.evidence_chain import verify_chain

    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_KEY", "qa9s-test-key-0123456789abcdef")
    coll = _Coll()
    monkeypatch.setattr(audit_log, "STORE_PATH", pathlib.Path(tmp_path) / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: coll)
    _seed(60, "ws-other", "x.other")
    _seed(15, "ws-mine", "x.mine")

    report = verify_chain(limit=limit, workspace_id="ws-mine")
    # 9c75ada4: limit=50 -> checked 0, verified True.
    assert report["checked"] == 15
    assert report["verified"] is True
    assert report["status"] == "verified"
    assert report["truncated_by_limit"] is False
    assert report["store"] == "mongo"
    _no_env_names(report)


def test_qa9s_repro_empty_workspace_is_nothing_checked(monkeypatch, tmp_path):
    from services import audit_log
    from services.evidence_chain import verify_chain

    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_KEY", "qa9s-test-key-0123456789abcdef")
    coll = _Coll()
    monkeypatch.setattr(audit_log, "STORE_PATH", pathlib.Path(tmp_path) / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: coll)
    _seed(5, "ws-other", "x.other")
    for scope in ("ws-nobody", ""):
        if not scope:
            coll.rows.clear()
        report = verify_chain(limit=50, workspace_id=scope)
        assert report["checked"] == 0
        assert report["verified"] is None
        assert report["status"] == "nothing_checked"
