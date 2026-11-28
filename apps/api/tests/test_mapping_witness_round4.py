"""Mapping Witness, review round 4: flag identity, source identity, abandoned writes, store size.

1. A Keep names exactly one flag: the flag id covers the suggested column, the
   reason and the evidence counts, and a keyed digest of the source sample, so an
   edited file with the same name and row count, or a flag pointing at another
   column, is another flag. An old Keep whose id no longer matches never applies.
2. Source identity folds case and whitespace only outside literals, quoted
   identifiers and comments, and keeps a file's or object's whole path.
3. A request past its deadline (504) writes nothing, and every writer to the
   file store is serialised, so concurrent writers both land.
4. Pair-set edits in one open session supersede the session's unsubmitted record
   instead of appending, and earlier reports are kept by hash, not again.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from types import SimpleNamespace

import pytest

from services.mapping_witness import STATUS_FLAGGED, witness_mappings
from tests.helpers import witness_fixtures as F
from tests.test_mapping_witness import MAP_DEST, _job_doc, _map_check, _run, submitted  # noqa: F401


@pytest.fixture()
def client(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from fastapi.testclient import TestClient

    from src.main import app

    return TestClient(app)


def _flag(report, source):
    [pair] = [p for p in report["pairs"] if p["status"] == STATUS_FLAGGED and p["source"] == source]
    return pair["flag"]


# ── 1. a Keep names exactly one flag ─────────────────────────────────────────


def test_an_edited_source_of_one_name_and_size_shows_its_flag_again(client, submitted, monkeypatch):  # noqa: F811
    """1(a): customers.csv re-uploaded with edited logins (same name, rows, counts, reason): the Keep is gone."""
    from services.ai_decision_ledger import KIND_WITNESS_FLAG, build_ledger
    from services.mapping_witness_service import snapshot_for_request

    monkeypatch.setattr("services.data_residency.enforce_request_residency", lambda r: None)
    src, _ = F.customers()
    ref = {"kind": "file", "name": "customers.csv"}
    session = "map-session-edit1"
    first = _map_check(client, src, session=session, source_ref=ref)
    flag = _flag(first["report"], "login")
    kept = client.post("/api/v1/transfer/mapping-witness/review", json={
        "mapping_digest": first["mapping_digest"], "session_id": session, "destination": MAP_DEST,
        "source": "login", "decision": "kept", "flag_id": flag["id"]})
    assert kept.status_code == 200, kept.text
    # The same file checked again: the same flag, and the Keep stands (confirmed earlier; must stay).
    again = _map_check(client, src, session=session, source_ref=ref, trigger="explicit")
    assert _flag(again["report"], "login")["id"] == flag["id"]

    edited = [{**r, "login": r["login"] + "x"} for r in src]  # same name, row count, non-null counts
    after = _map_check(client, edited, session=session, source_ref=ref)
    new_flag = _flag(after["report"], "login")
    assert new_flag["reason"] == flag["reason"] and new_flag["kind"] == flag["kind"]
    assert new_flag["id"] != flag["id"], "an edited source kept the old flag id, so the old Keep applies"
    request = _run(client, submitted, {"source_kind": "file", "source_format": "csv"}, session=session,
                   mappings=F.LOGIN_INTO_EMAIL_ONLY, file_name="customers.csv")
    snap = snapshot_for_request(request)
    assert snap["report_sha256"] == after["report_sha256"]
    assert snap["flag_reviews"] == []
    [row] = [e for e in build_ledger(_job_doc(monkeypatch, request))["entries"] if e["kind"] == KIND_WITNESS_FLAG]
    assert row["decision"]["status"] != "kept"


def test_two_keyed_flags_pointing_at_different_columns_are_two_flags():
    """1(b): the same pair flagged once toward username and once toward last_name has two ids."""
    src, dst = F.customers()
    moved = [{**d, "username": d["last_name"], "last_name": d["username"]} for d in dst]  # same counts
    to_username = _flag(witness_mappings(F.LOGIN_INTO_EMAIL_ONLY, src, dst, key=F.KEY), "login")
    to_last_name = _flag(witness_mappings(F.LOGIN_INTO_EMAIL_ONLY, src, moved, key=F.KEY), "login")
    assert to_username["kind"] == to_last_name["kind"]
    assert (to_username["suggested_target"], to_last_name["suggested_target"]) == ("username", "last_name")
    assert to_username["id"] != to_last_name["id"]


def test_a_stored_keep_whose_id_no_longer_matches_never_applies(monkeypatch, tmp_path):
    """A Keep stored under the old id formula (inputs, pair and kind only) is not applied to today's flag."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.mapping_witness_service as svc

    src, dst = F.customers()
    maps = F.LOGIN_INTO_EMAIL_ONLY
    report = witness_mappings(maps, src, dst, key=F.KEY)
    pair = next(p for p in report["pairs"] if p["source"] == "login")
    old_id = hashlib.sha256(
        f"{report['inputs_sha256']}|login|{pair['target']}|{pair['flag']['kind']}".encode()).hexdigest()[:20]
    assert old_id != pair["flag"]["id"]
    digest = svc.mapping_digest(maps, "dest-1")
    saved = svc.save_report(report, workspace_id="ws1", session="session-old-keep", identity="dest-1",
                            digest=digest, source="file|a.csv")
    data, key, record, _new = svc._activity("ws1", "session-old-keep", "dest-1")
    record["flag_reviews"] = [{"source": "login", "flagged_target": pair["target"], "flag_kind": pair["flag"]["kind"],
                               "flag_id": old_id, "report_sha256": saved["report_sha256"],
                               "source_identity": "file|a.csv", "mapping_digest": digest, "decision": "kept",
                               "new_target": None, "by": "ann@example.com", "at": svc._now()}]
    data[key] = record
    svc._DOC.save(data)
    snap = svc.build_snapshot(workspace_id="ws1", session="session-old-keep", identity="dest-1",
                              source="file|a.csv", mappings=maps)
    assert snap["report_sha256"] == saved["report_sha256"]
    assert snap["flag_reviews"] == []


def test_the_source_digest_is_keyed_and_carries_no_value(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.mapping_witness_service as svc
    from services.mapping_witness import source_sample_digest

    src, _ = F.customers()
    rows = [{"login": "admin"} for _ in range(20)]
    maps = [{"source": "login", "target": "email"}]
    unkeyed = hashlib.sha256(json.dumps({"columns": ["login"], "rows": ['["admin"]'] * 20},
                                        separators=(",", ":")).encode()).hexdigest()
    key_a, key_b = svc.workspace_sample_key("ws-a"), svc.workspace_sample_key("ws-b")
    assert key_a == svc.workspace_sample_key("ws-a") and key_a != key_b
    a = source_sample_digest(maps, rows, None, key_a)
    assert a != unkeyed and a != source_sample_digest(maps, rows, None, key_b)
    assert a == source_sample_digest(maps, list(reversed(rows)), None, key_a)
    report = svc.run_witness(mappings=maps, source_rows=rows, workspace_id="ws-a")
    assert a not in json.dumps(report) and "admin" not in json.dumps(report)


# ── 2. source identity ───────────────────────────────────────────────────────

_Q = "SELECT id, name FROM customers WHERE region = 'EMEA'"
SOURCE_PAIRS = [
    # (left, right, should_collide, why)
    ({"statement": _Q}, {"statement": _Q.lower().replace("'emea'", "'EMEA'")}, True, "keyword and name case"),
    ({"statement": _Q}, {"statement": "  select id ,name\n  from   customers where region='EMEA' ; "}, True,
     "whitespace between tokens"),
    ({"statement": _Q}, {"statement": _Q.replace("'EMEA'", "'emea'")}, False, "string-literal case"),
    ({"statement": 'SELECT * FROM "Orders"'}, {"statement": 'SELECT * FROM "orders"'}, False,
     "quoted-identifier case"),
    ({"statement": "SELECT * FROM t WHERE a = 'x  y'"}, {"statement": "SELECT * FROM t WHERE a = 'x y'"}, False,
     "whitespace inside a literal"),
    ({"statement": "SELECT * FROM t WHERE a = 'it''s'"}, {"statement": "SELECT * FROM t WHERE a = 'IT''S'"}, False,
     "an escaped quote inside a literal"),
    ({"statement": "SELECT `Col` FROM t"}, {"statement": "SELECT `col` FROM t"}, False, "backtick identifier case"),
    ({"statement": "SELECT $$Ab$$"}, {"statement": "SELECT $$ab$$"}, False, "dollar-quoted text"),
    ({"kind": "file", "name": "2023/customers.csv"}, {"kind": "file", "name": "2024/customers.csv"}, False,
     "two folders, one file name"),
    ({"kind": "database", "name": "exports/2023/customers.csv"},
     {"kind": "database", "name": "exports/2024/customers.csv"}, False, "two object paths"),
    ({"kind": "file", "name": "2023\\customers.csv"}, {"kind": "file", "name": "2023/customers.csv"}, True,
     "one path, either separator"),
    ({"kind": "file", "name": "customers.csv"}, {"kind": "file", "name": "customers.csv"}, True, "one file"),
    # Unquoted names fold only where the source engine folds them; paths never fold.
    ({"statement": "SELECT * FROM Orders", "engine": "postgres"},
     {"statement": "select * from orders", "engine": "postgresql"}, True, "postgres folds unquoted names"),
    ({"statement": "SELECT * FROM Orders", "engine": "oracle"},
     {"statement": "SELECT * FROM ORDERS", "engine": "oracle"}, True, "oracle folds unquoted names"),
    ({"statement": "SELECT * FROM Orders", "engine": "snowflake"},
     {"statement": "SELECT * FROM orders", "engine": "snowflake"}, True, "snowflake folds unquoted names"),
    ({"statement": "SELECT * FROM Orders", "engine": "mssql"},
     {"statement": "SELECT * FROM orders", "engine": "sqlserver"}, True, "sql server ignores name case"),
    ({"statement": "SELECT * FROM Orders", "engine": "sqlite"},
     {"statement": "SELECT * FROM orders", "engine": "sqlite"}, True, "sqlite ignores name case"),
    ({"statement": "SELECT * FROM Orders", "engine": "mysql"},
     {"statement": "SELECT * FROM orders", "engine": "mysql"}, False, "mysql keeps table-name case"),
    ({"statement": "SELECT * FROM Orders", "engine": "mariadb"},
     {"statement": "select *  from Orders", "engine": "mysql"}, True, "mysql still folds keywords and spacing"),
    ({"statement": "SELECT * FROM Orders"}, {"statement": "SELECT * FROM orders"}, False,
     "an engine not named keeps name case"),
    ({"statement": 'SELECT * FROM "Orders"', "engine": "postgresql"},
     {"statement": 'SELECT * FROM "orders"', "engine": "postgresql"}, False, "postgres quoted names keep case"),
    ({"name": "Orders", "engine": "postgresql"}, {"name": "orders", "engine": "postgresql"}, True,
     "a postgres table in either case"),
    ({"name": "Orders", "engine": "mysql"}, {"name": "orders", "engine": "mysql"}, False,
     "two mysql tables by case"),
    ({"name": "s3://b/Data/a.csv", "engine": "s3"}, {"name": "s3://b/data/a.csv", "engine": "s3"}, False,
     "s3 keys by case"),
    ({"name": "Data/a.csv", "engine": "azure_blob"}, {"name": "data/a.csv", "engine": "azure_blob"}, False,
     "azure blobs by case"),
    ({"kind": "file", "name": "Data/a.csv"}, {"kind": "file", "name": "data/a.csv"}, False, "local files by case"),
]


@pytest.mark.parametrize("left,right,collide,why", SOURCE_PAIRS, ids=[p[3] for p in SOURCE_PAIRS])
def test_source_identity_collides_only_for_the_same_source(left, right, collide, why):
    from services.mapping_witness_service import source_identity

    def ref(r):
        return {"kind": r.get("kind", "database"), "name": r.get("name", ""), "statement": r.get("statement", ""),
                "engine": r.get("engine", "")}

    assert (source_identity(ref(left)) == source_identity(ref(right))) is collide, why


# ── 3. abandoned and concurrent writes ───────────────────────────────────────


def test_a_check_past_its_deadline_writes_nothing(monkeypatch, tmp_path):
    """The route answered 504; the worker finishing later must not record the check."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from fastapi.testclient import TestClient

    import services.mapping_witness_service as svc
    import src.routers.verified_ai_router as router_mod
    from src.main import app

    done = threading.Event()
    outcome: list = []
    real_save = svc.save_report

    def save_and_signal(*args, **kwargs):
        try:
            outcome.append(real_save(*args, **kwargs))
        finally:
            done.set()

    monkeypatch.setattr(router_mod, "WITNESS_DEADLINE_SECONDS", 0.5)
    monkeypatch.setattr(svc, "run_witness", lambda **kw: time.sleep(1.2) or {"pairs": []})
    monkeypatch.setattr(svc, "save_report", save_and_signal)
    started = time.monotonic()
    res = TestClient(app).post("/api/v1/transfer/mapping-witness", json={
        "mappings": F.CORRECT_MAPPING, "session_id": "map-session-late1"})
    assert res.status_code == 504 and time.monotonic() - started < 1.2
    assert done.wait(10), "the abandoned worker never reached its save"  # it never leaks into the next test
    assert outcome == [None]
    records = [k for k in svc._DOC.load() if str(k).startswith("witness::")]
    assert records == []


def test_a_save_that_lands_just_before_the_deadline_is_answered_with_what_was_saved(monkeypatch, tmp_path):
    """The save passed the guard, then the deadline fired before the worker returned: the reply is the
    record that exists (200), never "did not finish"."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from fastapi.testclient import TestClient

    import services.mapping_witness_service as svc
    import src.routers.verified_ai_router as router_mod
    from src.main import app

    done = threading.Event()
    real_save = svc.save_report

    def save_then_stall(*args, **kwargs):
        try:
            saved = real_save(*args, **kwargs)
            time.sleep(1.0)  # past the 0.5 s deadline, after the save landed
            return saved
        finally:
            done.set()

    monkeypatch.setattr(router_mod, "WITNESS_DEADLINE_SECONDS", 0.5)
    monkeypatch.setattr(svc, "save_report", save_then_stall)
    src, _ = F.customers()
    res = TestClient(app).post("/api/v1/transfer/mapping-witness", json={
        "mappings": F.LOGIN_INTO_EMAIL_ONLY, "source_rows": src, "destination_table_exists": False,
        "session_id": "map-session-land1", "source_ref": {"kind": "file", "name": "customers.csv"}})
    assert done.wait(10)
    assert res.status_code == 200, res.text
    stored = svc.latest_report(workspace_id="", session="map-session-land1", digest=res.json()["mapping_digest"])
    assert stored is not None and stored["report_sha256"] == res.json()["report_sha256"]


def test_concurrent_writers_to_the_file_store_both_land(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.mapping_witness_service as svc

    real_load = svc._DOC._file_load

    def slow_load():
        data = real_load()
        time.sleep(0.2)  # both writers have read before either writes, unless they are serialised
        return data

    monkeypatch.setattr(svc._DOC, "_file_load", slow_load)
    errors: list = []

    def check(i):
        try:
            svc.save_report({"pairs": [], "n": i}, workspace_id="ws1", session=f"session-conc-{i}", identity="x",
                            digest=f"{i:064d}")
        except Exception as exc:  # noqa: BLE001 - reported below
            errors.append(exc)

    def approve():
        try:
            svc.record_map_decisions(workspace_id="ws1", session="session-conc-9", identity="x", actor="a@example.com",
                                     mappings=[{"source": "s", "target": "t", "approved": True}])
        except Exception as exc:  # noqa: BLE001 - reported below
            errors.append(exc)

    threads = [threading.Thread(target=check, args=(i,)) for i in range(2)] + [threading.Thread(target=approve)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert not errors
    stored = json.loads(svc._store_path().read_text(encoding="utf-8"))
    assert {f"witness::ws1::session-conc-{i}::{i:064d}" for i in range(2)} <= set(stored)
    assert any(k.startswith("activity::ws1::session-conc-9::") for k in stored)


# ── 4. store size ────────────────────────────────────────────────────────────


def test_pair_set_edits_in_one_session_supersede_instead_of_appending(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.mapping_witness_service as svc

    src, dst = F.customers()
    one = None
    for i in range(20):  # each edit is another pair set, so another mapping digest
        maps = [m for m in F.CORRECT_MAPPING if m["source"] != "family"] + [{"source": "family", "target": f"x_{i}"}]
        report = witness_mappings(maps, src, dst, key=F.KEY)
        for _ in range(3):  # re-checks of one pair set keep one record
            svc.save_report(report, workspace_id="ws1", session="session-edit-many", identity="dest-1",
                            digest=svc.mapping_digest(maps, "dest-1"), source="file|a.csv")
        if one is None:
            one = svc._store_path().stat().st_size
    stored = json.loads(svc._store_path().read_text(encoding="utf-8"))
    records = [k for k in stored if k.startswith("witness::")]
    assert len(records) == 1, len(records)
    [record] = [stored[k] for k in records]
    assert [("report" in e) for e in record["reports"]] == [False, False, True]
    # One record, its latest report whole, earlier ones and superseded pair sets by hash and flag id.
    assert svc._store_path().stat().st_size <= one * 1.5, (svc._store_path().stat().st_size, one)
    # The last pair set is still what a run with it finds.
    snap = svc.build_snapshot(workspace_id="ws1", session="session-edit-many", identity="dest-1",
                              source="file|a.csv", mappings=maps)
    assert snap["report_sha256"] == record["reports"][-1]["report_sha256"]


def test_a_submitted_record_and_other_sources_are_never_superseded(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.mapping_witness_service as svc
    from src.transfer.models import EndpointConfig

    src, dst = F.customers()
    dest = EndpointConfig(kind="database", format="postgresql", host="db.internal.test", port=5432,
                          database="crm", schema="public", table="customers")
    identity = svc.destination_identity(dest, "ws1")
    first = F.LOGIN_INTO_EMAIL_ONLY
    svc.save_report(witness_mappings(first, src, dst, key=F.KEY), workspace_id="ws1", session="session-sub1",
                    identity=identity, digest=svc.mapping_digest(first, identity), source="database|legacy")
    snap = svc.snapshot_for_request(SimpleNamespace(
        workspace_id="ws1", destination=dest, mappings=first, witness_session_id="session-sub1",
        source=SimpleNamespace(kind="database", table="legacy", collection="", extra=None), source_filename=""))
    assert snap["report"] is not None
    second = F.CORRECT_MAPPING
    svc.save_report(witness_mappings(second, src, dst, key=F.KEY), workspace_id="ws1", session="session-sub1",
                    identity=identity, digest=svc.mapping_digest(second, identity), source="database|legacy")
    svc.save_report(witness_mappings(second, src, dst, key=F.KEY), workspace_id="ws1", session="session-sub1",
                    identity=identity, digest=svc.mapping_digest(F.SWAPPED_NAMES, identity), source="database|other")
    stored = json.loads(svc._store_path().read_text(encoding="utf-8"))
    assert len([k for k in stored if k.startswith("witness::ws1::session-sub1::")]) == 3


def test_a_change_answered_after_the_edit_superseded_its_check_still_lands(monkeypatch, tmp_path):
    """Map records "changed" against the report that flagged the pair; the edit's own check may land first."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    import services.mapping_witness_service as svc

    src, dst = F.customers()
    before = F.LOGIN_INTO_EMAIL_ONLY
    after = [{**m, "target": "username"} if m["source"] == "login" else m for m in before]
    d_before, d_after = svc.mapping_digest(before, "dest-1"), svc.mapping_digest(after, "dest-1")
    svc.save_report(witness_mappings(before, src, dst, key=F.KEY), workspace_id="ws1", session="session-late-chg",
                    identity="dest-1", digest=d_before, source="file|a.csv")
    svc.save_report(witness_mappings(after, src, dst, key=F.KEY), workspace_id="ws1", session="session-late-chg",
                    identity="dest-1", digest=d_after, source="file|a.csv")
    assert svc.latest_report(workspace_id="ws1", session="session-late-chg", digest=d_before) is None
    entry = svc.record_review(workspace_id="ws1", session="session-late-chg", identity="dest-1", digest=d_before,
                              source="login", decision="changed", new_target="username", actor="ann@example.com")
    assert entry["flagged_target"] == "email"
    snap = svc.build_snapshot(workspace_id="ws1", session="session-late-chg", identity="dest-1",
                              source="file|a.csv", mappings=after)
    assert [(r["decision"], r["new_target"]) for r in snap["flag_reviews"]] == [("changed", "username")]
