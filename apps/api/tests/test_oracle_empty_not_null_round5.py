"""Round 5: Studio can take every Oracle ``''`` gate action, and a found ``''`` is not forgotten.

Studio calls ``POST /api/v1/preflight/oracle-empty/sign`` with the body
``apps/web/src/lib/oracleEmptyActions.ts`` builds, keeps what it returns on its
mappings, and sends them with the next ``/preflight/run``.
"""

from __future__ import annotations

import importlib
import sqlite3

import pytest

from services import oracle_empty_not_null as oe

_MAPS = [{"source": c, "target": c.upper(), "confidence": 0.99, "target_type": "TEXT"} for c in ("id", "note")]


@pytest.fixture(autouse=True)
def _fresh_measurements(monkeypatch):
    monkeypatch.setattr(oe, "_MEASURED", {})


@pytest.fixture
def sqlite_source(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(tmp_path))
    db = tmp_path / "src.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id TEXT PRIMARY KEY NOT NULL, note TEXT NOT NULL)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [("a", "x"), ("b", "")])
    conn.commit()
    conn.close()
    return {"type": "sqlite", "database": str(db)}


@pytest.fixture
def client(sqlite_source, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    router_module = importlib.import_module("src.routers.preflight_router")
    monkeypatch.setattr(
        router_module, "inspect_destination_for_preflight",
        lambda **_kw: {"connected": True, "table_exists": False, "db_type": "oracle",
                       "can_create_table": True, "can_write": True},
    )
    from src.main import app

    with TestClient(app) as c:
        yield c


def _run(client, cfg, mappings):
    resp = client.post("/api/v1/preflight/run", json={
        "columns": ["id", "note"], "column_types": {"id": "TEXT", "note": "TEXT"}, "row_count": 2,
        "mappings": mappings, "sample_rows": [{"id": "a", "note": "x"}],
        "source_kind": "database", "source_type": "sqlite", "source_config": cfg, "source_table": "t",
        "dest_kind": "database", "dest_type": "oracle", "dest_host": "127.0.0.1", "dest_table": "DEST",
    })
    assert resp.status_code == 200, resp.text
    return resp.json()


def _g3o(body):
    gate = next((g for g in body.get("gates") or [] if g.get("id") == oe.GATE_ID), None)
    blocker = next((b for b in body.get("blockers") or [] if b.get("id") == oe.GATE_ID), None)
    return gate, blocker


def _merge(mappings, signed):
    """What Studio does with the response: put each returned field on its (source, target) pair."""
    by = {(s["source"], s["target"]): s for s in signed["signed"]}
    out = []
    for m in mappings:
        hit = by.get((m["source"], m["target"]))
        out.append({**m, signed["key"]: hit[signed["key"]]} if hit else dict(m))
    return out


def test_studio_create_nullable_request_is_accepted_and_clears_the_block(client, sqlite_source):
    body = _run(client, sqlite_source, _MAPS)
    gate, blocker = _g3o(body)
    assert gate["status"] == "block" and body["passed"] is False
    # Studio reads the gate's own actions (the same list the blocker's guidance carries).
    assert gate["details"]["suggested_actions"] == blocker["guidance"]["suggested_actions"]
    action = next(a for a in gate["details"]["suggested_actions"] if a["kind"] == oe.OPTION)
    # Studio's request: the action's columns, the operator's reason, the pairs it maps.
    resp = client.post("/api/v1/preflight/oracle-empty/sign", json={
        "mappings": [{"source": m["source"], "target": m["target"], "target_type": m["target_type"]}
                     for m in _MAPS],
        "risk_acceptance": {"option": oe.OPTION, "columns": action["risk_acceptance"]["columns"],
                            "reason": "legacy rows carry '' for unknown"},
        "dest_table": "DEST",
    })
    assert resp.status_code == 200, resp.text
    signed = resp.json()
    assert signed["key"] == oe.CONTRACT_KEY and [s["target"] for s in signed["signed"]] == ["NOTE"]
    contract = signed["signed"][0][oe.CONTRACT_KEY]
    assert contract["approved_by"] == "local operator (authentication is off)"
    after = _run(client, sqlite_source, _merge(_MAPS, signed))
    gate, blocker = _g3o(after)
    assert blocker is None and gate["status"] == "pass", gate
    assert contract["risk_id"] in gate["message"]


def test_studio_check_now_request_is_accepted_and_the_next_validate_honours_it(client, sqlite_source,
                                                                                 monkeypatch):
    seen = []
    real = oe.measure

    def spy(targets, **kw):
        seen.append(sorted(kw.get("check_now") or []))
        return real(targets, **kw)

    monkeypatch.setattr(oe, "measure", spy)
    resp = client.post("/api/v1/preflight/oracle-empty/sign", json={
        "mappings": [{"source": m["source"], "target": m["target"]} for m in _MAPS],
        "risk_acceptance": {"option": oe.CHECK_OPTION, "columns": ["NOTE"]},
        "dest_table": "DEST",
    })
    assert resp.status_code == 200, resp.text
    signed = resp.json()
    stamp = signed["signed"][0][oe.CHECK_KEY]
    assert signed["key"] == oe.CHECK_KEY and stamp["signature"] and stamp["table"] == "DEST"
    _run(client, sqlite_source, _merge(_MAPS, signed))
    assert seen[-1] == ["NOTE"]
    # The stamp as Studio would keep it, edited by hand: ignored.
    forged = _merge(_MAPS, signed)
    forged[1][oe.CHECK_KEY] = {**forged[1][oe.CHECK_KEY], "expires_at": "2099-01-01T00:00:00+00:00"}
    _run(client, sqlite_source, forged)
    assert seen[-1] == []


def test_the_sign_route_refuses_other_options_and_unmapped_columns(client):
    bad = client.post("/api/v1/preflight/oracle-empty/sign", json={
        "mappings": [dict(_MAPS[1])], "risk_acceptance": {"option": "cast_and_continue", "columns": ["NOTE"]},
    })
    assert bad.status_code == 400 and "Nothing was signed" in bad.json()["detail"]
    unmapped = client.post("/api/v1/preflight/oracle-empty/sign", json={
        "mappings": [dict(_MAPS[1])], "risk_acceptance": {"option": oe.CHECK_OPTION, "columns": ["OTHER"]},
        "dest_table": "DEST",
    })
    assert unmapped.status_code == 400 and "unmapped" in unmapped.json()["detail"]


def test_the_sign_route_names_the_signed_in_user_and_needs_one_when_auth_is_on(monkeypatch):
    from fastapi import HTTPException

    from services import workspace_filter
    from src.routers.preflight_router import OracleEmptySignRequest, sign_oracle_empty_action
    from src.services import auth_service

    req = OracleEmptySignRequest(mappings=[dict(_MAPS[1])], dest_table="DEST", risk_acceptance={
        "option": oe.OPTION, "columns": ["NOTE"], "reason": "r", "approved_by": "someone else"})
    monkeypatch.setattr(workspace_filter, "current_actor", lambda: "alice@example.com")
    out = sign_oracle_empty_action(req)
    assert out["signed"][0][oe.CONTRACT_KEY]["approved_by"] == "alice@example.com"
    monkeypatch.setattr(workspace_filter, "current_actor", lambda: "")
    monkeypatch.setattr(auth_service, "auth_required", lambda: True)
    with pytest.raises(HTTPException) as exc:
        sign_oracle_empty_action(req)
    assert exc.value.status_code == 401


def test_a_found_empty_string_is_not_downgraded_by_a_later_shorter_check(tmp_path, monkeypatch):
    """Run 1 (Check now) finds the '' past the walk bound; run 2 stops earlier and still says found."""
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(tmp_path))
    db = tmp_path / "big.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(i, "" if i == 2999 else "x") for i in range(1, 3001)])
    conn.commit()
    conn.close()
    cfg = {"type": "sqlite", "database": str(db)}
    monkeypatch.setattr(oe, "SCAN_MAX_ROWS", 1000)
    # A transform the push-down cannot answer: the rows are walked, as on a large table.
    maps = [{"source": "note", "target": "NOTE", "target_type": "TEXT", "transform": "trim"}]
    ctx = dict(destination_db_type="oracle", column_types={"note": "TEXT"}, column_nullability={"note": False},
               dest_table_exists=False, source_kind="database", source_format="sqlite", source_config=cfg,
               source_table="t", destination_table="DEST", sync_mode="full_refresh_overwrite")
    asked = oe.stamp_check_now([dict(m) for m in maps], {"option": oe.CHECK_OPTION, "columns": ["NOTE"]},
                               table="DEST")
    first, _ = oe.preflight_hook({**ctx, "write_maps": asked})
    assert first["details"]["columns"][0]["found"] and first["details"]["columns"][0]["count"] == 1
    second, blockers = oe.preflight_hook({**ctx, "write_maps": [dict(m) for m in maps]})
    col = second["details"]["columns"][0]
    assert second["status"] == "block" and col["found"] and col["count"] == 1, second["message"]
    assert "NOTE (NOT NULL from the source) would receive 1 empty string(s)" in second["message"]
    assert "was checked for empty strings at" in second["message"] and "being reused until" in second["message"]
    ids = {a["id"] for b in blockers for a in b["suggested_actions"]}
    assert "oracle_empty_check_again" in ids
    # Checking again is a fresh check: a check that cannot finish is never answered from memory.
    monkeypatch.setattr(oe, "CHECK_NOW_SECONDS", 0.0)
    again, _ = oe.preflight_hook({**ctx, "write_maps": oe.stamp_check_now(
        [dict(m) for m in maps], {"option": oe.CHECK_OPTION, "columns": ["NOTE"]}, table="DEST")})
    assert not again["details"]["columns"][0].get("reused_check")
