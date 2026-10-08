"""Pilot result store, analysis algorithms, filter, ack ledger — real paths."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.ai.copilot.ack_ledger import PilotAckLedger, redact_payload  # noqa: E402
from src.ai.copilot.query_tools import (  # noqa: E402
    _analyze_rows,
    analyze_stored_result,
    filter_stored_result,
)
from src.ai.copilot.result_store import PilotResultStore  # noqa: E402
from src.ai.copilot.tools import infer_tools_from_message  # noqa: E402


def test_analyze_rows_numeric_and_null_signals(tmp_path: Path) -> None:
    rows = [
        {"id": 1, "amt": 10.5, "email": "a@b.com"},
        {"id": 2, "amt": 20.0, "email": None},
        {"id": 3, "amt": 30.0, "email": ""},
        {"id": 4, "amt": 40.0, "email": "c@d.com"},
    ]
    profile = _analyze_rows(rows, ["id", "amt", "email"])
    by_col = {c["column"]: c for c in profile["columns"]}
    assert by_col["id"]["inferred_kind"] == "integer"
    assert by_col["amt"]["inferred_kind"] == "number"
    assert by_col["amt"]["numeric"]["min"] == 10.5
    assert by_col["amt"]["numeric"]["max"] == 40.0
    assert by_col["email"]["nulls"] == 2
    assert by_col["email"]["null_rate"] == 0.5
    assert "email" in profile["signals"]["null_heavy_columns"]


def test_result_store_put_resolve_filter(tmp_path: Path) -> None:
    store = PilotResultStore(path=tmp_path / "results.json", ttl_sec=600)
    rid = store.put(
        rows=[
            {"id": 1, "status": "active"},
            {"id": 2, "status": "paused"},
            {"id": 3, "status": None},
        ],
        columns=["id", "status"],
        meta={"table": "orders", "connector_name": "Local"},
        session_id="sess1",
        source="sample_connector_object",
    )
    assert rid.startswith("pr_")
    doc = store.resolve(session_id="sess1")
    assert doc is not None
    assert doc["result_id"] == rid

    # Point tools at this store via monkeypatch of get_result_store
    import src.ai.copilot.result_store as rs
    import src.ai.copilot.query_tools as qt

    prev = rs._store
    rs._store = store
    try:
        filtered = filter_stored_result(
            session_id="sess1",
            column="status",
            op="is_null",
        )
        assert filtered.success is True
        assert filtered.output["match_count"] == 1
        assert filtered.output["rows"][0]["id"] == 3

        eq = filter_stored_result(
            result_id=rid,
            session_id="sess1",
            column="status",
            op="eq",
            value="active",
        )
        assert eq.success is True
        assert eq.output["match_count"] == 1

        # Omitting the id cannot see another session. Presenting the id can,
        # and that follow-up is then stored on the caller's own session.
        omitted = filter_stored_result(
            session_id="other",
            column="status",
            op="eq",
            value="active",
        )
        assert omitted.success is False
        assert "no stored sample" in omitted.error.lower()
        cross = filter_stored_result(
            result_id=rid,
            session_id="other",
            column="status",
            op="eq",
            value="active",
        )
        assert cross.success is True
        assert cross.output["match_count"] == 1

        analyzed = analyze_stored_result(session_id="sess1")
        # session latest is the filtered child
        assert analyzed.success is True
        assert analyzed.output["analysis"]["row_count_sampled"] >= 1

        focused = analyze_stored_result(result_id=rid, session_id="sess1", column="id")
        assert focused.success is True
        cols = focused.output["analysis"]["columns"]
        assert len(cols) == 1
        assert cols[0]["column"] == "id"
    finally:
        rs._store = prev


def test_nl_routes_followups() -> None:
    planned = infer_tools_from_message("analyze that result")
    assert "analyze_result" in [n for n, _ in planned]

    planned_f = infer_tools_from_message("filter where email is null")
    assert "filter_result" in [n for n, _ in planned_f]
    args = dict(planned_f)["filter_result"]
    assert args["column"] == "email"
    assert args["op"] == "is_null"

    planned_eq = infer_tools_from_message("show rows where status = active")
    assert "filter_result" in [n for n, _ in planned_eq]
    fargs = dict(planned_eq)["filter_result"]
    assert fargs["column"] == "status"
    assert fargs["op"] == "eq"
    assert fargs["value"] == "active"


def test_ack_ledger_redact_and_finalize(tmp_path: Path) -> None:
    ledger = PilotAckLedger(path=tmp_path / "acks.json", ttl_sec=600)
    aid = ledger.put(
        kind="create_connector",
        payload={
            "name": "Demo PG",
            "type": "postgresql",
            "host": "localhost",
            "password": "super-secret",
            "connection_string": "postgresql://u:p@h/db",
        },
        preview={"name": "Demo PG", "type": "postgresql", "has_password": True},
    )
    peek = ledger.peek(aid)
    assert peek is not None
    assert peek["kind"] == "create_connector"
    preview = peek.get("preview") or {}
    assert preview.get("password") in (None, "", "***")
    assert "super-secret" not in str(preview)
    assert "postgresql://u:p@h/db" not in str(preview)

    payload, err = ledger.get_pending_payload(aid)
    assert err == ""
    assert payload["password"] == "super-secret"

    safe = redact_payload(payload)
    assert safe["password"] == "***"
    assert safe["has_password"] is True

    claimed, cerr = ledger.claim(aid, actor="tester", reason="ok")
    assert cerr == ""
    assert claimed["password"] == "super-secret"
    busy, berr = ledger.claim(aid, actor="other")
    assert busy is None
    assert "already" in berr.lower()

    ledger.finalize(aid, actor="tester", reason="ok", result={"connector_id": "c1", "name": "Demo PG"})
    again, err2 = ledger.get_pending_payload(aid)
    assert err2 == ""
    assert again.get("_idempotent") is True
    assert again.get("connector_id") == "c1"


def test_result_store_no_cross_session_leak(tmp_path: Path) -> None:
    store = PilotResultStore(path=tmp_path / "results2.json", ttl_sec=600)
    rid = store.put(
        rows=[{"id": 1}],
        columns=["id"],
        meta={"table": "secret"},
        session_id="owner",
        source="sample_connector_object",
    )
    assert store.resolve(session_id="other") is None
    assert store.resolve() is None
    # The id is the credential. Omitting it still cannot read another session.
    assert store.resolve(result_id=rid, session_id="other") is not None
    assert store.resolve(result_id=rid)["meta"]["table"] == "secret"
    assert store.resolve(result_id=rid, session_id="owner") is not None


def test_ack_ledger_reload_keeps_consumed_for_idempotent_replay(tmp_path: Path) -> None:
    path = tmp_path / "acks_reload.json"
    ledger = PilotAckLedger(path=path, ttl_sec=600)
    aid = ledger.put(
        kind="create_connector",
        payload={"name": "X", "type": "postgresql", "password": "secret"},
        preview={"name": "X", "type": "postgresql"},
    )
    ledger.finalize(aid, actor="tester", reason="ok", result={"connector_id": "c9", "name": "X"})
    # Simulate API restart
    reloaded = PilotAckLedger(path=path, ttl_sec=600)
    payload, err = reloaded.claim(aid, actor="tester")
    assert err == ""
    assert payload.get("_idempotent") is True
    assert payload.get("connector_id") == "c9"


def test_explicit_result_id_reloads_across_stores(tmp_path: Path) -> None:
    """A sample with no session is readable later by id, including from a store
    that loaded the file before the write."""
    path = tmp_path / "results-reload.json"
    reader = PilotResultStore(path=path, ttl_sec=600)
    writer = PilotResultStore(path=path, ttl_sec=600)
    rid = writer.put(
        rows=[{"id": 7}],
        columns=["id"],
        meta={"table": "qa6c_quar"},
        session_id="",
        source="sample_connector_object",
    )
    found = reader.resolve(result_id=rid, session_id="later-chat")
    assert found is not None
    assert found["rows"] == [{"id": 7}]
    assert reader.resolve() is None
    assert reader.resolve(session_id="later-chat") is None
    reloaded = PilotResultStore(path=path, ttl_sec=600)
    assert reloaded.resolve(result_id=rid)["meta"]["table"] == "qa6c_quar"


def test_missing_result_id_is_not_the_no_sample_sentence(tmp_path: Path, monkeypatch) -> None:
    store = PilotResultStore(path=tmp_path / "results-miss.json", ttl_sec=600)
    monkeypatch.setattr(
        "src.ai.copilot.result_store.get_result_store",
        lambda: store,
    )
    missing = analyze_stored_result(result_id="pr_does_not_exist", session_id="owner")
    empty = analyze_stored_result(session_id="owner")
    assert missing.success is False and empty.success is False
    assert missing.error != empty.error
    assert "pr_does_not_exist" in missing.error
    assert "one hour" in missing.error
    assert "no stored sample" in empty.error.lower()
    filtered = filter_stored_result(result_id="pr_also_missing", column="id")
    assert "pr_also_missing" in filtered.error
    assert filtered.error != empty.error


def test_sample_still_routes() -> None:
    planned = infer_tools_from_message("sample airports on Local Postgres")
    names = [n for n, _ in planned]
    assert "sample_connector_object" in names
    assert "analyze_result" not in names
