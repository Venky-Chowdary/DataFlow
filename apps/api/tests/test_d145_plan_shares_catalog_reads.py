"""D145 — one plan reads each catalog object once.

``start_transfer`` / ``plan_transfer`` introspected the same table from every
step. Oracle answers each introspection by walking ALL_* dictionary views, so
a plan outlasted the MCP client's request timeout (-32001). Inside a plan the
identical introspection is now shared; outside it nothing is cached.
"""

from __future__ import annotations

import services.schema_introspect as si
from services.introspect_memo import introspect_memo_scope
from src.ai.copilot import transfer_tools


def _fake(monkeypatch):
    calls: list[dict] = []

    def fake(db_type, **kwargs):
        calls.append({"db_type": db_type, **kwargs})
        return {"ok": True, "columns": [{"name": "id", "type": "NUMBER"}], "n": len(calls)}

    monkeypatch.setattr(si, "_introspect_schema", fake)
    return calls


def test_identical_introspection_is_read_once_inside_a_scope(monkeypatch):
    calls = _fake(monkeypatch)
    with introspect_memo_scope():
        first = si.introspect_schema("oracle", host="h", table="T", schema="APP")
        again = si.introspect_schema("oracle", host="h", table="T", schema="APP")
        other = si.introspect_schema("oracle", host="h", table="U", schema="APP")
    assert len(calls) == 2
    assert first == again and other["n"] == 2


def test_callers_cannot_corrupt_the_shared_answer(monkeypatch):
    _fake(monkeypatch)
    with introspect_memo_scope():
        first = si.introspect_schema("oracle", host="h", table="T")
        first["columns"].append({"name": "junk"})
        again = si.introspect_schema("oracle", host="h", table="T")
    assert [c["name"] for c in again["columns"]] == ["id"]


def test_nothing_is_cached_outside_a_scope(monkeypatch):
    calls = _fake(monkeypatch)
    si.introspect_schema("oracle", host="h", table="T")
    si.introspect_schema("oracle", host="h", table="T")
    assert len(calls) == 2


def test_plan_transfer_runs_inside_a_scope(monkeypatch):
    import contextlib

    from services import introspect_memo

    entered: list[bool] = []

    @contextlib.contextmanager
    def recording():
        entered.append(True)
        yield

    monkeypatch.setattr(introspect_memo, "introspect_memo_scope", recording)
    # No connector resolves, so the plan returns its refusal; the scope was
    # opened before any step ran.
    transfer_tools.plan_transfer(source_connector_id="nope", source_table="t")
    assert entered == [True]
