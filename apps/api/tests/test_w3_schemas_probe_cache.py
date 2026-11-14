"""W3 round 2, ERR-25 cost: /query/schemas reuses a recent successful probe.

The schema selector calls /query/schemas on render. Since ERR-25 the route
probes the connection so a failed one is not reported as connected; a
successful probe is now reused for a short TTL per (workspace, connection).
A failed probe is never cached: the next call probes again.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest


@pytest.fixture
def schemas(monkeypatch):
    from services import connector_probe
    from src.routers import query_router

    calls: list[str] = []
    outcome = {"ok": True}

    def probe(connector_id, **_kw):
        calls.append(connector_id)
        if outcome["ok"]:
            return True, "Connected", {}
        return False, "PostgreSQL rejected the sign-in for user \"x\": the username or password is wrong.", {}

    monkeypatch.setattr(connector_probe, "probe_saved_connector", probe)
    monkeypatch.setattr(
        connector_probe,
        "endpoint_from_saved_connector",
        lambda *_a, **_k: SimpleNamespace(format="postgresql", database="db", schema="public"),
    )
    monkeypatch.setattr(query_router, "_check_workspace_read", lambda *_a, **_k: None)
    monkeypatch.setattr(query_router, "_check_connector_read", lambda *_a, **_k: None)
    query_router._clear_schemas_probe_cache()
    clock = {"now": 1000.0}
    monkeypatch.setattr(query_router, "_probe_clock", lambda: clock["now"])
    yield query_router, calls, outcome, clock
    query_router._clear_schemas_probe_cache()


def _call(query_router, connector_id="c1", workspace="ws-a"):
    import asyncio

    body = query_router.QuerySchemaRequest(connector_id=connector_id)
    return asyncio.run(query_router.query_schemas(body, SimpleNamespace(), workspace))


def test_a_successful_probe_is_reused_within_the_ttl(schemas):
    query_router, calls, _outcome, clock = schemas
    assert _call(query_router).connected is True
    clock["now"] += 10
    assert _call(query_router).connected is True
    assert calls == ["c1"], "the second render must reuse the recent successful probe"
    clock["now"] += query_router._SCHEMAS_PROBE_TTL_SECONDS + 1
    assert _call(query_router).connected is True
    assert calls == ["c1", "c1"], "past the TTL the connection is probed again"


def test_the_cache_is_keyed_by_workspace_and_connection(schemas):
    query_router, calls, _outcome, _clock = schemas
    _call(query_router, "c1", "ws-a")
    _call(query_router, "c1", "ws-b")
    _call(query_router, "c2", "ws-a")
    assert calls == ["c1", "c1", "c2"]


def test_a_failed_probe_is_never_cached(schemas):
    query_router, calls, outcome, _clock = schemas
    outcome["ok"] = False
    first = _call(query_router)
    second = _call(query_router)
    assert first.connected is False and second.connected is False
    assert "rejected the sign-in" in first.message
    assert calls == ["c1", "c1"], "a failure must be probed again, never served from cache"
    outcome["ok"] = True
    assert _call(query_router).connected is True
    assert calls == ["c1", "c1", "c1"]
