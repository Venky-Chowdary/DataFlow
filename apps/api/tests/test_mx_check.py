"""P2-3 — MX check: null MX, NXDOMAIN, no-MX-no-A, MX-host denylist, cache."""

from __future__ import annotations

import pytest

from services import denylist_store, mx_check


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setattr(denylist_store, "_path", lambda: tmp_path / "deny.json")
    mx_check._reset_for_tests()
    yield
    mx_check._reset_for_tests()


class _FakeAnswer:
    def __init__(self, exchange):
        self.exchange = exchange


class _NX(Exception):
    pass


def _fake_resolver(mx_map, a_map=None):
    class FakeResolver:
        timeout = 0.0
        lifetime = 0.0

        def resolve(self, domain, rtype):
            if rtype == "MX":
                if domain not in mx_map:
                    import dns.resolver
                    raise dns.resolver.NXDOMAIN
                if mx_map[domain] is None:
                    import dns.resolver
                    raise dns.resolver.NoAnswer
                return [_FakeAnswer(h) for h in mx_map[domain]]
            if rtype == "A":
                if a_map and domain in a_map:
                    return [object()]
                import dns.resolver
                raise dns.resolver.NoAnswer
            raise AssertionError(rtype)

    return FakeResolver


def test_null_mx_refused(monkeypatch):
    monkeypatch.setattr(mx_check, "_resolver",
                        _fake_resolver({"dead.example": ["."]}))
    r = mx_check.check_domain("dead.example")
    assert r["ok"] is False and r["reason"] == "null_mx"


def test_nxdomain_refused(monkeypatch):
    monkeypatch.setattr(mx_check, "_resolver", _fake_resolver({}))
    r = mx_check.check_domain("nope.invalid")
    assert r["ok"] is False and r["reason"] == "domain_nxdomain"


def test_no_mx_no_a_refused(monkeypatch):
    monkeypatch.setattr(mx_check, "_resolver",
                        _fake_resolver({"nomx.example": None}, {}))
    r = mx_check.check_domain("nomx.example")
    assert r["ok"] is False and r["reason"] == "no_mx_no_a"


def test_a_fallback_ok(monkeypatch):
    monkeypatch.setattr(mx_check, "_resolver",
                        _fake_resolver({"bare.example": None},
                                       {"bare.example": True}))
    r = mx_check.check_domain("bare.example")
    assert r["ok"] is True and r["reason"] == "a_fallback"


def test_denied_mx_host_refused(monkeypatch):
    denylist_store.add_entry(value="mx.burner.example", kind="mx_host",
                           source="team", reason="disposable mta", added_by="t")
    monkeypatch.setattr(mx_check, "_resolver",
                        _fake_resolver({"burner.example":
                                        ["mx.burner.example"]}))
    r = mx_check.check_domain("burner.example")
    assert r["ok"] is False and r["reason"] == "mx_host_denied"


def test_normal_mx_ok_and_cached(monkeypatch):
    calls = []
    resolver = _fake_resolver({"good.example": ["mx.good.example"]})
    orig = resolver.resolve

    def counting(self, domain, rtype):
        calls.append((domain, rtype))
        return orig(self, domain, rtype)

    resolver.resolve = counting
    monkeypatch.setattr(mx_check, "_resolver", resolver)
    r1 = mx_check.check_domain("good.example")
    r2 = mx_check.check_domain("good.example")
    assert r1["ok"] and r2["ok"]
    assert len(calls) == 1  # second hit the 24h cache
