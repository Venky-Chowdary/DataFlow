"""P0-1 — shared auth state: two API instances share lockout + revocation.

A fake Redis stands in for the shared backend: two SharedAuthState facades
(= two app instances) must see each other's writes.
"""

from __future__ import annotations

import time

import pytest

from services.auth_state import (
    SharedAuthState,
    _MemoryBackend,
    _RedisBackend,
)


class FakeRedis:
    """Dict-backed stand-in for redis-py covering what _RedisBackend calls."""

    def __init__(self):
        self.data: dict[str, str] = {}
        self.sets: dict[str, set] = {}
        self.counters: dict[str, tuple[int, float]] = {}

    def get(self, k):
        return self.data.get(k)

    def set(self, k, v, ex=None):
        self.data[k] = v

    def delete(self, k):
        self.data.pop(k, None)

    def sadd(self, k, m):
        self.sets.setdefault(k, set()).add(m)

    def smembers(self, k):
        return self.sets.get(k, set())

    def srem(self, k, m):
        self.sets.get(k, set()).discard(m)

    def incr(self, k):
        c, reset = self.counters.get(k, (0, time.time() + 3600))
        self.counters[k] = (c + 1, reset)
        return c + 1

    def expire(self, k, ttl, nx=False):
        return True

    def pipeline(self):
        return self

    def execute(self):
        return (self._count, True)


class FakeRedisWithPipe(FakeRedis):
    def incr(self, k):
        self._count = super().incr(k)
        return self._count


def _two_instances() -> tuple[SharedAuthState, SharedAuthState]:
    """One Redis, two app instances."""
    shared = FakeRedis()

    # _RedisBackend uses pipeline().incr for rate counters; give the fake a
    # pipeline that replays incr into the shared dict.
    class Pipe:
        def __init__(self, outer):
            self.outer = outer
            self.ops = []

        def incr(self, k):
            self.ops.append(("incr", k))
            return self

        def expire(self, k, ttl, nx=False):
            self.ops.append(("expire", k, ttl))
            return self

        def execute(self):
            count = 0
            for op in self.ops:
                if op[0] == "incr":
                    c, reset = shared.counters.get(op[1], (0, time.time() + 3600))
                    count = c + 1
                    shared.counters[op[1]] = (count, reset)
            return (count, True)

    shared.pipeline = lambda: Pipe(shared)
    return SharedAuthState(_RedisBackend(shared)), SharedAuthState(_RedisBackend(shared))


def test_revoked_session_visible_across_instances():
    inst_a, inst_b = _two_instances()
    doc = inst_a.create_session(user_id="ada@example.com", ip="1.2.3.4", device_id="dev-1")
    assert inst_b.session_active(doc["jti"])
    inst_a.revoke_session(doc["jti"])
    assert not inst_b.session_active(doc["jti"])


def test_revoke_all_for_user_across_instances():
    inst_a, inst_b = _two_instances()
    s1 = inst_a.create_session(user_id="u@x.com")
    s2 = inst_b.create_session(user_id="u@x.com")
    other = inst_a.create_session(user_id="other@x.com")
    assert inst_a.revoke_all_for_user("u@x.com") == 2
    assert not inst_b.session_active(s1["jti"])
    assert not inst_b.session_active(s2["jti"])
    assert inst_b.session_active(other["jti"])


def test_lockout_counter_shared_across_instances():
    inst_a, inst_b = _two_instances()
    for _ in range(5):
        ok, _ = inst_a.rate_limit_hit("lockout:1.2.3.4", limit=5, window_sec=900)
        assert ok
    ok, retry = inst_b.rate_limit_hit("lockout:1.2.3.4", limit=5, window_sec=900)
    assert not ok and retry == 900


def test_session_fields_and_idle_expiry():
    state = SharedAuthState(_MemoryBackend())
    doc = state.create_session(
        user_id="u@x.com", device_id="d1", ip="1.1.1.1", ua="agent",
        idle_seconds=60, absolute_seconds=3600)
    assert doc["idle_expires_at"] - doc["created_at"] == 60
    assert doc["absolute_expires_at"] - doc["created_at"] == 3600
    fetched = state.get_session(doc["jti"])
    assert fetched["user_id"] == "u@x.com" and fetched["device_id"] == "d1"
    state.touch_session(doc["jti"], idle_seconds=120)
    assert state.get_session(doc["jti"])["last_seen_at"] >= doc["last_seen_at"]


def test_list_sessions_for_user():
    state = SharedAuthState(_MemoryBackend())
    state.create_session(user_id="u@x.com", device_id="a")
    state.create_session(user_id="u@x.com", device_id="b")
    sessions = state.list_sessions("u@x.com")
    assert len(sessions) == 2
    assert {s["device_id"] for s in sessions} == {"a", "b"}
    assert state.list_sessions("nobody@x.com") == []


def test_memory_backend_window_counters_expire():
    state = SharedAuthState(_MemoryBackend())
    ok, _ = state.rate_limit_hit("k", limit=1, window_sec=1)
    assert ok
    ok, retry = state.rate_limit_hit("k", limit=1, window_sec=1)
    assert not ok
