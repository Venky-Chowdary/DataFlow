"""D244 — Redis preflight over a slow link spent 88.5 s on 30 hashes (0.8 s direct):
``read_keys_batch`` paid two round trips per key (TYPE, then HGETALL), and four
preflight gates read the same keys. Over a ~300 ms tunnel that alone is ~70 s, past
the 45 s budget. A page of keys must cost a constant number of round trips.
"""
from __future__ import annotations

import json

import pytest

redis = pytest.importorskip("redis")

_CFG = {"host": "127.0.0.1", "port": 6379, "database": "14"}
_PREFIX = "d244rt:"
_HANDSHAKE = {b"HELLO", b"CLIENT", b"SELECT", b"AUTH"}


@pytest.fixture()
def live(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    client = redis.Redis(host="127.0.0.1", port=6379, db=14, socket_connect_timeout=2)
    try:
        client.ping()
    except Exception as exc:  # pragma: no cover - environment
        pytest.skip(f"live Redis on 127.0.0.1:6379 unavailable: {exc}")
    for key in client.scan_iter(f"{_PREFIX}*"):
        client.delete(key)
    yield client
    for key in client.scan_iter(f"{_PREFIX}*"):
        client.delete(key)
    client.close()


def _count_round_trips(monkeypatch) -> list[int]:
    sent = [0]
    original = redis.connection.Connection.send_packed_command

    def counting(self, command, check_health=True):
        parts = command if isinstance(command, list) else [command]
        verb = b"".join(bytes(p) for p in parts).split(b"\r\n")[2].upper()
        # The per-connection handshake is paid once per client, not per key.
        if verb not in _HANDSHAKE:
            sent[0] += 1
        return original(self, command, check_health)

    monkeypatch.setattr(redis.connection.Connection, "send_packed_command", counting)
    return sent


def test_thirty_hashes_read_in_constant_round_trips(live, monkeypatch):
    from connectors.redis_reader import RedisScanState, read_keys_batch

    for i in range(30):
        live.hset(f"{_PREFIX}{i}", mapping={"id": str(i), "name": f"n{i}"})
    sent = _count_round_trips(monkeypatch)
    batch, _ = read_keys_batch(
        cfg=_CFG, pattern=f"{_PREFIX}*", limit=500, scan_state=RedisScanState()
    )
    assert len(batch.rows) == 30
    assert sent[0] <= 4, f"{sent[0]} round trips for 30 keys"


def test_batched_values_equal_per_key_reads_for_every_type(live):
    from connectors.redis_reader import RedisScanState, _read_redis_value, read_keys_batch

    live.set(f"{_PREFIX}s", "plain")
    live.hset(f"{_PREFIX}h", mapping={"a": "1", "b": "Ωmega"})
    live.rpush(f"{_PREFIX}l", "x", "y", "z")
    live.sadd(f"{_PREFIX}st", "m1", "m2")
    live.zadd(f"{_PREFIX}z", {"lead": 1.5, "tail": 2.0})
    live.xadd(f"{_PREFIX}x", {"f": "v"})
    batch, _ = read_keys_batch(
        cfg=_CFG, pattern=f"{_PREFIX}*", limit=500, scan_state=RedisScanState()
    )
    assert batch.headers[:3] == ["redis_key", "redis_value", "redis_type"]
    got = {row[0]: (row[2], row[1]) for row in batch.rows}
    assert set(got) == {f"{_PREFIX}{k}" for k in ("s", "h", "l", "st", "z", "x")}
    for key, (ktype, val) in got.items():
        expected = _read_redis_value(live, key, ktype)
        if ktype == "set":
            assert sorted(json.loads(val)) == sorted(json.loads(expected))
        else:
            assert val == expected, key


def test_batched_read_still_refuses_an_over_cap_collection(live, monkeypatch):
    import connectors.redis_reader as rr

    monkeypatch.setattr(rr, "_REDIS_COLLECTION_CAP", 2)
    live.hset(f"{_PREFIX}ok", mapping={"a": "1"})
    live.rpush(f"{_PREFIX}big", "1", "2", "3")
    with pytest.raises(RuntimeError, match="refuse silent truncation"):
        rr.read_keys_batch(
            cfg=_CFG, pattern=f"{_PREFIX}*", limit=500, scan_state=rr.RedisScanState()
        )
