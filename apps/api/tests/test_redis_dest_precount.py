"""Redis destination pre-count — the delta/no-op proof needs a real cardinality.

Redis had no branch in ``destination_row_count``, so every append and every
quiet incremental poll into a Redis keyspace measured ``None`` before the write
and the reconcile refused to prove a correct run ("pre-write destination count
was not measured"). A key-addressed destination is countable: the keys the
writer's prefix owns are exactly the population Gate-8 reads back.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.dest_precount import destination_row_count  # noqa: E402


def _client() -> Any:
    redis = pytest.importorskip("redis")
    try:
        client = redis.Redis(host="localhost", port=6379, socket_timeout=3)
        client.ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Redis unavailable: {exc}")
    return client


def _cfg() -> dict[str, Any]:
    return {"host": "localhost", "port": 6379, "database": "0"}


def test_absent_prefix_counts_zero_not_unknown() -> None:
    """A prefix nothing has written to is a known-empty destination, i.e. 0."""
    _client()
    prefix = f"precount_absent_{uuid.uuid4().hex[:10]}"
    assert destination_row_count("redis", _cfg(), schema="", table_name=prefix) == 0


def test_populated_prefix_counts_only_its_own_keys() -> None:
    """Keys under other prefixes must not inflate this destination's count."""
    client = _client()
    prefix = f"precount_{uuid.uuid4().hex[:10]}"
    other = f"precount_other_{uuid.uuid4().hex[:10]}"
    try:
        for i in range(7):
            client.set(f"{prefix}:{i}", "{}")
        for i in range(3):
            client.set(f"{other}:{i}", "{}")
        cfg = _cfg()
        assert destination_row_count("redis", cfg, schema="", table_name=prefix) == 7
        assert destination_row_count("redis", cfg, schema="", table_name=other) == 3
    finally:
        keys = list(client.scan_iter(match=f"{prefix}:*")) + list(
            client.scan_iter(match=f"{other}:*")
        )
        if keys:
            client.delete(*keys)
        client.close()


def test_unreachable_server_stays_unknown() -> None:
    """No writer acknowledgement substitute: an unreachable server is ``None``."""
    pytest.importorskip("redis")
    unreachable = {"host": "127.0.0.1", "port": 6399, "database": "0"}
    count = destination_row_count("redis", unreachable, schema="", table_name="orders")
    assert count is None
