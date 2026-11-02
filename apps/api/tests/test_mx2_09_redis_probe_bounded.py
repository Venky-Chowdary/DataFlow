"""MX2-09: staging PG→Redis hung for minutes with no product error.

Live repro: ``start_transfer`` → ``plan_transfer`` → ``introspect_endpoint`` →
``connectors.redis_kv.test_redis`` → ``PING``. The saved connector had
``ssl=True`` (the connector-store default) against a plaintext Redis, so the
TLS handshake never completed. redis-py 8 retries a timed-out connect ten
times with backoff, and the probe built its own client, so one PING took
~92 s and staging probes more than once.
"""

from __future__ import annotations

import socket
import threading
import time

import pytest


def _redis_up() -> bool:
    try:
        with socket.create_connection(("localhost", 6379), timeout=1):
            return True
    except OSError:
        return False


def test_shared_client_bounds_connect_and_retries() -> None:
    pytest.importorskip("redis")
    from connectors.redis_reader import _redis_client

    for cfg in (
        {"host": "localhost", "port": 6379, "database": "0"},
        {"connection_string": "redis://localhost:6379/0"},
    ):
        client = _redis_client(cfg, socket_timeout=8)
        kwargs = client.get_connection_kwargs()
        assert kwargs.get("socket_connect_timeout") is not None
        assert float(kwargs["socket_connect_timeout"]) <= 5
        assert float(kwargs["socket_timeout"]) == 8
        retry = kwargs.get("retry")
        assert retry is not None and getattr(retry, "_retries", 99) <= 1


def test_probe_uses_the_shared_client(monkeypatch) -> None:
    pytest.importorskip("redis")
    import connectors.redis_reader as rr
    from connectors.redis_kv import test_redis

    seen: list[dict] = []

    class _Client:
        def ping(self):
            return True

        def scan(self, cursor=0, count=None, match=None):
            return 0, []

        def close(self):
            return None

    def fake(cfg, **kwargs):
        seen.append({**cfg, **kwargs})
        return _Client()

    monkeypatch.setattr(rr, "_redis_client", fake)
    result = test_redis(
        host="h", port=6380, database="2", username="u", password="p",
        schema="", connection_string="", ssl=True,
    )
    assert result.ok, result.error
    assert seen and seen[0]["host"] == "h" and seen[0]["ssl"] is True
    assert seen[0]["port"] == 6380 and seen[0]["database"] == "2"


@pytest.mark.skipif(not _redis_up(), reason="Redis not reachable on localhost:6379")
def test_tls_against_plaintext_redis_fails_fast_with_a_clear_error() -> None:
    from connectors.redis_kv import test_redis

    box: dict = {}

    def run() -> None:
        box["result"] = test_redis(
            host="localhost", port=6379, database="0", username="", password="",
            schema="", connection_string="", ssl=True,
        )

    started = time.monotonic()
    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    worker.join(30)
    elapsed = time.monotonic() - started
    assert not worker.is_alive(), f"Redis probe still running after {elapsed:.0f}s"
    result = box["result"]
    assert result.ok is False
    assert "TLS" in (result.error or ""), result.error
    assert "ssl" in (result.error or "").lower()


@pytest.mark.skipif(not _redis_up(), reason="Redis not reachable on localhost:6379")
def test_plaintext_probe_still_connects() -> None:
    from connectors.redis_kv import test_redis

    result = test_redis(
        host="localhost", port=6379, database="0", username="", password="",
        schema="", connection_string="", ssl=False,
    )
    assert result.ok, result.error


def test_pilot_validate_shows_the_destination_probe_reason(monkeypatch) -> None:
    """Gate-2 said only "Destination not reachable" — the probe's fix was dropped."""
    reason = "Redis TLS handshake with r:6379 timed out. Set ssl=false on the connector."

    def fake_inspect(**_kwargs):
        return {"connected": False, "message": reason}

    monkeypatch.setattr(
        "services.preflight_service.inspect_destination_for_preflight", fake_inspect
    )
    monkeypatch.setattr(
        "services.preflight_service.run_transfer_policy_gates", lambda **_k: []
    )
    monkeypatch.setattr(
        "services.preflight_service.apply_policy_gates",
        lambda result, *_a, **_k: result,
    )
    from src.ai.copilot.transfer_tools import _run_preflight

    out = _run_preflight(
        src_conn={"id": "src", "name": "src", "type": "postgresql"},
        dst_conn={"id": "dst", "name": "dst", "type": "redis"},
        src_table="sales",
        dst_table="sales",
        src_rows=[{"name": "id", "inferred_type": "INTEGER", "nullable": False}],
        sample_rows=[{"id": 1}],
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
        mode="full_refresh_overwrite",
        schema_policy="manual_review",
        validation_mode="balanced",
        src_db_type="postgresql",
        source_config={"type": "postgresql"},
        dest_db_type="redis",
        dest_exists=None,
        source_primary_key="id",
    )
    g2 = next(g for g in out["gates"] if g["id"] == "g2_destination")
    assert g2["status"] == "block"
    assert reason in g2["message"], g2["message"]
