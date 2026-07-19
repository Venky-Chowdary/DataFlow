"""Proxy write resilience helpers used by CSV → SQL transfers."""

from __future__ import annotations

import time

from connectors.write_resilience import (
    build_write_batch_key,
    is_connection_lost,
    is_public_proxy_host,
    proxy_stream_batch_size,
    reconnect_backoff_seconds,
    should_retry_connection_lost,
    write_chunk_size,
)


def test_railway_proxy_is_detected():
    assert is_public_proxy_host("switchyard.proxy.rlwy.net")
    assert is_public_proxy_host("postgresql://u:p@caboose.proxy.rlwy.net:1234/db")
    assert not is_public_proxy_host("localhost")
    assert not is_public_proxy_host("db.internal")


def test_proxy_chunk_and_stream_batch_aligned():
    assert write_chunk_size("localhost", default=10_000) == 10_000
    proxy_chunk = write_chunk_size("caboose.proxy.rlwy.net", default=10_000)
    assert proxy_chunk <= 1000
    assert proxy_stream_batch_size(
        "caboose.proxy.rlwy.net",
        default=20_000,
    ) == proxy_chunk


def test_connection_lost_detects_psycopg_drop():
    msg = (
        "server closed the connection unexpectedly "
        "This probably means the server terminated abnormally "
        "before or while processing the request."
    )
    assert is_connection_lost(msg)
    assert is_connection_lost(RuntimeError(msg))


def test_reconnect_backoff_grows():
    assert reconnect_backoff_seconds(1) >= 0.5
    assert reconnect_backoff_seconds(4) > reconnect_backoff_seconds(1)


def test_should_retry_honors_attempt_budget():
    started = time.monotonic()
    assert should_retry_connection_lost(attempt=1, started_at=started, proxy=True)
    assert not should_retry_connection_lost(attempt=50, started_at=started, proxy=True)


def test_build_write_batch_key_stable():
    assert build_write_batch_key(table_name="orders", file_batch_idx=5) == "orders:5"


def test_raise_write_failure_connection_lost_is_retriable_even_after_partial():
    from src.transfer.stream import _raise_write_failure

    class _Result:
        error = "server closed the connection unexpectedly"
        rows_written = 9000

    try:
        _raise_write_failure(_Result(), "pg failed")
        raise AssertionError("expected ConnectionError")
    except ConnectionError as exc:
        assert "server closed" in str(exc).lower()


def test_postgres_writer_reconnects_and_skips_ledged_chunk(monkeypatch):
    """After an ambiguous commit, ledger skip prevents duplicate insert on retry."""
    from connectors import postgresql_writer as pgw

    ledger: set[tuple[str, str, int]] = set()
    write_calls = {"n": 0}
    commit_calls = {"n": 0}

    class FakeCursor:
        def execute(self, *_a, **_k):
            return None

        def fetchall(self):
            return []

        def fetchone(self):
            return None

        def copy_expert(self, *_a, **_k):
            write_calls["n"] += 1

        def executemany(self, *_a, **_k):
            write_calls["n"] += 1

        def close(self):
            return None

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            commit_calls["n"] += 1
            # First data commit looks like a proxy drop after the server accepted it.
            if commit_calls["n"] == 2:
                raise RuntimeError("server closed the connection unexpectedly")

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(pgw, "_open_pg", lambda **_k: FakeConn())
    monkeypatch.setattr(pgw, "ensure_postgres_write_ledger", lambda *_a, **_k: None)
    monkeypatch.setattr(pgw, "write_chunk_size", lambda *_a, **_k: 10)
    monkeypatch.setattr(pgw, "is_public_proxy_host", lambda *_a, **_k: True)
    monkeypatch.setattr(pgw.time, "sleep", lambda *_a, **_k: None)

    def fake_committed(_cur, *, schema, job_id, batch_key, chunk_idx):  # noqa: ARG001
        return (job_id, batch_key, chunk_idx) in ledger

    def fake_mark(_cur, *, schema, job_id, batch_key, chunk_idx, rows_written):  # noqa: ARG001
        # Simulate server-side commit of ledger+rows even when client sees a drop.
        ledger.add((job_id, batch_key, chunk_idx))

    monkeypatch.setattr(pgw, "postgres_chunk_committed", fake_committed)
    monkeypatch.setattr(pgw, "mark_postgres_chunk_committed", fake_mark)

    result = pgw.write_mapped_rows(
        host="caboose.proxy.rlwy.net",
        port=5432,
        database="db",
        username="u",
        password="p",
        schema="public",
        connection_string="",
        ssl=False,
        table_name="t1",
        headers=["id", "name"],
        data_rows=[["1", "a"], ["2", "b"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
        ],
        column_types={"id": "string", "name": "string"},
        create_table=True,
        job_id="job-ledger-1",
        write_batch_key="t1:1",
    )
    assert result.ok, result.error
    assert result.rows_written == 2
    # One physical write; retry skipped via ledger.
    assert write_calls["n"] == 1
    assert ("job-ledger-1", "t1:1", 0) in ledger
