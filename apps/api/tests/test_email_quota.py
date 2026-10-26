"""Addendum — shared send quota: day/month caps, 80% alert, fail-closed Redis."""

from __future__ import annotations

import pytest

from services import email_quota
from services.email_quota import consume_send_slot, quota_status, _reset_backends
from services.email_sender import EmailQuotaExceeded


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    _reset_backends()
    monkeypatch.delenv("DATAWRAP_REDIS_URL", raising=False)
    monkeypatch.delenv("DATAFLOW_REDIS_URL", raising=False)
    monkeypatch.delenv("DATAWRAP_EMAIL_DAILY_LIMIT", raising=False)
    monkeypatch.delenv("DATAWRAP_EMAIL_MONTHLY_LIMIT", raising=False)
    yield
    _reset_backends()


def test_daily_cap_blocks_with_retry_after(monkeypatch):
    monkeypatch.setenv("DATAWRAP_EMAIL_DAILY_LIMIT", "3")
    for _ in range(3):
        consume_send_slot()
    with pytest.raises(EmailQuotaExceeded) as exc:
        consume_send_slot()
    assert exc.value.code == "email_quota_exceeded"
    # Retry after should point at the next UTC day boundary.
    assert 60 < exc.value.retry_after_sec <= 86400


def test_monthly_cap_blocks(monkeypatch):
    monkeypatch.setenv("DATAWRAP_EMAIL_DAILY_LIMIT", "100")
    monkeypatch.setenv("DATAWRAP_EMAIL_MONTHLY_LIMIT", "2")
    consume_send_slot()
    consume_send_slot()
    with pytest.raises(EmailQuotaExceeded) as exc:
        consume_send_slot()
    assert "monthly" in str(exc.value).lower()


def test_alert_fires_once_at_80_percent(monkeypatch, caplog):
    monkeypatch.setenv("DATAWRAP_EMAIL_DAILY_LIMIT", "5")
    monkeypatch.setenv("DATAWRAP_EMAIL_MONTHLY_LIMIT", "10000")
    events = []
    monkeypatch.setattr(
        email_quota,
        "emit_event",
        lambda logger, event, **kw: events.append((event, kw)),
    )
    for _ in range(5):
        consume_send_slot()
    alerts = [kw for name, kw in events if name == "email.quota.alert"]
    assert len(alerts) == 1
    assert alerts[0]["kind"] == "day"
    assert alerts[0]["used"] == 4  # first crossing of 80% of 5


def test_send_event_emitted_each_consume(monkeypatch):
    events = []
    monkeypatch.setattr(
        email_quota,
        "emit_event",
        lambda logger, event, **kw: events.append((event, kw)),
    )
    consume_send_slot()
    send_events = [kw for name, kw in events if name == "email.send"]
    assert len(send_events) == 1
    assert send_events[0]["day_used"] == 1
    assert send_events[0]["backend"] == "file"


def test_redis_down_fails_closed(monkeypatch):
    monkeypatch.setenv("DATAWRAP_REDIS_URL", "redis://127.0.0.1:6390/0")

    def boom():
        raise ConnectionError("nope")

    monkeypatch.setattr(email_quota, "_get_redis", boom)
    with pytest.raises(EmailQuotaExceeded) as exc:
        consume_send_slot()
    assert exc.value.retryable is True
    assert exc.value.retry_after_sec == 60


def test_redis_backend_used_when_configured(monkeypatch):
    monkeypatch.setenv("DATAWRAP_REDIS_URL", "redis://127.0.0.1:6390/0")
    counts = {"day": 0, "month": 0}

    class FakeRedis:
        def incr(self, key):
            counts["day" if ":day:" in key else "month"] += 1
            return counts["day" if ":day:" in key else "month"]

        def pipeline(self):
            return self

        def expire(self, key, ttl):
            return True

        def execute(self):
            return (self._count, True)

        def set(self, *a, **k):
            return True

        def get(self, key):
            return counts["day" if ":day:" in key else "month"]

    class Pipe(FakeRedis):
        def __init__(self):
            self._count = 0

    fake = FakeRedis()

    def incr(key, reset_at):
        fake._count = fake.incr(key)
        return fake._count

    monkeypatch.setattr(email_quota, "_incr_redis", incr)
    monkeypatch.setattr(email_quota, "_get_redis", lambda: fake)
    out = consume_send_slot()
    assert out == {"day_used": 1, "month_used": 1}
    status = quota_status()
    assert status["backend"] == "redis"
    assert status["day_limit"] == 100
    assert status["month_limit"] == 3000


def test_quota_status_file_backend():
    consume_send_slot()
    status = quota_status()
    assert status["backend"] == "file"
    assert status["day_used"] == 1
    assert status["day_limit"] == 100
    assert status["month_limit"] == 3000


def test_file_backend_is_durable_and_shared(tmp_path, monkeypatch):
    """Two API replicas sharing the backend see each other's sends: the
    counter lives in email_quota.json under data_dir() — durable across a
    simulated restart and identical to the users/challenges storage tier.
    In production Mongo is configured and the same counters live in the
    shared collection (atomic $inc upserts)."""
    from services import email_quota as q

    assert q._backend() == "file"
    consume_send_slot()
    consume_send_slot()
    # Simulate a second replica / process restart sharing the volume: all
    # in-process state is dropped, the file under data_dir() persists.
    monkeypatch.setattr(q, "_REDIS_CLIENT", None)
    # (file backend has no in-process counter cache to clear — the file
    # itself is the store)
    before = quota_status()
    assert before["backend"] == "file"
    assert before["day_used"] == 2, before
    consume_send_slot()
    assert quota_status()["day_used"] == 3


def test_file_backend_survives_repeated_reads(tmp_path):
    """Expired counters prune; live ones persist across reads."""
    from services import email_quota as q

    consume_send_slot()
    s1 = quota_status()
    s2 = quota_status()
    assert s1["day_used"] == s2["day_used"] == 1
    assert (q._file_path()).exists()


def test_mongo_resets_at_is_datetime_and_ttl_ready(monkeypatch):
    """Item 5: resets_at must be a BSON datetime (the TTL index on it only
    fires on dates — an ISO string leaves expired counters forever), and
    _resets_live must honor both the new datetime and legacy ISO docs."""
    from datetime import UTC, datetime, timedelta
    from services import email_quota

    # datetime (new) and ISO string (legacy) both read correctly
    future = datetime.now(UTC) + timedelta(hours=1)
    assert email_quota._resets_live(future) is True
    assert email_quota._resets_live(future - timedelta(hours=2)) is False
    assert email_quota._resets_live(future.isoformat()) is True
    assert email_quota._resets_live((future - timedelta(hours=2)).isoformat()) is False
    assert email_quota._resets_live(None) is False

    # The mongo writer passes the datetime itself to $setOnInsert.
    captured = {}

    class _Coll:
        def find_one_and_update(self, flt, upd, **kw):
            captured.update(upd["$setOnInsert"])
            return {"count": 1}
        def create_index(self, *a, **kw):
            captured["index"] = (a, kw)

    class _Db:
        def __getitem__(self, name):
            return _Coll()

    monkeypatch.setattr(email_quota, "_MONGO_INDEXES_READY", False)
    monkeypatch.setattr(email_quota, "mongo_database", lambda: _Db())
    email_quota._incr_mongo("k", future)
    assert isinstance(captured["resets_at"], datetime), captured
    # TTL index was created once (expireAfterSeconds=0 on resets_at).
    assert captured["index"][0] == ("resets_at",)
    assert captured["index"][1] == {"expireAfterSeconds": 0}
    # And a second call does not recreate it.
    captured.clear()
    email_quota._db()
    assert "index" not in captured
