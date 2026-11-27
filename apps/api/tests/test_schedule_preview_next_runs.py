"""Re-review F: the schedule form's "Next 5 runs" comes from the scheduler itself.

The browser's own copy of the cron rules can still disagree with the server
on time-zone data (Africa/Casablanca, Africa/El_Aaiun: the browser's ICU and
this server's zoneinfo differ by an hour). POST /schedules/preview answers
with the functions save and the beat use, so the times it lists are the
times the scheduler will run.
"""

from __future__ import annotations

from datetime import datetime, timezone

from services.cron_schedule import next_run
from services.schedule_store import compute_next_run, preview_next_runs

NOW = datetime(2026, 11, 21, 10, 0, tzinfo=timezone.utc)


def test_a_cron_lists_the_schedulers_own_next_runs_in_any_zone():
    for zone in ("Africa/Casablanca", "Africa/El_Aaiun", "Australia/Lord_Howe", "America/New_York"):
        out = preview_next_runs("daily", "30 1 * * *", zone, now=NOW)
        assert out["kind"] == "clock"
        cursor, want = NOW, []
        for _ in range(5):
            cursor = next_run("30 1 * * *", cursor, zone)
            want.append(cursor.isoformat())
        assert out["runs"] == want, zone


def test_rolling_cadences_anchor_like_save_does():
    fresh = preview_next_runs("hourly", "", "UTC", now=NOW)
    assert fresh["kind"] == "rolling"
    assert fresh["runs"][0] == compute_next_run("hourly", NOW)  # what create/update store
    kept = preview_next_runs("hourly", "", "UTC", stored_next_run_at="2026-11-21T10:20:00+00:00",
                             cadence_changed=False, now=NOW)
    assert kept["runs"][0] == "2026-11-21T10:20:00+00:00" and kept["from_stored"] is True
    changed = preview_next_runs("hourly", "", "UTC", stored_next_run_at="2026-11-21T10:20:00+00:00",
                                cadence_changed=True, now=NOW)
    assert changed["runs"][0] == "2026-11-21T11:00:00+00:00"
    uneven = preview_next_runs("daily", "*/7 * * * *", "UTC", now=NOW)
    assert uneven["kind"] == "rolling" and uneven["every_minutes"] == 7


def test_paused_and_invalid():
    assert preview_next_runs("daily", "0 9 * * *", "UTC", enabled=False, now=NOW) == {"kind": "paused", "runs": []}
    assert preview_next_runs("daily", "0 0 * * * *", "UTC", now=NOW)["kind"] == "invalid"
    assert preview_next_runs("daily", "0 0 30 2 *", "UTC", now=NOW)["kind"] == "invalid"
    assert preview_next_runs("daily", "0 9 * * *", "Mars/Olympus", now=NOW)["kind"] == "invalid"
    assert preview_next_runs("fortnightly", "", "UTC", now=NOW)["kind"] == "invalid"
    assert len(preview_next_runs("daily", "* * * * *", "UTC", count=99, now=NOW)["runs"]) == 10


def test_route_is_a_read():
    from services.rbac import Permission, _required_permission

    assert _required_permission("POST", "/api/v1/schedules/preview") == Permission.SCHEDULE_READ
    assert _required_permission("POST", "/api/v1/schedules/") == Permission.SCHEDULE_MANAGE


def test_the_preview_is_fast_even_for_rare_dates_and_runs_off_the_event_loop():
    """Review: the route ran next_run's minute walk on the event loop (21 s for Feb 29 x 10)."""
    import inspect
    import time

    from src.routers.schedules_router import preview_schedule_next_runs

    assert not inspect.iscoroutinefunction(preview_schedule_next_runs), "sync: FastAPI runs it on the thread pool"
    for zone in ("UTC", "Africa/Casablanca", "Australia/Lord_Howe"):
        start = time.perf_counter()
        out = preview_next_runs("daily", "0 0 29 2 *", zone, count=10, now=NOW)
        assert out["kind"] == "clock" and len(out["runs"]) == 10
        assert time.perf_counter() - start < 0.5, zone
    refused = preview_next_runs("daily", "0 0 31 2 *", "UTC", count=10, now=NOW)
    assert refused["kind"] == "invalid" and "can never fire" in refused["message"]


def test_an_overdue_rolling_cadence_runs_now_then_counts_from_now_like_the_beat():
    """Review: the beat fires an overdue schedule at its next check, then next = now + delta."""
    out = preview_next_runs("hourly", "", "UTC", stored_next_run_at="2026-11-21T07:20:00+00:00",
                            cadence_changed=False, now=NOW)
    assert out["due_now"] is True
    assert out["runs"][:3] == [NOW.isoformat(), "2026-11-21T11:00:00+00:00", "2026-11-21T12:00:00+00:00"]
    assert out["runs"][1] == compute_next_run("hourly", NOW)  # what claim_due_schedules stores
    cron = preview_next_runs("daily", "0 9 * * *", "UTC", stored_next_run_at="2026-11-20T09:00:00+00:00",
                             cadence_changed=False, now=NOW)
    assert cron["due_now"] is True and cron["runs"][1] == "2026-11-22T09:00:00+00:00"


def test_a_list_cut_by_the_time_budget_says_so(monkeypatch):
    """Review round 4 #2: fewer runs than asked must not look like the whole answer."""
    import services.schedule_store as store

    full = preview_next_runs("daily", "0 9 * * *", "UTC", count=5, now=NOW)
    assert len(full["runs"]) == 5 and "truncated" not in full
    monkeypatch.setattr(store, "PREVIEW_TIME_BUDGET_SECONDS", -1.0)
    cut = preview_next_runs("daily", "0 9 * * *", "UTC", count=5, now=NOW)
    assert cut["truncated"] is True and 1 <= len(cut["runs"]) < 5
    overdue = preview_next_runs("daily", "0 9 * * *", "UTC", stored_next_run_at="2026-11-20T09:00:00+00:00",
                                cadence_changed=False, count=5, now=NOW)
    assert overdue["truncated"] is True and overdue["runs"] == [NOW.isoformat()]


def _request(email: str | None = None, *, host: str = "10.0.0.7", forwarded: str = "", api_key: str = ""):
    from starlette.requests import Request

    headers = [(b"x-forwarded-for", forwarded.encode())] if forwarded else []
    req = Request({"type": "http", "method": "POST", "path": "/api/v1/schedules/preview", "headers": headers,
                   "client": (host, 5000)})
    if api_key:
        req.state.user_email = email
        req.state.user = {"email": email, "role": "viewer", "auth_kind": "api_key", "api_key_id": api_key}
        req.state.api_key_id = api_key
        req.state.api_key_auth = True
    elif email:
        req.state.user_email = email
        req.state.user = {"email": email, "role": "member"}
    return req


def _exhaust(body, make_request) -> None:
    import pytest
    from fastapi import HTTPException

    from src.routers.schedules_router import preview_schedule_next_runs

    for _ in range(10):
        assert preview_schedule_next_runs(body, make_request())["kind"] == "clock"
    with pytest.raises(HTTPException) as refused:
        preview_schedule_next_runs(body, make_request())
    assert refused.value.status_code == 429 and int(refused.value.headers["Retry-After"]) >= 1


def test_each_caller_gets_ten_previews_per_ten_seconds_across_workers(monkeypatch):
    """Review round 4/5 #2: the limit is shared (services.auth_state: Redis when configured), per principal."""
    import uuid

    import services.auth_state as auth_state
    from src.routers.schedules_router import NextRunsPreviewBody, _preview_caller_key, preview_schedule_next_runs

    auth_state.reset_auth_state()
    body = NextRunsPreviewBody(interval="daily", cron="0 9 * * *", timezone="UTC")
    tag = uuid.uuid4().hex[:8]
    ana = f"ana-{tag}@example.com"
    _exhaust(body, lambda: _request(ana))
    # The same account from another address is the same caller; its key is not its email.
    assert _preview_caller_key(_request(ana, host="10.9.9.9")) == _preview_caller_key(_request(ana))
    assert ana not in _preview_caller_key(_request(ana))
    # An API key the account minted is its own caller, not the account's.
    assert preview_schedule_next_runs(body, _request(ana, api_key=f"key-{tag}"))["kind"] == "clock"
    assert preview_schedule_next_runs(body, _request(f"bo-{tag}@example.com"))["kind"] == "clock"


class _SharedRedis:
    """The few Redis calls the shared limiter makes, on one store every client sees."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    def client(self):
        server = self

        class _Pipe:
            def __init__(self) -> None:
                self.ops: list[tuple[str, str]] = []

            def incr(self, key):
                self.ops.append(("incr", key))

            def expire(self, key, ttl, nx=False):
                self.ops.append(("expire", key))

            def execute(self):
                out = []
                for op, key in self.ops:
                    if op == "incr":
                        server.counts[key] = server.counts.get(key, 0) + 1
                        out.append(server.counts[key])
                    else:
                        out.append(True)
                return out

        class _Client:
            def pipeline(self):
                return _Pipe()

        return _Client()


def test_two_workers_share_one_preview_allowance(monkeypatch):
    """Review round 5: an in-process bucket gave N workers x 10. Two workers on one Redis share the 10."""
    import itertools

    import pytest
    from fastapi import HTTPException

    import services.auth_state as auth_state
    from src.routers.schedules_router import NextRunsPreviewBody, preview_schedule_next_runs

    redis = _SharedRedis()
    workers = [auth_state.SharedAuthState(auth_state._RedisBackend(redis.client())) for _ in range(2)]
    turn = itertools.cycle(workers)
    monkeypatch.setattr(auth_state, "get_auth_state", lambda: next(turn))
    body = NextRunsPreviewBody(interval="daily", cron="0 9 * * *", timezone="UTC")
    for _ in range(10):
        assert preview_schedule_next_runs(body, _request("cy@example.com"))["kind"] == "clock"
    with pytest.raises(HTTPException) as refused:
        preview_schedule_next_runs(body, _request("cy@example.com"))
    assert refused.value.status_code == 429
    # Every call was counted on the shared store, alternating between the workers.
    assert list(redis.counts.values()) == [11]


def test_anonymous_callers_are_counted_by_the_trusted_client_address(monkeypatch):
    import uuid

    import services.auth_state as auth_state
    from src.routers.schedules_router import NextRunsPreviewBody, _preview_caller_key

    auth_state.reset_auth_state()
    body = NextRunsPreviewBody(interval="daily", cron="0 9 * * *", timezone="UTC")
    n = int(uuid.uuid4().hex[:4], 16) % 250 + 1
    # No proxy trusted: X-Forwarded-For is the caller's own claim and is ignored.
    monkeypatch.setenv("TRUSTED_PROXY_COUNT", "0")
    for name in ("DATAWRAP_TRUSTED_PROXY_COUNT", "DATAFLOW_TRUSTED_PROXY_COUNT"):
        monkeypatch.delenv(name, raising=False)
    assert _preview_caller_key(_request(host=f"10.1.{n}.1", forwarded="203.0.113.5")) == f"schedule-preview:ip:10.1.{n}.1"
    _exhaust(body, lambda: _request(host=f"10.1.{n}.1", forwarded=f"198.51.100.{uuid.uuid4().int % 250}"))
    # One trusted proxy: the address it saw is the caller, whatever the client put first.
    monkeypatch.setenv("TRUSTED_PROXY_COUNT", "1")
    assert _preview_caller_key(_request(host="10.0.0.1", forwarded=f"6.6.6.6, 192.0.2.{n}")) == f"schedule-preview:ip:192.0.2.{n}"
