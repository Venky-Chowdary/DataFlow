"""D308 x D19: the staged-preflight schema read never holds a job or a plan.

* A run with nothing staged does not read the destination schema at all
  (it used to, unbounded, before the D19 connect deadline started).
* A run whose staged result matches reads it under the destination connect
  deadline; a silent destination yields "unread" (no reuse), not a hang.
* An engine the snapshot cannot read (Snowflake) is not logged in to only to
  fail (D187: a remote Snowflake login is the slowest step of a plan).
* Staging records the version of the table it checked, not "" (round 7).
"""

from __future__ import annotations

import socket
import threading
import time

import pytest


class _Silent:
    def __init__(self) -> None:
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(16)
        self.port = self.sock.getsockname()[1]
        self.held: list[socket.socket] = []
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            self.held.append(conn)

    def close(self) -> None:
        self.sock.close()
        for conn in self.held:
            conn.close()


@pytest.fixture()
def silent(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    monkeypatch.setenv("DATAFLOW_DESTINATION_CONNECT_DEADLINE_SECONDS", "2")
    server = _Silent()
    yield server
    server.close()


def _dest(port: int):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(
        kind="database", format="mysql", host="127.0.0.1", port=port,
        database="d", username="u", password="p", table="t",
    )


def test_nothing_staged_means_no_schema_read(monkeypatch):
    import services.staged_preflight as staged

    def _boom(_dest):
        raise AssertionError("read the destination schema with nothing staged")

    monkeypatch.setattr(staged, "destination_schema_version", _boom)
    with staged.bind_staged_preflight(
        "", "fp", lambda: staged.bounded_destination_schema_version(object())
    ):
        assert staged.staged_preflight_for_bound() is None
    with staged.bind_staged_preflight(
        "pf_not_staged", "fp", lambda: staged.bounded_destination_schema_version(object())
    ):
        assert staged.staged_preflight_for_bound() is None


def test_a_matching_staged_run_on_a_silent_destination_ends_at_the_deadline(silent):
    import services.staged_preflight as staged

    staged.remember_staged_preflight(
        "pf_silent", "fp", {"passed": True, "gates": []}, schema_version="v1"
    )
    started = time.monotonic()
    with staged.bind_staged_preflight(
        "pf_silent", "fp",
        lambda: staged.bounded_destination_schema_version(_dest(silent.port)),
    ):
        elapsed = time.monotonic() - started
        assert staged.staged_preflight_for_bound() is None  # unread: no reuse
    assert elapsed < 6, elapsed


def test_snowflake_is_not_logged_in_to_for_a_snapshot_it_cannot_read(monkeypatch):
    import connectors.generic_sql as generic_sql
    import services.staged_preflight as staged

    def _no_login(_cfg):
        raise AssertionError("logged in to Snowflake for a schema version")

    monkeypatch.setattr(generic_sql, "get_sqlalchemy_engine", _no_login)
    version = staged.destination_schema_version({
        "type": "snowflake", "host": "acct.snowflakecomputing.com", "table": "T",
        "database": "DB", "username": "u", "password": "p",
    })
    assert version == "unread"


def test_staging_records_the_checked_tables_version(monkeypatch):
    import services.staged_preflight as staged

    seen: list[dict] = []

    def _snapshot(cfg):
        seen.append(dict(cfg))
        return {"columns": [("id", "int")]}

    monkeypatch.setattr(staged, "_read_schema_snapshot", _snapshot)
    conn = {"type": "postgresql", "host": "db.example", "port": 5432, "database": "app",
            "username": "u", "password": "p", "schema": "public"}
    assert staged.destination_schema_version(conn) == ""  # no table: nothing to read
    version = staged.destination_schema_version({**conn, "table": "orders"})
    assert version not in {"", "unread"}
    assert seen[-1]["table"] == "orders"


def test_the_fingerprint_covers_region_and_endpoint_url():
    from src.transfer.models import EndpointConfig, TransferRequest
    from services.staged_preflight import request_preflight_fingerprint

    def _req(**dest):
        return TransferRequest(
            source=EndpointConfig(kind="database", format="postgresql", connector_id="s", table="t"),
            destination=EndpointConfig(kind="database", format="s3", connector_id="d", table="t", **dest),
            sync_mode="full_refresh_overwrite",
        )

    base = request_preflight_fingerprint(_req())
    assert request_preflight_fingerprint(_req(region="eu-west-1")) != base
    assert request_preflight_fingerprint(_req(endpoint_url="https://s3.example")) != base
