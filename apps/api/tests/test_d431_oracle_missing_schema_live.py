"""D431 AC-3 on a live Oracle: a connector whose schema (user) does not exist.

The plan asks ALL_USERS before it proposes create-new, so a missing owner is
named instead of surfacing as ORA-01918 at run time. Runs in the CI oracle-live
job (``tests/test_*oracle*.py``); skipped where no Oracle listens.
"""

from __future__ import annotations

import socket

import pytest

from tests.helpers.live_env import oracle_creds

ORA = oracle_creds()


def _up() -> bool:
    try:
        with socket.create_connection((ORA["host"], ORA["port"]), timeout=1.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _up(), reason="Oracle not reachable")


def _cfg(schema: str) -> dict:
    return {"type": "oracle", "host": ORA["host"], "port": ORA["port"],
            "database": ORA["database"], "service_name": ORA["service_name"],
            "username": ORA["username"], "password": ORA["password"], "schema": schema,
            "ssl": False}


def test_missing_oracle_owner_is_named(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128,10.0.0.0/8,172.16.0.0/12")
    from services.dest_namespace import missing_destination_namespace

    assert missing_destination_namespace(_cfg("NOSUCHSCHEMA431")) == "NOSUCHSCHEMA431"


def test_existing_oracle_owner_is_not_refused(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128,10.0.0.0/8,172.16.0.0/12")
    from services.dest_namespace import missing_destination_namespace

    assert missing_destination_namespace(_cfg("SYS")) == ""
