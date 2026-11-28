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


def _preflight(schema: str):
    from services.preflight_service import run_file_preflight

    return run_file_preflight(
        columns=["id"], column_types={"id": "INTEGER"}, row_count=1,
        mappings=[{"source": "id", "target": "ID", "confidence": 0.99, "target_type": "NUMBER(10)",
                   "assignment_strategy": "create_compatible_new", "create_new": True}],
        destination_connected=True, sample_rows=[{"id": 1}], destination_db_type="oracle",
        destination_table_exists=False, destination_can_create=True, destination_can_write=True,
        destination_config=_cfg(schema), skip_population_fit=True, sync_mode="full_refresh_append",
    )


def _blocker_ids(pf) -> list[str]:
    return [str(b.get("id")) for b in pf.get("blockers") or []]


def test_studio_validate_refuses_a_missing_owner_and_passes_an_existing_one(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128,10.0.0.0/8,172.16.0.0/12")
    assert "g_dest_schema_missing" in _blocker_ids(_preflight("NOSUCHSCHEMA431"))
    assert "g_dest_schema_missing" not in _blocker_ids(_preflight("SYS"))


def test_execute_refuses_a_missing_owner_before_any_ddl(monkeypatch):
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128,10.0.0.0/8,172.16.0.0/12")
    from services.plan_execute_blockers import execute_plan_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    def request(schema: str) -> TransferRequest:
        dest = EndpointConfig(kind="database", format="oracle", host=ORA["host"], port=ORA["port"],
                              database=ORA["database"], username=ORA["username"],
                              password=ORA["password"], schema=schema, table="D431_T",
                              extra={"service_name": ORA["service_name"]})
        return TransferRequest(source=EndpointConfig(kind="file", format="csv"),
                               destination=dest, sync_mode="full_refresh_append")

    refusal = execute_plan_refusal(request("NOSUCHSCHEMA431"))
    assert refusal and refusal.gate_id == "g_dest_schema_missing"
    assert "NOSUCHSCHEMA431" in refusal and "ORA-" not in refusal
    assert execute_plan_refusal(request("SYS")) == ""
