"""D345: Databricks is Planned on this server (driver not installed); every
surface says that in the same words, never "Authentication failed".

Production (prod b1d7ca75): test_connector on a saved Databricks connector
answered "Authentication failed. Check username, password, and permissions.
Driver: SQLAlchemy dialect/driver for 'databricks' is not available ...", and
start_transfer was blocked at g2_destination and g6_target_ddl (plus an
unrelated Risk Contract finding) for a destination that can never connect.

PR 394's D310 commit already stopped the "Authentication failed" label and
refused MCP create_connector. Left over, reproduced here:

* test / list / sample / introspect answered with the SQLAlchemy dialect text
  and an install instruction for a file in this repository;
* plan_transfer succeeded with an empty destination ("Destination schema
  unavailable") and start_transfer failed on preflight gates;
* REST POST /connectors/saved saved a Databricks connector.

``databricks-sqlalchemy`` is deliberately kept off the API image
(requirements-databricks.txt: its oauthlib<4 pin conflicts with the security
pin), so the honest answer is "Planned / not available on this server".
"""

from __future__ import annotations

import os
import sqlite3

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

WS = "ws-d345"
HONEST = "not available on this server: databricks support is not installed here"


def _driver_shipped() -> bool:
    from src.transfer.connector_capabilities import driver_available, resolve_driver_type

    return driver_available(resolve_driver_type("databricks"), "databricks")


pytestmark = pytest.mark.skipif(_driver_shipped(), reason="the Databricks driver is installed here")


def _assert_honest(text: str) -> None:
    low = text.lower()
    assert HONEST in low, text
    assert "authentication failed" not in low, text
    assert "sqlalchemy dialect" not in low, text
    assert "requirements-databricks" not in low, text


@pytest.fixture
def world(tmp_path, monkeypatch):
    from services.connector_store import create_connector, delete_connector

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    monkeypatch.setenv("DATAFLOW_TOOL_DEADLINE_SECONDS", "3")
    monkeypatch.setenv("DATAFLOW_QUERY_TOOL_DEADLINE_SECONDS", "3")
    db = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute("INSERT INTO t VALUES (1, 'a')")
    conn.commit()
    conn.close()
    dbx = create_connector({
        "name": "d345-dbx", "type": "databricks", "host": "127.0.0.1", "port": 9,
        "database": "workspace", "schema": "default", "username": "token",
        "password": "dapi-not-a-real-token", "auth_mode": "pat", "workspace_id": WS,
    })
    src = create_connector({"name": "d345-src", "type": "sqlite", "database": str(db), "workspace_id": WS})
    try:
        yield dbx.id, src.id
    finally:
        delete_connector(dbx.id)
        delete_connector(src.id)


def _call(tool: str, args: dict):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.tools import DataPilotTools

    with bound_workspace_filter(WorkspaceFilter.only(WS), WS, "tester"):
        return DataPilotTools().execute(tool, args)


def _text(result) -> str:
    out = result.output if isinstance(result.output, dict) else {}
    return f"{result.error or ''} {out.get('message') or ''}"


def test_one_message_for_a_planned_type_without_its_driver():
    from src.transfer.connector_capabilities import planned_driver_message

    _assert_honest(planned_driver_message("databricks"))
    assert "no connection was attempted" in planned_driver_message("Databricks").lower()
    for ready in ("postgresql", "sqlite", "mysql", ""):
        assert planned_driver_message(ready) == ""


def test_missing_dialect_error_is_the_planned_message_not_the_auth_wrapper():
    from connectors.generic_sql import _dialect_unavailable
    from src.transfer.connector_registry import humanize_connection_error

    err = _dialect_unavailable("databricks://token:x@host:443/default", "databricks")
    _assert_honest(str(err))
    _assert_honest(humanize_connection_error("databricks", str(err)))


@pytest.mark.parametrize(
    "tool,extra",
    [
        ("test_connector", {}),
        ("list_connector_objects", {}),
        ("sample_connector_object", {"table": "t", "limit": 5}),
    ],
)
def test_calls_on_a_saved_connector_say_planned(world, tool, extra):
    dbx, _src = world
    _assert_honest(_text(_call(tool, {"connector_id": dbx, **extra})))


@pytest.mark.parametrize("tool", ["plan_transfer", "start_transfer"])
def test_plan_and_start_refuse_before_the_gates(world, tool):
    dbx, src = world
    result = _call(tool, {
        "source_connector_id": src, "source_table": "t",
        "dest_connector_id": dbx, "dest_table": "t",
    })
    assert not result.success, result.output
    assert "ack_id" not in str(result.output or "")
    text = _text(result)
    _assert_honest(text)
    assert "g2_destination" not in text and "g6_target_ddl" not in text


def test_create_connector_refuses_over_mcp_and_rest(world, monkeypatch):
    from fastapi import HTTPException

    import src.routers.saved_connectors_router as router

    mcp = _call("create_connector", {
        "name": "d345-new", "type": "databricks", "host": "127.0.0.1", "port": 9,
        "database": "workspace", "username": "token", "password": "dapi-not-a-real-token",
    })
    assert not mcp.success
    _assert_honest(_text(mcp))

    monkeypatch.setattr(router, "resolve_write_workspace", lambda request, ws: WS)
    monkeypatch.setattr(router, "require_admin_connector_access", lambda request, ctype: None)
    body = router.ConnectorSaveDTO(**{
        "name": "d345-rest", "type": "databricks", "host": "127.0.0.1", "port": 9,
        "database": "workspace", "username": "token", "password": "dapi-not-a-real-token",
    })
    with pytest.raises(HTTPException) as caught:
        router.save_connector(body, None, WS)
    assert caught.value.status_code == 422
    _assert_honest(str(caught.value.detail))
    from services.connector_store import list_connectors

    assert not [c for c in list_connectors(workspace_id=WS) if c.name == "d345-rest"]
