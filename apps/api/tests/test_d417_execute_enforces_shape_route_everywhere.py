"""D417 — Execute must refuse the plan blocker g_shape_route on every destination.

QA rounds 57-75: plan_transfer returned safe_to_start=false with g_shape_route
("Transform (pre-load) is not applied on the mirror route") for mirrors into
SQL Server and Oracle and for SFTP -> SQL Server, yet /transfer/run executed
them: some wrote 30 rows and went red, some went green. The same plan into
PostgreSQL or MySQL was refused at 0 rows.

Root cause: only the streaming runner (the staging-merge destinations
PostgreSQL, MySQL, SQLite, Snowflake, BigQuery, Redshift) asked the recipe
route question. File sources and every other destination take the buffered
runner, which applied the recipe and wrote. Execute now asks the plan's own
function before any read or write, on every runner, and the job record names
the blocker id.

The buffered runner is driven here by a real file source -> SQLite mirror (the
SFTP -> SQL Server shape); the SQL Server and Oracle requests are checked
against the shared function itself, which takes no connection.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine, takes_streaming_route  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

_RECIPE = {"steps": [{"op": "trim", "column": "name"}]}
_CSV = b"id,name\n1,  alpha \n2,beta  \n3, gamma\n"


@pytest.fixture()
def fake_store(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service", lambda: fake, raising=False
    )
    return fake


def _file_request(dst: Path, sync_mode: str, recipe=_RECIPE) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="people.csv",
        source_content=_CSV,
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="people"
        ),
        sync_mode=sync_mode,
        stream_contracts=[
            {"name": "people", "sync_mode": sync_mode, "primary_key": "id", "selected": True}
        ],
        shape_recipe=recipe,
        skip_preflight=True,
        validation_mode="strict",
    )


def _tables(path: Path) -> list[str]:
    if not path.exists():
        return []
    conn = sqlite3.connect(str(path))
    try:
        return [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    finally:
        conn.close()


def _run(fake, request) -> tuple:
    job_id = "d417" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    return result, fake.jobs.get(job_id) or {}


@pytest.mark.parametrize("sync_mode", ["mirror", "full_refresh_mirror"])
def test_buffered_mirror_with_recipe_is_refused_before_any_write(tmp_path, fake_store, sync_mode):
    from services.multi_stream_plan import history_shape_route_gate

    dst = tmp_path / "dst.db"
    request = _file_request(dst, sync_mode)
    assert not takes_streaming_route(request), "precondition: the buffered runner"
    plan_gate = history_shape_route_gate(_RECIPE, sync_mode)
    assert plan_gate and plan_gate["id"] == "g_shape_route"

    result, job = _run(fake_store, request)

    assert result.success is False
    assert result.error == plan_gate["message"]
    assert result.error_details["blocker_id"] == "g_shape_route"
    assert result.error_details["rows_written"] == 0
    assert result.records_transferred == 0
    assert job["status"] == "failed"
    assert job["error"] == plan_gate["message"]
    assert job["error_details"]["blocker_id"] == "g_shape_route"
    assert "people" not in _tables(dst), "nothing may be created or written"


def test_scd2_column_pipeline_is_refused_on_the_buffered_runner(tmp_path, fake_store):
    dst = tmp_path / "dst.db"
    result, job = _run(fake_store, _file_request(dst, "scd2"))
    assert result.success is False
    assert job["error_details"]["blocker_id"] == "g_shape_route"
    assert "people" not in _tables(dst)


def test_same_recipe_on_a_read_side_route_still_runs(tmp_path, fake_store):
    """AC-2: a route that applies pre-load transforms is not refused."""
    from services.multi_stream_plan import history_shape_route_gate

    dst = tmp_path / "dst.db"
    assert history_shape_route_gate(_RECIPE, "full_refresh_overwrite") is None
    result, _job = _run(fake_store, _file_request(dst, "full_refresh_overwrite"))
    assert result.success is True, result.error
    conn = sqlite3.connect(str(dst))
    try:
        names = [r[0] for r in conn.execute("SELECT name FROM people ORDER BY id")]
    finally:
        conn.close()
    assert names == ["alpha", "beta", "gamma"]


def test_mirror_without_a_recipe_is_not_refused(tmp_path, fake_store):
    dst = tmp_path / "dst.db"
    result, _job = _run(fake_store, _file_request(dst, "mirror", recipe=None))
    assert result.success is True, result.error


@pytest.mark.parametrize("dest_format", ["mssql", "sqlserver", "oracle", "postgresql", "mysql"])
@pytest.mark.parametrize("sync_mode", ["mirror", "full_refresh_mirror", "cdc", "scd2"])
def test_every_destination_gets_the_plan_verdict(dest_format, sync_mode):
    """Execute's up-front check is the plan's gate, whatever the destination."""
    from services.multi_stream_plan import history_shape_route_gate
    from services.plan_execute_blockers import execute_plan_refusal

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table="t"),
        destination=EndpointConfig(kind="database", format=dest_format, table="t"),
        sync_mode=sync_mode,
        shape_recipe=_RECIPE,
    )
    plan_gate = history_shape_route_gate(_RECIPE, sync_mode)
    refusal = execute_plan_refusal(request)
    assert plan_gate is not None
    assert refusal == plan_gate["message"]
    assert refusal.gate_id == "g_shape_route"


def test_scd2_contract_step_recipe_is_allowed_by_plan_and_execute():
    """AC-2: scd2 with its history contract step really applies the recipe."""
    from services.multi_stream_plan import history_shape_route_gate
    from services.plan_execute_blockers import execute_plan_refusal

    recipe = {"steps": [{"op": "scd2", "options": {"keys": ["id"], "tracked": ["name"]}}]}
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table="t"),
        destination=EndpointConfig(kind="database", format="mssql", table="t"),
        sync_mode="scd2",
        shape_recipe=recipe,
    )
    assert history_shape_route_gate(recipe, "scd2") is None
    assert execute_plan_refusal(request) == ""


def test_stream_contract_mode_decides_like_the_plan():
    """The run's effective sync mode (the contract's) is the one judged, on both sides."""
    from services.multi_stream_plan import history_shape_route_gate
    from services.plan_execute_blockers import execute_plan_refusal

    contracts = [{"name": "t", "sync_mode": "mirror", "primary_key": "id", "selected": True}]
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table="t"),
        destination=EndpointConfig(kind="database", format="oracle", table="t"),
        sync_mode="full_refresh_overwrite",
        stream_contracts=contracts,
        shape_recipe=_RECIPE,
    )
    gate = history_shape_route_gate(_RECIPE, "full_refresh_overwrite", contracts)
    assert gate is not None
    assert execute_plan_refusal(request) == gate["message"]
