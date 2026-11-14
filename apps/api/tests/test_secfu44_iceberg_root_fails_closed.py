"""Follow-up review 5 (H1, blocking): the per-workspace Iceberg root fails closed.

Commit 73bb6a520 read the caller's workspace from context variables and
treated "no workspace bound" as a platform caller with the whole root. Code
that runs in a plain ``threading.Thread`` or a ``ThreadPoolExecutor`` without
``contextvars.copy_context()`` has no binding, so work done for workspace B
there read workspace A's tables (probe rev35/q1), and a relative name anchored
at the root, not B's area (q2: B saved ``_workspaces/ws-alpha/sales`` as a
relative warehouse, which save placed in B's area and an unbound thread placed
in A's). ``POST /schedules/{id}/accept-source-schema``
(``schedules_router._probe_source_schema_timed``) is such a path: it resolved
to A's area and wrote and deleted a probe file there.

Now:

* an unbound caller under multi-tenant auth gets the whole root only inside
  ``platform_iceberg_scope()`` (system code) or as a platform administrator's
  all-workspace view; otherwise every workspace area and every operator-assigned
  flat entry is refused (with workspace isolation on, every flat entry too);
* the thread hops that resolve connectors for a request carry its context
  (accept-source-schema, the upsert-key door, the schedule source probe) and
  the scheduler's drift probe binds the schedule's workspace;
* a relative Iceberg name may not contain ``_workspaces`` or ``..``, so save
  and use cannot anchor it differently.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

WS_A = "ws-alpha"
WS_B = "ws-bravo"


@pytest.fixture
def root(tmp_path, monkeypatch):
    for prefix in ("DATAFLOW", "DATAWRAP"):
        for name in ("ICEBERG_ROOT", "CONFINE_LOCAL_FILES", "REQUIRE_AUTH", "REQUIRE_WORKSPACE",
                     "ALLOW_PLATFORM_AWS_CREDENTIALS", "ALLOW_PLATFORM_GCP_AZURE_CREDENTIALS"):
            monkeypatch.delenv(f"{prefix}_{name}", raising=False)
    jail = tmp_path / "iceroot"
    jail.mkdir()
    monkeypatch.setenv("DATAFLOW_ICEBERG_ROOT", str(jail))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setattr(
        "socket.socket.connect", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("a test dialled"))
    )
    return jail.resolve()


def _bound(workspace: str):
    from services.export_paths import connector_export_scope

    return connector_export_scope(workspace)


def _seed(catalog_dir: Path, marker: str) -> None:
    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog

    catalog_dir.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog("dataflow", uri=f"sqlite:///{catalog_dir}/cat.db", warehouse=catalog_dir.as_uri())
    catalog.create_namespace("ns")
    table = catalog.create_table("ns.t", schema=pa.schema([("s", pa.string())]))
    table.append(pa.table({"s": [marker]}))


@pytest.fixture
def victim(root):
    area = root / "_workspaces" / WS_A / "victim"
    _seed(area, "A-SECRET")
    return area


def _endpoint(cs: str, warehouse: str = "mine") -> dict:
    return {"connection_string": cs, "extra": {"catalog_type": "sql"}, "table": "t", "schema": "ns",
            "warehouse": warehouse}


def _read(endpoint: dict) -> list:
    from connectors.iceberg_catalog import load_catalog

    return load_catalog(endpoint).load_table(("ns", "t")).scan().to_arrow().column("s").to_pylist()


def _attempt(fn) -> object:
    try:
        return fn()
    except (ValueError, RuntimeError) as exc:
        return exc


# --- an unbound thread is refused, not a platform caller ----------------------------------------


def test_q1_a_plain_thread_working_for_b_cannot_read_a(victim):
    box: dict = {}
    with _bound(WS_B):
        worker = threading.Thread(target=lambda: box.update(out=_attempt(lambda: _read(
            _endpoint(f"sqlite:///{victim}/cat.db")))))
        worker.start()
        worker.join()
    assert isinstance(box["out"], ValueError), box["out"]


def test_q1_an_executor_working_for_b_cannot_read_a(victim):
    with _bound(WS_B), ThreadPoolExecutor(1) as pool:
        out = pool.submit(_attempt, lambda: _read(_endpoint(f"sqlite:///{victim}/cat.db"))).result()
    assert isinstance(out, ValueError), out


def test_an_unbound_caller_is_refused_and_relative_names_do_not_anchor_at_the_root(victim, root):
    from connectors.iceberg_catalog import _jail_local_path

    assert isinstance(_attempt(lambda: _read(_endpoint(f"sqlite:///{victim}/cat.db"))), ValueError)
    out = _attempt(lambda: _jail_local_path("victim", "warehouse"))
    assert isinstance(out, ValueError) and "workspace" in str(out)
    assert str(root) not in str(out)


def test_the_platform_marker_keeps_the_whole_root(victim):
    from services.iceberg_workspace_root import platform_iceberg_scope

    with platform_iceberg_scope():
        assert _read(_endpoint(f"sqlite:///{victim}/cat.db", str(victim))) == ["A-SECRET"]


def test_a_platform_administrators_all_workspace_view_keeps_the_whole_root(victim):
    from services.connector_access import ConnectorCaller, bind_connector_caller

    with bind_connector_caller(ConnectorCaller(actor="root@example.com", platform_admin=True)):
        assert _read(_endpoint(f"sqlite:///{victim}/cat.db", str(victim))) == ["A-SECRET"]


def test_flat_legacy_entries_without_isolation(root, monkeypatch):
    """Isolation off (no workspaces enforced): an unbound caller opens unassigned flat entries only."""
    from services.iceberg_workspace_root import assign_flat_entry

    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "0")
    monkeypatch.setattr("services.team_store.signup_enabled", lambda: False)
    _seed(root / "legacy", "LEGACY")
    _seed(root / "_workspaces" / WS_A / "victim", "A-SECRET")
    assert _read(_endpoint("sqlite:///legacy/cat.db", "legacy")) == ["LEGACY"]
    assert isinstance(_attempt(lambda: _read(_endpoint(
        f"sqlite:///{root}/_workspaces/{WS_A}/victim/cat.db"))), ValueError)
    assign_flat_entry("legacy", WS_A)
    assert isinstance(_attempt(lambda: _read(_endpoint("sqlite:///legacy/cat.db", "legacy"))), ValueError)


# --- relative names cannot reach the container --------------------------------------------------


@pytest.mark.parametrize("name", [f"_workspaces/{WS_A}/victim", f"_workspaces/{WS_A}/victim/zzz",
                                  "x/../_workspaces", "../iceroot/x", "a/../b"])
def test_q2_a_relative_name_with_workspaces_or_dotdot_is_refused_everywhere(victim, name):
    from connectors.iceberg_catalog import _jail_local_path
    from services.egress_guard import connector_save_refusal

    refusal = connector_save_refusal({"name": "b", "type": "iceberg", "workspace_id": WS_B, "table": "t",
                                      "warehouse": name, "extra": {"catalog_type": "sql"}})
    assert refusal and WS_A not in refusal
    with _bound(WS_B):
        assert isinstance(_attempt(lambda: _jail_local_path(name, "warehouse")), ValueError)
    from services.iceberg_workspace_root import platform_iceberg_scope

    with platform_iceberg_scope():
        assert isinstance(_attempt(lambda: _jail_local_path(name, "warehouse")), ValueError)


# --- the thread hops carry the request's binding ------------------------------------------------


def test_accept_source_schema_probe_stays_in_bs_area(victim, root):
    """The real route helper: B's relative warehouse is B's, A's area is never touched."""
    from src.routers.schedules_router import _probe_source_schema_timed
    from src.transfer.models import EndpointConfig

    _seed(root / "_workspaces" / WS_B / "own", "B-ROWS")
    before = sorted(p.name for p in victim.rglob("*"))
    with _bound(WS_B):
        own = _probe_source_schema_timed(EndpointConfig(
            kind="database", format="iceberg", connection_string="sqlite:///own/cat.db", warehouse="own",
            schema="ns", table="t", extra={"catalog_type": "sql"}))
        other = _attempt(lambda: _probe_source_schema_timed(EndpointConfig(
            kind="database", format="iceberg", warehouse=str(victim), schema="ns", table="t",
            extra={"catalog_type": "sql"})))
    assert "s" in str(own.get("columns") or own.get("schema") or own), own
    assert "A-SECRET" not in str(other) and str(victim) not in str(other)
    assert sorted(p.name for p in victim.rglob("*")) == before


def _record_scope(box: list):
    from services.iceberg_workspace_root import current_scope

    def record(*_a, **_k):
        box.append(current_scope().workspace)
        return {}
    return record


def test_the_upsert_key_door_thread_carries_the_binding(root, monkeypatch):
    from services import upsert_key_door

    seen: list = []
    monkeypatch.setattr("services.source_schema_authority._saved_source_endpoint",
                        lambda *a, **k: SimpleNamespace(extra={}))
    monkeypatch.setattr("services.procedure_source.is_callable_source", lambda *_a: False)
    monkeypatch.setattr("src.transfer.endpoint_intelligence.introspect_endpoint", _record_scope(seen))
    with _bound(WS_B):
        upsert_key_door._catalog_key("c1", "t", "ns", "")
    assert seen == [WS_B]


def test_the_schedule_source_probe_thread_carries_the_binding(root, monkeypatch):
    from services import schedule_source_probe

    seen: list = []
    monkeypatch.setattr("services.dest_precount.destination_table_exists", _record_scope(seen))
    with _bound(WS_B):
        schedule_source_probe._bounded_exists("iceberg", {}, "ns", "t")
    assert seen == [WS_B]


def test_the_scheduler_drift_probe_binds_the_schedules_workspace(root, monkeypatch):
    from services import schedule_runner

    seen: list = []
    monkeypatch.setattr(schedule_runner, "probe_schedule_source_schema", _record_scope(seen))
    sched = SimpleNamespace(id="s1", workspace_id=WS_B, source_schema={}, source_primary_key=[])
    schedule_runner._guard_source_schema_drift(sched, SimpleNamespace(source=None))
    assert seen == [WS_B]
