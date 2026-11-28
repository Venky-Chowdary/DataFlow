"""D431 — no safe_to_start=true for a destination the plan could not read.

QA round 68: MongoDB -> Oracle upsert. plan_transfer said safe_to_start=true
with no blockers while every engine mapping was ``pending_dest_schema`` with an
empty target_type ("Destination schema unavailable"). /transfer/run accepted
those mappings and failed "ORA-00931: missing identifier"; rounds 65/70/71
ended the same plan state in "No column mappings".

Root cause: no gate owned "Map rows wait on an unread destination". Whether a
plan blocked depended on unrelated gates happening to fire on the empty types,
and Execute let the writer guess (create the table from the source types, or
drop every pending column). One shared rule now: the plan blocks with
``g_dest_schema_unread`` ("plan the transfer again"), and Execute refuses a
request that still carries those rows, before any DDL or write, naming the
blocker in the job record.
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
from services import connector_store  # noqa: E402
from src.ai.copilot import tools as tools_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

GATE = "g_dest_schema_unread"


@pytest.fixture()
def route(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    connector_store._backend_choice = None
    tools_mod._tools = None
    names = []
    for role in ("src", "dst"):
        db = tmp_path / f"d431_{role}.db"
        conn = sqlite3.connect(db)
        if role == "src":
            conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, sku TEXT, note TEXT)")
            conn.executemany("INSERT INTO orders VALUES (?, ?, ?)", [(1, "a", "x"), (2, "b", "y")])
            conn.commit()
        conn.close()
        saved = connector_store.create_connector({
            "name": f"D431{role}", "type": "sqlite", "role": "both",
            "connection_string": f"sqlite:///{db.resolve().as_posix()}", "workspace_id": "",
        })
        names.append(saved.name)
    return names


def _plan(names, monkeypatch=None, dest_read=None):
    from src.ai.copilot import transfer_tools as tt

    if dest_read is not None:
        real = tt._introspect

        def fake(conn, table, purpose="", **kw):
            if purpose == "destination":
                return dict(dest_read)
            return real(conn, table, purpose=purpose, **kw)

        monkeypatch.setattr(tt, "_introspect", fake)
    res = tt.plan_transfer(
        source_connector_name=names[0], source_table="orders",
        dest_connector_name=names[1], dest_table="orders_out",
        sync_mode="upsert", upsert_key="id",
    )
    assert res.success, res.error
    return res.output


def _blocker(out: dict) -> dict | None:
    for b in out["preflight"].get("blockers") or []:
        if b.get("id") == GATE:
            return b
    return None


@pytest.mark.parametrize(
    "dest_read",
    [
        # A read that timed out (tunnel or network blip).
        {"ok": False, "error": "connection timed out", "columns": [], "db_type": "sqlite", "cfg": {}},
        # Connected, but the catalog returned no columns and no existence verdict.
        {"ok": True, "error": "", "columns": [], "db_type": "sqlite", "cfg": {}},
    ],
)
def test_plan_with_unread_destination_is_not_safe_and_names_the_blocker(route, monkeypatch, dest_read):
    out = _plan(route, monkeypatch, dest_read)
    pending = [m for m in out["engine_mappings"] if m.get("assignment_strategy") == "pending_dest_schema"]
    assert pending, "precondition: Map could not read the destination"
    assert out["safe_to_start"] is False
    blocker = _blocker(out)
    assert blocker is not None, out["preflight"].get("blockers")
    assert "could not be read" in blocker["message"]
    assert "plan the transfer again" in blocker["message"]
    gate = next(g for g in out["preflight"].get("gates") or [] if g.get("id") == GATE)
    assert gate["status"] == "block"
    # No create-new DDL is proposed for a destination nobody read.
    assert all(not str(m.get("target_type") or "").strip() for m in pending)
    assert not any(m.get("create_new") for m in pending)


def test_readable_destination_keeps_its_verdict(route):
    """AC-4: a destination that was read gets assigned mappings and no false blocker."""
    out = _plan(route)
    assert out["safe_to_start"] is True
    assert _blocker(out) is None
    assert not any(
        m.get("assignment_strategy") == "pending_dest_schema" for m in out["engine_mappings"]
    )


def test_preflight_names_the_unread_destination_blocker():
    """The verdict names its own cause, not whichever unrelated gate fired on the empty types."""
    from services.preflight_service import run_file_preflight

    mappings = [
        {"source": c, "target": c, "confidence": 0.99, "target_type": "",
         "assignment_strategy": "pending_dest_schema", "create_new": False,
         "requires_review": True, "source_type": "VARCHAR(20)"}
        for c in ("sku", "note")
    ]
    pf = run_file_preflight(
        columns=["sku", "note"], column_types={"sku": "VARCHAR(20)", "note": "VARCHAR(20)"},
        row_count=2, mappings=mappings, destination_connected=True,
        sample_rows=[{"sku": "a", "note": "x"}], destination_db_type="oracle",
        destination_table_exists=None, skip_population_fit=True, sync_mode="full_refresh_append",
    )
    assert pf["passed"] is False
    ids = [b.get("id") for b in pf.get("blockers") or []]
    assert GATE in ids
    assert any(g.get("id") == GATE and g.get("status") == "block" for g in pf.get("gates") or [])


def test_omitted_pending_column_does_not_block():
    from services.plan_execute_blockers import dest_schema_unread_gate

    rows = [
        {"source": "id", "target": "id", "target_type": "INTEGER"},
        {"source": "tmp", "target": "tmp", "assignment_strategy": "pending_dest_schema",
         "intentional_omit": True},
    ]
    assert dest_schema_unread_gate(rows) is None


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


def _pending_request(src: Path, dst: Path, *, multi: bool = False) -> TransferRequest:
    pending = [
        {"source": c, "target": c, "source_type": t, "target_type": "",
         "assignment_strategy": "pending_dest_schema", "requires_review": True}
        for c, t in (("id", "INTEGER"), ("sku", "TEXT"), ("note", "TEXT"))
    ]
    contracts = [{"name": "orders", "sync_mode": "upsert", "primary_key": "id", "selected": True}]
    if multi:
        contracts = [
            {**contracts[0], "mappings": pending},
            {"name": "orders2", "sync_mode": "upsert", "primary_key": "id", "selected": True,
             "mappings": [dict(m, assignment_strategy="identity_passthrough",
                               target_type=m["source_type"]) for m in pending]},
        ]
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="orders"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="orders_out"),
        sync_mode="upsert",
        stream_contracts=contracts,
        mappings=[] if multi else pending,
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


def test_execute_refuses_pending_mappings_before_any_ddl(tmp_path, fake_store):
    """AC-2: /transfer/run with the plan's unread mappings is refused, retriable."""
    src = tmp_path / "src.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, sku TEXT, note TEXT)")
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?)", [(1, "a", "x"), (2, "b", "y")])
    conn.commit()
    conn.close()
    dst = tmp_path / "dst.db"
    job_id = "d431" + uuid.uuid4().hex[:16]
    fake_store.update_job_status(job_id, "pending", transfer_request={})

    result = UniversalTransferEngine().execute_tracked(_pending_request(src, dst), job_id)

    assert result.success is False
    assert "could not be read" in result.error
    assert result.error_details["blocker_id"] == GATE
    assert result.error_details["retriable"] is True
    job = fake_store.jobs[job_id]
    assert job["status"] == "failed"
    assert job["error_details"]["blocker_id"] == GATE
    assert "orders_out" not in _tables(dst), "no DDL or write before the refusal"


def test_execute_checks_each_streams_own_map_rows():
    from services.plan_execute_blockers import execute_plan_refusal

    request = _pending_request(Path("/nonexistent/src.db"), Path("/nonexistent/dst.db"), multi=True)
    refusal = execute_plan_refusal(request)
    assert refusal and refusal.gate_id == GATE


# -- AC-3: a destination schema that does not exist ------------------------


class _FakeNamespaceEngine:
    """Answers the namespace query only; records what it was asked."""

    def __init__(self, exists: bool):
        self.exists = exists
        self.asked: list[tuple[str, dict]] = []

    def connect(self):
        engine = self

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return False

            def execute(self, stmt, params=None):
                engine.asked.append((str(stmt), dict(params or {})))

                class _R:
                    def first(self_inner):
                        return (1,) if engine.exists else None

                return _R()

        return _Conn()


@pytest.mark.parametrize("db_type,schema,query", [
    ("oracle", "nosuchschema69", "all_users"),
    ("sqlserver", "nosuch", "sys.schemas"),
])
def test_missing_namespace_is_named(monkeypatch, db_type, schema, query):
    from services import dest_namespace

    fake = _FakeNamespaceEngine(exists=False)
    monkeypatch.setattr("connectors.generic_sql._engine", lambda cfg: fake)
    cfg = {"type": db_type, "schema": schema, "username": "app", "host": "db.example.com"}
    missing = dest_namespace.missing_destination_namespace(cfg)
    assert missing.lower() == schema.lower()
    assert query in fake.asked[0][0]
    fake.exists = True
    assert dest_namespace.missing_destination_namespace(cfg) == ""


def test_default_schema_and_other_engines_are_not_asked(monkeypatch):
    from services import dest_namespace

    fake = _FakeNamespaceEngine(exists=False)
    monkeypatch.setattr("connectors.generic_sql._engine", lambda cfg: fake)
    assert dest_namespace.missing_destination_namespace(
        {"type": "oracle", "schema": "APP", "username": "app"}) == ""
    assert dest_namespace.missing_destination_namespace(
        {"type": "sqlserver", "schema": "dbo", "username": "sa"}) == ""
    assert dest_namespace.missing_destination_namespace(
        {"type": "postgresql", "schema": "nosuch", "username": "app"}) == ""
    assert fake.asked == []


def test_plan_into_a_missing_oracle_schema_fails_naming_it(route, monkeypatch):
    """Round 69: NOSUCHSCHEMA69 tested green; the plan said safe with create-new
    mappings and the run failed ORA-01918. The plan now names the schema."""
    fake = _FakeNamespaceEngine(exists=False)
    monkeypatch.setattr("connectors.generic_sql._engine", lambda cfg: fake)
    dest_read = {
        "ok": True, "error": "", "columns": [], "db_type": "oracle", "table_exists": False,
        "config": {"type": "oracle", "schema": "NOSUCHSCHEMA69", "username": "app",
                   "host": "ora.example.com", "port": 1521},
    }
    from src.ai.copilot import transfer_tools as tt

    real = tt._introspect

    def fake_read(conn, table, purpose="", **kw):
        if purpose == "destination":
            return dict(dest_read)
        return real(conn, table, purpose=purpose, **kw)

    monkeypatch.setattr(tt, "_introspect", fake_read)
    res = tt.plan_transfer(
        source_connector_name=route[0], source_table="orders",
        dest_connector_name=route[1], dest_table="orders_out",
        sync_mode="upsert", upsert_key="id",
    )
    assert res.success is False
    assert "Destination schema NOSUCHSCHEMA69 does not exist" in res.error
    assert "ORA-" not in res.error
