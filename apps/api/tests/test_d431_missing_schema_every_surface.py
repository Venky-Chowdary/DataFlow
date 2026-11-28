"""D431 AC-3 on every surface: a destination schema the server does not have.

Review of 45472044a: the missing-schema check ran in the Pilot plan only.
Studio Validate (``run_file_preflight``) and Execute (``request_refusal``) never
asked, so an Oracle connector whose schema NOSUCHSCHEMA does not exist passed
Validate and the run failed with a raw ORA-01918. The probe is now one shared
destination check (``services.dest_namespace``): a preflight gate and an
Execute refusal before any DDL, bounded by its own deadline. An unanswered
probe stays unconfirmed and never blocks; an existing schema is unaffected.
"""

from __future__ import annotations

import os
import threading
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

GATE = "g_dest_schema_missing"
_ORACLE = {"type": "oracle", "host": "ora.example.com", "port": 1521, "database": "FREEPDB1",
           "username": "app", "password": "x", "schema": "NOSUCHSCHEMA69"}


class _Catalog:
    """A fake engine: answers the namespace query only and records every statement."""

    def __init__(self, exists: bool | None, *, delay: float = 0.0, names=None):
        self.exists = exists
        self.delay = delay
        #: When set, the stored owner/schema names: a row only for an exact match.
        self.names = names
        self.asked: list[str] = []
        self.params: list[dict] = []
        self.lock = threading.Lock()

    def connect(self):
        catalog = self

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return False

            def execute(self, stmt, params=None):
                with catalog.lock:
                    catalog.asked.append(str(stmt))
                    catalog.params.append(dict(params or {}))
                if catalog.delay:
                    time.sleep(catalog.delay)
                if catalog.exists is None and catalog.names is None:
                    raise OSError("catalog did not answer")

                hit = (catalog.exists if catalog.names is None
                       else str((params or {}).get("name")) in catalog.names)

                class _R:
                    def first(self_inner):
                        return (1,) if hit else None

                return _R()

        return _Conn()


@pytest.fixture()
def catalog(monkeypatch):
    from services.sql_object_identity import ObjectIdentity

    holder = {"engine": _Catalog(False), "table": "absent"}
    monkeypatch.setattr("connectors.generic_sql._engine", lambda cfg: holder["engine"])

    def identity(engine, table, schema=None, *, columns=None, dialect=""):
        state = holder["table"]
        return ObjectIdentity(schema, table, state == "present", resolved=state != "unreadable")

    monkeypatch.setattr("services.sql_object_identity.resolve_object_identity", identity)
    return holder


def _validate(cfg, table_exists=False):
    from services.preflight_service import run_file_preflight

    return run_file_preflight(
        columns=["id", "name"], column_types={"id": "INTEGER", "name": "VARCHAR(20)"},
        row_count=2,
        mappings=[{"source": c, "target": c.upper(), "confidence": 0.99,
                   "target_type": t, "assignment_strategy": "create_compatible_new",
                   "create_new": True}
                  for c, t in (("id", "NUMBER(10)"), ("name", "VARCHAR2(20)"))],
        destination_connected=True, sample_rows=[{"id": 1, "name": "a"}],
        destination_db_type="oracle", destination_table_exists=table_exists,
        destination_can_create=True, destination_can_write=True,
        destination_config=cfg, skip_population_fit=True, sync_mode="full_refresh_append",
    )


def _gate(pf):
    return next((b for b in pf.get("blockers") or [] if b.get("id") == GATE), None)


def test_studio_validate_refuses_a_missing_schema(catalog):
    pf = _validate(dict(_ORACLE))
    assert pf["passed"] is False
    gate = _gate(pf)
    assert gate is not None, pf.get("blockers")
    assert "Destination schema NOSUCHSCHEMA69 does not exist" in gate["message"]
    assert "ORA-" not in gate["message"] and GATE not in gate["message"]
    assert any("all_users" in q for q in catalog["engine"].asked)


def test_studio_validate_existing_schema_is_unaffected(catalog):
    catalog["engine"] = _Catalog(True)
    assert _gate(_validate(dict(_ORACLE))) is None
    # A table that was read proves its schema, and an unknown one is not judged:
    # only the create path (table proven absent) asks, as Execute does.
    catalog["engine"] = _Catalog(False)
    assert _gate(_validate(dict(_ORACLE), table_exists=True)) is None
    assert _gate(_validate(dict(_ORACLE), table_exists=None)) is None
    assert catalog["engine"].asked == []


def test_an_unanswered_probe_never_blocks(catalog, monkeypatch):
    catalog["engine"] = _Catalog(None)
    assert _gate(_validate(dict(_ORACLE))) is None
    from services import dest_namespace

    monkeypatch.setattr(dest_namespace, "NAMESPACE_PROBE_SECONDS", 0.5)
    catalog["engine"] = _Catalog(False, delay=3.0)
    started = time.monotonic()
    assert dest_namespace.missing_destination_namespace(dict(_ORACLE)) == ""
    assert time.monotonic() - started < 2.5, "the probe is bounded by its own deadline"


@pytest.fixture()
def fake_store(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: fake, raising=False)
    return fake


def _oracle_request() -> TransferRequest:
    dest = EndpointConfig(kind="database", format="oracle", host="ora.example.com", port=1521,
                          database="FREEPDB1", username="app", password="x",
                          schema="NOSUCHSCHEMA69", table="ORDERS_OUT")
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        source_filename="orders.csv", source_content=b"id,name\n1,a\n2,b\n",
        destination=dest, sync_mode="full_refresh_append", skip_preflight=True,
    )


def test_execute_refuses_a_missing_schema_before_any_ddl(catalog, fake_store):
    job_id = "d431ns" + uuid.uuid4().hex[:12]
    fake_store.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(_oracle_request(), job_id)
    assert result.success is False
    assert "Destination schema NOSUCHSCHEMA69 does not exist" in result.error
    assert result.error_details["blocker_id"] == GATE
    assert fake_store.jobs[job_id]["error_details"]["blocker_id"] == GATE
    # Only the catalog question was asked: no CREATE, no INSERT.
    asked = catalog["engine"].asked
    assert asked and all("all_users" in q for q in asked), asked


def test_execute_existing_or_unconfirmed_schema_is_not_refused(catalog):
    from services.plan_execute_blockers import execute_plan_refusal

    catalog["engine"] = _Catalog(True)
    assert execute_plan_refusal(_oracle_request()) == ""
    catalog["engine"] = _Catalog(None)
    assert execute_plan_refusal(_oracle_request()) == ""


def test_existing_table_append_is_not_refused_when_the_schema_probe_sees_no_row(catalog):
    """Review of 7daf8e9bd: SQL Server's sys.schemas hides a schema the login
    holds only table grants in. An existing-table append passed Validate and was
    refused at Execute. Plan, Validate and Execute now agree: no refusal, and the
    schema is never probed when the table exists."""
    from services.plan_execute_blockers import execute_plan_refusal

    catalog["table"] = "present"
    catalog["engine"] = _Catalog(False)  # sys.schemas / ALL_USERS: no row
    for cfg in (dict(_ORACLE), {**_ORACLE, "type": "sqlserver", "schema": "etl", "port": 1433}):
        assert _gate(_validate(cfg, table_exists=True)) is None
        request = _oracle_request()
        request.destination.format = cfg["type"]
        request.destination.schema = cfg["schema"]
        assert execute_plan_refusal(request) == ""
    assert catalog["engine"].asked == [], "the schema is not probed beside an existing table"
    catalog["table"] = "unreadable"
    assert execute_plan_refusal(_oracle_request()) == ""


@pytest.mark.parametrize("db_type,configured,probed", [
    ("oracle", "AppData", "AppData"),        # mixed case: the writer's quoted owner
    ("oracle", "appdata", "APPDATA"),        # all-lowercase folds upper, as the writer folds
    ("oracle", '"AppData"', "AppData"),      # quoted: exact
    ("sqlserver", "[etl]", "etl"),           # bracketed: the stored name
    ("sqlserver", '"Etl"', "Etl"),
])
def test_the_probe_asks_for_the_name_the_writer_uses(catalog, db_type, configured, probed):
    from services import dest_namespace

    catalog["engine"] = _Catalog(None, names={probed})
    cfg = {**_ORACLE, "type": db_type, "schema": configured}
    assert dest_namespace.missing_destination_namespace(cfg) == ""
    assert catalog["engine"].params[-1]["name"] == probed
    # And the same catalog without that exact name is "missing", named as configured.
    catalog["engine"] = _Catalog(None, names={"SOMETHING_ELSE"})
    assert dest_namespace.missing_destination_namespace(cfg) == probed


def test_a_mixed_case_owner_that_exists_passes_validate(catalog):
    catalog["engine"] = _Catalog(None, names={"AppData"})
    assert _gate(_validate({**_ORACLE, "schema": "AppData"})) is None
    assert _gate(_validate({**_ORACLE, "schema": "NoSuchOwner"})) is not None


@pytest.mark.parametrize("text,schema", [
    ("ORA-01918: user 'NOSUCHSCHEMA69' does not exist", "NOSUCHSCHEMA69"),
    ('(2760, b\'The specified schema name "etl" either does not exist or you do not have '
     'permission to use it.DB-Lib error message 20018\')', "etl"),
])
def test_a_create_that_hits_a_missing_schema_gets_a_named_message(text, schema):
    """When the probe stayed unconfirmed, CREATE's own error is translated, not raw."""
    from services.error_taxonomy import classify

    out = classify(text, side="destination", phase="ddl")
    assert out.code == "object.schema_not_found", out
    assert out.object.get("schema") == schema
    assert schema in out.message and "ORA-" not in out.message
