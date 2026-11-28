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

    def __init__(self, exists: bool | None, *, delay: float = 0.0):
        self.exists = exists
        self.delay = delay
        self.asked: list[str] = []
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
                if catalog.delay:
                    time.sleep(catalog.delay)
                if catalog.exists is None:
                    raise OSError("catalog did not answer")

                class _R:
                    def first(self_inner):
                        return (1,) if catalog.exists else None

                return _R()

        return _Conn()


@pytest.fixture()
def catalog(monkeypatch):
    holder = {"engine": _Catalog(False)}
    monkeypatch.setattr("connectors.generic_sql._engine", lambda cfg: holder["engine"])
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
    # A table that was read proves its schema: not even asked.
    catalog["engine"] = _Catalog(False)
    assert _gate(_validate(dict(_ORACLE), table_exists=True)) is None
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
