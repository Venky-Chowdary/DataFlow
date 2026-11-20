"""D308 live: staging logs in to the destination once, and Confirm still reuses it.

QA (rounds 42-51): MySQL-source plans timed out at 45 s while the processlist
showed a fresh connection every 3-6 s. AC-1 asks an instrumented test to count
one connection per endpoint during staging. Measured on f5869f8c5 for an
overwrite into an existing table: the destination took two logins, the
preflight's introspection and then the staged schema snapshot
(``staged_preflight._read_schema_snapshot``) on its own pooled engine.

The snapshot now borrows the introspection's session inside the tool call.
Confirm still re-reads the schema through the guarded engine (egress guard,
TLS) before the run, so the two reads must hash the same or reuse can never
happen (AC-2): this test reads it the way Confirm does and compares.

Not asserted here (open, see p2-gaps.md): Confirm does not reuse the staged
result live, because the request it hashes has its saved connectors resolved
(host, port, credentials, stamped extras, selected delivery) while staging
hashed the connector ids.
"""

from __future__ import annotations

import collections
import os
import time
import traceback
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.r13_mapping_harness import drop, mysql_cfg, pg_cfg, quote, run_sql  # noqa: E402

ROWS = 30
DDL = "CREATE TABLE {t} (id INT PRIMARY KEY, name VARCHAR(40), amount INT)"


@pytest.fixture
def logins(monkeypatch):
    """Driver-level login counter per engine; snapshot logins counted apart."""
    import psycopg2
    import pymysql

    seen: collections.Counter = collections.Counter()

    def _note(engine: str) -> None:
        seen[engine] += 1
        if any(f.name == "_read_schema_snapshot" for f in traceback.extract_stack()):
            seen[f"{engine}_snapshot"] += 1

    real_pg = psycopg2.connect

    def pg_connect(*args, **kwargs):
        _note("postgresql")
        return real_pg(*args, **kwargs)

    real_my = pymysql.connections.Connection.connect

    def my_connect(self, *args, **kwargs):
        _note("mysql")
        return real_my(self, *args, **kwargs)

    monkeypatch.setattr(psycopg2, "connect", pg_connect)
    monkeypatch.setattr(pymysql.connections.Connection, "connect", my_connect)
    return seen


@pytest.mark.parametrize("src_kind,dst_kind", [("mysql", "postgresql"), ("postgresql", "mysql")])
def test_staging_logs_in_to_the_destination_once_and_confirm_reads_the_same_version(
    src_kind, dst_kind, logins, monkeypatch
):
    import services.staged_preflight as staged_mod
    from conftest import spend_pilot_ack
    from services import connector_executor, engine_pool
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tools import get_pilot_tools
    from src.transfer.adapters import resolve_endpoint
    from src.transfer.models import EndpointConfig

    cfgs = {"postgresql": pg_cfg(), "mysql": mysql_cfg()}
    tag = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d308s_{tag}", f"d308d_{tag}"
    run_sql(src_kind, DDL.format(t=quote(src_kind, src_t)))
    run_sql(
        src_kind,
        f"INSERT INTO {quote(src_kind, src_t)} VALUES (%s, %s, %s)",  # nosec B608
        params=[(i, f"n{i}", i) for i in range(1, ROWS + 1)],
    )
    run_sql(dst_kind, DDL.format(t=quote(dst_kind, dst_t)))
    src = create_connector({"name": f"s-{tag}", "type": src_kind, "role": "source", **cfgs[src_kind]})
    dst = create_connector({"name": f"d-{tag}", "type": dst_kind, "role": "destination", **cfgs[dst_kind]})
    monkeypatch.delenv("DATAFLOW_TOOL_DEADLINE_SECONDS", raising=False)
    connector_executor.reset_for_tests()
    engine_pool.reset_for_tests()
    try:
        logins.clear()
        started = time.monotonic()
        staged = get_pilot_tools().execute("start_transfer", {
            "source_connector_id": src.id, "source_table": src_t,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "full_refresh_overwrite", "primary_key": "id",
        })
        elapsed = time.monotonic() - started
        assert staged.success, staged.error
        assert elapsed < 45, elapsed
        # AC-1 for the destination: one login, and none of it the snapshot.
        assert logins[dst_kind] == 1, dict(logins)
        assert logins[f"{dst_kind}_snapshot"] == 0, dict(logins)

        # AC-2 precondition: Confirm's guarded, out-of-scope read of the
        # resolved destination hashes the same as staging's borrowed read.
        run_id = str(staged.output.get("preflight_run_id") or "") or next(reversed(staged_mod._RUNS))
        stored = staged_mod._RUNS[run_id]["schema_version"]
        assert stored and stored != "unread", stored
        confirm_dest = resolve_endpoint(
            EndpointConfig(kind="database", format=dst_kind, connector_id=dst.id, table=dst_t)
        )
        assert staged_mod.bounded_destination_schema_version(confirm_dest) == stored

        job_id = spend_pilot_ack(staged.output["ack_id"], "d308-test")["job_id"]
        deadline = time.time() + 240
        job: dict = {}
        while time.time() < deadline:
            job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
            if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
                break
            time.sleep(0.5)
        assert job.get("status") == "completed", job
        landed = run_sql(dst_kind, f"SELECT COUNT(*) FROM {quote(dst_kind, dst_t)}")  # nosec B608
        assert int(landed[0][0]) == ROWS
    finally:
        delete_connector(src.id)
        delete_connector(dst.id)
        drop(src_kind, src_t)
        drop(dst_kind, dst_t)
