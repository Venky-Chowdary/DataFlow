"""D308 AC-2/AC-3 live: Confirm reuses an unchanged staged preflight, and misses on a change.

Before: staging aligned its fingerprint on the approval (connector ids and a
table) while Confirm hashed the job's request after its saved connectors were
resolved (host, credentials, TLS, stamped extras) and before delivery
selection. The two never matched, so every Confirm re-ran the gates and
logged in again (measured on f5869f8c5: reuse never fired).

Now both sides hash the saved connector's current resolution of the same
request. An unchanged request reuses the staged result and the job's own
preflight takes no destination login. A change to the connector's host or
credentials, the table, the sync mode or the mappings between staging and
Confirm misses (fail safe: the gates run).
"""

from __future__ import annotations

import collections
import copy
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
def route(request, monkeypatch):
    from services import connector_executor, engine_pool
    from services.connector_store import create_connector, delete_connector

    src_kind, dst_kind = request.param
    cfgs = {"postgresql": pg_cfg(), "mysql": mysql_cfg()}
    tag = uuid.uuid4().hex[:8]
    src_t, dst_t = f"d308r_s_{tag}", f"d308r_d_{tag}"
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
        yield {"src": src, "dst": dst, "src_t": src_t, "dst_t": dst_t,
               "src_kind": src_kind, "dst_kind": dst_kind}
    finally:
        delete_connector(src.id)
        delete_connector(dst.id)
        drop(src_kind, src_t)
        drop(dst_kind, dst_t)


def _stage(route: dict, **overrides):
    from src.ai.copilot.tools import get_pilot_tools

    args = {
        "source_connector_id": route["src"].id, "source_table": route["src_t"],
        "dest_connector_id": route["dst"].id, "dest_table": route["dst_t"],
        "sync_mode": "full_refresh_overwrite", "primary_key": "id",
    }
    args.update(overrides)
    staged = get_pilot_tools().execute("start_transfer", args)
    assert staged.success, staged.error
    return staged


def _ack_payload(ack_id: str) -> dict:
    from src.ai.copilot.ack_ledger import get_ack_ledger

    payload, err = get_ack_ledger().get_pending_payload(ack_id)
    assert payload, err
    return copy.deepcopy(payload)


def _confirm_request(payload: dict):
    """The request Confirm builds and the job binds, before the run."""
    from services.confirmed_transfer import transfer_request_from_ack
    from src.transfer.engine import get_transfer_engine

    request = transfer_request_from_ack(payload)
    get_transfer_engine()._resolve_saved_connectors(request)
    return request


def _reused(payload: dict) -> bool:
    import services.staged_preflight as staged
    from src.transfer.job_scope import tracked_job_scope

    request = _confirm_request(payload)
    with tracked_job_scope(request, "d308r" + uuid.uuid4().hex[:8], store=lambda: None):
        return staged.staged_preflight_for_bound() is not None


@pytest.mark.parametrize(
    "route", [("mysql", "postgresql"), ("postgresql", "mysql")], indirect=True
)
@pytest.mark.parametrize("expired", [False, True], ids=["fresh", "expired"])
def test_unchanged_confirm_reuses_and_takes_no_preflight_login(route, monkeypatch, expired):
    """Reuse fires and the gates log in nowhere; an expired stage (control) re-runs them.

    Execute still reads the destination's keys and privileges for the writer
    (``inspect_destination_for_preflight``): that is writer metadata, not the
    gates, and it happens with or without reuse.
    """
    import psycopg2
    import pymysql

    import services.staged_preflight as staged_mod
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import get_pilot_tools

    staged = _stage(route)
    if expired:
        monkeypatch.setattr(staged_mod, "_TTL_SECONDS", -1)
    hits: list[bool] = []
    real_bound = staged_mod.staged_preflight_for_bound

    def spy():
        out = real_bound()
        hits.append(out is not None)
        return out

    monkeypatch.setattr(staged_mod, "staged_preflight_for_bound", spy)
    preflight_logins: collections.Counter = collections.Counter()

    def _note(engine: str) -> None:
        if any(f.name == "run_file_preflight" for f in traceback.extract_stack()):
            preflight_logins[engine] += 1

    real_pg, real_my = psycopg2.connect, pymysql.connections.Connection.connect

    def pg_connect(*args, **kwargs):
        _note("postgresql")
        return real_pg(*args, **kwargs)

    def my_connect(self, *args, **kwargs):
        _note("mysql")
        return real_my(self, *args, **kwargs)

    monkeypatch.setattr(psycopg2, "connect", pg_connect)
    monkeypatch.setattr(pymysql.connections.Connection, "connect", my_connect)

    job_id = spend_pilot_ack(staged.output["ack_id"], "d308r-test")["job_id"]
    deadline = time.time() + 240
    job: dict = {}
    while time.time() < deadline:
        job = dict(get_pilot_tools().execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(0.5)
    assert job.get("status") == "completed", job
    if expired:
        # Control: past the TTL the gates run again and log in.
        assert hits and hits[0] is False, hits
        assert sum(preflight_logins.values()) >= 1, dict(preflight_logins)
    else:
        # AC-2: the job ran on the staged result, and its gates logged in to
        # neither the destination nor the source.
        assert hits and hits[0] is True, hits
        assert sum(preflight_logins.values()) == 0, dict(preflight_logins)
    landed = run_sql(route["dst_kind"], f"SELECT COUNT(*) FROM {quote(route['dst_kind'], route['dst_t'])}")  # nosec B608
    assert int(landed[0][0]) == ROWS


@pytest.mark.parametrize("route", [("postgresql", "mysql")], indirect=True)
@pytest.mark.parametrize("change", ["none", "host", "credentials", "table", "sync_mode", "mappings"])
def test_a_change_between_stage_and_confirm_misses(route, change):
    """AC-3: any changed input or connector re-runs the gates."""
    from services.connector_store import update_connector

    staged = _stage(route)
    payload = _ack_payload(staged.output["ack_id"])
    if change == "host":
        # Same server, another spelling: the saved dial target changed.
        update_connector(route["dst"].id, {"host": "localhost"})
    elif change == "credentials":
        update_connector(route["dst"].id, {"password": "rotated-" + uuid.uuid4().hex[:6]})
    elif change == "table":
        payload["destination"]["table"] = route["dst_t"] + "_other"
    elif change == "sync_mode":
        payload["sync_mode"] = "full_refresh_append"
    elif change == "mappings":
        payload["mappings"] = [m for m in payload.get("mappings") or [] if m.get("source") != "amount"]
        assert payload["mappings"], payload
    assert _reused(payload) is (change == "none"), change
