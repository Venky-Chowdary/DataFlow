"""Ops review, low findings.

- The read cache ran on the wall clock: a backwards clock step kept a revoked
  setting cached for as long as the step.
- After failed import attempts the file was still marked imported, so that
  process never retried.
- An imported log line got ``seq = now - 1000 s`` and could sort after live
  records appended earlier.
- The API's shutdown joins (5 + 5 + 30 + 5 s) roughly equalled its grace
  period.
- The chart turned DocumentDB-style TLS on for every install, breaking an
  upgrade against a self-hosted MongoDB without TLS.
- The D321 latency bound had become relative only (up to 3.5 s, the very time
  a blocked loop needs to free in its second test).
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

import pytest

from services import shared_doc_store, tenant_store
from services.shared_doc_store import SharedDoc, SharedLog
from tests._control_plane_fakes import install_shared_store

REPO = Path(__file__).resolve().parents[3]
CHART = REPO / "deploy/enterprise/helm/dataflow"
ACK = {"controlPlane": {"podLocalStoresAck": "transfer_plans,pilot_results"}}


# ---------------------------------------------------------------------------
# The cache window survives a backwards clock step
# ---------------------------------------------------------------------------


def test_cache_window_survives_a_backwards_clock_step(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    tenant = tenant_store.create_tenant("ws-clock", "Clock", ip_allowlist=["203.0.113.0/24"])
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "0.2")
    assert tenant_store.get_tenant(tenant.id).ip_allowlist == ["203.0.113.0/24"]  # now cached

    # Another replica tightens the allowlist; then this host's clock steps back an hour.
    item = db["control_plane_items"].find_one({"_id": f"tenant_settings:{tenant.id}"})
    body = json.loads(item["body"])
    body["ip_allowlist"] = ["198.51.100.7/32"]
    db["control_plane_items"].update_one({"_id": item["_id"]}, {"$set": {"body": json.dumps(body)}, "$inc": {"rev": 1}})
    real_time = time.time
    monkeypatch.setattr(shared_doc_store.time, "time", lambda: real_time() - 3600)
    time.sleep(0.3)
    assert tenant_store.get_tenant(tenant.id).ip_allowlist == ["198.51.100.7/32"], (
        "a backwards clock step kept the old allowlist cached"
    )


# ---------------------------------------------------------------------------
# A failed import is retried
# ---------------------------------------------------------------------------


def test_failed_import_attempts_leave_the_file_to_be_retried(tmp_path, monkeypatch):
    from pymongo.errors import DuplicateKeyError

    db = install_shared_store(monkeypatch)
    path = tmp_path / "doc.json"
    path.write_text(json.dumps({"items": ["from-the-file"]}))
    doc = SharedDoc(
        f"test_retry_{uuid.uuid4().hex[:8]}",
        file_load=lambda: json.loads(path.read_text()), file_save=lambda _d: None, file_path=lambda: path,
        empty=lambda: {"items": []},
    )
    coll = db[shared_doc_store.DOCS_COLLECTION]
    real_insert = coll.insert_one

    def racing_insert(_document):
        # Another replica keeps creating and removing the document under us.
        raise DuplicateKeyError("duplicate key")

    monkeypatch.setattr(coll, "insert_one", racing_insert)
    assert doc.import_local_file() is None
    assert not doc._imported, "a file whose import never landed was marked imported"

    monkeypatch.setattr(coll, "insert_one", real_insert)
    assert doc.import_local_file()["action"] == "imported"
    assert doc.load()["items"] == ["from-the-file"]


# ---------------------------------------------------------------------------
# Imported log records never sort after live ones
# ---------------------------------------------------------------------------


def test_imported_log_records_sort_before_live_records(tmp_path, monkeypatch):
    install_shared_store(monkeypatch)
    path = tmp_path / "anchors.jsonl"
    path.write_text('{"id": "old-1"}\n{"id": "old-2"}\n')
    log = SharedLog(f"test_seq_{uuid.uuid4().hex[:8]}", file_path=lambda: path)
    coll = shared_doc_store._collection(shared_doc_store.RECORDS_COLLECTION)
    docs = coll.database[shared_doc_store.DOCS_COLLECTION]
    digest = shared_doc_store._file_digest(path)[0]
    # This pod has claimed the import; meanwhile another pod appended live, two hours ago.
    docs.insert_one({"_id": f"import:{log.name}", "kind": "import", "store": log.name, "sha256": digest,
                     "state": "importing", "at": time.time()})
    coll.insert_one(log._doc({"id": "live"}, seq=time.time_ns() - 7200 * 10**9))
    log.import_local_file()
    assert [r["id"] for r in log.records(coll)] == ["old-1", "old-2", "live"]


# ---------------------------------------------------------------------------
# The API's shutdown joins fit a budget
# ---------------------------------------------------------------------------


def test_api_shutdown_joins_fit_inside_one_budget(monkeypatch):
    from services import background_lifecycle

    calls: list[tuple[str, float | None]] = []

    def slow(name):
        def stop(timeout=None):
            calls.append((name, timeout))
            time.sleep(min(1.0, timeout if timeout is not None else 1.0))
        return stop

    monkeypatch.setattr(
        background_lifecycle, "_stoppers",
        lambda: [(f"s{n}", slow(f"s{n}"), 5.0) for n in range(5)],
    )
    monkeypatch.setenv("DATAFLOW_API_SHUTDOWN_BUDGET_SECONDS", "1.5")
    started = time.monotonic()
    assert background_lifecycle.stop_background_threads() == []
    took = time.monotonic() - started
    assert took < 1.5 + 0.4, f"shutdown joins took {took:.1f}s with a 1.5s budget"
    assert all(timeout is not None and timeout <= 1.5 for _name, timeout in calls)


def test_default_shutdown_budget_fits_the_chart_grace_period():
    from services.background_lifecycle import shutdown_budget_seconds
    from tests._helm_lite import render_chart

    api = next(
        o for o in render_chart(CHART, overrides=ACK)
        if o.get("kind") == "Deployment" and o["metadata"]["labels"]["app.kubernetes.io/component"] == "api"
    )
    pod = api["spec"]["template"]["spec"]
    container = pod["containers"][0]
    env = {e["name"]: e.get("value") for e in container["env"]}
    pre_stop = int(container["lifecycle"]["preStop"]["exec"]["command"][1])
    graceful = int(env["DATAFLOW_GRACEFUL_SHUTDOWN_SECONDS"])
    margin = pod["terminationGracePeriodSeconds"] - (pre_stop + graceful + shutdown_budget_seconds())
    assert margin >= 5, f"only {margin}s between the last join and SIGKILL"


# ---------------------------------------------------------------------------
# DocumentDB TLS only when the chart is told it is DocumentDB
# ---------------------------------------------------------------------------


def _mongo_url(overrides: dict) -> str:
    from tests._helm_lite import render_chart

    for obj in render_chart(CHART, overrides={**ACK, **overrides}):
        if obj.get("kind") == "Deployment" and obj["metadata"]["labels"]["app.kubernetes.io/component"] == "api":
            return {e["name"]: e.get("value") for e in obj["spec"]["template"]["spec"]["containers"][0]["env"]}["MONGODB_URL"]
    raise AssertionError("no API deployment")


def test_self_hosted_mongodb_keeps_tls_off_on_upgrade():
    url = _mongo_url({})
    assert "tls=false" in url and "tls=true" not in url, url
    assert "retryWrites=false" not in url


def test_documentdb_gets_tls_and_no_retryable_writes():
    url = _mongo_url({"data": {"documentdb": {"engine": "documentdb", "host": "docdb.example.internal"}}})
    assert "tls=true" in url and "retryWrites=false" in url, url
    import yaml

    production = yaml.safe_load((CHART / "values-production.yaml").read_text(encoding="utf-8"))
    assert production["data"]["documentdb"]["engine"] == "documentdb", "the EKS values talk to Amazon DocumentDB"


# ---------------------------------------------------------------------------
# D321 keeps an absolute ceiling
# ---------------------------------------------------------------------------


def test_d321_limit_has_an_absolute_ceiling_below_any_blocked_wait():
    pytest.importorskip("oracledb")
    from tests import test_d321_api_answers_while_a_connector_hangs as d321

    # A route behind a blocked loop in the second test waits until the release.
    route_blocked = d321.RELEASE_AFTER - (d321.DEADLINE + 1.5)
    # In the first, the 0.5 s sleep wakes at the connector deadline at the earliest.
    loop_late = d321.DEADLINE - 0.5
    assert d321.FAST < loop_late, "the event-loop bound must catch a loop blocked until the deadline"
    for baseline in (0.001, 0.2, 0.5, 2.0, 30.0):
        limit = d321._limits({"route": baseline})["route"]
        assert d321.FAST <= limit <= d321.CEILING
        assert route_blocked - limit >= 1.0, "the ceiling must stay well below a blocked wait"
