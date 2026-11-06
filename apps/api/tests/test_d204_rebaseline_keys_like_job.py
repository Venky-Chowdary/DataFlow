"""D204 — rebaseline_transfer must move the baseline the next append reads.

Prod repro (MySQL -> Neon, full_refresh_append): a clean append failed on the
X-97 continuity check, the operator ran ``rebaseline_transfer`` by connector id
("previous": null) and by name, both reported success, and the next append
still failed against the OLD 3-row baseline.

Root cause: the Pilot Confirm rebuilt the destination endpoint from the saved
connector's fields without its ``connector_id``. A job started from that
connector keys its baseline on ``id:<connector_id>``; the rebuilt endpoint fell
back to ``host:<h>:<p>`` and the rebaseline landed under a key no append reads.
"""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.adapters as adapters  # noqa: E402
from services import append_continuity as ac  # noqa: E402
from src.transfer.adapters import resolve_endpoint  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402

_SAVED = {
    "type": "postgresql", "host": "10.0.0.2", "port": 5432, "database": "wh",
    "schema": "public", "username": "w", "password": "pw",
    "connection_string": "", "warehouse": "", "ssl": False, "extra": {},
}


@pytest.fixture()
def saved_connector(monkeypatch, tmp_path):
    monkeypatch.setattr(
        adapters, "_lookup_saved_connector",
        lambda cid, workspace_id=None: dict(_SAVED) if cid == "c-dst" else None,
    )
    import services.connector_store as connector_store

    monkeypatch.setattr(
        connector_store, "get_connector",
        lambda cid, workspace_id=None: object() if cid == "c-dst" else None,
    )
    monkeypatch.setattr(ac, "_collection", lambda: None)
    monkeypatch.setattr(ac, "store_path", lambda: tmp_path / "append_continuity.json")
    return SimpleNamespace(id="c-dst", name="QAH Neon", **_SAVED)


def _job_destination() -> EndpointConfig:
    """The destination exactly as a job started from the saved connector carries it."""
    return resolve_endpoint(
        EndpointConfig(kind="database", format="postgresql", connector_id="c-dst", table="qah_q_0203")
    )


def test_confirm_endpoint_keys_like_the_job(saved_connector):
    from src.routers.copilot_router import _saved_connector_endpoint

    job_key = ac.resolve_target(_job_destination()).key
    confirm_key = ac.resolve_target(_saved_connector_endpoint(saved_connector, "qah_q_0203")).key
    assert confirm_key == job_key
    assert "id:c-dst" in confirm_key


def test_rebaseline_replaces_the_baseline_the_next_append_reads(saved_connector, monkeypatch):
    import services.audit_log as audit_log

    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: kw)
    target = ac.resolve_target(_job_destination())
    # The failed append's baseline: the 3-row overwrite result.
    ac.save_baseline(target.key, {"rows": 3, "checksum": "12adc22d" * 8, "algo": "engine_v1"}, "6ae434dd")
    current = {"rows": 5, "checksum": "223b53d3" * 8, "algo": "engine_v1"}
    monkeypatch.setattr(ac, "fingerprint", lambda target, algo=None: dict(current))

    from src.routers.copilot_router import _saved_connector_endpoint

    out = ac.rebaseline(
        _saved_connector_endpoint(saved_connector, "qah_q_0203"),
        actor="qa", reason="D204: accepted the 5-row destination",
    )
    assert out["previous"]["rows"] == 3, "the rebaseline must see the baseline the append failed on"
    # The next append compares against the accepted 5-row state and passes.
    baseline = ac.load_baseline(target.key)
    assert baseline["rows"] == 5 and baseline["checksum"] == current["checksum"]
    assert ac.compare("qah_q_0203", baseline, current) is None


def test_rebaseline_covers_the_host_spelling_of_the_same_table(saved_connector, monkeypatch):
    """A job started from a raw host keys on host:port; a rebaseline through the
    saved connector must still find and replace that baseline."""
    import services.audit_log as audit_log

    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: kw)
    target = ac.resolve_target(_job_destination())
    assert target.alt_keys and "host:10.0.0.2:5432" in target.alt_keys[0]
    host_key = target.alt_keys[0]
    ac.save_baseline(host_key, {"rows": 3, "checksum": "a" * 64, "algo": "engine_v1"}, "job-raw-host")
    current = {"rows": 5, "checksum": "b" * 64, "algo": "engine_v1"}
    monkeypatch.setattr(ac, "fingerprint", lambda target, algo=None: dict(current))

    out = ac.rebaseline(_job_destination(), actor="qa", reason="accept")
    assert out["previous"]["job_id"] == "job-raw-host"
    for key in target.all_keys:
        assert ac.load_baseline(key)["rows"] == 5


def test_rebaseline_removal_clears_every_spelling(saved_connector, monkeypatch):
    import services.audit_log as audit_log

    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: kw)
    target = ac.resolve_target(_job_destination())
    for key in target.all_keys:
        ac.save_baseline(key, {"rows": 3, "checksum": "a" * 64, "algo": "engine_v1"}, "j")
    monkeypatch.setattr(ac, "fingerprint", lambda target, algo=None: None)
    monkeypatch.setattr(ac, "destination_missing", lambda target: True)
    ac.rebaseline(_job_destination(), actor="qa", reason="table dropped on purpose")
    assert all(ac.load_baseline(k) is None for k in target.all_keys)
