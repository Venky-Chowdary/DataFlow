"""W3 round 2: one release of ``legacy_code`` for API / MCP clients keyed on the old codes.

The taxonomy (services/error_taxonomy.py) replaced five job-failure codes.
Wherever a new code stands where an old one did, the envelope, the job's
humanised failure and the job record also carry ``legacy_code`` (deprecated,
announced in the OpenAPI description).
"""

from __future__ import annotations

import pytest

# Each old code, a failure that used to produce it, and the code that now does.
CASES = [
    ("destination_permission_denied", 'permission denied for table "orders"', "destination", "access.dest_write_denied"),
    ("destination_column_missing", "(1054, \"Unknown column 'email' in 'field list'\")", "destination", "object.column_not_found"),
    ("table_not_found", 'relation "railway.users" does not exist', "", "object.table_not_found"),
    ("destination_lock_timeout", "Lock wait timeout exceeded; try restarting transaction (1205)", "", "timeout.lock"),
    ("transfer_failed", "weird proprietary widget glitch 42", "", "unclassified"),
]


@pytest.mark.parametrize("legacy, raw, side, code", CASES)
def test_each_old_code_maps_to_its_taxonomy_code(legacy, raw, side, code):
    from services.error_handling import humanize_transfer_failure
    from services.error_taxonomy import LEGACY_CODES, classify

    assert LEGACY_CODES[code] == legacy
    human = humanize_transfer_failure(RuntimeError(raw), side=side)
    assert human["code"] == code, human
    assert human["legacy_code"] == legacy
    envelope = classify(RuntimeError(raw), side=side).envelope("ref-1")["error"]
    assert envelope["code"] == code and envelope["legacy_code"] == legacy


def test_a_new_cause_has_no_legacy_code():
    from services.error_taxonomy import classify

    envelope = classify(RuntimeError('null value in column "v" violates not-null constraint')).envelope("r")
    assert "legacy_code" not in envelope["error"]


def test_the_job_record_carries_the_legacy_code():
    from src.transfer.job_failure import _job_failure_fields

    details, extras = _job_failure_fields(RuntimeError('relation "orders" does not exist'))
    assert details["code"] == "object.table_not_found"
    assert details["legacy_code"] == "table_not_found"
    assert extras["error_legacy_code"] == "table_not_found"


def test_openapi_description_announces_the_deprecation():
    from src.main import app

    description = app.description
    assert "legacy_code" in description and "deprecated" in description.lower()
    for legacy, *_ in CASES:
        assert legacy in description


def test_a_failed_job_record_keeps_the_legacy_code_through_the_route(tmp_path, monkeypatch):
    """Execute against a missing source table: the stored job carries both codes."""
    import uuid

    from tests import r4_route_harness as h

    class _MP:
        def __init__(self, mp):
            self._mp = mp

        def setenv(self, k, v, *a, **kw):
            if k == "DATAFLOW_SEED_DEMO" and v == "":
                v = "false"
            return self._mp.setenv(k, v, *a, **kw)

        def __getattr__(self, n):
            return getattr(self._mp, n)

    h.isolate(tmp_path, _MP(monkeypatch))
    from fastapi.testclient import TestClient

    from services.connector_store import create_connector
    from services.mongodb_service import get_mongodb_service
    from src.main import app

    def conn(role):
        return create_connector({"name": f"w3err-lg-{uuid.uuid4().hex[:6]}", "role": role,
                                 "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})

    src, dst = conn("source"), conn("destination")
    missing = f"w3err_lg_missing_{uuid.uuid4().hex[:6]}"
    with TestClient(app) as client:
        r = client.post("/api/v1/transfer/execute", json={
            "source": {"kind": "database", "format": "postgresql", "connector_id": src.id, "table": missing},
            "destination": {"kind": "database", "format": "postgresql", "connector_id": dst.id,
                            "table": f"{missing}_dst"},
            "mappings": [{"source": "id", "target": "id", "confidence": 1.0}],
            "sync_mode": "append", "validation_mode": "strict", "async_mode": False, "skip_preflight": True,
            "stream_contracts": [{"name": missing, "selected": True, "sync_mode": "append", "primary_key": "id"}],
        })
    body = r.json()
    out = body.get("detail") if isinstance(body.get("detail"), dict) else body
    job = get_mongodb_service().get_job(str(out.get("job_id"))) or {}
    assert job.get("error_code") == "object.table_not_found", job.get("error_code")
    assert job.get("error_legacy_code") == "table_not_found"
    assert (job.get("error_details") or {}).get("legacy_code") == "table_not_found"
