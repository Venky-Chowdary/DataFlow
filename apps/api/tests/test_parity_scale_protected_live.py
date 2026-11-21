"""Parity at Scale phase 3: protected values never reach the job document or the job API (review defect 5).

A column the run masks, hashes, redacts or omits is protected. The scan's
state is checkpointed into the job document and its evidence is kept there;
neither may hold a protected source value (judge examples, keyless content
examples, causes), and a protected key is never stored at all. The job API
serves neither checkpoints nor evidence bodies.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parity_engines  # noqa: E402
from test_parity_scale_verification_live import N, _endpoint, _env, _sql  # noqa: E402,F401

pytestmark = pytest.mark.skipif(not parity_engines.available("postgresql"),
                                reason="PostgreSQL not reachable (DATAFLOW_PG_TEST_DSN)")
SECRET = "secret-person"


def _masked(value: str) -> str:
    from services.reconciliation import apply_transform

    out, err = apply_transform(value, "mask_pii")
    assert err is None
    return str(out)


@pytest.fixture()
def pii_tables():
    import uuid

    src, dst = f"psd1_p_s_{uuid.uuid4().hex[:8]}", f"psd1_p_d_{uuid.uuid4().hex[:8]}"
    ddl = "id bigint, name varchar(80), amt numeric(12,2)"
    _sql(f'CREATE TABLE "{src}" ({ddl})',
         f"INSERT INTO \"{src}\" SELECT g, '{SECRET}-' || g, g * 1.25 FROM generate_series(1, {N}) g",
         f'CREATE TABLE "{dst}" ({ddl})')
    conn = parity_engines.connect("postgresql")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'SELECT id, name, amt FROM "{src}"')
        rows = [(i, _masked(n), a) for i, n, a in cur.fetchall()]
        cur.executemany(f'INSERT INTO "{dst}" VALUES (%s, %s, %s)', rows)
    conn.close()
    yield src, dst
    _sql(f'DROP TABLE IF EXISTS "{src}"', f'DROP TABLE IF EXISTS "{dst}"')


def _job(src: str, dst: str, *, key: bool = True) -> str:
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service().create_transfer_job({
        "status": "completed",
        "transfer_request": {
            "source": _endpoint(src), "destination": _endpoint(dst),
            "mappings": [{"source": "id", "target": "id"},
                         {"source": "name", "target": "name", "transform": "mask_pii"},
                         {"source": "amt", "target": "amt"}],
            "sync_mode": "full_refresh_overwrite",
        },
        "destination_summary": {"table": dst, **({"primary_key_columns": ["id"]} if key else {})},
        "reconciliation": {"passed": True, "source_rows": N, "target_rows": N, "phase": "post_write",
                           "coverage_limit": {"rows": N, "limit": 100},
                           "verification_verdict_qualifier": "coverage_limited"},
    })


def _stored(job_id: str) -> str:
    from services.mongodb_service import get_mongodb_service

    return json.dumps(get_mongodb_service().get_job(job_id), default=str)


def test_no_protected_value_is_stored_or_served(pii_tables) -> None:
    from services.mongodb_service import get_mongodb_service
    from services.parity_scale import explorer
    from services.parity_scale.verification import start_for_job

    from src.transfer.models import sanitize_job_for_api

    src, dst = pii_tables
    # Row 7: the protected cell and an ordinary cell differ; row 9: only the protected cell.
    _sql(f"UPDATE \"{dst}\" SET name = 'masked-wrong', amt = 0 WHERE id = 7",
         f"UPDATE \"{dst}\" SET name = 'masked-wrong-too' WHERE id = 9")
    job_id = _job(src, dst)
    block = start_for_job(job_id, background=False)
    assert block["state"] == "failed", block
    job = get_mongodb_service().get_job(job_id)
    evidence = job["parity_scale"]["tables"][0]["evidence"]
    assert evidence["differences"]["by_column"]["name"]  # the protected column is named, with its count
    stored = _stored(job_id)
    assert SECRET not in stored  # no raw source value of the protected column anywhere on the job
    values = explorer.values(job, dst)
    assert [r["key"] for r in values["rows"]] == [["7"], ["9"]]
    for row in values["rows"]:
        name = next(c for c in row["cells"] if c["column"] == "name")
        assert name["source"] == name["destination"] == "protected"
    served = sanitize_job_for_api(job)
    assert "parity_scale_checkpoints" not in served and "parity_scale_lease" not in served
    assert all("evidence" not in t for t in served["parity_scale"]["tables"])
    assert served["parity_scale"]["tables"][0]["outcome"] == "differences_found"
    assert SECRET not in json.dumps(served, default=str)


def test_keyless_content_examples_mask_protected_columns(pii_tables) -> None:
    from services.parity_scale.verification import start_for_job

    src, dst = pii_tables
    _sql(f'DELETE FROM "{dst}" WHERE id = 5')
    job_id = _job(src, dst, key=False)
    assert start_for_job(job_id, background=False)["state"] == "failed"
    stored = _stored(job_id)
    assert SECRET not in stored
    assert '"protected"' in stored  # the missing row's example is there, its protected cell masked


def test_a_protected_key_is_never_stored(monkeypatch) -> None:
    from services.parity_scale import verification
    from services.parity_scale.driver import ParityScan

    seen: dict[str, Any] = {}

    def start(plan, *, store, open_side, config, scan_id):
        seen["config"] = config
        raise RuntimeError("stop after the configuration")

    monkeypatch.setattr(verification, "plan_for", lambda spec: object())
    monkeypatch.setattr(ParityScan, "start", staticmethod(start))
    spec = {"name": "t", "protected": ["id"], "key_protected": True}
    with pytest.raises(RuntimeError):
        verification._run("job", spec, "psd1-x", None, False, "tok")
    config = seen["config"]
    assert config.keys_stored_cap == 0 and config.examples_cap == 0 and config.mode == "H"
    assert config.protected == ("id",)
