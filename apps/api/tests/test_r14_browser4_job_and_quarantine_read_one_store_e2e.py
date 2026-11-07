"""QA browser 4 — 998 written / 2 rejected: the job page and the quarantine
view must read the same store.

The two rejected rows were held out by the run (a transform divert), so they
live in the control-plane quarantine DLQ, not in the job document's
``rejected_details`` sample. The job page counted ``rejected_rows=2``, opened
the quarantine panel, and the panel's fetch answered HTTP 500 (a divert
finding carries its record in ``row`` — an unhashable dict) or labelled the
rows "preflight", so "Open quarantine" showed nothing and the page said no
rows were quarantined.

Now ``GET /connectors/jobs/{id}`` hydrates its findings from the same reader
the quarantine view uses, and both answer 2 rows with row, column, value and
reason. Live PostgreSQL -> PostgreSQL, run twice into the table run 1 created.
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests import r4_route_harness as h
from tests import test_r12_snowflake_transform_e2e as sfmod

pytestmark = pytest.mark.timeout(600)

BAD = {642: "abc", 917: "3/0"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    # services.config parses this flag as a boolean; '' is not one.
    monkeypatch.delenv("DATAFLOW_SEED_DEMO", raising=False)
    from fastapi.testclient import TestClient
    from src.main import app

    return TestClient(app)


def _rows() -> list[tuple]:
    out = []
    for i in range(1, 1001):
        ratio = BAD.get(i, str(i % 97) if i % 50 else None)
        label = {7: None, 8: " ", 9: "Zoë 東京 ✓"}.get(i, f"r{i}")
        out.append((i, ratio, label))
    return out


def _run(client, src: str, dst: str) -> dict:
    data = {
        "source_kind": "database", "dest_kind": "database",
        "source_table": src, "dest_table": dst, "async_mode": "false",
        "validation_mode": "balanced", "sync_mode": "full_refresh_overwrite",
        "source_format": "postgresql", "source_schema": "public",
        "dest_format": "postgresql", "dest_schema": "public",
        **sfmod._side("source", sfmod.PG_ENDPOINT), **sfmod._side("dest", sfmod.PG_ENDPOINT),
        "shape_recipe_json": (
            '{"steps": [{"op": "cast_column", "column": "ratio", '
            '"options": {"to_type": "integer"}, "on_error": "divert"}]}'
        ),
    }
    from services.shape_models import ShapeRecipe

    data["approved_shape_recipe_hash"] = ShapeRecipe.parse(json.loads(data["shape_recipe_json"])).recipe_hash
    resp = client.post("/api/v1/transfer/run", data=data)
    assert resp.status_code == 200, resp.text[:1500]
    return h.wait_job(str(resp.json().get("job_id") or ""))


def test_998_written_2_rejected_job_page_and_quarantine_agree(client):
    src = sfmod._seed_pg("id integer PRIMARY KEY, ratio text, label text", _rows())
    dst = f"r14b4_{uuid.uuid4().hex[:6]}"
    try:
        for run in (1, 2):
            job = _run(client, src, dst)
            job_id = str(job.get("job_id") or job.get("_id") or "")
            assert job.get("status") == "completed_with_quarantine", (run, job.get("status"), job.get("error"))
            count = h.query("postgresql", f'SELECT count(*) FROM public."{dst}"')[0][0]  # nosec B608
            assert count == 998, (run, count)

            page = client.get(f"/api/v1/connectors/jobs/{job_id}")
            assert page.status_code == 200, page.text[:500]
            page_body = page.json()
            summary = page_body.get("destination_summary") or {}
            assert int(page_body.get("rejected_rows") or summary.get("rejected_rows") or 0) == 2
            page_rows = {int(d["row"]["id"]): d for d in page_body.get("rejected_details") or []}

            quarantine = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine")
            assert quarantine.status_code == 200, quarantine.text[:800]
            q = quarantine.json()
            assert q["rejected_rows"] == 2 and q["finding_rows"] == 2 and q["open_count"] == 2, q
            assert q["source"] == "write", q["source"]
            q_rows = {int(d["row"]["id"]): d for d in q["quarantine"]}

            assert set(page_rows) == set(q_rows) == set(BAD), (page_rows.keys(), q_rows.keys())
            for rid, value in BAD.items():
                for row in (page_rows[rid], q_rows[rid]):
                    assert row["column"] == "ratio" and row["value"] == value, row
                    assert "is not a number" in row["reason"], row
    finally:
        h.drop_table("postgresql", src)
        h.drop_table("postgresql", dst)
