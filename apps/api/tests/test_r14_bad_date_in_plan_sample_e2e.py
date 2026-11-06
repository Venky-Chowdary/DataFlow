"""QA browser 3: a bad date inside the plan sample is a quarantined row, not a refused plan.

A CSV whose row 642 held ``31/02/2025`` looped Studio back to Source with a
type mismatch. Round 11 made a non-calendar date *beyond* the sample a row
holdout; inside the sample, balanced mode still refused the whole plan
(the commit says so): the cell read as text, demoted the column to VARCHAR,
and three gates (fidelity collapse VARCHAR->DATE, schema drift type_change,
the dry-run reconciliation) refused it.

The line pinned here (SFTP CSV, through MCP start_transfer -> confirm -> job,
into live PostgreSQL — an existing DATE table and the table a first run
creates):

* balanced + an invalid-calendar cell in the sample: the plan passes, every
  good row lands exactly, exactly the bad rows are quarantined, each with its
  "not a real calendar date" reason in the quarantine view Studio opens;
* balanced + a non-date word in the sample: still refused (only the
  invalid-calendar rule is a holdout);
* strict + an invalid-calendar cell in the sample: still refused.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.test_r5_bad_date_in_file_e2e import (  # noqa: F401
    BAD_DATES,
    _assert_held_out_exactly,
    _csv,
    _job,
    _quarantined_values,
    _rows,
    _start,
    dest_table,
    sftp,
)

pytestmark = pytest.mark.timeout(600)

IN_SAMPLE = {2: "2024-02-30", 7: "2025-02-31", **BAD_DATES}


@pytest.mark.parametrize("existing", [True, False], ids=["existing_date_table", "created_table"])
def test_balanced_holds_out_an_invalid_date_inside_the_sample(sftp, dest_table, existing):  # noqa: F811
    from src.main import app

    table = dest_table("postgresql", "date" if existing else None)
    staged = _start(sftp(_csv("date", IN_SAMPLE)), "postgresql", table, mode="balanced")
    assert staged.success, staged.error
    job = _job(staged)
    if existing:
        _assert_held_out_exactly("postgresql", table, "date", IN_SAMPLE, job)
    else:
        assert job.get("status") == "completed_with_quarantine", job.get("error")
        assert int(job.get("rejected_rows") or 0) == len(IN_SAMPLE)
        assert sorted(_rows("postgresql", table)) == [
            i for i in range(1, 5001) if i not in IN_SAMPLE
        ]
    for value in IN_SAMPLE.values():
        assert "not a real" in _quarantined_values("postgresql", table)[value]

    # The reason reaches the quarantine view Studio's Open quarantine loads.
    job_id = job.get("job_id") or job.get("id") or job.get("_id")
    assert job_id, sorted(job)
    with TestClient(app) as client:
        body = client.get(f"/api/v1/connectors/jobs/{job_id}/quarantine").json()
    reasons = " ".join(str(r.get("reason") or "") for r in body.get("quarantine") or [])
    assert "2024-02-30" in reasons and "not a real calendar date" in reasons, body


def test_balanced_still_refuses_a_word_in_a_date_column_inside_the_sample(sftp, dest_table):  # noqa: F811
    table = dest_table("postgresql", "date")
    staged = _start(sftp(_csv("date", {2: "next tuesday", **BAD_DATES})), "postgresql", table, mode="balanced")
    assert not staged.success
    assert "next tuesday" in str(staged.error), staged.error


def test_strict_still_refuses_an_invalid_date_inside_the_sample(sftp, dest_table):  # noqa: F811
    table = dest_table("postgresql", "date")
    staged = _start(sftp(_csv("date", {2: "2024-02-30"})), "postgresql", table, mode="strict")
    assert not staged.success
    assert "2024-02-30" in str(staged.error), staged.error


# ------------------------------------------- stored upload, strict (QA row 642)

def _row642_csv():
    import datetime as dt

    lines, rows = ["id,order_date,amount"], []
    for i in range(1, 1001):
        d = (dt.date(2025, 1, 1) + dt.timedelta(days=i % 300)).strftime("%d/%m/%Y")
        if i == 642:
            d = "31/02/2025"
        lines.append(f"{i},{d},{i}.25")
        rows.append({"id": str(i), "order_date": d, "amount": f"{i}.25"})
    return ("\n".join(lines) + "\n").encode(), rows


@pytest.fixture
def studio(tmp_path, monkeypatch):
    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(tmp_path / "up"))
    monkeypatch.delenv("DATAFLOW_SEED_DEMO", raising=False)
    from src.main import app

    with TestClient(app) as c:
        yield c, h


def test_strict_names_row_642_and_offers_hold_out_not_only_review_mappings(studio):
    """Strict Validate on the stored upload names the cell; explain offers the real routes."""
    import uuid

    client, h = studio
    csv, rows = _row642_csv()
    up = client.post("/api/v1/connectors/upload", files={"file": ("orders.csv", csv, "text/csv")}).json()
    schema = up["schema"]
    if not up.get("file_id"):
        # The upload route stores the file only with an object store; the
        # Studio preflight reads the same stored upload by id.
        from services.file_parser import store_upload

        up["file_id"] = store_upload("orders.csv", csv)["file_id"]
    maps = [{"source": k, "target": k, "confidence": 0.99, "source_type": "VARCHAR", "target_type": v}
            for k, v in schema.items()]
    body = {
        "columns": up["columns"], "column_types": schema, "row_count": 1000, "mappings": maps,
        "dest_type": "postgresql", "dest_host": h.PG["host"], "dest_port": h.PG["port"],
        "dest_database": h.PG["database"], "dest_username": h.PG["username"],
        "dest_password": h.PG["password"], "dest_schema": "public",
        "dest_table": f"r14b3_{uuid.uuid4().hex[:6]}", "sample_rows": rows,
        "sync_mode": "full_refresh_overwrite", "validation_mode": "strict",
        "source_kind": "file", "source_type": "csv", "source_file_id": up.get("file_id") or "",
    }
    pf = client.post("/api/v1/preflight/run", json=body).json()
    assert "31/02/2025" in str(pf.get("blockers")), pf.get("blockers")

    actions = client.post("/api/v1/preflight/explain", json={"preflight": pf}).json()["suggested_actions"]
    hold = [a for a in actions if a.get("kind") == "quarantine_and_rerun"]
    assert hold, actions
    assert actions[0]["kind"] == "quarantine_and_rerun", actions
    assert hold[0]["column"] == "order_date" and 642 in hold[0]["rows"], hold[0]
    assert "31/02/2025" in hold[0]["values"] and hold[0]["reason"], hold[0]
    assert hold[0]["risk_acceptance"]["on_cast_failure"] == "quarantine"

    from services.quarantine_from_preflight import quarantine_rows_from_preflight

    cells = [r for r in quarantine_rows_from_preflight(pf) if r.get("row") == 642]
    assert cells, quarantine_rows_from_preflight(pf)
    assert cells[0].get("value") == "31/02/2025" and cells[0].get("column") == "order_date", cells[0]
    assert str(cells[0].get("reason") or "").strip(), cells[0]
