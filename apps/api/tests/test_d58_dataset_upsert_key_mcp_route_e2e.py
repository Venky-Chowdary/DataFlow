"""D58 — an uploaded file upserts on the key the caller names, through MCP.

QA (production): ``start_dataset_transfer`` with sync_mode=upsert had no way to
name the key. The earlier fix added ``upsert_key`` to the schema only. Through
the real route the key still never reached preflight: the stage built its
stream contract into the job payload *after* preflight, so the sync-contract
gate saw no contract and refused every keyed file load ("Missing primary key —
no stream contract carries one").

Route: MCP ``tools/call start_dataset_transfer`` -> ``/api/v1/copilot/confirm``
-> job, twice, into PostgreSQL and MySQL 8. Run 1 creates the table, run 2
upserts a changed file into it. NULL, "", whitespace and unicode survive.
"""

from __future__ import annotations

import csv
import uuid

import pytest

from tests import r4_route_harness as h

# Live engines: a hung run fails instead of holding a session (pytest-timeout).
pytestmark = pytest.mark.timeout(300)

_RUN1 = [
    ["id", "region", "name", "note"],
    ["1", "EU", "Ada", "plain"],
    ["2", "EU", "Łukasz 山田", ""],
    ["3", "US", "", "   "],
]
_RUN2 = [
    ["id", "region", "name", "note"],
    ["1", "EU", "Ada", "plain"],
    ["2", "EU", "Zoë", "changed 🎉"],
    ["3", "US", "", "   "],
    ["4", "US", "new", "x"],
]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    h.use_upload_dir(monkeypatch, uploads)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c, uploads


def _write(path, rows) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)


def _norm(v):
    return None if v is None else str(v)


# MySQL refuses a composite key over a text column at preflight (g6, no prefix
# length) — a disclosed limit, not D58 — so its composite case is not run.
@pytest.mark.parametrize(
    "kind,key",
    [("postgresql", "id"), ("postgresql", "id, region"), ("mysql", "id")],
)
def test_file_upsert_on_named_key_runs_twice_without_duplicates(client, kind, key):
    from services.connector_store import create_connector

    c, uploads = client
    token = uuid.uuid4().hex[:8]
    dataset = f"people_{token}"
    table = f"d58_{kind[:5]}_{token}"
    dst = create_connector(
        {"name": f"d58-{kind}-{token}", "role": "destination", "workspace_id": "",
         "ssl": False, **h.dest_config(kind)}
    )
    args = {
        "dataset_name": dataset,
        "dest_connector_id": dst.id,
        "dest_table": table,
        "sync_mode": "upsert",
        "upsert_key": key,
    }
    try:
        _write(uploads / f"{dataset}.csv", _RUN1)
        h.stage_and_run(c, "start_dataset_transfer", args)
        _write(uploads / f"{dataset}.csv", _RUN2)
        h.stage_and_run(c, "start_dataset_transfer", args)

        q = h.quote
        rows = h.query(
            kind,
            f"SELECT {q(kind, 'id')}, {q(kind, 'region')}, {q(kind, 'name')}, "  # nosec B608
            f"{q(kind, 'note')} FROM {q(kind, table)}",
        )
        got = sorted((int(float(r[0])),) + tuple(_norm(v) for v in r[1:]) for r in rows)
        # An empty CSV cell is the file's NULL; whitespace and unicode are values.
        want = sorted(
            (int(r[0]),) + tuple(None if v == "" else v for v in r[1:]) for r in _RUN2[1:]
        )
        assert got == want
    finally:
        h.drop_table(kind, table)


# MySQL: a text key with no declared width is refused at g6 (CR-D126's family).
@pytest.mark.parametrize("kind", ["postgresql"])
def test_qa_sample_hr_upsert_on_emp_id(client, kind):
    """QA's exact call: start_dataset_transfer(sample_hr, upsert_key=emp_id)."""
    import json
    import shutil
    from pathlib import Path

    from services.connector_store import create_connector

    c, uploads = client
    shutil.copy(Path(__file__).resolve().parent / "fixtures" / "sample_hr.json", uploads / "sample_hr.json")
    token = uuid.uuid4().hex[:8]
    table = f"d58_hr_{kind[:5]}_{token}"
    dst = create_connector(
        {"name": f"d58hr-{kind}-{token}", "role": "destination", "workspace_id": "",
         "ssl": False, **h.dest_config(kind)}
    )
    args = {"dataset_name": "sample_hr", "dest_connector_id": dst.id, "dest_table": table,
            "sync_mode": "upsert", "upsert_key": "emp_id",
            # full_name is a PII column by name; the review is the operator's.
            "pii_acknowledgement": {"approved_by": "qa", "reason": "HR load approved"}}
    try:
        h.stage_and_run(c, "start_dataset_transfer", args)
        rows = json.loads((uploads / "sample_hr.json").read_text())
        rows[1]["full_name"] = "John Q. Doe"
        rows.append({"emp_id": "E006", "full_name": "Zoë Ångström", "dept_code": "ENG",
                     "hire_dt": "2023-02-01", "salary_amt": "90000"})
        (uploads / "sample_hr.json").write_text(json.dumps(rows), encoding="utf-8")
        h.stage_and_run(c, "start_dataset_transfer", args)
        q = h.quote
        got = dict(h.query(
            kind, f"SELECT {q(kind, 'emp_id')}, {q(kind, 'full_name')} FROM {q(kind, table)}"  # nosec B608
        ))
        assert got == {r["emp_id"]: r["full_name"] for r in rows}
    finally:
        h.drop_table(kind, table)
