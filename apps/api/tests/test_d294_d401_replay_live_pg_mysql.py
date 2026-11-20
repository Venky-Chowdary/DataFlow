"""D294 + D401 live: quarantine replay writes the edited cells, never a mask.

Story pair: PostgreSQL -> existing MariaDB/MySQL (and PostgreSQL), append,
balanced. Three source rows are held out (qty 'NOTANUMBER', d '2024-13-45',
qty 99999999999). ``name`` is a PII column, so the job document shows it
masked ('BA***TY'); the DLQ keeps the raw cell.

The operator replays in either payload shape the replay accepts:

* flat     ``rows=[{row, id, name, qty, d}]``           (D294 AC-1)
* record   ``rows=[{source_pk, pk_value, values:{...}}]`` (D294 AC-2)

and either sends only the cells they changed, or echoes the whole preview row
(masked ``name`` included) with the bad cell fixed. Every case must land the
edited value, keep the raw unedited cells ('BAD_QTY', not 'BA***TY'), and
close the ledger (D401 AC-1).
"""

from __future__ import annotations

import asyncio
import importlib
import os
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.r13_mapping_harness import (  # noqa: E402
    drop,
    endpoint,
    isolated,  # noqa: F401 - fixture
    quote,
    run_sql,
)

SOURCE = [
    (11, "alice", "10", "2024-01-01"),
    (12, "BAD_QTY", "NOTANUMBER", "2024-03-01"),
    (13, "bob", "20", "2024-02-01"),
    (14, "BAD_DATE", "40", "2024-13-45"),
    (15, "BAD_BIG", "99999999999", "2024-06-01"),
]
FIXES = {12: {"qty": "30"}, 14: {"d": "2024-04-30"}, 15: {"qty": "60"}}
DEST_DDL = {
    "postgresql": "CREATE TABLE {t} (id INT PRIMARY KEY, name VARCHAR(20), qty INT, d DATE)",
    "mysql": "CREATE TABLE {t} (id INT PRIMARY KEY, name VARCHAR(20), qty INT, d DATE)",
}


def _request():
    class _State:
        pass

    class _Req:
        state = _State()
        headers: dict = {}

    return _Req()


def _preview_rows(job: dict) -> dict[int, dict]:
    """What the operator sees: the job document's (masked) findings by id."""
    out: dict[int, dict] = {}
    for d in job.get("rejected_details") or []:
        cells = d.get("source_values") or d.get("values") or {}
        out[int(cells["id"])] = {"row": d.get("row"), "cells": dict(cells)}
    return out


def _payload(shape: str, echo: bool, preview: dict[int, dict]) -> list[dict]:
    rows = []
    for pk, fix in FIXES.items():
        seen = preview[pk]
        cells = {**seen["cells"], **fix} if echo else {"id": seen["cells"]["id"], **fix}
        if shape == "flat":
            rows.append({"row": seen["row"], **cells})
        else:
            rows.append({"source_pk": "id", "pk_value": pk, "values": cells})
    return rows


@pytest.fixture(params=["postgresql", "mysql"])
def parent_job(request, tmp_path, isolated, monkeypatch):  # noqa: F811
    import services.quarantine_dlq as dlq
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest, transfer_request_to_dict

    dest_kind = request.param
    endpoint(dest_kind, "probe")  # skips when the engine is not configured
    endpoint("postgresql", "probe")
    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    router = importlib.import_module("src.routers.connectors_router")
    monkeypatch.setattr(router, "get_mongodb_service", lambda: isolated)

    tag = uuid.uuid4().hex[:8]
    src_table, dst_table = f"d294s_{tag}", f"d294d_{tag}"
    run_sql(
        "postgresql",
        f'CREATE TABLE "{src_table}" (id INT PRIMARY KEY, name TEXT, qty TEXT, d TEXT)',
    )
    run_sql(
        "postgresql",
        f'INSERT INTO "{src_table}" VALUES (%s, %s, %s, %s)',  # nosec B608
        params=SOURCE,
    )
    run_sql(dest_kind, DEST_DDL[dest_kind].format(t=quote(dest_kind, dst_table)))
    req = TransferRequest(
        source=endpoint("postgresql", src_table),
        destination=endpoint(dest_kind, dst_table),
        mappings=[{"source": c, "target": c} for c in ("id", "name", "qty", "d")],
        sync_mode="full_refresh_append",
        validation_mode="balanced",
        skip_preflight=True,
    )
    job_id = "d294l" + uuid.uuid4().hex[:16]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    try:
        result = UniversalTransferEngine().execute_tracked(req, job_id)
        assert result.success, result.error
        job = isolated.get_job(job_id)
        assert job["status"] == "completed_with_quarantine", job["status"]
        isolated.update_job_fields(
            job_id,
            {"id": job_id, "job_id": job_id, "transfer_request": transfer_request_to_dict(req)},
        )
        yield dest_kind, job_id, dst_table, isolated.get_job(job_id)
    finally:
        drop("postgresql", src_table)
        drop(dest_kind, dst_table)


@pytest.mark.parametrize("echo", [False, True], ids=["edits_only", "echo_preview"])
@pytest.mark.parametrize("shape", ["flat", "record"])
def test_replay_lands_the_edits_and_the_raw_cells(parent_job, shape, echo):
    from services.quarantine_dlq import canonical_replay_details, open_quarantine_details
    from src.routers.connectors_router import QuarantineReplayRequest, replay_job_quarantine

    dest_kind, job_id, table, job = parent_job
    preview = _preview_rows(job)
    assert sorted(preview) == sorted(FIXES), preview
    # The job document is masked for display; the replay must not need it raw.
    assert preview[12]["cells"]["name"] == "BA***TY", preview[12]

    rows = _payload(shape, echo, preview)
    resp = asyncio.run(
        replay_job_quarantine(job_id, QuarantineReplayRequest(rows=rows), _request())
    )
    assert resp.get("rows_written") == 3 and not resp.get("rejected"), resp

    got = {
        int(r[0]): (r[1], int(r[2]), str(r[3]))
        for r in run_sql(  # nosec B608
            dest_kind, f"SELECT id, name, qty, d FROM {quote(dest_kind, table)}"
        )
    }
    want = {}
    for pk, name, qty, d in SOURCE:
        fix = FIXES.get(pk, {})
        want[pk] = (name, int(fix.get("qty", qty)), fix.get("d", d))
    assert got == want, got
    assert open_quarantine_details(canonical_replay_details({"id": job_id})) == []


def test_flat_echo_keeps_the_raw_cell_and_a_mask_with_nothing_raw_is_refused():
    """Unit form of the flat echo leg (no engine needed)."""
    from services.quarantine_dlq import MaskedQuarantineReplayError, overlay_cell_edit

    stored = {
        "row": 2,
        "column": "qty",
        "value": "NOTANUMBER",
        "values": {"id": "12", "name": "BAD_QTY", "qty": "NOTANUMBER"},
        "source_values": {"id": "12", "name": "BAD_QTY", "qty": "NOTANUMBER"},
    }
    merged = overlay_cell_edit({"row": 2, "id": "12", "name": "BA***TY", "qty": "30"}, [stored])
    assert merged["source_values"] == {"id": "12", "name": "BAD_QTY", "qty": "30"}
    assert merged["value"] == "30"
    masked_only = {**stored, "values": {**stored["values"], "name": "BA***TY"}}
    masked_only["source_values"] = dict(masked_only["values"])
    with pytest.raises(MaskedQuarantineReplayError, match="ledger stays open"):
        overlay_cell_edit({"row": 2, "id": "12", "name": "BA***TY", "qty": "30"}, [masked_only])
