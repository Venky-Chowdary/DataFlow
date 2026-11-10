"""D294: replay_quarantine writes the operator's edited cells, not the originals.

Repro (QA, Postgres -> existing MariaDB, append, balanced): three rows are
held out (qty 'NOTANUMBER', d '2024-13-45', qty 99999999999). Replaying them
with edited rows staged ``edited_rows: 3`` but the replay re-sent the original
bad cells and quarantined every row again:

* a flat edit ``{row, id, name, qty, d}`` bound to its finding, but its
  top-level cells were never applied (only ``{column, value}`` and the
  ``values`` bag were read);
* an edit that reached the ``values`` bag was shadowed by the stored
  ``source_values`` bag (the replay reads that first) and by the finding's
  own ``value`` cell, which the replay puts on top of the row;
* a quarantine-record edit ``{source_pk: 'id', pk_value, values}`` matched no
  finding (no row number, ``source_pk`` names the column), so the edits were
  replayed as bare scraps and collapsed into one row: two of three were lost
  while the replay reported success.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import sqlite3
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.r13_mapping_harness import (  # noqa: E402
    drop,
    endpoint,
    isolated,  # noqa: F401 - fixture
    run_sql,
    sqlite_endpoint,
)

SOURCE = [
    (1, "a", "10", "2024-01-01"),
    (2, "b", "20", "2024-02-01"),
    (3, "c", "NOTANUMBER", "2024-03-01"),
    (4, "d", "40", "2024-13-45"),
    (5, "e", "50", "2024-05-01"),
    (6, "f", "99999999999", "2024-06-01"),
]
EDITED = {
    3: {"id": 3, "name": "c", "qty": "30", "d": "2024-03-01"},
    4: {"id": 4, "name": "d", "qty": "40", "d": "2024-04-30"},
    6: {"id": 6, "name": "f", "qty": "60", "d": "2024-06-01"},
}


def _flat() -> list[dict]:
    return [{"row": n, **cells} for n, cells in EDITED.items()]


def _record_named_pk() -> list[dict]:
    return [
        {"source_pk": "id", "pk_value": cells["id"], "values": dict(cells)}
        for cells in EDITED.values()
    ]


def _record_stored_pk() -> list[dict]:
    return [
        {
            "source_pk": str(n),
            "pk_value": {"id": str(n)},
            "values": {k: str(v) for k, v in cells.items()},
        }
        for n, cells in EDITED.items()
    ]


# ---------------------------------------------------------------------------
# Unit: the replay row is built from the edit, whatever bag held the original
# ---------------------------------------------------------------------------


def _stored(n: int, column: str, value: str) -> dict:
    row = {"id": str(n), "name": "*", "qty": "1", "d": "2024-01-01", column: value}
    return {
        "row": n,
        "column": column,
        "value": value,
        "values": dict(row),
        "source_values": dict(row),
        "pk_value": {"id": str(n)},
        "source_pk": str(n),
        "source_pk_proven": True,
    }


@pytest.mark.parametrize("shape", [_flat, _record_named_pk, _record_stored_pk])
def test_replay_records_carry_the_edited_cells(shape):
    from services.quarantine_dlq import overlay_cell_edit
    from src.routers.connectors_router import _quarantine_details_to_records

    durable = [
        _stored(3, "qty", "NOTANUMBER"),
        _stored(4, "d", "2024-13-45"),
        _stored(6, "qty", "99999999999"),
    ]
    details = [overlay_cell_edit(d, durable) for d in shape()]
    records, _ = _quarantine_details_to_records(details)
    got = {int(r["id"]): (r["qty"], r["d"], r["name"]) for r in records}
    assert got == {
        n: (str(c["qty"]), c["d"], c["name"]) for n, c in EDITED.items()
    }, got


def test_echoed_finding_with_only_values_edited_replays_the_edit():
    """The UI echoes the whole finding back; only ``values.qty`` changed.
    The stale ``source_values`` and ``value`` must not win over the edit."""
    from services.quarantine_dlq import overlay_cell_edit
    from src.routers.connectors_router import _quarantine_details_to_records

    stored = _stored(3, "qty", "NOTANUMBER")
    posted = {**stored, "values": {**stored["values"], "qty": "30"}}
    records, _ = _quarantine_details_to_records([overlay_cell_edit(posted, [stored])])
    assert records[0]["qty"] == "30", records


# ---------------------------------------------------------------------------
# End to end: engine run into an existing MySQL table, then the replay endpoint
# ---------------------------------------------------------------------------


def _request():
    class _State:
        pass

    class _Req:
        state = _State()
        headers: dict = {}

    return _Req()


@pytest.fixture
def parent_job(tmp_path, isolated, monkeypatch):  # noqa: F811
    import services.quarantine_dlq as dlq
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest, transfer_request_to_dict

    endpoint("mysql", "probe")  # skips when MySQL is not configured
    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    router = importlib.import_module("src.routers.connectors_router")
    monkeypatch.setattr(router, "get_mongodb_service", lambda: isolated)

    src = tmp_path / "src.db"
    conn = sqlite3.connect(str(src))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, qty TEXT, d TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", SOURCE)
    conn.commit()
    conn.close()
    table = f"d294_{uuid.uuid4().hex[:8]}"
    run_sql("mysql", f"CREATE TABLE `{table}` (id INT PRIMARY KEY, name VARCHAR(20), qty INT, d DATE)")
    req = TransferRequest(
        source=sqlite_endpoint(src),
        destination=endpoint("mysql", table),
        mappings=[{"source": c, "target": c} for c in ("id", "name", "qty", "d")],
        sync_mode="full_refresh_append",
        validation_mode="balanced",
        skip_preflight=True,
    )
    job_id = "d294" + uuid.uuid4().hex[:16]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    try:
        assert result.success, result.error
        job = isolated.get_job(job_id)
        assert job["status"] == "completed_with_quarantine", job["status"]
        assert int(job.get("rejected_rows") or 0) == 3
        isolated.update_job_fields(
            job_id,
            {"id": job_id, "job_id": job_id, "transfer_request": transfer_request_to_dict(req)},
        )
        yield job_id, table
    finally:
        drop("mysql", table)


@pytest.mark.parametrize("shape", [_flat, _record_named_pk, _record_stored_pk])
def test_edited_replay_writes_the_edits_into_mysql(parent_job, shape):
    from services.quarantine_dlq import canonical_replay_details, open_quarantine_details
    from src.routers.connectors_router import QuarantineReplayRequest, replay_job_quarantine

    job_id, table = parent_job
    resp = asyncio.run(
        replay_job_quarantine(job_id, QuarantineReplayRequest(rows=shape()), _request())
    )
    assert resp.get("rows_written") == 3 and not resp.get("rejected"), resp
    got = {
        int(r[0]): (r[1], int(r[2]), str(r[3]))
        for r in run_sql("mysql", f"SELECT id, name, qty, d FROM `{table}`")  # nosec B608
    }
    want = {n: (n_, int(q), d) for n, n_, q, d in SOURCE if n not in EDITED}
    want.update({n: (c["name"], int(c["qty"]), c["d"]) for n, c in EDITED.items()})
    assert got == want, got
    assert open_quarantine_details(canonical_replay_details({"id": job_id})) == []


def test_invalid_edit_is_reported_with_the_edited_value(parent_job):
    from fastapi import HTTPException

    from src.routers.connectors_router import QuarantineReplayRequest, replay_job_quarantine

    job_id, table = parent_job
    rows = [{"row": 3, **EDITED[3], "qty": "STILLBAD"}]
    try:
        resp = asyncio.run(
            replay_job_quarantine(job_id, QuarantineReplayRequest(rows=rows), _request())
        )
    except HTTPException as exc:
        detail = str(exc.detail)
    else:
        assert not resp.get("rows_written"), resp
        detail = str(resp)
    assert "STILLBAD" in detail and "NOTANUMBER" not in detail, detail
