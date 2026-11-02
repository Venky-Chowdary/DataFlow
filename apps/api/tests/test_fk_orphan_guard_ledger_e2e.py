"""Write-path FK orphan guard — the ledger must close exactly.

Children whose parent is absent at the destination are quarantined, never
written. Each orphan must be counted once, as a reject: not also as a
"shaped out" removal (a correct run looked lossy), and — when an entire page
is orphans — not dropped by the empty-page early return (silent loss).
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _db(path: Path, parents: list[int], children: list[tuple[int, int]] | None) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany("INSERT INTO parent VALUES (?, ?)", [(p, f"p{p}") for p in parents])
        if children is not None:
            conn.execute(
                "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER "
                "REFERENCES parent(id), v TEXT)"
            )
            conn.executemany(
                "INSERT INTO child VALUES (?, ?, ?)", [(c, p, f"v{c}") for c, p in children]
            )
    conn.close()


def _child_ids(path: Path) -> list[int]:
    conn = sqlite3.connect(str(path))
    try:
        return [r[0] for r in conn.execute("SELECT id FROM child ORDER BY id")]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


@pytest.mark.parametrize(
    "children,expected_orphans",
    [
        ([(i, (i % 3) + 1) for i in range(1, 31)], 10),  # parent 3 missing at dest
        ([(i, 99) for i in range(1, 21)], 20),  # every row on the page is an orphan
    ],
    ids=["mixed_page", "all_orphan_page"],
)
def test_orphans_are_quarantined_once_and_the_ledger_closes(
    tmp_path: Path, monkeypatch, children, expected_orphans
):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    persisted: list[dict] = []
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(
        dlq,
        "persist_rejected_rows",
        lambda **kw: persisted.extend(kw.get("rejected_details") or []) or len(kw.get("rejected_details") or []),
    )
    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _db(src, parents=[1, 2, 3, 99], children=children)
    _db(dst, parents=[1, 2], children=None)

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="child"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="child"),
        sync_mode="full_refresh_append",
        skip_preflight=True,
        validation_mode="balanced",
    )
    job_id = "fkg" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    summary = result.destination_summary or {}

    landed = _child_ids(dst)
    total = len(children)
    assert len(landed) == total - expected_orphans, (landed, result.error)
    orphan_details = [
        d for d in persisted if d.get("error_class") == "orphan_foreign_key"
    ] or [
        d for d in (summary.get("rejected_details") or [])
        if d.get("error_class") == "orphan_foreign_key"
    ]
    assert len(orphan_details) == expected_orphans, "orphans missing from quarantine"
    if expected_orphans == total:
        # Nothing could land: a failed run that names the cause, rows in the DLQ.
        assert not result.success
        assert "Orphan foreign key" in (result.error or "")
        return
    assert result.success, result.error
    assert int(summary.get("rejected_rows") or 0) == expected_orphans, summary
    assert int(summary.get("rows_shaped_out") or 0) == 0, (
        "orphans double-counted as recipe removals"
    )
    assert (result.reconciliation or {}).get("passed") is True, result.reconciliation
