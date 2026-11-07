"""D35 - a quarantined row's identity must not depend on which batch it was in.

The file stream numbered each rejected row within its own write batch
(``row`` 2 in batch 1, ``row`` 2 in batch 4), while the database stream already
rebased to the absolute source row. Replay identity falls back to the row
number when a finding carries no proven source key, so two different bad rows
from different batches shared ``row:2`` - promoting one closed both, and the
operator was told to look at a line that was not the bad one.

Real CSV / SQLite -> SQLite runs through the engine with a 5-row batch.
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
import src.transfer.file_stream as file_stream_mod  # noqa: E402
import src.transfer.stream as stream_mod  # noqa: E402
from services.quarantine_dlq import replay_row_identity  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

BAD_ROWS = (17, 23)  # 1-based data rows holding a non-integer age


@pytest.fixture()
def fake(monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    mongo = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: mongo)
    monkeypatch.setattr(file_stream_mod, "CHUNK_SIZE", 5)
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 5)
    return mongo


_MAPPINGS = [
    {"source": "age", "target": "age", "target_type": "INTEGER", "confidence": 0.9},
]


def _run(fake, source: EndpointConfig, dst: Path, **extra):
    request = TransferRequest(
        source=source,
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dst), table="t"
        ),
        mappings=_MAPPINGS,
        sync_mode="full_refresh_overwrite",
        validation_mode="balanced",
        skip_preflight=True,
        **extra,
    )
    job_id = uuid.uuid4().hex[:24]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    return sorted(
        (result.destination_summary or {}).get("rejected_details") or [],
        key=lambda d: int(d.get("row") or 0),
    )


def _ages() -> list[str]:
    return ["bad" if i in BAD_ROWS else str(i) for i in range(1, 26)]


def test_file_stream_numbers_rejects_by_absolute_source_row(fake, tmp_path):
    # No key column: the row number is the only identity the finding has.
    content = ("age\n" + "\n".join(_ages()) + "\n").encode()
    details = _run(
        fake,
        EndpointConfig(kind="file", format="csv"),
        tmp_path / "d.db",
        source_filename="s.csv",
        source_content=content,
    )
    assert [d["row"] for d in details] == list(BAD_ROWS), details
    identities = {replay_row_identity(d) for d in details}
    assert len(identities) == len(BAD_ROWS), identities


def test_database_stream_numbers_rejects_by_absolute_source_row(fake, tmp_path):
    src = tmp_path / "s.db"
    with sqlite3.connect(src) as conn:
        conn.execute("CREATE TABLE t (age TEXT)")
        conn.executemany("INSERT INTO t VALUES (?)", [(a,) for a in _ages()])
    details = _run(
        fake,
        EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        tmp_path / "d.db",
    )
    assert [d["row"] for d in details] == list(BAD_ROWS), details
    assert len({replay_row_identity(d) for d in details}) == len(BAD_ROWS)
