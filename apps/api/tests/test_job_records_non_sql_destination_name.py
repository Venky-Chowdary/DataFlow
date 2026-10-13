"""Job records name the object a non-SQL destination wrote.

QA reads ``get_job`` / ``list_jobs`` through MCP. A CSV export finished with
``destination_collection=""`` (``dest_table=null``, ``destination="csv"``) even
though the writer summary named the file, and the streamed Redis / object
store / vector writers returned no ``table`` at all — the batch path named
it, the streaming path did not.
"""

from __future__ import annotations

import socket
import sqlite3
import uuid

import pytest

from tests.test_rt02_incremental_append_l1_dest_count import isolated  # noqa: F401
from src.ai.copilot.job_reads import summarize_listed_job
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _redis_up() -> bool:
    try:
        socket.create_connection(("localhost", 6379), timeout=1).close()
        return True
    except OSError:
        return False


def _run(isolated, tmp_path, destination):  # noqa: F811
    db = tmp_path / "src.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany("INSERT INTO items VALUES (?, ?)", [(i, f"n{i}") for i in range(1, 6)])
    job_id = "jobdst" + uuid.uuid4().hex[:16]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(
        TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(db), table="items"),
            destination=destination,
            mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
            sync_mode="full_refresh_overwrite",
            skip_preflight=True,
        ),
        job_id,
    )
    assert result.success, result.error
    return isolated.get_job(job_id) or {}


def test_csv_export_job_records_the_export_file(isolated, tmp_path):  # noqa: F811
    doc = _run(isolated, tmp_path, EndpointConfig(kind="file_export", format="csv"))
    filename = (doc.get("destination_summary") or {}).get("filename")
    assert filename and filename.endswith(".csv"), doc.get("destination_summary")
    assert doc.get("destination_collection") == filename
    listed = summarize_listed_job(doc)
    assert listed["dest_table"] == filename and listed["destination"] == filename, listed


@pytest.mark.skipif(not _redis_up(), reason="Redis not reachable on localhost:6379")
def test_streamed_redis_write_names_its_key_prefix(isolated, tmp_path):  # noqa: F811
    prefix = "jobdst_" + uuid.uuid4().hex[:8]
    doc = _run(isolated, tmp_path, EndpointConfig(
        kind="database", format="redis", host="localhost", port=6379, database="0", table=prefix,
    ))
    summary = doc.get("destination_summary") or {}
    assert summary.get("table") == prefix, summary
    assert doc.get("destination_collection") == prefix
    assert summarize_listed_job(doc)["dest_table"] == prefix
