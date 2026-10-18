"""Audit blocker: quarantine read-back crashed on dict rows (unhashable dedupe
key). A shape-divert detail carries ``row`` as a record — the read paths must
hydrate it, not TypeError."""
import os

import pytest

import services.quarantine_dlq as dlq

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _seed_shape_divert_event(dlq_path, job_id="job-1"):
    """The exact detail shape _persist_shape_diverts writes."""
    dlq.append_dlq_event(
        job_id=job_id,
        action="quarantine_chunk",
        details={
            "rejected_details": [
                {
                    "row": {"id": "2", "amt": "BAD"},
                    "reason": "transform divert — step 1 'cast_column': 'BAD' is not an integer",
                    "failure_reason": "'BAD' is not an integer",
                    "transform_attempted": "shape step 1 'cast_column'",
                    "shape_step": 1,
                    "shape_op": "cast_column",
                    "source_row_index": 2,
                }
            ]
        },
        path=dlq_path,
    )


def test_quarantine_details_from_dlq_reads_dict_row(tmp_path, monkeypatch):
    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    _seed_shape_divert_event(tmp_path / "dlq.jsonl")
    rows = dlq.quarantine_details_from_dlq("job-1")
    assert len(rows) == 1
    assert rows[0]["row"] == {"id": "2", "amt": "BAD"}
    assert rows[0]["shape_op"] == "cast_column"


def test_quarantine_details_dedupes_identical_events(tmp_path, monkeypatch):
    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    path = tmp_path / "dlq.jsonl"
    _seed_shape_divert_event(path)
    _seed_shape_divert_event(path)  # same row persisted twice -> deduped to 1
    rows = dlq.quarantine_details_from_dlq("job-1")
    assert len(rows) == 1


def test_canonical_replay_details_returns_the_row(tmp_path, monkeypatch):
    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    _seed_shape_divert_event(tmp_path / "dlq.jsonl", job_id="job-9")
    job = {"id": "job-9", "rejected_details": []}
    rows = dlq.canonical_replay_details(job)
    assert len(rows) == 1
    assert rows[0]["row"]["id"] == "2"


def test_end_to_end_shape_divert_is_replayable(tmp_path, isolated, monkeypatch):
    """Real SQLite->SQLite cast-divert run -> DLQ persist -> canonical replay
    read-back returns the diverted source row."""
    from tests.test_shape_buffered_ops_stream_e2e import _exec

    dlq_path = tmp_path / "dlq.jsonl"
    monkeypatch.setattr(dlq, "DLQ_PATH", dlq_path)

    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _exec(src, "CREATE TABLE t (id TEXT, v TEXT)")
    _exec(src, "INSERT INTO t (id, v) VALUES (?, ?)", [("1", "10"), ("2", "BAD")])

    import uuid
    from tests.test_shape_buffered_ops_stream_e2e import _request
    from src.transfer.engine import UniversalTransferEngine

    job_id = "dlqread" + uuid.uuid4().hex[:12]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(
        _request(
            src,
            dst,
            [
                {
                    "op": "cast_column",
                    "column": "v",
                    "options": {"to_type": "integer"},
                    "on_error": "divert",
                }
            ],
        ),
        job_id,
    )
    assert result.success, result.error
    rows = dlq.canonical_replay_details({"id": job_id})
    assert len(rows) == 1
    assert rows[0]["row"] == {"id": "2", "v": "BAD"}
