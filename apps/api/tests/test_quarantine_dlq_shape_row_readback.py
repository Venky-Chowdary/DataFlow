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


def test_end_to_end_shape_divert_is_persisted_and_readable(tmp_path, isolated, monkeypatch):
    """Real SQLite->SQLite cast-divert run -> DLQ persist -> canonical replay
    read-back returns the diverted source row. (Read-back only — the write
    path is covered by the replay tests below.)"""
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


# ---------------------------------------------------------------------------
# Replay WRITE path — dict-row findings must hydrate into writable records.
# ---------------------------------------------------------------------------


def _shape_detail() -> dict:
    return {
        "row": {"id": "2", "amt": "BAD"},
        "reason": "transform divert — step 1 'cast_column': 'BAD' is not an integer",
        "failure_reason": "'BAD' is not an integer",
        "shape_step": 1,
        "shape_op": "cast_column",
        "source_row_index": 2,
    }


def test_quarantine_details_to_records_hydrates_dict_row():
    from src.routers.connectors_router import _quarantine_details_to_records

    records, columns = _quarantine_details_to_records([_shape_detail()])
    assert records == [{"id": "2", "amt": "BAD"}]
    assert columns == ["id", "amt"]


def test_merge_edited_row_binds_to_whole_record_finding():
    from src.routers.connectors_router import _merge_edited_row_payloads

    records, merged_details = _merge_edited_row_payloads(
        [_shape_detail()], [{"id": "2", "amt": "BAD"}], [{"id": "2", "amt": 42}]
    )
    assert records == [{"id": "2", "amt": 42}]
    assert merged_details == [_shape_detail()]


def test_merge_edited_row_refuses_an_edit_that_agrees_on_nothing():
    import pytest
    from fastapi import HTTPException

    from src.routers.connectors_router import _merge_edited_row_payloads

    with pytest.raises(HTTPException) as exc:
        _merge_edited_row_payloads(
            [_shape_detail()],
            [{"id": "2", "amt": "BAD"}],
            [{"id": "9", "amt": 42}],  # disagrees on every shared cell
        )
    assert exc.value.status_code == 400


def _replay_request():
    class _State:
        pass

    class _Req:
        state = _State()
        headers: dict = {}

    return _Req()


def test_shape_divert_edited_replay_writes_row_to_destination(tmp_path, isolated, monkeypatch):
    """Cast-divert run -> DLQ -> edit v to 42 via the replay endpoint ->
    destination read-back contains (2, '42') and the finding closes."""
    import asyncio
    import sqlite3
    import uuid

    import importlib

    router = importlib.import_module("src.routers.connectors_router")
    from src.routers.connectors_router import QuarantineReplayRequest, replay_job_quarantine
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import transfer_request_to_dict
    from tests.test_shape_buffered_ops_stream_e2e import _exec, _request

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    monkeypatch.setattr(router, "get_mongodb_service", lambda: isolated)

    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _exec(src, "CREATE TABLE t (id TEXT, v TEXT)")
    _exec(src, "INSERT INTO t (id, v) VALUES (?, ?)", [("1", "10"), ("2", "BAD")])

    req = _request(
        src,
        dst,
        [{"op": "cast_column", "column": "v", "options": {"to_type": "integer"}, "on_error": "divert"}],
    )
    job_id = "dlqedit" + uuid.uuid4().hex[:12]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    assert result.success, result.error
    isolated.update_job_fields(
        job_id,
        {"id": job_id, "job_id": job_id, "transfer_request": transfer_request_to_dict(req)},
    )

    resp = asyncio.run(
        replay_job_quarantine(job_id, QuarantineReplayRequest(rows=[{"id": "2", "v": 42}]), _replay_request())
    )
    assert resp.get("rows_written", 0) >= 1, resp

    conn = sqlite3.connect(str(dst))
    try:
        got = [(r[0], str(r[1])) for r in conn.execute("SELECT id, v FROM t ORDER BY id")]
    finally:
        conn.close()
    assert ("2", "42") in got, got

    open_rows = dlq.open_quarantine_details(dlq.canonical_replay_details({"id": job_id}))
    assert open_rows == [], "the remediated finding must close after a written replay"


def test_shape_divert_unedited_replay_behaves_sensibly(tmp_path, isolated, monkeypatch):
    """Unedited replay of a shape-diverted row must not 400 or silently drop:
    either the row lands verbatim (replay-as-is) or the shape recipe diverts
    it again — but never a 'no row payload' crash."""
    import asyncio
    import sqlite3
    import uuid

    import importlib

    router = importlib.import_module("src.routers.connectors_router")
    from src.routers.connectors_router import QuarantineReplayRequest, replay_job_quarantine
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import transfer_request_to_dict
    from tests.test_shape_buffered_ops_stream_e2e import _exec, _request

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    monkeypatch.setattr(router, "get_mongodb_service", lambda: isolated)

    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _exec(src, "CREATE TABLE t (id TEXT, v TEXT)")
    _exec(src, "INSERT INTO t (id, v) VALUES (?, ?)", [("1", "10"), ("2", "BAD")])

    req = _request(
        src,
        dst,
        [{"op": "cast_column", "column": "v", "options": {"to_type": "integer"}, "on_error": "divert"}],
    )
    job_id = "dlqraw" + uuid.uuid4().hex[:12]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    assert result.success, result.error
    isolated.update_job_fields(
        job_id,
        {"id": job_id, "job_id": job_id, "transfer_request": transfer_request_to_dict(req)},
    )

    from fastapi import HTTPException

    refused = None
    try:
        asyncio.run(
            replay_job_quarantine(job_id, QuarantineReplayRequest(), _replay_request())
        )
    except HTTPException as exc:
        refused = exc

    conn = sqlite3.connect(str(dst))
    try:
        landed = [r[0] for r in conn.execute("SELECT id FROM t WHERE id = '2'")]
    finally:
        conn.close()
    still_open = dlq.open_quarantine_details(dlq.canonical_replay_details({"id": job_id}))
    # Honest outcomes only: the row landed verbatim, or the write was refused
    # loudly (the child re-validation names the failing cell) and the finding
    # stays open for the operator. Never a silent drop, never a hydration
    # crash.
    if refused is not None:
        assert landed == [], refused.detail
        assert still_open, "a refused replay must leave the finding open"
        assert "BAD" in str(refused.detail)
    else:
        assert landed == ["2"] or still_open, (
            f"row neither written (landed={landed}) nor still quarantined ({still_open})"
        )


def test_dlq_readback_restores_real_null_not_sentinel(tmp_path, monkeypatch):
    """A1-D2: a NULL persisted as __DF_SQL_NULL__ reads back as None, never
    the literal sentinel text."""
    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    dlq.append_dlq_event(
        job_id="job-null",
        action="quarantine_chunk",
        details={
            "rejected_details": [
                {
                    "row": {"id": "2", "cc": "__DF_SQL_NULL__"},
                    "reason": "transform divert — step 0 'divert_rows': test",
                    "shape_step": 0,
                    "shape_op": "divert_rows",
                    "source_row_index": 2,
                    "value": "__DF_SQL_NULL__",
                }
            ]
        },
        path=tmp_path / "dlq.jsonl",
    )
    rows = dlq.quarantine_details_from_dlq("job-null")
    assert rows[0]["row"] == {"id": "2", "cc": None}
    assert rows[0]["value"] is None


def test_shape_divert_null_cell_replay_writes_sql_null(tmp_path, isolated, monkeypatch):
    """A1-D2: divert a row carrying a NULL column -> unedited replay -> the
    destination holds SQL NULL, not the sentinel text."""
    import asyncio
    import sqlite3
    import uuid

    import importlib

    router = importlib.import_module("src.routers.connectors_router")
    from src.routers.connectors_router import QuarantineReplayRequest, replay_job_quarantine
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import transfer_request_to_dict
    from tests.test_shape_buffered_ops_stream_e2e import _exec, _request

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    monkeypatch.setattr(router, "get_mongodb_service", lambda: isolated)

    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _exec(src, "CREATE TABLE t (id TEXT, v TEXT)")
    _exec(src, "INSERT INTO t (id, v) VALUES (?, ?)", [("1", "10"), ("2", None)])

    req = _request(
        src,
        dst,
        [{"op": "divert_rows", "options": {"condition": "id == '2'", "reason": "test divert"}}],
    )
    job_id = "dlqnull" + uuid.uuid4().hex[:12]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    assert result.success, result.error
    isolated.update_job_fields(
        job_id,
        {"id": job_id, "job_id": job_id, "transfer_request": transfer_request_to_dict(req)},
    )

    # Read-back exposes a real NULL, not the sentinel.
    detail = dlq.canonical_replay_details({"id": job_id})[0]
    assert detail["row"] == {"id": "2", "v": None}, detail["row"]

    resp = asyncio.run(replay_job_quarantine(job_id, QuarantineReplayRequest(), _replay_request()))
    assert resp.get("rows_written", 0) >= 1, resp

    # R4: the operator-facing reconcile_sample shows real NULL too — the
    # writer wire sentinel must not leak into the sample.
    sample = ((resp.get("destination_summary") or {}).get("reconcile_sample")) or []
    assert sample, "reconcile_sample expected on the child job summary"
    assert not any(
        str(v) == "__DF_SQL_NULL__" for row in sample for v in row.values()
    ), sample
    assert any(row.get("v") is None for row in sample), sample

    conn = sqlite3.connect(str(dst))
    try:
        got = conn.execute("SELECT v FROM t WHERE id = '2'").fetchall()
    finally:
        conn.close()
    assert got == [(None,)], f"sentinel text must never land in a destination: {got}"


def test_mark_dlq_promoted_skips_missing_dest_quarantine_table(tmp_path):
    """R3: shape-divert findings live in the control-plane DLQ, not a
    <dest>_df_quarantine table — the promote stamp on an absent table is a
    skip, not an error that would leave the replay closure diverging."""
    import sqlite3

    from services.dest_quarantine import mark_dlq_promoted
    from src.transfer.models import EndpointConfig

    db = tmp_path / "dst.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE t (id TEXT)")
    conn.commit()
    conn.close()

    dest = EndpointConfig(
        kind="database", format="sqlite", database=str(db), table="t"
    )
    meta = mark_dlq_promoted(dest, qids=[], job_id="r3job")
    assert meta.get("skipped") and not meta.get("error"), meta


def test_replay_strips_overwrite_flags_from_destination(tmp_path, isolated, monkeypatch):
    """P0-R1: a parent run with sync_mode=full_refresh_overwrite stamps
    overwrite-class flags (pg_overwrite_in_transaction, overwrite_backup*)
    on the request destination extra. Replay is a keyed upsert — forwarding
    those flags to the writer truncates the table and deletes landed rows."""
    import asyncio
    import sqlite3
    import uuid

    import importlib

    router = importlib.import_module("src.routers.connectors_router")
    import src.transfer.adapters as adapters_mod
    from src.routers.connectors_router import QuarantineReplayRequest, replay_job_quarantine
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import transfer_request_to_dict
    from tests.test_shape_buffered_ops_stream_e2e import _exec, _request

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    monkeypatch.setattr(router, "get_mongodb_service", lambda: isolated)

    captured = {}
    real_write = adapters_mod.write_destination_database

    def _spy(endpoint, records, columns, schema, mappings, **options):
        captured["extra"] = dict(getattr(endpoint, "extra", None) or {})
        return real_write(endpoint, records, columns, schema, mappings, **options)

    monkeypatch.setattr(adapters_mod, "write_destination_database", _spy)

    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _exec(src, "CREATE TABLE t (id TEXT, v TEXT, note TEXT)")
    _exec(
        src,
        "INSERT INTO t (id, v, note) VALUES (?, ?, ?)",
        [("1", "10", "a"), ("2", "BAD", None), ("3", "30", "c")],
    )

    req = _request(
        src,
        dst,
        [{"op": "cast_column", "column": "v", "options": {"to_type": "integer"}, "on_error": "divert"}],
    )
    # The parent's PG overwrite drop stamps this flag on the request
    # destination extra; it is serialized into the stored transfer_request.
    req.destination.extra = dict(req.destination.extra or {})
    req.destination.extra["pg_overwrite_in_transaction"] = True
    req.destination.extra["overwrite_backup"] = "t_backup_x"

    job_id = "p0r1flag" + uuid.uuid4().hex[:12]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    assert result.success, result.error
    isolated.update_job_fields(
        job_id,
        {"id": job_id, "job_id": job_id, "transfer_request": transfer_request_to_dict(req)},
    )

    resp = asyncio.run(
        replay_job_quarantine(
            job_id,
            QuarantineReplayRequest(rows=[{"id": "2", "v": 42, "note": None}]),
            _replay_request(),
        )
    )
    assert resp.get("rows_written", 0) >= 1, resp

    extra = captured.get("extra") or {}
    assert "pg_overwrite_in_transaction" not in extra, extra
    assert "overwrite_backup" not in extra, extra

    conn = sqlite3.connect(str(dst))
    try:
        got = [(r[0], str(r[1]), r[2]) for r in conn.execute("SELECT id, v, note FROM t ORDER BY id")]
    finally:
        conn.close()
    assert got == [("1", "10", "a"), ("2", "42", None), ("3", "30", "c")], got


def test_pg_upsert_write_never_forwards_overwrite_flag(monkeypatch):
    """Adapter layer: pg_overwrite_in_transaction only applies to insert-mode
    loads — a keyed upsert call carrying it would truncate the table."""
    import connectors.postgresql_writer as pgw
    from connectors.writer_common import WriteResult
    from src.transfer.adapters import write_destination_database
    from src.transfer.models import EndpointConfig

    captured: dict = {}

    def _fake_writer(**kwargs):
        captured.update(kwargs)
        return WriteResult(
            ok=True,
            rows_written=1,
            table_name="t",
            target_schema="public",
            checksum="",
            chunks_completed=1,
        )

    monkeypatch.setattr(pgw, "write_mapped_rows", _fake_writer)
    import services.dest_precount as dest_precount

    monkeypatch.setattr(dest_precount, "precount_destination", lambda endpoint, cfg: 2)
    import services.row_conservation as row_conservation

    monkeypatch.setattr(
        row_conservation,
        "prepare_keyed_upsert",
        lambda records, **kw: (records, None),
    )

    endpoint = EndpointConfig(
        kind="database",
        format="postgresql",
        host="pg.internal",
        port=5432,
        database="d",
        table="t",
        extra={"pg_overwrite_in_transaction": True},
    )
    write_destination_database(
        endpoint,
        [{"id": "2", "v": "42"}],
        ["id", "v"],
        {"id": "string", "v": "string"},
        [{"source": "id", "target": "id"}, {"source": "v", "target": "v"}],
        write_mode="upsert",
        conflict_columns=["id"],
        sync_mode="incremental_deduped",
        protect_landed_object=True,
    )
    assert "pg_overwrite_in_transaction" not in captured, sorted(captured)


def test_overwrite_parent_replay_preserves_landed_rows(tmp_path, isolated, monkeypatch):
    """P0-R1 contract: replay into an overwrite-loaded destination keeps every
    previously landed row and lands the remediated one."""
    import asyncio
    import sqlite3
    import uuid

    import importlib

    router = importlib.import_module("src.routers.connectors_router")
    from src.routers.connectors_router import QuarantineReplayRequest, replay_job_quarantine
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import transfer_request_to_dict
    from tests.test_shape_buffered_ops_stream_e2e import _exec, _request

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    monkeypatch.setattr(router, "get_mongodb_service", lambda: isolated)

    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _exec(src, "CREATE TABLE t (id TEXT, v TEXT, note TEXT)")
    _exec(
        src,
        "INSERT INTO t (id, v, note) VALUES (?, ?, ?)",
        [("1", "10", "a"), ("2", "BAD", None), ("3", "30", "c")],
    )

    req = _request(
        src,
        dst,
        [{"op": "cast_column", "column": "v", "options": {"to_type": "integer"}, "on_error": "divert"}],
    )
    assert req.sync_mode == "full_refresh_overwrite"
    job_id = "p0r1keep" + uuid.uuid4().hex[:12]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    assert result.success, result.error
    isolated.update_job_fields(
        job_id,
        {"id": job_id, "job_id": job_id, "transfer_request": transfer_request_to_dict(req)},
    )

    resp = asyncio.run(
        replay_job_quarantine(
            job_id,
            QuarantineReplayRequest(rows=[{"id": "2", "v": 42, "note": None}]),
            _replay_request(),
        )
    )
    assert resp.get("rows_written", 0) >= 1, resp

    conn = sqlite3.connect(str(dst))
    try:
        got = [(r[0], str(r[1]), r[2]) for r in conn.execute("SELECT id, v, note FROM t ORDER BY id")]
    finally:
        conn.close()
    assert got == [("1", "10", "a"), ("2", "42", None), ("3", "30", "c")], (
        "replay must never remove a landed row"
    )


def test_replay_landed_rows_survive_diverging_closure(tmp_path, isolated, monkeypatch):
    """Gate-8 failure path: even when the post-write closure verdict diverges
    (no _df_quarantine store on this engine), the landed rows are left
    untouched — a failed proof must not remove data."""
    import asyncio
    import sqlite3
    import uuid

    import importlib

    router = importlib.import_module("src.routers.connectors_router")
    from src.routers.connectors_router import QuarantineReplayRequest, replay_job_quarantine
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import transfer_request_to_dict
    from tests.test_shape_buffered_ops_stream_e2e import _exec, _request

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    monkeypatch.setattr(router, "get_mongodb_service", lambda: isolated)

    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _exec(src, "CREATE TABLE t (id TEXT, v TEXT, note TEXT)")
    _exec(
        src,
        "INSERT INTO t (id, v, note) VALUES (?, ?, ?)",
        [("1", "10", "a"), ("2", "BAD", None), ("3", "30", "c")],
    )

    req = _request(
        src,
        dst,
        [{"op": "cast_column", "column": "v", "options": {"to_type": "integer"}, "on_error": "divert"}],
    )
    job_id = "p0r1gate" + uuid.uuid4().hex[:12]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(req, job_id)
    assert result.success, result.error
    isolated.update_job_fields(
        job_id,
        {"id": job_id, "job_id": job_id, "transfer_request": transfer_request_to_dict(req)},
    )

    asyncio.run(
        replay_job_quarantine(
            job_id,
            QuarantineReplayRequest(rows=[{"id": "2", "v": 42, "note": None}]),
            _replay_request(),
        )
    )
    # Closure verdict may diverge on engines without a *_df_quarantine table;
    # landed rows must still be intact.
    conn = sqlite3.connect(str(dst))
    try:
        got = [(r[0], str(r[1])) for r in conn.execute("SELECT id, v FROM t ORDER BY id")]
    finally:
        conn.close()
    assert got == [("1", "10"), ("2", "42"), ("3", "30")], got
