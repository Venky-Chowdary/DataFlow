"""Capture-once journal: extract once, replay to many destinations — real engine runs."""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services import transfer_journal as tj  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_property2_golden_path_never_blocked import _FakeMongo  # noqa: E402


_JOB_FIELDS: dict[str, dict] = {}


class _Mongo(_FakeMongo):
    def update_job_fields(self, job_id, fields, **k):
        _JOB_FIELDS.setdefault(str(job_id), {}).update(fields)
        return True


@pytest.fixture(autouse=True)
def _env(tmp_path, monkeypatch):
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    monkeypatch.setenv("DATAFLOW_JOURNAL_DIR", str(tmp_path / "journals"))
    monkeypatch.delenv("DATAFLOW_JOURNAL_CAPTURE", raising=False)
    yield


def _seed(path: Path, n: int) -> None:
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, amount REAL)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?, ?)",
            [(i, f"name-{i}", i * 1.5) for i in range(1, n + 1)],
        )
    conn.close()


def _rows(path: Path, table: str = "t") -> list[tuple]:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(f"SELECT id, name, amount FROM {table} ORDER BY id").fetchall()
    finally:
        conn.close()


def _db_request(src: Path, dst: Path, *, capture: bool = True) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(
            kind="database", format="sqlite", database=str(src), table="t",
            extra={"journal_capture": True} if capture else {},
        ),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )


def _replay_request(journal_id: str, dst: Path) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="file", extra={"journal_id": journal_id}),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )


def _run(request: TransferRequest):
    job_id = uuid.uuid4().hex
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    result._job_fields = _JOB_FIELDS.get(job_id, {})
    return result


def test_capture_once_then_replay_to_two_destinations_without_reading_the_source(tmp_path):
    src, d0 = tmp_path / "src.db", tmp_path / "d0.db"
    _seed(src, 120)
    first = _run(_db_request(src, d0))
    assert first.success, first.error
    journal = first.source_summary["journal"]
    assert journal["captured"] is True, journal
    assert journal["streams"][0]["rows"] == 120
    assert first._job_fields["journal"] == journal  # the operator sees it on the job
    expected = _rows(src)
    assert _rows(d0) == expected

    # The live source is gone: replay can only come from the journal.
    src.unlink()
    for name in ("d1.db", "d2.db"):
        dst = tmp_path / name
        replay = _run(_replay_request(journal["journal_id"], dst))
        assert replay.success, replay.error
        assert replay.records_transferred == 120
        assert replay.source_summary["journal_replay"]["journal_id"] == journal["journal_id"]
        assert "journal" not in replay.source_summary  # a replay is not re-captured
        assert replay._job_fields["journal_replay"]["journal_id"] == journal["journal_id"]
        got = [(int(i), str(n), float(a)) for i, n, a in _rows(dst)]
        assert got == expected
        assert replay.reconciliation.get("passed") is not False


def test_capture_is_opt_in(tmp_path):
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed(src, 5)
    result = _run(_db_request(src, dst, capture=False))
    assert result.success, result.error
    assert "journal" not in (result.source_summary or {})
    assert not (tmp_path / "journals").exists() or not any((tmp_path / "journals").rglob(tj.MANIFEST_NAME))


def test_file_source_journal_keeps_exact_bytes(tmp_path):
    content = b"id,name\n1,a\n2,b\n3,c\n"
    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv", extra={"journal_capture": True}),
        source_filename="people.csv",
        source_content=content,
        destination=EndpointConfig(kind="database", format="sqlite", database=str(tmp_path / "a.db"), table="p"),
        skip_preflight=True,
    )
    result = _run(req)
    assert result.success, result.error
    jid = result.source_summary["journal"]["journal_id"]
    resolved = tj.resolve_replay(jid)
    assert Path(resolved["path"]).read_bytes() == content
    assert resolved["filename"] == "people.csv"

    replay = _run(TransferRequest(
        source=EndpointConfig(kind="file", extra={"journal_id": jid}),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(tmp_path / "b.db"), table="p"),
        skip_preflight=True,
    ))
    assert replay.success, replay.error
    assert replay.records_transferred == 3


def test_failed_run_leaves_no_replayable_journal(tmp_path):
    src = tmp_path / "src.db"
    _seed(src, 3)
    req = _db_request(src, tmp_path / "dst.db")
    req.destination.table = "bad name; drop"
    result = _run(req)
    assert not result.success
    assert tj.list_journals() == []
    assert not list((tmp_path / "journals").rglob("*.partial")) if (tmp_path / "journals").exists() else True


def test_tampered_journal_is_refused(tmp_path):
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed(src, 10)
    jid = _run(_db_request(src, dst)).source_summary["journal"]["journal_id"]
    path = Path(tj.resolve_replay(jid)["path"])
    lines = path.read_text().splitlines()
    path.write_text("\n".join(lines[:-1]) + "\n")  # silently truncated
    replay = _run(_replay_request(jid, tmp_path / "x.db"))
    assert not replay.success
    assert "integrity" in (replay.error or "")


def test_unknown_or_malformed_journal_is_refused(tmp_path):
    for jid in ("does-not-exist.1", "../etc/passwd"):
        replay = _run(_replay_request(jid, tmp_path / "x.db"))
        assert not replay.success
        assert "Journal" in (replay.error or "")


def test_journal_is_invisible_to_another_workspace(tmp_path):

    writer = tj.JournalWriter(job_id="j1", workspace_id="ws-acme", request=_db_request(tmp_path / "s", tmp_path / "d"))
    writer.capture_records("t", ["id"], [{"id": 1}])
    jid = writer.commit(delivered_rows=1)["journal_id"]
    assert tj.find_journal(jid)[1]["workspace_id"] == "ws-acme"
    token = tj_bind("ws-mallory")
    try:
        with pytest.raises(tj.JournalError):
            tj.resolve_replay(jid)
        assert tj.list_journals() == []
    finally:
        tj_unbind(token)


def tj_bind(ws: str):
    from services import workspace_filter as wf

    return wf._bound.set(wf.WorkspaceFilter.only(ws))


def tj_unbind(token) -> None:
    from services import workspace_filter as wf

    wf._bound.reset(token)


def test_encodings_are_counted_never_silent(tmp_path):
    writer = tj.JournalWriter(job_id="j2", workspace_id="", request=_db_request(tmp_path / "s", tmp_path / "d"))
    writer.capture_records(
        "t",
        ["d", "ts", "amt", "blob", "nan"],
        [{"d": date(2024, 1, 2), "ts": datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc), "amt": Decimal("12345678901234567890.123456789"),
          "blob": b"\x00\xff", "nan": float("nan")}],
    )
    out = writer.commit(delivered_rows=1)
    manifest = tj.find_journal(out["journal_id"])[1]
    enc = manifest["streams"][0]["encodings"]
    assert enc == {
        "d": {"date_iso": 1}, "ts": {"datetime_iso": 1}, "amt": {"decimal_text": 1},
        "blob": {"bytes_base64": 1}, "nan": {"non_finite_float_text": 1},
    }
    row = json.loads(Path(tj.resolve_replay(out["journal_id"])["path"]).read_text())
    assert row["amt"] == "12345678901234567890.123456789"  # exact digits kept


def test_commit_refuses_when_rows_bypassed_the_journal(tmp_path):
    writer = tj.JournalWriter(job_id="j3", workspace_id="", request=_db_request(tmp_path / "s", tmp_path / "d"))
    writer.capture_records("t", ["id"], [{"id": 1}])
    out = writer.commit(delivered_rows=5)
    assert out["captured"] is False and "outside the journaled read path" in out["reason"]
    assert not writer.dir.exists()


def test_streamed_page_is_captured_once_even_when_filtered_twice(tmp_path):
    from types import SimpleNamespace

    req = _db_request(tmp_path / "s", tmp_path / "d")
    writer, _ = tj.begin_capture(req, "job-x")
    batch = SimpleNamespace(headers=["id"], rows=[[1], [2]])
    tj.capture_batch("job-x", "t", batch)
    tj.capture_batch("job-x", "t", batch)
    assert writer.captured_rows == 2
    tj.abort_capture("job-x", "test")


def test_resume_and_cdc_runs_say_why_they_are_not_captured(tmp_path):
    req = _db_request(tmp_path / "s", tmp_path / "d")
    assert tj.begin_capture(req, "r1", resume=True)[0] is None
    req.sync_mode = "cdc"
    writer, reason = tj.begin_capture(req, "r2")
    assert writer is None and "CDC" in reason


def test_prune_drops_expired_and_orphaned(tmp_path):
    req = _db_request(tmp_path / "s", tmp_path / "d")
    w = tj.JournalWriter(job_id="old", workspace_id="", request=req)
    w.capture_records("t", ["id"], [{"id": 1}])
    jid = w.commit(delivered_rows=1)["journal_id"]
    orphan = tj.JournalWriter(job_id="orphan", workspace_id="", request=req)
    orphan.capture_records("t", ["id"], [{"id": 1}])
    import time as _t

    counts = tj.prune_journals(now=_t.time() + 30 * 86400)
    assert counts == {"expired": 1, "orphaned": 1}
    with pytest.raises(tj.JournalError):
        tj.find_journal(jid)
