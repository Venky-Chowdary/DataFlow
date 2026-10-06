"""Unit tests for the CDC transfer runner.

Real database connectivity is not required; the source and destination readers/
writers are patched so we can exercise the CDC engine logic end-to-end.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.cdc_transfer import CdcEngine, run_cdc_database_transfer
from src.transfer.models import EndpointConfig


def _batch(headers: list[str], rows: list[list[str]]) -> SimpleNamespace:
    return SimpleNamespace(headers=headers, rows=rows)


def test_cdc_engine_snapshot_returns_insert_only_batches():
    headers = ["id", "value"]
    rows = [["1", "a"], ["2", "b"]]
    with patch("src.transfer.cdc_transfer._read_batch") as mock_read:
        mock_read.side_effect = [(_batch(headers, rows), None), (_batch(headers, []), None)]
        engine = CdcEngine(
            src_cfg={"database": "test"},
            src_type="generic_sql",
            table_name="src",
            cursor_field="id",
            primary_key="id",
            watermark=None,
            columns=headers,
        )
        batches = list(engine.snapshot())
    assert len(batches) == 1
    assert len(batches[0].inserts) == 2
    assert not batches[0].deletes


def test_cdc_engine_poll_filters_by_watermark():
    headers = ["id", "value"]
    rows = [["3", "c"], ["4", "d"]]
    with patch("src.transfer.cdc_transfer._read_batch") as mock_read:
        mock_read.side_effect = [(_batch(headers, rows), None), (_batch(headers, []), None)]
        engine = CdcEngine(
            src_cfg={"database": "test"},
            src_type="generic_sql",
            table_name="src",
            cursor_field="id",
            primary_key="id",
            watermark="2",
            columns=headers,
        )
        batches = list(engine.poll())
    assert len(batches) == 1
    assert [r["id"] for r in batches[0].inserts] == ["3", "4"]


def test_cdc_engine_detects_soft_delete_tombstone():
    headers = ["id", "value", "deleted_at"]
    rows = [["1", "a", ""], ["2", "b", "2024-01-01"], ["3", "c", ""]]
    with patch("src.transfer.cdc_transfer._read_batch") as mock_read:
        mock_read.side_effect = [(_batch(headers, rows), None), (_batch(headers, []), None)]
        engine = CdcEngine(
            src_cfg={"database": "test"},
            src_type="generic_sql",
            table_name="src",
            cursor_field="id",
            primary_key="id",
            watermark=None,
            columns=headers,
            schema={"id": "integer", "value": "string", "deleted_at": "timestamp"},
        )
        batches = list(engine.snapshot())
    assert len(batches[0].inserts) == 2
    assert batches[0].deletes == ["2"]


def test_run_cdc_database_transfer_requires_pk_and_cursor():
    source = EndpointConfig(kind="database", format="generic_sql", database="test", table="src")
    destination = EndpointConfig(kind="database", format="generic_sql", database="test", table="dst")
    with pytest.raises(ValueError, match="primary_key"):
        run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "id", "target": "id"}],
            schema={"id": "integer", "value": "string"},
            stream_contracts=[{"sync_mode": "cdc"}],
        )


def test_run_cdc_database_transfer_performs_initial_snapshot(tmp_path, monkeypatch):
    # A previous run of this test left watermark "2" in the workspace cursor
    # file. Resume then seeks past 2, the mock still returns those rows, and
    # the engine refuses the spin. The scenario is an initial snapshot.
    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "sync_cursors.json")
    source = EndpointConfig(kind="database", format="generic_sql", database="test", table="src")
    destination = EndpointConfig(kind="database", format="generic_sql", database="test", table="dst")
    headers = ["id", "value"]
    rows = [["1", "a"], ["2", "b"]]

    mock_read = MagicMock(side_effect=[(_batch(headers, rows), None), (_batch(headers, []), None)])
    mock_write = MagicMock(return_value=(2, "abc", {}))
    mock_delete = MagicMock(return_value=0)

    with (
        patch("src.transfer.cdc_transfer._read_batch", mock_read),
        patch("src.transfer.cdc_transfer._write_batch", mock_write),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", mock_delete),
    ):
        rows_written, ddl_log, dest_summary, columns = run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "id", "target": "id"}, {"source": "value", "target": "value"}],
            schema={"id": "integer", "value": "string"},
            stream_contracts=[{"sync_mode": "cdc", "primary_key": "id", "cursor_field": "id"}],
            job_id="cdc-test",
        )

    assert rows_written == 2
    assert dest_summary["cdc"]["inserts"] == 2
    assert columns == ["id", "value"]
    assert mock_write.call_count == 1
    assert mock_write.call_args[1]["write_mode"] == "upsert"


@pytest.mark.fake_mongo
def test_run_cdc_database_transfer_uses_mongodb_change_stream(tmp_path, monkeypatch):
    """Exercise the MongoDB change-stream branch of the CDC runner."""
    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "sync_cursors.json")
    from services.cdc_engine import ChangeBatch

    source = EndpointConfig(kind="database", format="mongodb", database="test", table="orders")
    destination = EndpointConfig(kind="database", format="generic_sql", database="test", table="dst")

    mock_write = MagicMock(return_value=(2, "abc", {}))
    mock_delete = MagicMock(return_value=0)

    class FakeChangeStreamCdc:
        def __init__(self, *args, **kwargs):
            pass

        def is_available(self):
            return True

        def snapshot(self):
            yield ChangeBatch(inserts=[{"_id": "1", "amount": "100.00"}, {"_id": "2", "amount": "200.00"}])

        def poll(self):
            return iter([])

    with (
        patch("src.transfer.cdc_transfer.MongodbChangeStreamCdc", FakeChangeStreamCdc),
        patch("src.transfer.cdc_transfer._write_batch", mock_write),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", mock_delete),
        patch("src.transfer.cdc_transfer.get_watermark", return_value=None),
    ):
        rows_written, ddl_log, dest_summary, columns = run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "_id", "target": "_id"}, {"source": "amount", "target": "amount"}],
            schema={"_id": "string", "amount": "decimal"},
            stream_contracts=[{"sync_mode": "cdc", "primary_key": "_id"}],
            job_id="cdc-mongo-test",
        )

    assert rows_written == 2
    assert dest_summary["cdc"]["inserts"] == 2
    assert any("change_stream" in line for line in ddl_log)


def test_query_cdc_restart_keeps_the_tie_break(tmp_path, monkeypatch):
    """Rows sharing updated_at must survive a restart.

    The page applied ids 1 and 2. Id 3 has the same timestamp and was not in
    that page. A cursor-only watermark would seek past the timestamp and drop
    it. The stored token is (updated_at, id), and the next poll seeks with it.
    """
    from services.keyset_pagination import split_cursor_bookmark
    from services.sync_cursor import get_watermark

    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "sync_cursors.json")
    source = EndpointConfig(kind="database", format="generic_sql", database="test", table="src")
    destination = EndpointConfig(kind="database", format="generic_sql", database="test", table="dst")
    headers = ["id", "updated_at"]
    page = [["1", "2024-01-01"], ["2", "2024-01-01"]]
    mock_read = MagicMock(side_effect=[(_batch(headers, page), None), (_batch(headers, []), None)])
    mock_write = MagicMock(return_value=(2, "abc", {}))

    with (
        patch("src.transfer.cdc_transfer._read_batch", mock_read),
        patch("src.transfer.cdc_transfer._write_batch", mock_write),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", return_value=0),
    ):
        rows_written, _ddl, summary, _columns = run_cdc_database_transfer(
            source,
            destination,
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "updated_at", "target": "updated_at"},
            ],
            schema={"id": "integer", "updated_at": "timestamp"},
            stream_contracts=[
                {
                    "sync_mode": "cdc",
                    "primary_key": "id",
                    "cursor_field": "updated_at",
                }
            ],
            job_id="cdc-tie",
        )

    assert rows_written == 2
    assert summary["cdc"]["inserts"] == 2
    stored = get_watermark("generic_sql:test:src→generic_sql:test:dst:stream")
    assert stored is not None
    cursor_part, pk_part = split_cursor_bookmark(stored, has_tiebreak=True)
    assert cursor_part == "2024-01-01"
    assert pk_part == "2"

    resume_calls: list[dict] = []

    def _resume_read(*_args, **kwargs):
        resume_calls.append(kwargs)
        return _batch(headers, [["3", "2024-01-01"]]), None

    engine = CdcEngine(
        src_cfg={"database": "test"},
        src_type="generic_sql",
        table_name="src",
        cursor_field="updated_at",
        primary_key="id",
        watermark=stored,
        columns=headers,
        batch_size=2,
    )
    with patch("src.transfer.cdc_transfer._read_batch", side_effect=_resume_read):
        resumed = list(engine.poll())
    assert resume_calls[0]["cursor_after"] == stored
    assert resume_calls[0]["cursor_primary_key"] == "id"
    assert [row["id"] for row in resumed[0].inserts] == ["3"]


def test_interrupted_offset_snapshot_does_not_publish_a_resume_cursor(tmp_path, monkeypatch):
    """Page 1 of an offset dump is not a resume cursor.

    Id 2 is on the next page. Publishing id 1 would make snapshot_mode=initial
    skip the rest of the table. A crash leaves no watermark, and the next run
    snapshots both rows.
    """
    from services.sync_cursor import get_watermark

    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "sync_cursors.json")
    orig_init = CdcEngine.__init__

    def _one_row_pages(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        self.batch_size = 1

    monkeypatch.setattr(CdcEngine, "__init__", _one_row_pages)
    source = EndpointConfig(kind="database", format="generic_sql", database="test", table="src")
    destination = EndpointConfig(kind="database", format="generic_sql", database="test", table="dst")
    headers = ["id", "value"]
    pages = {0: [["1", "a"]], 1: [["2", "b"]]}
    checkpoints: list[object] = []

    def _read(*args, **_kwargs):
        offset = args[4]
        return _batch(headers, pages.get(offset, [])), None

    writes = {"n": 0}

    def _write(*_args, **_kwargs):
        writes["n"] += 1
        if writes["n"] >= 2:
            raise RuntimeError("snapshot died on the second page")
        return 1, "abc", {}

    mock_write = MagicMock(side_effect=_write)
    cursor_key = "generic_sql:test:src→generic_sql:test:dst:stream"
    with (
        patch("src.transfer.cdc_transfer._read_batch", side_effect=_read),
        patch("src.transfer.cdc_transfer._write_batch", mock_write),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", return_value=0),
        pytest.raises(RuntimeError, match="second page"),
    ):
        run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "id", "target": "id"}, {"source": "value", "target": "value"}],
            schema={"id": "integer", "value": "string"},
            stream_contracts=[{"sync_mode": "cdc", "primary_key": "id", "cursor_field": "id"}],
            job_id="cdc-snap",
            on_checkpoint=lambda _chunk, _total, _rows, payload: checkpoints.append(
                payload.get("watermark")
            ),
        )
    assert get_watermark(cursor_key) is None
    assert checkpoints
    assert all(mark is None for mark in checkpoints)

    written: list[list] = []

    def _write_all(*args, **_kwargs):
        written.extend(args[5])
        return 1, "abc", {}

    with (
        patch("src.transfer.cdc_transfer._read_batch", side_effect=_read),
        patch("src.transfer.cdc_transfer._write_batch", side_effect=_write_all),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", return_value=0),
    ):
        rows_written, _ddl, _summary, _columns = run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "id", "target": "id"}, {"source": "value", "target": "value"}],
            schema={"id": "integer", "value": "string"},
            stream_contracts=[{"sync_mode": "cdc", "primary_key": "id", "cursor_field": "id"}],
            job_id="cdc-snap-2",
        )
    assert rows_written == 2
    assert [row[0] for row in written] == ["1", "2"]
    assert get_watermark(cursor_key) == "2"


def test_interrupted_log_snapshot_keeps_the_open_dump_token(tmp_path, monkeypatch):
    """A phase=snapshot page is a keyset resume, unlike an offset page.

    The crash stores table + last primary key and does not ack. The next run
    calls snapshot() with that token and then stores the streaming handoff.
    """
    import json

    from services.cdc_engine import ChangeBatch

    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr("services.sync_cursor._mongo_cursors", lambda: None)
    calls = {"n": 0, "acks": [], "resumes": []}

    class FakeCdc:
        def __init__(self, *args, **kwargs):
            self.resume = kwargs.get("resume_token")
            calls["resumes"].append(self.resume)

        def is_available(self):
            return True

        def snapshot(self):
            calls["n"] += 1
            if calls["n"] == 1:
                yield ChangeBatch(
                    inserts=[{"id": "1"}],
                    resume_token="slot=s|phase=snapshot|lsn=0/1|table=src|last_pk=1",
                    table="src",
                )
                raise RuntimeError("log snapshot died after the first page")
            assert "phase=snapshot" in str(self.resume)
            yield ChangeBatch(
                inserts=[{"id": "2"}],
                resume_token="slot=s|phase=snapshot|lsn=0/1|table=src|last_pk=2",
                table="src",
            )
            yield ChangeBatch(
                resume_token="slot=s|phase=streaming|lsn=0/1",
                table="src",
            )

        def poll(self):
            if False:
                yield ChangeBatch()

        def ack(self, token=None):
            calls["acks"].append(token)

        def close(self):
            pass

    source = EndpointConfig(
        kind="database", format="postgresql", database="app", table="src", schema="public"
    )
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "dst.db"), table="dst"
    )
    checkpoints: list[object] = []
    applied: list[str] = []

    def fake_apply(*args, **kwargs):
        change = args[4]
        applied.extend(row.get("id", "") for row in change.inserts)
        return (len(change.inserts), "ck", {}, 0)

    def stored() -> list[str]:
        path = tmp_path / "sync_cursors.json"
        if not path.exists():
            return []
        data = json.loads(path.read_text(encoding="utf-8"))
        return [str(entry.get("watermark") or "") for entry in data.get("cursors") or []]

    def run():
        return run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "id", "target": "id"}],
            schema={"id": "string"},
            stream_contracts=[{
                "name": "src",
                "sync_mode": "cdc",
                "primary_key": "id",
                "cursor_field": "id",
                "snapshot_mode": "initial",
            }],
            job_id="cdc-log-snap",
            on_checkpoint=lambda _chunk, _total, _rows, payload: checkpoints.append(
                payload.get("watermark")
            ),
        )

    with (
        patch("src.transfer.cdc_transfer.PostgreSqlChangeStreamCdc", FakeCdc),
        patch("src.transfer.cdc_transfer._apply_change_batch", side_effect=fake_apply),
        patch.dict("os.environ", {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "1"}),
    ):
        with pytest.raises(RuntimeError, match="log snapshot died after the first page"):
            run()
        assert calls["n"] == 1
        assert calls["acks"] == []
        assert applied == ["1"]
        assert any("phase=snapshot" in token and "last_pk=1" in token for token in stored())
        assert any("phase=snapshot" in str(mark) for mark in checkpoints)

        checkpoints.clear()
        rows_written, _ddl, summary, _columns = run()
        assert calls["n"] == 2
        assert "2" in applied
        assert rows_written >= 1
        final = str((summary.get("cdc") or {}).get("watermark") or "")
        assert "phase=streaming" in final
        assert calls["acks"]
        assert "phase=streaming" in str(calls["acks"][-1])


def test_checkpoint_does_not_rewind_a_stored_cdc_cursor(tmp_path, monkeypatch):
    """A throttled job checkpoint must not replace the cursor store.

    The store holds the keyset tie-break. The checkpoint still has the scalar
    cursor. After an idle poll the store is unchanged, and the read seeks
    with the tie-break.
    """
    from services.keyset_pagination import encode_keyset_bookmark
    from services.sync_cursor import get_watermark, set_watermark

    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr("services.sync_cursor._mongo_cursors", lambda: None)
    stored = encode_keyset_bookmark(["2024-01-01", "2"])
    cursor_key = "generic_sql:test:src→generic_sql:test:dst:stream"
    set_watermark(cursor_key, stored)
    seen: list[dict] = []

    def _read(*_args, **kwargs):
        seen.append(dict(kwargs))
        return _batch(["id", "updated_at"], []), None

    source = EndpointConfig(kind="database", format="generic_sql", database="test", table="src")
    destination = EndpointConfig(kind="database", format="generic_sql", database="test", table="dst")
    with (
        patch("src.transfer.cdc_transfer._read_batch", side_effect=_read),
        patch("src.transfer.cdc_transfer._write_batch", return_value=(0, "c", {})),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", return_value=0),
    ):
        run_cdc_database_transfer(
            source,
            destination,
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "updated_at", "target": "updated_at"},
            ],
            schema={"id": "integer", "updated_at": "string"},
            stream_contracts=[{
                "sync_mode": "cdc",
                "primary_key": "id",
                "cursor_field": "updated_at",
                "snapshot_mode": "initial",
            }],
            job_id="cdc-rewind",
            checkpoint={"watermark": "2024-01-01", "chunk_index": 2},
        )
    assert get_watermark(cursor_key) == stored
    assert seen
    assert seen[0].get("cursor_after") == stored
    assert seen[0].get("cursor_primary_key") == "id"


def test_empty_store_resumes_from_the_job_checkpoint(tmp_path, monkeypatch):
    """The cursor file is gone. The job checkpoint still has the position."""
    from services.checkpoint_service import Checkpoint
    from services.sync_cursor import get_watermark

    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr("services.sync_cursor._mongo_cursors", lambda: None)
    seen: list[dict] = []

    def _read(*_args, **kwargs):
        seen.append(dict(kwargs))
        return _batch(["id"], []), None

    source = EndpointConfig(kind="database", format="generic_sql", database="test", table="src")
    destination = EndpointConfig(kind="database", format="generic_sql", database="test", table="dst")
    with (
        patch("src.transfer.cdc_transfer._read_batch", side_effect=_read),
        patch("src.transfer.cdc_transfer._write_batch", return_value=(0, "c", {})),
        patch("src.transfer.cdc_transfer.delete_by_primary_keys", return_value=0),
    ):
        run_cdc_database_transfer(
            source,
            destination,
            mappings=[{"source": "id", "target": "id"}],
            schema={"id": "integer"},
            stream_contracts=[{
                "sync_mode": "cdc",
                "primary_key": "id",
                "cursor_field": "id",
                "snapshot_mode": "initial",
            }],
            job_id="cdc-recover",
            checkpoint=Checkpoint(job_id="cdc-recover", cursor_value="4"),
        )
    assert seen
    assert seen[0].get("cursor_after") == "4"
    assert get_watermark("generic_sql:test:src→generic_sql:test:dst:stream") == "4"


