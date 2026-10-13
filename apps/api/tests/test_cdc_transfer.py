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


def test_eos_uppercase_oracle_update_overwrites_lowercase_destination(tmp_path):
    import sqlite3

    from services.cdc_engine import ChangeBatch
    from src.transfer.cdc_transfer import _apply_change_batch

    db_path = tmp_path / "cdc_case_mismatch.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            'CREATE TABLE orders (id TEXT PRIMARY KEY, amount TEXT, "_df_lsn" TEXT)'
        )
        conn.execute(
            'INSERT INTO orders (id, amount, "_df_lsn") VALUES (?, ?, ?)',
            ("1", "10.00", "0/10"),
        )

    rows, _, _, _ = _apply_change_batch(
        dest_type="sqlite",
        destination=None,
        dest_cfg={"database": str(db_path)},
        dest_table="orders",
        change=ChangeBatch(
            updates=[{"ID": "1", "AMOUNT": "99"}],
            resume_token={"lsn": "0/20"},
        ),
        mappings=[
            {"source": "ID", "target": "id"},
            {"source": "AMOUNT", "target": "amount"},
        ],
        column_types={"ID": "string", "AMOUNT": "string"},
        headers=["ID", "AMOUNT"],
        pk_target_col=["id"],
        chunk_idx=0,
        total_chunks=1,
        delivery_guarantee="exactly_once",
        delivery_pinned=True,
        cursor_key="cdc-case-mismatch",
        stream_name="orders",
    )

    with sqlite3.connect(db_path) as conn:
        amount, lsn = conn.execute(
            'SELECT amount, "_df_lsn" FROM orders WHERE id = ?', ("1",)
        ).fetchone()

    assert rows == 1
    assert amount == "99"
    assert lsn == "0/20"


def test_sqlserver_cdc_classifier_preserves_uppercase_source_columns():
    from connectors.sqlserver_cdc_native import classify_mssql_cdc_rows

    inserts, updates, deletes = classify_mssql_cdc_rows(
        [{"__$operation": 4, "ID": "1", "AMOUNT": "99"}],
        primary_key="ID",
    )

    assert not inserts
    assert updates == [{"ID": "1", "AMOUNT": "99"}]
    assert not deletes


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


def _cdc_reads(monkeypatch, tmp_path):
    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr("services.sync_cursor._mongo_cursors", lambda: None)
    seen: list[dict] = []

    def _read(*args, **kwargs):
        table = args[2] if len(args) > 2 else kwargs.get("table")
        seen.append({"table": table, "cursor_after": kwargs.get("cursor_after")})
        return _batch(["id"], []), None

    return seen, _read


def _two_table_cdc(source_table: str = "orders"):
    source = EndpointConfig(
        kind="database", format="generic_sql", database="test", table=source_table
    )
    destination = EndpointConfig(
        kind="database", format="generic_sql", database="test", table="orders"
    )
    contracts = [
        {
            "name": "orders",
            "sync_mode": "cdc",
            "primary_key": "id",
            "cursor_field": "id",
            "snapshot_mode": "initial",
        },
        {
            "name": "users",
            "sync_mode": "cdc",
            "primary_key": "id",
            "cursor_field": "id",
            "snapshot_mode": "initial",
        },
    ]
    return source, destination, contracts


def test_sequential_checkpoint_does_not_seek_the_other_table(tmp_path, monkeypatch):
    """Orders' stored position must not become users' keyset seek.

    The job checkpoint is one cursor. Both per-table stores are empty.
    Users snapshots from the start.
    """
    seen, _read = _cdc_reads(monkeypatch, tmp_path)
    source, destination, contracts = _two_table_cdc()
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
            stream_contracts=contracts,
            job_id="cdc-two",
            checkpoint={"watermark": "4", "stream": "orders"},
        )
    by_table: dict[str, list] = {}
    for row in seen:
        by_table.setdefault(str(row["table"]), []).append(row["cursor_after"])
    assert "4" in by_table["orders"]
    assert all(cursor is None for cursor in by_table["users"])


def test_sequential_snapshot_token_stays_on_its_table(tmp_path, monkeypatch):
    """A phase=snapshot token names its table. The other table does not seek it."""
    seen, _read = _cdc_reads(monkeypatch, tmp_path)
    source, destination, contracts = _two_table_cdc()
    token = "slot=s|phase=snapshot|lsn=0/1|table=orders|last_pk=1"
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
            stream_contracts=contracts,
            job_id="cdc-snap-owner",
            checkpoint={"watermark": token},
        )
    users = [row["cursor_after"] for row in seen if row["table"] == "users"]
    orders = [row["cursor_after"] for row in seen if row["table"] == "orders"]
    assert users
    assert all(cursor is None or "last_pk=1" not in str(cursor) for cursor in users)
    assert token in orders


def test_unnamed_checkpoint_is_not_applied_to_every_table(tmp_path, monkeypatch):
    """A scalar with no stream name is not a cursor for both tables."""
    seen, _read = _cdc_reads(monkeypatch, tmp_path)
    source, destination, contracts = _two_table_cdc()
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
            stream_contracts=contracts,
            job_id="cdc-unnamed",
            checkpoint={"watermark": "4"},
        )
    assert seen
    assert all(row["cursor_after"] is None for row in seen)


def test_single_table_does_not_seek_a_shared_route_token(tmp_path, monkeypatch):
    """A shared streaming handoff names no table. One table must snapshot.

    Seeking that LSN would skip the dump. The rows already written are
    upserted again (at-least-once).
    """
    seen, _read = _cdc_reads(monkeypatch, tmp_path)
    source = EndpointConfig(kind="database", format="generic_sql", database="test", table="orders")
    destination = EndpointConfig(kind="database", format="generic_sql", database="test", table="orders")
    token = "slot=s|phase=streaming|lsn=0/1A"
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
                "name": "orders",
                "sync_mode": "cdc",
                "primary_key": "id",
                "cursor_field": "id",
                "snapshot_mode": "initial",
            }],
            job_id="cdc-one-route",
            checkpoint={"watermark": token, "cdc_shared_reader": True},
        )
    assert seen
    assert all(row["cursor_after"] is None for row in seen)
    assert all(token not in str(row["cursor_after"]) for row in seen)
