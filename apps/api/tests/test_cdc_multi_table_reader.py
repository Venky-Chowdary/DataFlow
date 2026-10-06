"""Multi-table single-reader CDC (Debezium-class demux) proofs."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc, encode_pg_resume_token
from services.cdc_engine import ChangeBatch
from services.cdc_multi_table import (
    MultiTableTransactionBuffer,
    can_share_log_reader,
    normalize_table_list,
    shared_route_cursor_key,
    tables_digest,
)


def test_normalize_and_digest_stable() -> None:
    assert normalize_table_list(["Users", "orders", "users"]) == ["Users", "orders"]
    assert tables_digest(["b", "a"]) == tables_digest(["a", "b"])
    assert can_share_log_reader("postgresql", 2) is True
    assert can_share_log_reader("mysql", 2) is True
    assert can_share_log_reader("sqlserver", 2) is True
    assert can_share_log_reader("mssql", 2) is True
    assert can_share_log_reader("oracle", 2) is True
    assert can_share_log_reader("mongodb", 2) is False
    assert can_share_log_reader("postgresql", 1) is False
    assert can_share_log_reader("sqlserver", 1) is False
    assert can_share_log_reader("oracle", 1) is False


def test_multi_table_txn_buffer_demux_and_ack_barrier() -> None:
    buf = MultiTableTransactionBuffer()
    buf.begin("42", lsn="0/1")
    buf.insert("orders", {"id": "1"}, lsn="0/2")
    buf.insert("users", {"id": "u1"}, lsn="0/3")
    buf.update("orders", {"id": "1", "amount": "9"}, lsn="0/4")
    batches = buf.commit(lsn="0/5", resume_token="slot=x|phase=streaming|lsn=0/5", table_order=["orders", "users"])
    assert len(batches) == 2
    assert batches[0].table == "orders"
    assert batches[0].ack_barrier is False
    # insert+update same PK nets to one insert (recreation-safe coalesce).
    assert len(batches[0].inserts) == 1 and batches[0].updates == []
    assert batches[0].inserts[0]["amount"] == "9"
    assert batches[1].table == "users"
    assert batches[1].ack_barrier is True
    assert batches[0].resume_token == batches[1].resume_token


def test_pg_shared_reader_slot_and_publication_names() -> None:
    cdc = PostgreSqlChangeStreamCdc(
        {"database": "app", "job_id": "j1"},
        table=["orders", "users"],
        primary_key="id",
        primary_keys={"orders": "id", "users": "user_id"},
        cursor_key="cdc-shared:postgresql:app:x:j1",
        output_plugin="test_decoding",
    )
    assert cdc.tables == ["orders", "users"]
    assert "mt_" in cdc.slot_name
    assert cdc.primary_keys["users"] == "user_id"
    assert cdc._lease.meta.get("shared_reader") is True


def test_pg_shared_poll_demuxes_two_tables() -> None:
    cdc = PostgreSqlChangeStreamCdc(
        {"database": "test", "schema": "public"},
        table=["orders", "users"],
        primary_key="id",
        cursor_key="shared-ck",
        output_plugin="test_decoding",
        resume_token=encode_pg_resume_token("df_test_mt", lsn="0/1000", phase="streaming"),
    )
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = [
        ("0/16B3700", "BEGIN 9"),
        ("0/16B3710", "table public.orders: INSERT: id[int4]:1 amount[numeric]:10"),
        ("0/16B3720", "table public.users: INSERT: id[int4]:2 name[text]:'a'"),
        ("0/16B3748", "COMMIT"),
    ]
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("connectors.postgresql_change_stream.get_connection", return_value=conn), \
         patch.object(cdc, "_ensure_slot", return_value="0/1000"), \
         patch.object(cdc, "_ensure_decode_schema", return_value={}), \
         patch.object(cdc, "_poll_signal_table"), \
         patch("services.cdc_incremental_runner.interleave_incremental_snapshot", return_value=iter(())):
        batches = list(cdc.poll())

    tagged = [b for b in batches if b.total_changes]
    assert len(tagged) == 2
    by_table = {b.table: b for b in tagged}
    assert "orders" in by_table and "users" in by_table
    assert by_table["orders"].inserts[0]["id"] == "1"
    assert by_table["users"].inserts[0]["id"] == "2"
    assert tagged[-1].ack_barrier is True
    assert tagged[0].ack_barrier is False


def test_shared_transfer_path_applies_per_table(tmp_path, monkeypatch) -> None:
    from src.transfer.cdc_transfer import _run_cdc_shared_multi_table
    from src.transfer.models import EndpointConfig
    from services.sync_cursor import SyncContract

    class FakeCdc:
        def __init__(self, *a, **k):
            self.closed = False

        def is_available(self):
            return True

        def snapshot(self):
            yield ChangeBatch(
                inserts=[{"id": "1"}],
                resume_token="slot=s|phase=snapshot|lsn=0/1",
                table="orders",
            )
            yield ChangeBatch(
                inserts=[{"id": "u1"}],
                resume_token="slot=s|phase=snapshot|lsn=0/1",
                table="users",
            )
            yield ChangeBatch(
                resume_token="slot=s|phase=streaming|lsn=0/1",
                ack_barrier=True,
            )

        def poll(self):
            yield ChangeBatch(
                updates=[{"id": "1", "amount": "2"}],
                resume_token="slot=s|phase=streaming|lsn=0/2",
                table="orders",
                ack_barrier=False,
            )
            yield ChangeBatch(
                inserts=[{"id": "u2"}],
                resume_token="slot=s|phase=streaming|lsn=0/2",
                table="users",
                ack_barrier=True,
            )
            return
            yield  # pragma: no cover

        def ack(self, token=None):
            self.acked = token

        def close(self):
            self.closed = True

    source = EndpointConfig(
        kind="database", format="postgresql", database="app", table="orders", schema="public"
    )
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "dst.db"), table="orders"
    )
    selected = [
        SyncContract(name="orders", primary_key="id", sync_mode="cdc"),
        SyncContract(name="users", primary_key="id", sync_mode="cdc"),
    ]
    applied: list[str] = []

    def fake_apply(*args, **kwargs):
        change = args[4]
        applied.append(change.table or "")
        return (len(change.inserts) + len(change.updates), "ck", {}, len(change.deletes))

    with patch("src.transfer.cdc_transfer.PostgreSqlChangeStreamCdc", FakeCdc), \
         patch("src.transfer.cdc_transfer._apply_change_batch", side_effect=fake_apply), \
         patch("src.transfer.cdc_transfer.resolve_dest_table", side_effect=lambda *_a, **_k: "t"), \
         patch.dict("os.environ", {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "2"}):
        rows, ddl, summary, _ = _run_cdc_shared_multi_table(
            source,
            destination,
            [{"source": "id", "target": "id"}],
            {"id": "string"},
            None,
            sync_mode="cdc",
            stream_contracts=[
                {"name": "orders", "selected": True, "primary_key": "id"},
                {"name": "users", "selected": True, "primary_key": "id"},
            ],
            selected=selected,
            job_id="job-shared",
            checkpoint=None,
            checkpoint_service=None,
            backfill_new_fields=False,
            validation_mode="strict",
            limit=0,
        )

    assert rows >= 4
    assert "shared_reader" in ddl[0]
    assert summary.get("cdc_shared_reader") is True
    assert "orders" in applied and "users" in applied
    route_key = shared_route_cursor_key(
        engine="postgresql", database="app", tables=["orders", "users"],
        dest_type="sqlite", dest_database="/tmp/d.db",
    )
    assert route_key.startswith("cdc-shared:")
    assert "job" not in route_key  # slot derives from the route, not the run


def test_interrupted_shared_snapshot_still_dumps_the_unread_table(tmp_path, monkeypatch) -> None:
    """A crash after table A's page must not mark the route snapshot-complete.

    The stored token stays phase=snapshot (table + last primary key). The next
    run calls snapshot() again so the unread table is dumped. A streaming
    token after the dump finishes does not start another snapshot.
    """
    import json

    import pytest

    from src.transfer.cdc_transfer import _run_cdc_shared_multi_table
    from src.transfer.models import EndpointConfig
    from services.sync_cursor import SyncContract

    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr("services.sync_cursor._mongo_cursors", lambda: None)

    calls = {"snapshot_calls": 0, "resumes": []}
    applied: list[str] = []
    checkpoints: list[Any] = []

    class FakeCdc:
        def __init__(self, *a, **k):
            self.resume = k.get("resume_token")
            calls["resumes"].append(self.resume)

        def is_available(self):
            return True

        def snapshot(self):
            calls["snapshot_calls"] += 1
            if calls["snapshot_calls"] == 1:
                yield ChangeBatch(
                    inserts=[{"id": "1"}],
                    resume_token="slot=s|phase=snapshot|lsn=0/1|table=orders|last_pk=1",
                    table="orders",
                )
                raise RuntimeError("snapshot died after the orders page")
            assert "phase=snapshot" in str(self.resume)
            yield ChangeBatch(
                inserts=[{"id": "9"}],
                resume_token="slot=s|phase=snapshot|lsn=0/1|table=users|last_pk=9",
                table="users",
            )
            yield ChangeBatch(
                resume_token="slot=s|phase=streaming|lsn=0/1",
                ack_barrier=True,
            )

        def poll(self):
            calls["polls"] = int(calls.get("polls") or 0) + 1
            if False:
                yield ChangeBatch()

        def ack(self, token=None):
            calls.setdefault("acks", []).append(token)

        def close(self):
            pass

    source = EndpointConfig(
        kind="database", format="postgresql", database="app", table="orders", schema="public"
    )
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "dst.db"), table="orders"
    )
    selected = [
        SyncContract(name="orders", primary_key="id", sync_mode="cdc"),
        SyncContract(name="users", primary_key="id", sync_mode="cdc"),
    ]

    def fake_apply(*args, **kwargs):
        change = args[4]
        applied.append(change.table or "")
        return (len(change.inserts) + len(change.updates), "ck", {}, len(change.deletes))

    def run():
        return _run_cdc_shared_multi_table(
            source,
            destination,
            [{"source": "id", "target": "id"}],
            {"id": "string"},
            lambda _chunk, _total, _rows, payload: checkpoints.append(payload.get("watermark")),
            sync_mode="cdc",
            stream_contracts=[
                {"name": "orders", "selected": True, "primary_key": "id", "snapshot_mode": "initial"},
                {"name": "users", "selected": True, "primary_key": "id", "snapshot_mode": "initial"},
            ],
            selected=selected,
            job_id="job-shared-crash",
            checkpoint=None,
            checkpoint_service=None,
            backfill_new_fields=False,
            validation_mode="strict",
            limit=0,
        )

    def stored() -> list[str]:
        path = tmp_path / "cursors.json"
        if not path.exists():
            return []
        data = json.loads(path.read_text(encoding="utf-8"))
        return [str(entry.get("watermark") or "") for entry in data.get("cursors") or []]

    with patch("src.transfer.cdc_transfer.PostgreSqlChangeStreamCdc", FakeCdc), \
         patch("src.transfer.cdc_transfer._apply_change_batch", side_effect=fake_apply), \
         patch("src.transfer.cdc_transfer.resolve_dest_table", side_effect=lambda *_a, **_k: "t"), \
         patch.dict("os.environ", {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "1"}):
        with pytest.raises(RuntimeError, match="snapshot died after the orders page"):
            run()
        assert calls["snapshot_calls"] == 1
        assert applied == ["orders"]
        assert any("phase=snapshot" in token and "table=orders" in token for token in stored())
        assert checkpoints
        assert all(
            token is None or "phase=snapshot" in str(token) for token in checkpoints
        )
        assert any("phase=snapshot" in str(token) for token in checkpoints)

        checkpoints.clear()
        _rows, _ddl, summary, _errs = run()
        assert calls["snapshot_calls"] == 2
        assert "users" in applied
        assert any("phase=streaming" in token for token in stored())
        assert summary.get("cdc", {}).get("watermark")
        assert "phase=streaming" in str(summary["cdc"]["watermark"])

        before = calls["snapshot_calls"]
        run()
        assert calls["snapshot_calls"] == before


def test_sqlserver_shared_snapshot_crash_keeps_the_open_dump(tmp_path, monkeypatch) -> None:
    """SQL Server snapshot pages are not a log barrier, and they are a resume.

    ack_barrier stays false, so the page is not acked. The phase=snapshot
    token is still stored. The next run continues snapshot() at last_pk
    instead of re-reading every table from the start.
    """
    import json

    import pytest

    from connectors.sqlserver_cdc_native import encode_mssql_cdc_token
    from src.transfer.cdc_transfer import _run_cdc_shared_multi_table
    from src.transfer.models import EndpointConfig
    from services.sync_cursor import SyncContract

    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr("services.sync_cursor._mongo_cursors", lambda: None)

    page = encode_mssql_cdc_token(
        "0000001a", table="orders", phase="snapshot", offset=1, last_pk="1"
    )
    calls = {"snapshot_calls": 0, "acks": [], "resumes": []}
    applied: list[str] = []

    class FakeCdc:
        def __init__(self, *args, **kwargs):
            self.resume = kwargs.get("resume_token")
            calls["resumes"].append(self.resume)

        def is_available(self):
            return True

        def snapshot(self):
            calls["snapshot_calls"] += 1
            if calls["snapshot_calls"] == 1:
                yield ChangeBatch(
                    inserts=[{"id": "1"}],
                    resume_token=page,
                    table="orders",
                    ack_barrier=False,
                )
                raise RuntimeError("sqlserver snapshot died after orders")
            assert "snapshot" in str(self.resume)
            assert "orders" in str(self.resume)
            yield ChangeBatch(
                inserts=[{"id": "9"}],
                resume_token=encode_mssql_cdc_token(
                    "0000001a", table="users", phase="snapshot", offset=1, last_pk="9"
                ),
                table="users",
                ack_barrier=False,
            )
            yield ChangeBatch(
                resume_token=encode_mssql_cdc_token(
                    "0000001a", table="orders,users", phase="streaming"
                ),
                ack_barrier=True,
            )

        def poll(self):
            if False:
                yield ChangeBatch()

        def ack(self, token=None):
            calls["acks"].append(token)

        def close(self):
            pass

    source = EndpointConfig(
        kind="database", format="sqlserver", database="app", table="orders", schema="dbo"
    )
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "dst.db"), table="orders"
    )
    selected = [
        SyncContract(name="orders", primary_key="id", sync_mode="cdc"),
        SyncContract(name="users", primary_key="id", sync_mode="cdc"),
    ]

    def fake_apply(*args, **kwargs):
        change = args[4]
        applied.append(change.table or "")
        return (len(change.inserts) + len(change.updates), "ck", {}, len(change.deletes))

    def run():
        return _run_cdc_shared_multi_table(
            source,
            destination,
            [{"source": "id", "target": "id"}],
            {"id": "string"},
            None,
            sync_mode="cdc",
            stream_contracts=[
                {"name": "orders", "selected": True, "primary_key": "id", "snapshot_mode": "initial"},
                {"name": "users", "selected": True, "primary_key": "id", "snapshot_mode": "initial"},
            ],
            selected=selected,
            job_id="job-ss-crash",
            checkpoint=None,
            checkpoint_service=None,
            backfill_new_fields=False,
            validation_mode="strict",
            limit=0,
        )

    def stored() -> list[str]:
        path = tmp_path / "cursors.json"
        if not path.exists():
            return []
        data = json.loads(path.read_text(encoding="utf-8"))
        return [str(entry.get("watermark") or "") for entry in data.get("cursors") or []]

    with patch("src.transfer.cdc_transfer.SqlServerNativeCdc", FakeCdc), \
         patch("src.transfer.cdc_transfer._apply_change_batch", side_effect=fake_apply), \
         patch("src.transfer.cdc_transfer.resolve_dest_table", side_effect=lambda *_a, **_k: "t"), \
         patch.dict("os.environ", {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "1"}):
        with pytest.raises(RuntimeError, match="sqlserver snapshot died after orders"):
            run()
        assert calls["snapshot_calls"] == 1
        assert calls["acks"] == []
        assert applied == ["orders"]
        assert any('"phase":"snapshot"' in token and "orders" in token for token in stored())

        _rows, _ddl, summary, _errs = run()
        assert calls["snapshot_calls"] == 2
        assert "users" in applied
        assert "phase" in str(summary.get("cdc", {}).get("watermark") or "")
        assert "streaming" in str(summary["cdc"]["watermark"])
        assert calls["acks"]
        assert "streaming" in str(calls["acks"][-1])
