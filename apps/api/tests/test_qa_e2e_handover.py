"""Box E2E regressions from the studio-ui handover.

Each test is one of the seven defects that still failed on tip ddf9ddcf:
glob pickup, missing-file status, checksum reprocess, append Gate-8 sample,
lenient quarantine, schedule duplicate policy, and the enable/redact/folder/drift set.
"""

from __future__ import annotations

import io
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path

from connectors.sftp_reader import list_file_stats_on
from connectors.writer_common import transform_error_policy_for_validation_mode
from services.connector_store import SavedConnector, mask_connector
from services.duplicate_policy import collapse_keep_last
from services.file_load_ledger import record_file_loaded, stored_file_digest
from services.file_pickup import FileNotArrived, RemoteFile, plan_pickup, select_new_files
from services.object_streaming import download_sftp_object
from services.preflight_cursor_gate import build_sync_contract_gate
from services.preflight_policy_gates import apply_policy_gates
from src.transfer.connector_registry import humanize_connection_error


class _Attr:
    def __init__(self, filename: str, size: int, mtime: int) -> None:
        self.filename = filename
        self.st_size = size
        self.st_mtime = mtime
        self.st_mode = stat.S_IFREG


class _FakeSFTP:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = files
        self.got: list[str] = []

    def listdir_attr(self, path: str):
        prefix = "" if path in {"", ".", "/"} else path.rstrip("/") + "/"
        rows = []
        for name, payload in self.files.items():
            if not name.startswith(prefix):
                continue
            base = name[len(prefix) :]
            if "/" in base:
                continue
            rows.append(_Attr(base, len(payload), 100))
        return rows

    def open(self, path: str, _mode: str = "rb"):
        if path not in self.files:
            raise FileNotFoundError(2, "No such file")
        return io.BytesIO(self.files[path])

    def getfo(self, path: str, handle) -> None:
        self.got.append(path)
        if path not in self.files:
            raise FileNotFoundError(2, "No such file")
        handle.write(self.files[path])

    def close(self) -> None:
        return None


class _Transport:
    def close(self) -> None:
        return None


def test_sftp_glob_lists_stats_and_does_not_getfo_the_pattern(monkeypatch, tmp_path: Path):
    files = {
        "/drop/orders_1.csv": b"id\n1\n",
        "/drop/orders_2.csv": b"id\n2\n",
        "/drop/notes.txt": b"nope\n",
    }
    fake = _FakeSFTP(files)
    monkeypatch.setattr(
        "connectors.sftp_common.connect_sftp",
        lambda _cfg: (_Transport(), fake),
    )
    listed = list_file_stats_on(fake, "/drop", "orders_*.csv")
    assert [row["name"] for row in listed] == ["orders_1.csv", "orders_2.csv"]
    for row in listed:
        assert row["size"] > 0
        assert row["mtime"] == "100"
        assert len(row["checksum"]) == 64
    manifest = [dict(listed[0])]
    new = select_new_files(
        [RemoteFile(r["name"], r["size"], r["checksum"], r["mtime"]) for r in listed],
        manifest,
        "orders_*.csv",
    )
    assert [item.name for item in new] == ["orders_2.csv"]
    changed = RemoteFile(
        listed[0]["name"], listed[0]["size"], "f" * 64, listed[0]["mtime"]
    )
    assert select_new_files([changed], manifest, "orders_*.csv") == [changed]

    dest = tmp_path / "spill.csv"
    download_sftp_object(
        dest,
        {"host": "sftp.example", "username": "ada", "password": "secret"},
        "/drop",
        "orders_*.csv",
    )
    assert fake.got == ["/drop/orders_1.csv"]
    assert b"orders_*.csv" not in dest.read_bytes()
    assert dest.read_bytes() == b"id\n1\n"


def test_missing_sftp_file_says_expected_file_not_arrived(monkeypatch, tmp_path: Path):
    fake = _FakeSFTP({})
    monkeypatch.setattr(
        "connectors.sftp_common.connect_sftp",
        lambda _cfg: (_Transport(), fake),
    )
    try:
        download_sftp_object(
            tmp_path / "missing.csv",
            {"host": "sftp.example", "username": "ada", "password": "secret"},
            "/drop",
            "orders.csv",
        )
    except FileNotArrived as exc:
        assert "expected file not arrived" in str(exc)
        assert "[Errno 2]" not in str(exc)
    else:
        raise AssertionError("missing file must raise FileNotArrived")
    waiting = plan_pickup(
        pattern="orders.csv",
        files=[],
        manifest=[],
        wait_seconds=120,
        waited_seconds=0,
    )
    assert waiting.retryable is True
    assert "expected file not arrived" in waiting.message


def test_checksum_change_is_visible_and_file_upsert_does_not_require_a_cursor(
    monkeypatch, tmp_path: Path
):
    import services.file_load_ledger as ledger

    monkeypatch.setattr(ledger, "STORE_PATH", tmp_path / "ledger.json")
    monkeypatch.setattr(ledger, "_mongo_ledger", lambda: None)
    record_file_loaded("route-1", "abc")
    assert stored_file_digest("route-1") == "abc"
    assert stored_file_digest("route-1") != "def"

    gate = build_sync_contract_gate(
        [{"name": "orders", "primary_key": "id", "selected": True}],
        sync="incremental_deduped",
        validation="strict",
        dest="postgresql",
        src="sftp",
        kind="file",
        source_columns=["id", "amount"],
        pass_status="pass",
        block_status="block",
    )
    assert gate["status"] == "pass", gate
    assert gate["details"]["requires_cursor"] is False
    assert "cursor" not in gate["message"].lower()

    database = build_sync_contract_gate(
        [{"name": "orders", "primary_key": "id", "selected": True}],
        sync="incremental_deduped",
        validation="strict",
        dest="postgresql",
        src="postgresql",
        kind="database",
        source_columns=["id", "amount"],
        pass_status="pass",
        block_status="block",
    )
    assert database["status"] == "block"
    assert "cursor" in database["message"].lower()


def test_append_copy_stashes_reconcile_sample(monkeypatch, tmp_path: Path):
    import sqlite3

    from src.transfer.file_stream import stream_file_to_database
    from src.transfer.models import EndpointConfig

    db = tmp_path / "dest.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE landing (id INTEGER, name TEXT)")

    def fake_copy(**_kwargs):
        return (
            2,
            [],
            {"streaming": True, "rows_written": 2},
            ["id", "name"],
        )

    monkeypatch.setattr("services.copy_csv_local.try_copy_local_csv", fake_copy)
    _written, _ddl, summary, _columns = stream_file_to_database(
        b"id,name\n1,ada\n2,grace\n",
        "orders.csv",
        EndpointConfig(kind="database", format="sqlite", database=str(db), table="landing"),
        [
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "name", "target": "name", "confidence": 0.99},
        ],
        {"id": "integer", "name": "string"},
        sync_mode="full_refresh_append",
        skip_preflight=True,
    )
    assert summary.get("reconcile_sample"), summary
    assert summary["reconcile_sample"][0]["id"] in {1, "1"}


def test_lenient_quarantines_instead_of_blocking_the_load():
    from services.data_integrity import run_integrity_audit

    assert transform_error_policy_for_validation_mode("lenient") == "quarantine"
    assert transform_error_policy_for_validation_mode("strict") == "fail"
    report = run_integrity_audit(
        mappings=[{"source": "qty", "target": "qty", "confidence": 0.99, "transform": "integer"}],
        source_columns=["qty"],
        source_schemas=[{"name": "qty", "inferred_type": "VARCHAR"}],
        sample_rows=[{"qty": "1"}, {"qty": "nope"}],
        validation_mode="lenient",
        destination_db_type="postgresql",
    )
    assert report["blocks_transfer"] is False
    assert report["validation_mode"] == "lenient"


def test_schedule_duplicate_policy_and_in_file_keep_last(tmp_path, monkeypatch):
    from services import schedule_store
    from services.schedule_runner import build_schedule_request

    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    sched = schedule_store.create_schedule(
        {
            "name": "orders-drop",
            "source_connector_id": "sftp-1",
            "source_table": "orders_*.csv",
            "dest_connector_id": "pg-1",
            "dest_table": "orders",
            "interval": "daily",
            "enabled": False,
            "primary_key": "id",
            "in_file_dedupe": "keep_last",
            "mappings": [{"source": "id", "target": "id", "confidence": 1}],
        }
    )
    src = {"type": "sftp", "host": "sftp.example", "database": "/drop"}
    dst = {"type": "postgresql", "host": "db.example", "database": "app"}
    request = build_schedule_request(sched, src, dst)
    assert request.source.extra["duplicate_policy"] == "upsert"
    assert request.destination.extra["in_file_dedupe"] == "keep_last"
    kept, dropped = collapse_keep_last(
        [{"id": "1", "amount": "1"}, {"id": "1", "amount": "9"}, {"id": "2", "amount": "3"}],
        ["id"],
    )
    assert dropped == 1
    assert kept == [{"id": "1", "amount": "9"}, {"id": "2", "amount": "3"}]

    schedule_store.update_schedule(sched.id, {"duplicate_policy": "append"})
    appended = build_schedule_request(schedule_store.get_schedule(sched.id), src, dst)
    assert appended.source.extra["duplicate_policy"] == "append"


def test_enable_waits_password_is_redacted_wrong_folder_is_not_auth_and_drift_is_once(
    tmp_path, monkeypatch
):
    from services import schedule_store

    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    sched = schedule_store.create_schedule(
        {
            "name": "nightly-orders",
            "source_connector_id": "sftp-1",
            "source_table": "orders_*.csv",
            "dest_connector_id": "pg-1",
            "dest_table": "orders",
            "interval": "daily",
            "cron": "0 6 * * *",
            "timezone": "UTC",
            "enabled": False,
            "primary_key": "id",
            "mappings": [{"source": "id", "target": "id", "confidence": 1}],
        }
    )
    schedule_store.update_schedule(
        sched.id, {"next_run_at": "2020-01-01T00:00:00+00:00"}
    )
    enabled = schedule_store.update_schedule(sched.id, {"enabled": True})
    assert enabled is not None and enabled.enabled is True
    due = datetime.fromisoformat(enabled.next_run_at)
    if due.tzinfo is None:
        due = due.replace(tzinfo=timezone.utc)
    assert due > datetime.now(timezone.utc)
    assert due > datetime.now(timezone.utc) - timedelta(seconds=1)
    assert due.year >= 2026

    saved = SavedConnector(
        id="c1",
        name="sftp",
        type="sftp",
        role="source",
        host="sftp.example",
        password="s3cret-plain",
        connection_string="sftp://ada:s3cret-plain@sftp.example/drop",
        extra={"password": "s3cret-plain", "host_key": "SHA256:abc"},
    )
    masked = mask_connector(saved)
    blob = str(masked)
    assert "s3cret-plain" not in blob
    assert masked["password"] == "****"
    assert masked["extra"]["password"] == "****"
    assert masked["extra"]["host_key"] == "SHA256:abc"

    folder = humanize_connection_error(
        "sftp", "SFTP authenticated but path not found: /missing-drop"
    )
    assert "Authentication failed" not in folder
    assert "path not found" in folder.lower()
    errno = humanize_connection_error("sftp", "[Errno 2] No such file")
    assert "Authentication failed" not in errno
    still_auth = humanize_connection_error(
        "sftp", "Authentication failed: password rejected for alice"
    )
    assert "Authentication failed" in still_auth

    result = apply_policy_gates(
        {
            "blockers": [
                {
                    "id": "schema_drift",
                    "message": "Schema change detected — policy 'manual_review' requires review",
                }
            ],
            "gates": [
                {
                    "id": "schema_drift",
                    "status": "block",
                    "message": "Schema change detected — policy 'manual_review' requires review",
                }
            ],
            "passed": False,
        },
        [
            {
                "id": "schema_drift",
                "status": "block",
                "message": "Schema drift: column amount changed",
            }
        ],
    )
    drift_messages = [
        b["message"]
        for b in result["blockers"]
        if b.get("id") == "schema_drift" or "schema" in str(b.get("message")).lower()
    ]
    assert len(drift_messages) == 1
