"""SFTP daily-Excel: real workbook in, real workbook out, existing-table sync modes.

CSV over SFTP was already transfer-live. Daily Excel was started and not
finished: ingest failed because object-store spill is ``.tmp`` and openpyxl
keys format off that suffix, and dest ``.xlsx`` was silently rewritten to
CSV (``out.xlsx.csv``) — D11. This file proves both halves against the
in-process SFTP server, into an existing table, under overwrite / append /
upsert. Dest COUNT is ``count_excel_rows`` on disk, never the writer's ack.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.object_store_common import (  # noqa: E402
    normalize_object_base_key,
    resolve_object_store_export_format,
)
from services.excel_parser import count_excel_rows, iter_excel_dicts  # noqa: E402
from services.format_converter import _write_excel_bytes  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from src.transfer.registry import PRODUCTION_SKU, validate_transfer  # noqa: E402
from tests.helpers.live_env import pg_creds, pg_up  # noqa: E402

COLUMNS = ["id", "amount", "flag"]
DAY1 = [["1", "10.50", "yes"], ["2", "20.25", "no"]]
DAY2 = [["1", "10.50", "yes"], ["2", "21.00", "no"], ["3", "5.00", "yes"]]


def _xlsx(rows: list[list[str]], columns: list[str] = COLUMNS) -> bytes:
    body, _mime = _write_excel_bytes(columns, rows)
    return body


def _sftp_endpoint(server, remote_path: str) -> EndpointConfig:
    cfg = server.endpoint_config(remote_path)
    return EndpointConfig(
        kind="database",
        format="sftp",
        host=cfg["host"],
        port=cfg["port"],
        username=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        table=cfg["table"],
        extra={"host_key": cfg["host_key"]},
    )


def _mappings(*columns: str) -> list[dict]:
    return [{"source": c, "target": c, "confidence": 0.99} for c in columns]


def _run(source: EndpointConfig, destination: EndpointConfig, **kwargs):
    request = TransferRequest(
        source=source,
        destination=destination,
        skip_preflight=True,
        validation_mode="strict",
        **kwargs,
    )
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def _pg_endpoint(table: str) -> EndpointConfig:
    creds = pg_creds()
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host=str(creds["host"]),
        port=int(creds["port"]),
        database=str(creds["database"]),
        username=str(creds["username"]),
        password=str(creds["password"]),
        schema="public",
        table=table,
    )


def _pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    creds = pg_creds()
    try:
        conn = psycopg2.connect(
            host=str(creds["host"]),
            port=int(creds["port"]),
            dbname=str(creds["database"]),
            user=str(creds["username"]),
            password=str(creds["password"]),
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    conn.autocommit = True
    return conn


def test_object_store_keeps_xlsx_key() -> None:
    assert resolve_object_store_export_format("daily.xlsx") == "excel"
    assert resolve_object_store_export_format("export.csv") == "csv"
    assert resolve_object_store_export_format("notes.txt") is None
    assert normalize_object_base_key("exports/daily.xlsx") == "exports/daily.xlsx"


def test_load_workbook_from_spill_tmp(tmp_path: Path) -> None:
    from services.excel_parser import _load_workbook

    spill = tmp_path / "cache-key.tmp"
    spill.write_bytes(_xlsx(DAY1))
    wb = _load_workbook(spill)
    try:
        rows = list(wb.active.iter_rows(values_only=True))
    finally:
        wb.close()
    assert rows[0] == ("id", "amount", "flag")
    assert [str(r[0]) for r in rows[1:]] == ["1", "2"]


def test_sftp_excel_is_a_live_route() -> None:
    ok, msg = validate_transfer("file", "excel", "database", "sftp")
    assert ok, msg
    ok, msg = validate_transfer("database", "sftp", "database", "postgresql")
    assert ok, msg
    assert ("file", "excel", "database", "sftp") in PRODUCTION_SKU


def test_sftp_dest_xlsx_is_a_workbook_not_csv(local_sftp) -> None:
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    result = _run(
        EndpointConfig(kind="file", format="csv"),
        _sftp_endpoint(local_sftp, "/out.xlsx"),
        source_content=b"id,amount,flag\n1,10.50,yes\n2,20.25,no\n",
        source_filename="in.csv",
        mappings=_mappings(*COLUMNS),
        sync_mode="full_refresh_overwrite",
    )
    assert result.success is True, result.error
    path = Path(local_sftp.local_path("/out.xlsx"))
    assert path.exists()
    assert not Path(local_sftp.local_path("/out.xlsx.csv")).exists()
    raw = path.read_bytes()
    assert raw[:2] == b"PK"
    assert count_excel_rows(path) == 2
    rows = list(iter_excel_dicts(path))
    assert [r["flag"] for r in rows] == ["yes", "no"]
    assert [r["amount"] for r in rows] == ["10.50", "20.25"]


def test_sftp_dest_empty_xlsx_is_still_a_workbook(local_sftp) -> None:
    """Empty population is still OOXML — dest COUNT is a measured 0 (D11)."""
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    from connectors.sftp_writer import write_mapped_rows

    cfg = local_sftp.endpoint_config("/empty.xlsx")
    written = write_mapped_rows(
        host=cfg["host"],
        port=cfg["port"],
        username=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        table_name=cfg["table"],
        headers=COLUMNS,
        data_rows=[],
        mappings=_mappings(*COLUMNS),
        column_types={c: "TEXT" for c in COLUMNS},
        host_key=cfg["host_key"],
    )
    assert written.ok is True, written.error
    path = Path(local_sftp.local_path("/empty.xlsx"))
    assert path.read_bytes()[:2] == b"PK"
    assert count_excel_rows(path) == 0


def test_sftp_dest_unknown_suffix_refuses_csv_rewrite(local_sftp) -> None:
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    result = _run(
        EndpointConfig(kind="file", format="csv"),
        _sftp_endpoint(local_sftp, "/out.fwf"),
        source_content=b"id,amount\n1,10.50\n",
        source_filename="in.csv",
        mappings=_mappings("id", "amount"),
        sync_mode="full_refresh_overwrite",
    )
    assert result.success is False
    assert "refusing to write CSV" in (result.error or "")
    assert not Path(local_sftp.local_path("/out.fwf.csv")).exists()


def test_sftp_xlsx_to_sqlite_existing_overwrite_and_append(local_sftp) -> None:
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    Path(local_sftp.local_path("/daily.xlsx")).write_bytes(_xlsx(DAY1))
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "ledger.db")
        conn = sqlite3.connect(db)
        conn.execute("CREATE TABLE ledger (id TEXT, amount TEXT, flag TEXT)")
        conn.execute("INSERT INTO ledger VALUES ('0', '0.00', 'seed')")
        conn.commit()
        conn.close()
        dest = EndpointConfig(
            kind="database",
            format="sqlite",
            connection_string=db,
            database=db,
            table="ledger",
        )
        over = _run(
            _sftp_endpoint(local_sftp, "/daily.xlsx"),
            dest,
            mappings=_mappings(*COLUMNS),
            sync_mode="full_refresh_overwrite",
        )
        assert over.success is True, over.error
        back = sqlite3.connect(db)
        try:
            n = back.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
            flags = [r[0] for r in back.execute("SELECT flag FROM ledger ORDER BY id")]
        finally:
            back.close()
        assert n == 2
        assert flags == ["yes", "no"]

        Path(local_sftp.local_path("/daily.xlsx")).write_bytes(_xlsx(DAY1))
        ap = _run(
            _sftp_endpoint(local_sftp, "/daily.xlsx"),
            dest,
            mappings=_mappings(*COLUMNS),
            sync_mode="full_refresh_append",
        )
        assert ap.success is True, ap.error
        back = sqlite3.connect(db)
        try:
            n = back.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
        finally:
            back.close()
        assert n == 4


def test_sftp_xlsx_to_sqlite_upsert_updates_existing(local_sftp) -> None:
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    Path(local_sftp.local_path("/daily.xlsx")).write_bytes(_xlsx(DAY1))
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "ledger.db")
        dest = EndpointConfig(
            kind="database",
            format="sqlite",
            connection_string=db,
            database=db,
            table="ledger",
        )
        first = _run(
            _sftp_endpoint(local_sftp, "/daily.xlsx"),
            dest,
            mappings=_mappings(*COLUMNS),
            sync_mode="full_refresh_overwrite",
        )
        assert first.success is True, first.error
        Path(local_sftp.local_path("/daily.xlsx")).write_bytes(_xlsx(DAY2))
        second = _run(
            _sftp_endpoint(local_sftp, "/daily.xlsx"),
            dest,
            mappings=_mappings(*COLUMNS),
            sync_mode="upsert",
            stream_contracts=[
                {
                    "name": "ledger",
                    "selected": True,
                    "sync_mode": "upsert",
                    "primary_key": "id",
                }
            ],
        )
        assert second.success is True, second.error
        back = sqlite3.connect(db)
        try:
            rows = list(back.execute("SELECT id, amount, flag FROM ledger ORDER BY id"))
        finally:
            back.close()
        assert [str(r[0]) for r in rows] == ["1", "2", "3"]
        assert [str(r[1]) for r in rows] == ["10.50", "21.00", "5.00"]


@pytest.mark.skipif(not pg_up(), reason="Postgres not authenticated")
def test_sftp_xlsx_to_existing_postgres_overwrite(local_sftp) -> None:
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    Path(local_sftp.local_path("/daily.xlsx")).write_bytes(_xlsx(DAY1))
    table = f"sftp_xlsx_{uuid.uuid4().hex[:10]}"
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{table}" (id TEXT PRIMARY KEY, amount TEXT, flag TEXT)'
            )
            cur.execute(f"INSERT INTO \"{table}\" VALUES ('0', '0.00', 'seed')")
        result = _run(
            _sftp_endpoint(local_sftp, "/daily.xlsx"),
            _pg_endpoint(table),
            mappings=_mappings(*COLUMNS),
            sync_mode="full_refresh_overwrite",
        )
        assert result.success is True, result.error
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            n = cur.fetchone()[0]
            cur.execute(f'SELECT flag FROM "{table}" ORDER BY id')
            flags = [r[0] for r in cur.fetchall()]
        assert n == 2
        assert flags == ["yes", "no"]
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()


TRIM_RECIPE = {"version": 1, "steps": [{"op": "trim", "column": "flag"}]}
PADDED = [["1", "10.50", "  yes  "], ["2", "20.25", " no "]]
PADDED_DAY2 = [["1", "10.50", "  yes  "], ["2", "21.00", " no "], ["3", "5.00", " yes"]]


def _trim_hash() -> str:
    from services.shape_models import ShapeRecipe

    return ShapeRecipe.parse(TRIM_RECIPE, source_columns=COLUMNS).recipe_hash


def _sqlite_dest(db: str, table: str = "ledger") -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=db,
        database=db,
        table=table,
    )


def _seed_ledger(db: str) -> None:
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE ledger (id TEXT, amount TEXT, flag TEXT)")
    conn.execute("INSERT INTO ledger VALUES ('0', '0.00', 'seed')")
    conn.commit()
    conn.close()


def _ledger_rows(db: str) -> list[tuple]:
    back = sqlite3.connect(db)
    try:
        return list(back.execute("SELECT id, amount, flag FROM ledger ORDER BY id"))
    finally:
        back.close()


def test_sftp_xlsx_trim_recipe_lands_without_padding(local_sftp) -> None:
    """Approved trim recipe is applied on the SFTP spill path, not dropped."""
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    Path(local_sftp.local_path("/daily.xlsx")).write_bytes(_xlsx(PADDED))
    recipe_hash = _trim_hash()
    assert recipe_hash
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "ledger.db")
        _seed_ledger(db)
        result = _run(
            _sftp_endpoint(local_sftp, "/daily.xlsx"),
            _sqlite_dest(db),
            mappings=_mappings(*COLUMNS),
            sync_mode="full_refresh_overwrite",
            shape_recipe=dict(TRIM_RECIPE),
            approved_shape_recipe_hash=recipe_hash,
        )
        assert result.success is True, result.error
        summary = result.destination_summary or {}
        assert summary.get("shape_recipe_hash") == recipe_hash
        rows = _ledger_rows(db)
        assert [str(r[0]) for r in rows] == ["1", "2"]
        assert [str(r[2]) for r in rows] == ["yes", "no"]


def test_sftp_xlsx_incremental_append_delta_and_noop(local_sftp, monkeypatch, tmp_path) -> None:
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    from services import sync_cursor

    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)

    Path(local_sftp.local_path("/daily.xlsx")).write_bytes(_xlsx(DAY1))
    contracts = [
        {
            "name": "ledger",
            "selected": True,
            "sync_mode": "incremental_append",
            "cursor_field": "id",
            "primary_key": "id",
        }
    ]
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "ledger.db")
        dest = _sqlite_dest(db)
        first = _run(
            _sftp_endpoint(local_sftp, "/daily.xlsx"),
            dest,
            mappings=_mappings(*COLUMNS),
            sync_mode="incremental_append",
            stream_contracts=contracts,
        )
        assert first.success is True, first.error
        assert [str(r[0]) for r in _ledger_rows(db)] == ["1", "2"]

        Path(local_sftp.local_path("/daily.xlsx")).write_bytes(_xlsx(DAY2))
        second = _run(
            _sftp_endpoint(local_sftp, "/daily.xlsx"),
            dest,
            mappings=_mappings(*COLUMNS),
            sync_mode="incremental_append",
            stream_contracts=contracts,
        )
        assert second.success is True, second.error
        rows = _ledger_rows(db)
        assert [str(r[0]) for r in rows] == ["1", "2", "3"]
        # Incremental append does not rewrite id=2; only the new id lands.
        assert [str(r[1]) for r in rows] == ["10.50", "20.25", "5.00"]

        third = _run(
            _sftp_endpoint(local_sftp, "/daily.xlsx"),
            dest,
            mappings=_mappings(*COLUMNS),
            sync_mode="incremental_append",
            stream_contracts=contracts,
        )
        assert third.success is True, third.error
        assert len(_ledger_rows(db)) == 3


def _isolate_schedule_stores(tmp_path, monkeypatch) -> None:
    import services.connector_store as connector_store
    import services.schedule_store as schedule_store

    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")


def _wait_schedule(schedule_id: str, *, timeout: float = 45.0):
    import time

    import services.schedule_store as schedule_store

    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = schedule_store.get_schedule(schedule_id)
        if (
            last is not None
            and not last.running
            and last.last_status in ("completed", "failed", "needs_approval")
        ):
            return last
        time.sleep(0.1)
    raise AssertionError(
        f"schedule {schedule_id} did not finish: "
        f"running={getattr(last, 'running', None)} "
        f"status={getattr(last, 'last_status', None)} "
        f"error={((getattr(last, 'run_history', None) or [{}])[-1] or {}).get('error')}"
    )


def test_sftp_xlsx_two_minute_schedule_replays_trim(
    local_sftp, monkeypatch, tmp_path
) -> None:
    """File-backed 2-minute cron + the same hashed trim recipe, two due beats."""
    if local_sftp is None:
        pytest.skip("local SFTP server unavailable")
    _isolate_schedule_stores(tmp_path, monkeypatch)

    import services.connector_store as connector_store
    import services.schedule_runner as schedule_runner
    import services.schedule_store as schedule_store
    from datetime import datetime, timedelta, timezone

    Path(local_sftp.local_path("/daily.xlsx")).write_bytes(_xlsx(PADDED))
    db = str(tmp_path / "ledger.db")
    _seed_ledger(db)
    recipe_hash = _trim_hash()
    cfg = local_sftp.endpoint_config("/daily.xlsx")
    source = connector_store.create_connector(
        {
            "name": "sftp-daily-xlsx",
            "type": "sftp",
            "role": "source",
            "host": cfg["host"],
            "port": cfg["port"],
            "username": cfg["username"],
            "password": cfg["password"],
            "database": cfg["database"],
            "host_key": cfg["host_key"],
        }
    )
    dest = connector_store.create_connector(
        {
            "name": "ledger-sqlite",
            "type": "sqlite",
            "role": "destination",
            "connection_string": db,
            "database": db,
            "ssl": False,
        }
    )
    sched = schedule_store.create_schedule(
        {
            "name": "sftp-excel-2min",
            "source_connector_id": source.id,
            "source_table": cfg["table"],
            "dest_connector_id": dest.id,
            "dest_table": "ledger",
            "interval": "hourly",
            "cron": "*/2 * * * *",
            "timezone": "UTC",
            "sync_mode": "full_refresh_overwrite",
            "validation_mode": "strict",
            "mappings": _mappings(*COLUMNS),
            "shape_recipe": dict(TRIM_RECIPE),
            "approved_shape_recipe_hash": recipe_hash,
            "enabled": True,
        }
    )
    assert sched.cron == "*/2 * * * *"
    assert sched.approved_shape_recipe_hash == recipe_hash
    nxt = datetime.fromisoformat(schedule_store.compute_next_run(
        "hourly",
        datetime(2026, 1, 1, 10, 1, tzinfo=timezone.utc),
        cron="*/2 * * * *",
        tz="UTC",
    ))
    assert nxt == datetime(2026, 1, 1, 10, 2, tzinfo=timezone.utc)

    past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    schedule_store.update_schedule(sched.id, {"next_run_at": past})
    started = schedule_runner._run_due_schedules()
    assert started == 1
    beat1 = _wait_schedule(sched.id)
    assert beat1.last_status == "completed", (
        (beat1.run_history or [{}])[-1].get("error") if beat1.run_history else beat1.last_status
    )
    rows1 = _ledger_rows(db)
    assert [str(r[2]) for r in rows1] == ["yes", "no"]

    Path(local_sftp.local_path("/daily.xlsx")).write_bytes(_xlsx(PADDED_DAY2))
    schedule_store.update_schedule(sched.id, {"next_run_at": past, "running": False})
    started = schedule_runner._run_due_schedules()
    assert started == 1
    beat2 = _wait_schedule(sched.id)
    assert beat2.last_status == "completed", (
        (beat2.run_history or [{}])[-1].get("error") if beat2.run_history else beat2.last_status
    )
    rows2 = _ledger_rows(db)
    assert [str(r[0]) for r in rows2] == ["1", "2", "3"]
    assert [str(r[1]) for r in rows2] == ["10.50", "21.00", "5.00"]
    assert [str(r[2]) for r in rows2] == ["yes", "no", "yes"]
    assert beat2.approved_shape_recipe_hash == recipe_hash
    assert beat2.shape_recipe == TRIM_RECIPE
    assert beat2.run_count >= 2
