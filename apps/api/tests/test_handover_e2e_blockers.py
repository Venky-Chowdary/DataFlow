"""Round-3 handover blockers: key on every mode, manifest scope, gates, append, dates."""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from preflight.gates import gate_g6_target_ddl, gate_g8_reconciliation
from preflight.models import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    GateStatus,
    PreflightContext,
    SourceConfig,
    TransferPlan,
)
from services.copy_incremental import create_key_for_mode
from services.copy_mysql_pg import _pg_create_sql
from services.duplicate_policy import DuplicatePolicyError
from services.file_manifest import claim_file, complete_file
from services.file_pickup import RemoteFile, plan_pickup
from services.schedule_runner import build_schedule_request
from services.schedule_store import PipelineSchedule

_SRC = {
    "_id": "src",
    "id": "src",
    "type": "postgresql",
    "host": "h",
    "port": 5432,
    "database": "db",
    "schema": "public",
    "username": "u",
    "password": "p",
}
_DST = {
    "_id": "dst",
    "id": "dst",
    "type": "snowflake",
    "host": "h2",
    "database": "wh",
    "username": "u",
    "password": "p",
    "warehouse": "W",
}
_MAPPINGS = [{"source": "id", "target": "id"}]
_MODES = (
    "full_refresh_append",
    "full_refresh_overwrite",
    "incremental_append",
    "incremental_deduped",
)
_KEYED = ("upsert", "skip_duplicates")


def _sched(**overrides) -> PipelineSchedule:
    payload = {
        "id": "sched-1",
        "name": "orders",
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": "orders_wh",
        "interval": "daily",
        "mappings": _MAPPINGS,
        "primary_key": "id",
    }
    payload.update(overrides)
    return PipelineSchedule.from_dict(payload)


@pytest.mark.parametrize("mode", _MODES)
@pytest.mark.parametrize("policy", _KEYED)
def test_keyed_policy_passes_the_primary_key_for_every_mode(mode: str, policy: str) -> None:
    req = build_schedule_request(
        _sched(sync_mode=mode, duplicate_policy=policy), _SRC, _DST
    )
    assert req.sync_mode == mode
    assert req.duplicate_policy == ("dedupe_skip" if policy == "skip_duplicates" else "upsert")
    assert req.stream_contracts
    assert req.stream_contracts[0]["sync_mode"] == mode
    assert req.stream_contracts[0]["primary_key"] == "id"
    assert req.destination.extra["primary_key"] == "id"
    assert req.destination.extra["duplicate_policy"] == req.duplicate_policy


@pytest.mark.parametrize("mode", _MODES)
@pytest.mark.parametrize("policy", _KEYED)
def test_keyed_policy_without_a_key_fails_before_write(mode: str, policy: str) -> None:
    sched = _sched(sync_mode=mode, duplicate_policy=policy, primary_key="")
    with pytest.raises(DuplicatePolicyError, match="primary key"):
        build_schedule_request(sched, _SRC, _DST)


@pytest.mark.parametrize("mode", _MODES)
def test_explicit_append_passes_a_key_without_becoming_upsert(mode: str) -> None:
    req = build_schedule_request(
        _sched(sync_mode=mode, duplicate_policy="append"), _SRC, _DST
    )
    assert req.duplicate_policy == "append"
    assert req.sync_mode == mode
    assert req.stream_contracts[0]["primary_key"] == "id"
    assert req.stream_contracts[0]["sync_mode"] == mode


def test_full_refresh_without_a_key_or_policy_stays_an_insert() -> None:
    req = build_schedule_request(
        _sched(sync_mode="full_refresh_append", duplicate_policy="", primary_key=""),
        _SRC,
        _DST,
    )
    assert req.stream_contracts == []
    assert req.duplicate_policy == ""
    assert req.sync_mode == "full_refresh_append"


def test_existing_full_refresh_contract_receives_the_schedule_key() -> None:
    req = build_schedule_request(
        _sched(
            sync_mode="full_refresh_append",
            duplicate_policy="upsert",
            stream_contracts=[{"stream": "orders", "sync_mode": "full_refresh_append"}],
        ),
        _SRC,
        _DST,
    )
    assert req.stream_contracts[0]["primary_key"] == "id"
    assert req.stream_contracts[0]["sync_mode"] == "full_refresh_append"


def test_a_stream_keeps_its_own_primary_key() -> None:
    req = build_schedule_request(
        _sched(
            sync_mode="full_refresh_overwrite",
            duplicate_policy="skip_duplicates",
            stream_contracts=[{"stream": "orders", "primary_key": "order_id"}],
        ),
        _SRC,
        _DST,
    )
    assert req.stream_contracts[0]["primary_key"] == "order_id"
    assert req.duplicate_policy == "dedupe_skip"


def _csv_with_duplicate_keys() -> bytes:
    lines = ["id,name"]
    lines.extend(f"{i},first" for i in range(1, 51))
    lines.extend(f"{i},later" for i in range(1, 11))
    return ("\n".join(lines) + "\n").encode()


def _sqlite_dest(path: Path, table: str):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(kind="database", format="sqlite", database=str(path), table=table)


def _landed(path: Path, table: str) -> list[tuple]:
    with sqlite3.connect(path) as conn:
        return list(conn.execute(f'SELECT id, name FROM "{table}" ORDER BY id'))


@pytest.mark.parametrize("mode", ("full_refresh_append", "full_refresh_overwrite"))
@pytest.mark.parametrize("policy", ("upsert", "skip_duplicates"))
def test_full_refresh_keyed_policy_does_not_land_duplicate_keys(
    tmp_path: Path, mode: str, policy: str
) -> None:
    from src.transfer.file_stream import stream_file_to_database

    db = tmp_path / "dest.db"
    table = "orders"
    written, _ddl, summary, _cols = stream_file_to_database(
        _csv_with_duplicate_keys(),
        "orders.csv",
        _sqlite_dest(db, table),
        [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        {"id": "integer", "name": "string"},
        sync_mode=mode,
        stream_contracts=[{"primary_key": "id", "sync_mode": mode, "selected": True}],
        duplicate_policy=policy,
        validation_mode="strict",
        skip_preflight=True,
    )
    rows = _landed(db, table)
    assert len(rows) == 50, (written, summary.get("duplicate_policy"), rows[:3], rows[-3:])
    assert summary.get("duplicate_policy") == (
        "dedupe_skip" if policy == "skip_duplicates" else "upsert"
    )
    name_for_one = next(name for row_id, name in rows if int(row_id) == 1)
    if policy == "upsert":
        assert name_for_one == "later"
    else:
        assert name_for_one == "first"


def test_upsert_without_a_key_raises_before_rows_land(tmp_path: Path) -> None:
    from src.transfer.file_stream import stream_file_to_database

    db = tmp_path / "dest.db"
    with pytest.raises(DuplicatePolicyError, match="business key|primary key"):
        stream_file_to_database(
            _csv_with_duplicate_keys(),
            "orders.csv",
            _sqlite_dest(db, "orders"),
            [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
            {"id": "integer", "name": "string"},
            sync_mode="full_refresh_append",
            duplicate_policy="upsert",
            skip_preflight=True,
        )
    if db.exists():
        with sqlite3.connect(db) as conn:
            found = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='orders'"
            ).fetchone()
        assert found is None


def test_explicit_append_on_full_refresh_keeps_duplicate_rows(tmp_path: Path) -> None:
    from src.transfer.file_stream import stream_file_to_database

    db = tmp_path / "dest.db"
    stream_file_to_database(
        _csv_with_duplicate_keys(),
        "orders.csv",
        _sqlite_dest(db, "orders"),
        [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        {"id": "integer", "name": "string"},
        sync_mode="full_refresh_append",
        stream_contracts=[{"primary_key": "id", "sync_mode": "full_refresh_append"}],
        duplicate_policy="append",
        skip_preflight=True,
    )
    with sqlite3.connect(db) as conn:
        count = conn.execute('SELECT COUNT(*) FROM "orders"').fetchone()[0]
    assert count == 60


def _claim(conn, **kwargs) -> str:
    base = dict(
        dialect="sqlite",
        schema="public",
        connector="sftp:/:orders_*.csv",
        path="orders_a.csv",
        size=10,
        mtime="2026-10-15T00:00:00Z",
        sha256="abc",
        job_id="job-1",
    )
    base.update(kwargs)
    return claim_file(conn, **base)


def test_manifest_scope_loads_the_same_file_into_a_second_table() -> None:
    conn = sqlite3.connect(":memory:")
    assert _claim(conn, dest_table="orders_a", schedule_id="sched-a") == "load"
    complete_file(
        conn,
        dialect="sqlite",
        connector="sftp:/:orders_*.csv",
        path="orders_a.csv",
        sha256="abc",
        rows=40,
        dest_table="orders_a",
        schedule_id="sched-a",
    )
    assert _claim(conn, dest_table="orders_b", schedule_id="sched-b") == "load"
    assert _claim(conn, dest_table="orders_a", schedule_id="sched-a") == "skip"


def test_legacy_manifest_upgrades_and_does_not_skip_a_named_table() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE dataflow_file_manifest ("
        "connector TEXT NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL, "
        "mtime TEXT NOT NULL, sha256 TEXT NOT NULL, status TEXT NOT NULL, "
        "job_id TEXT NOT NULL, rows INTEGER NOT NULL DEFAULT 0, "
        "PRIMARY KEY (connector, path, sha256))"
    )
    conn.execute(
        "INSERT INTO dataflow_file_manifest VALUES (?,?,?,?,?,?,?,?)",
        ("sftp:/:orders_*.csv", "orders_a.csv", 10, "t", "abc", "loaded", "old", 40),
    )
    assert _claim(conn, dest_table="", schedule_id="") == "skip"
    assert _claim(conn, dest_table="orders_b", schedule_id="sched-b") == "load"


def _plan(*, in_file_dedupe: str, sync_mode: str = "upsert") -> TransferPlan:
    return TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            columns=[ColumnSchema(name="id", inferred_type="VARCHAR")],
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            table_exists=True,
            can_write=True,
            target_columns=[ColumnSchema(name="id", inferred_type="VARCHAR")],
        ),
        mappings=[
            ColumnMapping(source="id", target="id", confidence=1.0, transform="none")
        ],
        sync_mode=sync_mode,
        contract_primary_key="id",
        validation_mode="strict",
        in_file_dedupe=in_file_dedupe,
    )


class _DupeProbe(PreflightContext):
    def probe_unique_constraint(self, columns):  # noqa: ANN001
        return [{"columns": list(columns), "count": 2}]


def test_keep_last_passes_gate_6_and_block_still_blocks() -> None:
    blocked = gate_g6_target_ddl(_DupeProbe(plan=_plan(in_file_dedupe="block")))
    assert blocked.status == GateStatus.BLOCK
    assert "duplicate" in blocked.message.lower()
    kept = gate_g6_target_ddl(_DupeProbe(plan=_plan(in_file_dedupe="keep_last")))
    assert kept.status != GateStatus.BLOCK


def test_keep_last_and_per_file_scope_on_gate_8() -> None:
    blocked = gate_g8_reconciliation(
        PreflightContext(
            plan=_plan(in_file_dedupe="block"),
            sample_rows=[{"id": "1"}, {"id": "1"}],
        )
    )
    assert blocked.status == GateStatus.BLOCK
    kept = gate_g8_reconciliation(
        PreflightContext(
            plan=_plan(in_file_dedupe="keep_last"),
            sample_rows=[{"id": "1"}, {"id": "1"}],
        )
    )
    assert kept.status == GateStatus.PASS
    shared_across_files = gate_g8_reconciliation(
        PreflightContext(
            plan=_plan(in_file_dedupe="block"),
            sample_rows=[
                {"id": "1", "_df_file": "a.csv"},
                {"id": "1", "_df_file": "b.csv"},
            ],
        )
    )
    assert shared_across_files.status == GateStatus.PASS
    inside_one_file = gate_g8_reconciliation(
        PreflightContext(
            plan=_plan(in_file_dedupe="block"),
            sample_rows=[
                {"id": "1", "_df_file": "a.csv"},
                {"id": "1", "_df_file": "a.csv"},
            ],
        )
    )
    assert inside_one_file.status == GateStatus.BLOCK


def test_lenient_cast_failure_quarantines_and_keeps_good_rows() -> None:
    plan = TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            columns=[ColumnSchema(name="n", inferred_type="VARCHAR")],
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            table_exists=False,
            can_create_table=True,
            target_columns=[
                ColumnSchema(name="n", inferred_type="INTEGER", nullable=True)
            ],
        ),
        mappings=[
            ColumnMapping(source="n", target="n", confidence=1.0, transform="integer")
        ],
        sync_mode="full_refresh_append",
        validation_mode="lenient",
    )
    result = gate_g8_reconciliation(
        PreflightContext(
            plan=plan,
            sample_rows=[{"n": "12"}, {"n": "nope"}],
        )
    )
    assert result.status == GateStatus.PASS, result.message
    assert result.details.get("target_rows") == 1
    assert result.details.get("contracted_holdout_count") == 1
    assert "quarantine" in str(result.details.get("note") or "").lower()


def test_lenient_not_null_stop_column_still_blocks() -> None:
    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column="n",
        source_type="VARCHAR",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="stop column",
        execution_policy="STOP_COLUMN",
    ).to_dict()
    plan = TransferPlan(
        source=SourceConfig(
            kind="file",
            connected=True,
            columns=[ColumnSchema(name="n", inferred_type="VARCHAR")],
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            table_exists=True,
            can_write=True,
            target_columns=[
                ColumnSchema(name="n", inferred_type="INTEGER", nullable=False)
            ],
        ),
        mappings=[
            ColumnMapping(
                source="n",
                target="n",
                confidence=1.0,
                transform="integer",
                risk_contract=contract,
            )
        ],
        sync_mode="full_refresh_append",
        validation_mode="lenient",
    )
    result = gate_g8_reconciliation(
        PreflightContext(plan=plan, sample_rows=[{"n": "nope"}])
    )
    assert result.status == GateStatus.BLOCK
    assert "NOT NULL" in result.message


def test_incremental_append_create_has_no_primary_key() -> None:
    pairs = [("id", "id"), ("name", "name")]
    ddls = ["INTEGER", "TEXT"]
    append_sql = _pg_create_sql(
        "public", "events", pairs, ddls, create_key_for_mode("incremental_append", "id")
    )
    deduped_sql = _pg_create_sql(
        "public", "events", pairs, ddls, create_key_for_mode("incremental_deduped", "id")
    )
    assert "PRIMARY KEY" not in append_sql
    assert "PRIMARY KEY" in deduped_sql
    assert create_key_for_mode("incremental_append", "id") == []
    assert create_key_for_mode("incremental_deduped", "id") == ["id"]


def test_dated_series_warns_when_today_has_not_arrived() -> None:
    today = date(2026, 10, 16)
    files = [
        RemoteFile(name="orders_2026-10-14.csv", size=10, checksum="a"),
        RemoteFile(name="orders_2026-10-15.csv", size=10, checksum="b"),
    ]
    manifest = [
        {"name": item.name, "checksum": item.checksum, "size": item.size}
        for item in files
    ]
    plan = plan_pickup(
        pattern="orders_*.csv",
        files=files,
        manifest=manifest,
        today=today,
    )
    assert plan.action == "warn"
    assert plan.message == "expected file for 2026-10-16 not arrived"
    assert "no new or changed files" not in plan.message


def test_stale_running_heartbeat_is_not_live_and_a_fresh_one_is(monkeypatch) -> None:
    from services import schedule_store

    class Jobs:
        def __init__(self, job):
            self._job = job

        def get_job(self, job_id):  # noqa: ANN001
            return self._job

    class Leases:
        def is_held(self, job_id):  # noqa: ANN001
            return False

    monkeypatch.setattr(
        "services.worker_leases.get_worker_lease_store", lambda: Leases()
    )
    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service",
        lambda: Jobs(
            {"status": "running", "heartbeat_at": "2020-01-01T00:00:00+00:00"}
        ),
    )
    assert schedule_store._job_is_live("job-stale") is False
    fresh = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service",
        lambda: Jobs({"status": "running", "heartbeat_at": fresh}),
    )
    assert schedule_store._job_is_live("job-fresh") is True


def test_sftp_pages_reuse_one_download(monkeypatch, tmp_path: Path) -> None:
    from connectors import sftp_reader

    calls = {"n": 0}

    def fake_stat(_cfg):
        return (4, 100)

    def fake_download(_kind, path, _cfg, _bucket, _key):
        calls["n"] += 1
        Path(path).write_bytes(b"id\n1\n2\n3\n")

    monkeypatch.setattr(sftp_reader, "_remote_stat", fake_stat)
    monkeypatch.setattr(sftp_reader, "download_for_object_store", fake_download)
    cfg = {
        "host": "cache-host",
        "port": 2222,
        "username": "dataflow",
        "password": "dataflow",
        "database": "/inbound",
        "table": f"orders-{tmp_path.name}.csv",
    }
    first = sftp_reader.read_object(cfg=cfg, offset=0, limit=1)
    second = sftp_reader.read_object(cfg=cfg, offset=1, limit=1)
    assert calls["n"] == 1
    assert first.rows and second.rows

    def changed(_cfg):
        return (4, 200)

    monkeypatch.setattr(sftp_reader, "_remote_stat", changed)
    sftp_reader.read_object(cfg=cfg, offset=0, limit=1)
    assert calls["n"] == 2
