"""Unit proof for the open go-live rows touched in this wave.

These tests do not re-run the QA matrix and do not claim a live green.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, inspect, text


def test_records_after_failure_keeps_a_committed_prefix():
    from src.transfer.job_failure import _records_after_failure

    assert _records_after_failure(5, 0) == 5
    assert _records_after_failure(0, 5) == 5
    assert _records_after_failure(None, None) == 0
    assert _records_after_failure("nope", 2) == 2


def test_blocked_write_names_the_committed_prefix():
    """DEF-B2-012: a later refusal must not zero a checkpointed count."""
    from src.transfer.job_failure import account_blocked_write

    class _Blocked(Exception):
        rows_written = 0

        def __str__(self) -> str:
            return "Destination already stores these keys: 5 key value(s)"

    class _Mongo:
        def get_job(self, job_id):
            assert job_id == "job-55"
            return {"records_processed": 50}

    kept, message = account_blocked_write(_Mongo(), "job-55", _Blocked())
    assert kept == 50
    assert "5 key value(s)" in message
    assert "50 row(s) were already written and were not removed." in message
    again, same = account_blocked_write(_Mongo(), "job-55", _Blocked())
    assert again == 50
    assert same.count("already written") == 1


def test_mysql_timestamp_zero_is_modified_without_backfill():
    """DEF-B-010: the millisecond gap is an ALTER, not a silent write."""
    from connectors.schema_drift import widen_existing_columns_native

    class _Cursor:
        def __init__(self) -> None:
            self.sql: list[str] = []
            self._rows: list[tuple] = []

        def execute(self, sql, params=None):
            self.sql.append(sql)
            if "information_schema" in sql.lower():
                self._rows = [("occurred_at", "timestamp", None, None, None)]
            else:
                self._rows = []

        def fetchall(self):
            return list(self._rows)

    cursor = _Cursor()
    issued = widen_existing_columns_native(
        cursor,
        "mysql",
        None,
        "events",
        ["occurred_at"],
        ["DATETIME(3)"],
        backfill=False,
        source_types={"occurred_at": "TIMESTAMP"},
        temporal_fsp_without_backfill=True,
    )
    assert any("MODIFY COLUMN" in sql and "DATETIME(3)" in sql for sql in issued)
    assert any("time_zone" in sql for sql in cursor.sql)
    quiet = _Cursor()
    assert (
        widen_existing_columns_native(
            quiet,
            "mysql",
            None,
            "events",
            ["occurred_at"],
            ["DATETIME(3)"],
            backfill=False,
            source_types={"occurred_at": "TIMESTAMP"},
        )
        == []
    )


def test_ensure_product_lsn_column_on_an_existing_table(tmp_path: Path):
    """DEF-B2-014: _df_lsn is added; it is not a missing source column."""
    from connectors.schema_drift import ensure_product_lsn_column

    engine = create_engine(f"sqlite:///{tmp_path / 'orders.db'}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)"))
    physical = {"id": "INTEGER", "ID": "INTEGER", "name": "TEXT", "NAME": "TEXT"}
    err = ensure_product_lsn_column(
        engine,
        "orders",
        None,
        ["id", "name", "_df_lsn"],
        physical,
        table_existed=True,
    )
    assert err is None
    names = {str(col["name"]).lower() for col in inspect(engine).get_columns("orders")}
    assert "_df_lsn" in names
    again = ensure_product_lsn_column(
        engine,
        "orders",
        None,
        ["id", "name", "_df_lsn"],
        physical,
        table_existed=True,
    )
    assert again is None


def test_cdc_prepare_does_not_stage_mysql_grants_for_other_engines():
    """DEF-B2-004: SQL Server, Oracle, and TimescaleDB are not a MySQL grant."""
    from src.ai.copilot.lifecycle_tools import describe_cdc_prepare

    mysql = describe_cdc_prepare("mysql")
    assert mysql["stage"] is True
    assert "gtid_mode" in mysql["change"]

    maria = describe_cdc_prepare("mariadb")
    assert maria["stage"] is True
    assert "gtid_mode is MySQL-only" in maria["change"]

    for engine in ("sqlserver", "oracle", "timescaledb"):
        described = describe_cdc_prepare(engine)
        assert described["stage"] is False
        assert "GRANT REPLICATION" not in described["change"]
    assert "sys.sp_cdc_enable_db" in describe_cdc_prepare("sqlserver")["change"]
    assert "supplemental logging" in describe_cdc_prepare("oracle")["change"]
    assert "not supported" in describe_cdc_prepare("timescaledb")["change"]
    assert "wal_level" in describe_cdc_prepare("postgresql")["change"]


def test_timescaledb_cdc_contract_blocks_even_when_format_was_aliased():
    """DEF-B2-005: g9 must not say the sync contract is valid."""
    from services.preflight_service import run_transfer_policy_gates

    contract = {
        "name": "metrics",
        "selected": True,
        "primary_key": ["id"],
        "cursor_semantics": "cdc_position",
    }
    gates = run_transfer_policy_gates(
        sync_mode="cdc",
        source_type="postgresql",
        source_kind="database",
        dest_type="postgresql",
        source_config={"type": "timescaledb"},
        stream_contracts=[contract],
    )
    g9 = next(gate for gate in gates if gate["id"] == "g9_sync_contract")
    assert g9["status"] != "pass", g9
    issues = " ".join(g9.get("details", {}).get("issues") or [])
    assert "timescaledb" in issues
    assert "not supported" in issues


def test_capabilities_name_the_cdc_sources():
    from src.transfer.registry import get_capabilities

    caps = get_capabilities()
    sources = set(caps.get("cdc_capable_sources") or [])
    assert "postgresql" in sources
    assert "mysql" in sources
    assert "timescaledb" not in sources
    assert "timescaledb" in str(caps.get("cdc_note") or "")
