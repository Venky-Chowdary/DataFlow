"""Retest R1 defects: null profile, SQL Server bind, CDC key repair, schedules.

Named fixtures only. No live service is required.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.dialects import mssql

from connectors.generic_sql import (
    _encoding_dest_type,
    _logical_type_from_sa,
    _refuse_sqlserver_datetime_rounding,
    _sa_type_for_logical,
)
from connectors.writer_common import append_write_quarantine_detail
from connectors.write_resilience import is_connection_lost
from services.cdc_value_digest import rows_absent_by_primary_key
from services.decimal_observe import cursor_declared_numeric_types
from services.dest_dialect_facts import _collation_compatible_with_dest
from services.encoding_capacity import bind_unicode_text
from services.mapping_pipeline import run_mapping_pipeline
from services.source_engine_scope import bind_source_engine
from services.sync_cursor import resolve_incremental_read_scope
from services.type_system import national_charset_would_invent
from src.ai.copilot.schedule_cadence import describe_stored_cadence, parse_cadence
from src.ai.copilot.transfer_tools import _ground_data_rules, _primary_key_csv


def test_column_profile_null_rate_keeps_seven_of_eight() -> None:
    samples = ["__DF_SQL_NULL__"] * 7 + ["12.50"]
    result = run_mapping_pipeline(
        ["amount"],
        ["amount"],
        source_schemas=[{"name": "amount", "inferred_type": "NUMERIC(12,2)", "samples": samples}],
        target_schemas=[{"name": "amount", "inferred_type": "DECIMAL(12,2)"}],
        source_samples={"amount": samples},
        destination_db_type="mysql",
        source_db_type="postgresql",
        destination_table_exists=True,
        use_llm=False,
    )
    profile = result["mappings"][0]["column_profile"]
    assert profile["null_rate"] == pytest.approx(0.875, abs=0.001)


def test_nvarchar_max_holds_cjk_varchar_quarantines() -> None:
    held = bind_unicode_text("郎", engine="sqlserver", dest_type="NVARCHAR(MAX)")
    assert held == "郎"
    with pytest.raises(ValueError, match="capacity"):
        bind_unicode_text(
            "郎",
            engine="sqlserver",
            dest_type="VARCHAR(50) COLLATE Latin1_General_CI_AS",
        )
    # Unclassified SQL Server text must not pass through for the driver to
    # substitute '?'.
    with pytest.raises(ValueError, match="varchar capacity"):
        bind_unicode_text("郎", engine="sqlserver", dest_type="")


def test_logical_type_keeps_nvarchar_max_and_classic_datetime() -> None:
    assert _logical_type_from_sa(mssql.NVARCHAR()) == "NVARCHAR(MAX)"
    assert _logical_type_from_sa(mssql.NVARCHAR(50)) == "NVARCHAR(50)"
    assert _logical_type_from_sa(mssql.VARCHAR(50)) == "VARCHAR(50)"
    assert _logical_type_from_sa(mssql.DATETIME()) == "DATETIME"
    assert _logical_type_from_sa(mssql.DATETIME2()) == "DATETIME2(7)"
    compiled = _sa_type_for_logical(
        "timestamptz", "mssql", "sqlserver"
    ).compile(dialect=mssql.dialect())
    assert str(compiled) == "DATETIMEOFFSET(7)"
    naive = _sa_type_for_logical("datetime", "mssql", "sqlserver")
    assert str(naive.compile(dialect=mssql.dialect())) == "DATETIME2(7)"


def test_classic_datetime_refuses_microsecond_before_bind() -> None:
    value = datetime(2026, 2, 1, 10, 1, 0, 1)
    with pytest.raises(ValueError, match="1/300"):
        _refuse_sqlserver_datetime_rounding(
            value,
            logical="DATETIME",
            sa_type=mssql.DATETIME(),
            db_type="sqlserver",
        )
    _refuse_sqlserver_datetime_rounding(
        value,
        logical="DATETIME2(7)",
        sa_type=mssql.DATETIME2(precision=7),
        db_type="sqlserver",
    )


def test_encoding_type_follows_the_physical_class() -> None:
    assert _encoding_dest_type("string", mssql.NVARCHAR(), dialect_name="mssql", db_type="sqlserver") == "NVARCHAR(MAX)"
    assert _encoding_dest_type("NVARCHAR(MAX)", mssql.VARCHAR(50), dialect_name="mssql", db_type="sqlserver") == "VARCHAR(50)"


def test_postgres_varchar_to_nvarchar_is_not_lossy_when_source_is_bound() -> None:
    with bind_source_engine("postgresql"):
        assert national_charset_would_invent(
            "VARCHAR(64)", "NVARCHAR(64)", dest_db="sqlserver"
        ) is False


def test_mysql_timestamp_source_stays_timestamp_six() -> None:
    result = run_mapping_pipeline(
        ["ts"],
        [],
        source_schemas=[{"name": "ts", "inferred_type": "TIMESTAMP", "native_type": "timestamp"}],
        source_db_type="mysql",
        destination_db_type="mysql",
        destination_table_exists=False,
        use_llm=False,
    )
    target = str(result["mappings"][0].get("target_type") or "").upper()
    assert target.startswith("TIMESTAMP")
    aware = run_mapping_pipeline(
        ["ts"],
        [],
        source_schemas=[{"name": "ts", "inferred_type": "TIMESTAMPTZ"}],
        source_db_type="mysql",
        destination_db_type="mysql",
        destination_table_exists=False,
        use_llm=False,
    )
    assert str(aware["mappings"][0].get("target_type") or "").upper().startswith("TIMESTAMP")
    foreign = run_mapping_pipeline(
        ["ts"],
        [],
        source_schemas=[{"name": "ts", "inferred_type": "TIMESTAMPTZ"}],
        source_db_type="postgresql",
        destination_db_type="mysql",
        destination_table_exists=False,
        use_llm=False,
    )
    assert str(foreign["mappings"][0].get("target_type") or "").upper().startswith("DATETIME")


def test_mariadb_uca1400_is_not_a_mysql_collation() -> None:
    assert _collation_compatible_with_dest("mysql", "utf8mb4_uca1400_ai_ci") is False
    assert _collation_compatible_with_dest("mariadb", "utf8mb4_uca1400_ai_ci") is True
    assert is_connection_lost("OperationalError: Unknown collation 'utf8mb4_uca1400_ai_ci'") is False


def test_pg_varchar_to_sqlserver_nvarchar_is_preserve_when_engine_is_bound() -> None:
    from services.type_system import is_lossy_coercion

    with bind_source_engine("postgresql"):
        assert is_lossy_coercion(
            "VARCHAR(100)",
            "NVARCHAR(100) COLLATE LATIN1_GENERAL_BIN",
            dest_db="sqlserver",
        ) is False


def test_bare_five_field_cron_is_a_schedule() -> None:
    spec = parse_cadence("*/5 * * * *")
    assert spec.question in {None, ""}
    assert spec.cron == "*/5 * * * *"
    assert spec.resolved is True
    assert describe_stored_cadence("daily", "*/5 * * * *", "UTC") == "Every 5 minutes UTC"
    yearly = parse_cadence("0 0 1 1 *")
    assert yearly.cron == "0 0 1 1 *"
    assert yearly.question in {None, ""}


def test_weekdays_and_hourly_minute_keep_their_anchor() -> None:
    weekdays = parse_cadence("weekdays at 21:40 UTC")
    assert weekdays.cron == "40 21 * * 1-5"
    assert weekdays.question in {None, ""}
    hourly = parse_cadence("hourly at minute 7")
    assert hourly.cron == "7 * * * *"
    assert "every 7 days" not in (hourly.description or "")
    assert "every 7 days" not in (weekdays.description or "")


def test_incremental_append_is_not_rewritten_to_upsert() -> None:
    rules, err = _ground_data_rules(
        source_filter=None,
        upsert_key="id",
        dedupe_key="",
        source_columns=["id", "updated_at"],
        source_label="orders",
        mode="incremental_append",
    )
    assert err == ""
    assert rules.get("sync_mode") != "upsert"
    assert _primary_key_csv(None) == ""
    assert _primary_key_csv(["id", "line"]) == "id,line"
    assert _primary_key_csv("id") == "id"


def test_mysql_and_mariadb_do_not_share_an_incremental_bookmark() -> None:
    common = dict(
        sync_mode="incremental_append",
        stream_contracts=[{
            "name": "qa6b_wm",
            "sync_mode": "incremental_append",
            "cursor_field": "updated_at",
            "selected": True,
        }],
        source_type="postgresql",
        source_database="qa_dataflow",
        source_object="qa6b_wm",
        dest_type="mysql",
        dest_database="qa_dataflow",
        dest_object="qa6b_wm",
    )
    mysql = resolve_incremental_read_scope(
        **common,
        destination={"format": "mysql", "host": "mysql.internal", "port": 3306},
    )
    maria = resolve_incremental_read_scope(
        **common,
        destination={"format": "mariadb", "host": "maria.internal", "port": 3306},
    )
    assert mysql.cursor_key != maria.cursor_key
    assert "engine:mariadb" in maria.cursor_key
    assert "engine:mariadb" not in mysql.cursor_key


def test_cursor_cast_wins_over_the_sample_guess() -> None:
    from services.procedure_source import _overlay_declared_numerics

    merged = _overlay_declared_numerics(
        ["note", "day"],
        (
            ("note", 253, None, None, 400, 39, True),
            ("day", 10, None, None, None, None, True),
        ),
        {"note": "DECIMAL(12,2)", "day": "INTEGER"},
    )
    assert merged["note"] == "VARCHAR"
    assert merged["day"] == "DATE"


def test_mariadb_text_precision_is_not_a_decimal() -> None:
    declared = cursor_declared_numeric_types(
        ["note"],
        (("note", 253, None, None, 400, 39, True),),
    )
    assert declared == {}
    kept = cursor_declared_numeric_types(
        ["AMOUNT"],
        (("AMOUNT", object(), None, None, 10, 2, True),),
    )
    assert kept == {"AMOUNT": "DECIMAL(10,2)"}


def test_offset_text_is_not_refined_to_datetime() -> None:
    from services.schema_introspect import _refine_columns_by_samples

    class _Cur:
        def execute(self, *_a, **_k):
            return None

        def fetchall(self):
            return [("2026-02-01T10:01:00+05:30",)]

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()

    columns = [{"name": "note", "inferred_type": "TEXT", "data_type": "text"}]
    refined = _refine_columns_by_samples(_Conn(), columns, "t", "public")
    assert refined[0]["inferred_type"] == "TEXT"


def test_cdc_key_repair_finds_inserts_identity_scan_declines(monkeypatch) -> None:
    source_rows = [{"id": "1", "ts": "2026-02-01T00:00:00+00:00"}, {"id": "2", "ts": "x"}]
    dest_rows = [{"id": "1", "ts": "2026-02-01 00:00:00"}]

    def _scan(_db, _cfg, _table, columns):
        if "ts" in columns and columns[0] == "id":
            return source_rows if _db == "postgresql" else dest_rows
        return source_rows if _db == "postgresql" else dest_rows

    monkeypatch.setattr(
        "services.cdc_value_digest._scan_table",
        lambda db, cfg, table, columns: source_rows if db == "postgresql" else dest_rows,
    )
    missing = rows_absent_by_primary_key(
        source_type="postgresql",
        source_cfg={},
        source_table="events",
        dest_type="mysql",
        dest_cfg={},
        dest_table="events",
        mappings=[
            {"source": "id", "target": "id", "transform": "none"},
            {"source": "ts", "target": "ts", "transform": "datetime"},
        ],
        primary_key="id",
    )
    assert missing is not None
    assert [row["id"] for row in missing] == ["2"]
    del _scan


def test_sentinel_is_not_a_rejected_upsert_row() -> None:
    details: list[dict] = []
    append_write_quarantine_detail(
        details,
        {"row": 1, "column": "amount", "value": "__DF_SQL_NULL__", "reason": "bind"},
        mapped_row={"amount": "__DF_SQL_NULL__"},
        target_cols=["amount"],
    )
    assert details == []
    append_write_quarantine_detail(
        details,
        {
            "row": 2,
            "column": "amount",
            "value": "__DF_SQL_NULL__",
            "reason": "NOT NULL column refused SQL NULL",
        },
        mapped_row={"amount": None},
        target_cols=["amount"],
    )
    assert len(details) == 1


def test_claim_moves_a_past_next_run_forward(tmp_path, monkeypatch) -> None:
    import services.schedule_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    sched = store.create_schedule({
        "name": "stuck",
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": "orders",
        "interval": "hourly",
        "mappings": [{"source": "id", "target": "id"}],
    })
    rows = store._load_all()
    rows[0] = store.PipelineSchedule.from_dict({
        **rows[0].to_dict(),
        "next_run_at": "2026-10-08T01:45:00+00:00",
        "enabled": True,
    })
    store._save_all(rows)
    claimed = store.mark_schedule_running(sched.id, "retest")
    assert claimed is not None
    nxt = store._parse_ts(claimed.next_run_at)
    assert nxt is not None
    assert nxt > datetime.now(timezone.utc) - timedelta(seconds=1)


def test_silent_running_job_releases_the_claim(monkeypatch) -> None:
    import services.schedule_store as store

    class _Svc:
        def get_job(self, _job_id):
            return {
                "status": "running",
                "updated_at": "2020-01-01T00:00:00+00:00",
            }

    monkeypatch.setattr(
        "services.mongodb_service.get_mongodb_service",
        lambda: _Svc(),
    )
    assert store._job_is_live("dead-worker") is False


def test_unicode_source_into_sql_latin1_varchar_is_a_fidelity_collapse() -> None:
    """DEF-R1-002: width-identical VARCHAR completed with ? in the cells."""
    from services.type_system import is_lossy_coercion

    dest = "VARCHAR(50) COLLATE SQL_LATIN1_GENERAL_CP1_CI_AS"
    with bind_source_engine("postgresql"):
        assert is_lossy_coercion("VARCHAR(50)", dest, dest_db="sqlserver") is True
    with bind_source_engine("sqlserver"):
        assert is_lossy_coercion(dest, dest, dest_db="sqlserver") is False
    with bind_source_engine("postgresql"):
        assert (
            is_lossy_coercion(
                "VARCHAR(50)",
                "VARCHAR(50) COLLATE Latin1_General_100_CI_AS_SC_UTF8",
                dest_db="sqlserver",
            )
            is False
        )


def test_latin1_varchar_is_not_safe_by_declaration() -> None:
    from services.population_fit_scan import bounded_targets

    targets, _undecidable, safe = bounded_targets(
        [
            {
                "source": "name",
                "target": "name",
                "source_type": "VARCHAR(50)",
                "target_type": "VARCHAR(50) COLLATE SQL_LATIN1_GENERAL_CP1_CI_AS",
            }
        ],
        dest_db="sqlserver",
        source_kind="database",
        source_format="postgresql",
    )
    assert "name" not in safe
    assert any(t.source == "name" for t in targets)
