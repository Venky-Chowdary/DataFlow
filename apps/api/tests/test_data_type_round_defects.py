"""Data-types round: overwrite constraints, Oracle NUMBER, Mongo block, quarantine count, SQL Server subnormal.

Unit proof only. Not a live MySQL / Postgres / MariaDB / SQL Server / Oracle / Mongo retest.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest


def test_overwrite_keeps_relational_table_and_still_drops_warehouses():
    from connectors.table_manager import overwrite_clear_kind, overwrite_empty_statements
    from services.db_type_utils import dest_schema_is_recreated_on_overwrite

    for engine in ("postgresql", "postgres", "mysql", "mariadb", "oracle", "sqlserver", "mssql"):
        assert overwrite_clear_kind(engine) == "empty", engine
        assert dest_schema_is_recreated_on_overwrite(engine) is False, engine
    assert overwrite_clear_kind("mongodb") == "rename_collection"
    assert overwrite_clear_kind("snowflake") == "drop"
    assert dest_schema_is_recreated_on_overwrite("snowflake") is True
    truncate_sql, delete_sql = overwrite_empty_statements("postgresql", '"public"."users"')
    assert "TRUNCATE" in truncate_sql and "RESTART IDENTITY" in truncate_sql
    assert delete_sql.startswith("DELETE FROM")
    assert overwrite_empty_statements("sqlite", '"users"')[0] == ""


def test_snowflake_overwrite_still_clears_doomed_types():
    from src.transfer.engine import _destination_schema_probe
    from src.transfer.models import EndpointConfig

    dest = EndpointConfig(kind="database", format="snowflake", table="users")
    with patch(
        "src.transfer.endpoint_intelligence.introspect_endpoint",
        return_value={
            "schema": {"id": "NUMBER", "name": "VARCHAR(64)"},
            "schema_nullability": {"id": False},
            "table_exists": True,
        },
    ):
        schema, exists = _destination_schema_probe(dest, sync_mode="full_refresh_overwrite")
    assert exists is True
    assert schema == {}


def test_failed_mongo_overwrite_renames_aside_instead_of_dropping():
    from connectors.table_manager import (
        discard_mongodb_overwrite,
        restore_mongodb_overwrite,
        retire_mongodb_overwrite,
    )

    class _Col:
        def __init__(self, db, name):
            self.db = db
            self.name = name

        def rename(self, new):
            self.db.cols[new] = self.db.cols.pop(self.name)
            self.name = new

        def list_indexes(self):
            return list(self.db.cols[self.name].get("indexes") or [])

        def create_index(self, keys, **kwargs):
            self.db.cols[self.name].setdefault("copied", []).append((list(keys), kwargs))

    class _Db:
        def __init__(self):
            self.dropped: list[str] = []
            self.cols = {
                "orders": {
                    "docs": [{"_id": 1, "sku": "A"}],
                    "indexes": [
                        {"name": "_id_", "key": {"_id": 1}},
                        {"name": "u_sku", "key": {"sku": 1}, "unique": True},
                    ],
                    "create_options": {
                        "validator": {"sku": {"$type": "string"}},
                        "validationLevel": "strict",
                    },
                }
            }

        def list_collection_names(self):
            return list(self.cols)

        def drop_collection(self, name):
            self.dropped.append(name)
            self.cols.pop(name, None)

        def create_collection(self, name, **options):
            self.cols[name] = {"docs": [], "indexes": [], "create_options": dict(options)}

        def command(self, name, filter=None):
            wanted = (filter or {}).get("name")
            opts = (self.cols.get(wanted) or {}).get("create_options") or {}
            return {"cursor": {"firstBatch": [{"name": wanted, "options": opts}]}}

        def __getitem__(self, name):
            return _Col(self, name)

    db = _Db()
    with patch("connectors.table_manager._mongo_db", return_value=db):
        assert retire_mongodb_overwrite({}, "missing") is None
        backup = retire_mongodb_overwrite({}, "orders")
    assert backup == "orders__df_bak"
    assert db.dropped == []
    assert db.cols["orders__df_bak"]["docs"] == [{"_id": 1, "sku": "A"}]
    assert db.cols["orders"]["docs"] == []
    assert db.cols["orders"]["create_options"]["validationLevel"] == "strict"
    assert db.cols["orders"]["copied"][0][1]["unique"] is True

    with patch("connectors.table_manager._mongo_db", return_value=db):
        restore_mongodb_overwrite({}, "orders", backup)
    assert "orders__df_bak" not in db.cols
    assert db.cols["orders"]["docs"] == [{"_id": 1, "sku": "A"}]
    assert db.dropped == ["orders"]

    with patch("connectors.table_manager._mongo_db", return_value=db):
        backup = retire_mongodb_overwrite({}, "orders")
        discard_mongodb_overwrite({}, backup)
    assert backup not in db.cols
    assert "orders" in db.cols


def test_oracle_number_keeps_38_digits_and_nan_does_not_crash():
    from connectors.oracle_numbers import install_oracle_exact_fetch, oracle_number_from_driver

    pi = "3.1415926535897932384626433832795028842"
    got = oracle_number_from_driver(pi)
    assert isinstance(got, Decimal)
    assert str(got) == pi
    assert got != Decimal("3.141592653589793")
    nan = oracle_number_from_driver("nan")
    assert isinstance(nan, Decimal) and nan.is_nan()
    assert oracle_number_from_driver("NaN").is_nan()

    install_oracle_exact_fetch()
    from sqlalchemy.dialects.oracle.cx_oracle import (
        OracleDialect_cx_oracle,
        _OracleInteger,
    )

    detected = OracleDialect_cx_oracle._detect_decimal(None, "nan")
    assert isinstance(detected, Decimal) and detected.is_nan()

    class _Cursor:
        arraysize = 1

        def var(self, *args, **kwargs):
            self.kwargs = kwargs
            return kwargs["outconverter"]

    class _Dialect:
        dbapi = type("Api", (), {"STRING": "STRING"})()

    cur = _Cursor()
    convert = _OracleInteger()._cx_oracle_var(_Dialect(), cur)
    assert convert("nan").is_nan()
    assert str(convert(pi)) == pi


def test_sqlserver_subnormal_quarantines_one_row():
    from connectors.sql_bind import coerce_float_wire
    from connectors.writer_common import quarantine_unfit_floats

    with pytest.raises(ValueError, match="quarantined"):
        coerce_float_wire(5e-324, ddl_type="FLOAT", engine="sqlserver")
    with pytest.raises(ValueError, match="quarantined"):
        coerce_float_wire(5e-324, ddl_type="REAL", engine="mysql")
    assert coerce_float_wire(5e-324, ddl_type="DOUBLE", engine="mysql") == 5e-324
    assert coerce_float_wire(1.5, ddl_type="FLOAT", engine="sqlserver") == 1.5

    details: list[dict] = []
    kept = quarantine_unfit_floats(
        [(5e-324,), (1.5,)],
        ["amt"],
        ["FLOAT"],
        details,
        "quarantine",
        dest_db="sqlserver",
    )
    assert kept == [(1.5,)]
    assert len(details) == 1
    assert details[0]["column"] == "amt"


def test_objectid_wire_does_not_block_every_collection_and_names_a_real_column():
    from preflight.gates import gate_g4_mapping_confidence
    from preflight.models import (
        ColumnMapping,
        ColumnSchema,
        DestinationConfig,
        PreflightContext,
        SourceConfig,
        TransferPlan,
    )
    from services.create_new_risk_stamp import (
        apply_create_new_risk_stamps,
        create_new_risk_locks_review,
    )
    from services.root_cause_engine import build_root_causes
    from services.type_system import assess_create_new_type_risk

    preserving = assess_create_new_type_risk(
        "OBJECTID", "VARCHAR(24)", destination_db_type="postgresql"
    )
    assert any(r["kind"] == "objectid_carrier_equivalent" for r in preserving)
    assert all(not create_new_risk_locks_review(r) for r in preserving)
    collapsing = assess_create_new_type_risk(
        "OBJECTID", "VARCHAR(8)", destination_db_type="postgresql"
    )
    assert any(r["kind"] == "objectid_domain" and create_new_risk_locks_review(r) for r in collapsing)

    stamped = apply_create_new_risk_stamps(
        [
            {
                "source": "_id",
                "target": "_id",
                "source_type": "OBJECTID",
                "target_type": "VARCHAR(24)",
                "assignment_strategy": "identity_passthrough",
                "create_new": True,
                "confidence": 0.84,
                "fidelity": "preserve",
            }
        ],
        "postgresql",
    )
    row = stamped[0]
    assert row["requires_review"] is False
    assert float(row["confidence"]) >= 0.85

    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="mongodb",
            connected=True,
            columns=[ColumnSchema(name="email", inferred_type="VARCHAR")],
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            table_exists=False,
            can_write=True,
            target_columns=[ColumnSchema(name="email", inferred_type="VARCHAR")],
        ),
        mappings=[ColumnMapping(source="email", target="email", confidence=0.50)],
        confidence_threshold=0.85,
        validation_mode="strict",
    )
    result = gate_g4_mapping_confidence(PreflightContext(plan=plan, sample_rows=[]))
    assert result.status.value == "block"
    assert "email" in result.message

    roots = build_root_causes(
        {
            "gates": [
                {
                    "id": "g4_mapping_confidence",
                    "status": "block",
                    "message": result.message,
                    "details": {"low_confidence": ["email→email (0.50)"]},
                }
            ],
            "blockers": [],
            "row_count": 1,
        }
    )
    conf = [r for r in roots if r.kind == "mapping_confidence"]
    assert conf and "email" in conf[0].summary


def test_reported_rejects_match_stored_findings():
    from src.transfer.job_quarantine import (
        align_rejects_to_stored_table,
        align_reported_rejects,
    )

    summary = {
        "rejected_rows": 7,
        "rejected_details": [{"row": i, "reason": "unfit"} for i in range(1, 6)],
    }
    assert align_reported_rejects(summary) == 5
    assert summary["rejected_rows"] == 5
    assert summary["rejected_count_aligned_from"] == 7

    align_rejects_to_stored_table(summary, {"ok": False, "rows_written": 0})
    assert summary["rejected_rows"] == 5
    align_rejects_to_stored_table(
        summary, {"ok": True, "skipped": False, "rows_written": 5}
    )
    assert summary["rejected_rows"] == 5


def test_datetime2_six_binds_the_microsecond():
    """ODBC SQL_TIMESTAMP keeps 3 digits. DATETIME2(6) must bind the other 3."""
    from datetime import datetime

    from sqlalchemy.dialects import mssql

    from connectors.generic_sql import _to_sa_value
    from connectors.sqlserver_datetime2 import install_sqlserver_datetime2_bind

    micro = datetime(2026, 2, 1, 10, 1, 0, 123456)
    bound = _to_sa_value(
        micro, "DATETIME2(6)", mssql.DATETIME2(precision=6), "mssql", "sqlserver"
    )
    assert isinstance(bound, str)
    assert bound == "2026-02-01 10:01:00.123456"
    seven = _to_sa_value(micro, "DATETIME2", mssql.DATETIME2(), "mssql", "sqlserver")
    assert seven == "2026-02-01 10:01:00.1234560"
    install_sqlserver_datetime2_bind()
    install_sqlserver_datetime2_bind()
    proc = mssql.DATETIME2(precision=6).bind_processor(None)
    assert proc(micro) == "2026-02-01 10:01:00.123456"
    assert proc("2026-02-01 10:01:00.123456") == "2026-02-01 10:01:00.123456"
    assert proc(None) is None
    narrow = mssql.DATETIME2(precision=3).bind_processor(None)
    assert narrow(micro) == micro
    with pytest.raises(ValueError, match="1/300"):
        _to_sa_value(micro, "DATETIME", mssql.DATETIME(), "mssql", "sqlserver")
    with pytest.raises(ValueError, match=r"DATETIME2\(3\)"):
        _to_sa_value(
            datetime(2028, 2, 29, 23, 59, 59, 999999),
            "DATETIME2(3)",
            mssql.DATETIME2(precision=3),
            "mssql",
            "sqlserver",
        )
    kept = _to_sa_value(micro, "TIMESTAMP", None, "postgresql", "postgresql")
    assert isinstance(kept, datetime)

    class _Dbapi:
        DATETIME = "SQL_TYPE_TIMESTAMP"
        SQL_WVARCHAR = "SQL_WVARCHAR"

    from sqlalchemy.dialects.mssql.pyodbc import MSDialect_pyodbc

    wide = mssql.DATETIME2(precision=6).dialect_impl(MSDialect_pyodbc())
    assert wide.get_dbapi_type(_Dbapi) == ("SQL_WVARCHAR", 26, 0)
    narrow_type = mssql.DATETIME2(precision=3).dialect_impl(MSDialect_pyodbc())
    assert narrow_type.get_dbapi_type(_Dbapi) == "SQL_TYPE_TIMESTAMP"


_LATIN = "VARCHAR(20) COLLATE SQL_LATIN1_GENERAL_CP1_CI_AS"


def test_ascii_into_latin1_is_not_blocked_and_a_signed_contract_clears_type_lock():
    from services.column_case import column_population
    from services.source_engine_scope import bind_source_engine

    assert column_population([{"SKU": "ABC"}], "sku") == ["ABC"]
    # A code-page sink is a collapse only when the source can emit Unicode.
    with bind_source_engine("postgresql"):
        _assert_latin1_population_and_contract([{"source": "sku", "target": "sku"}])


def _assert_latin1_population_and_contract(mapping) -> None:
    from services.ddl_compatibility import evaluate_ddl_compatibility
    from services.migration_risk_contract import create_migration_risk_contract
    from services.type_coercion_validator import validate_mapping_coercions

    _ok, measured = evaluate_ddl_compatibility(
        mappings=mapping,
        source_schema={"sku": "VARCHAR(20)"},
        target_schema={"sku": _LATIN},
        sample_rows=[{"SKU": "ABC"}, {"sku": "12"}],
        table_exists=True,
        dest_connected=True,
        dest_db_type="sqlserver",
        destination_table="products",
    )
    assert not any("Lossy type coercion" in issue for issue in measured), measured
    _unread_ok, unread = evaluate_ddl_compatibility(
        mappings=mapping,
        source_schema={"sku": "VARCHAR(20)"},
        target_schema={"sku": _LATIN},
        sample_rows=None,
        table_exists=True,
        dest_connected=True,
        dest_db_type="sqlserver",
        destination_table="products",
    )
    assert any("Lossy type coercion" in issue for issue in unread), unread

    common = dict(
        source_types={"sku": "VARCHAR(20)"},
        target_types={"sku": _LATIN},
        dest_db_type="sqlserver",
        dest_table_exists=True,
        schema_policy="type_locked",
    )
    outside = validate_mapping_coercions(
        [{"source": "sku", "target": "sku", "target_type": _LATIN}],
        samples_by_source={"sku": ["ABC", "山田"]},
        **common,
    )
    assert any(issue["severity"] == "block" for issue in outside), outside
    ascii = validate_mapping_coercions(
        [{"source": "sku", "target": "sku", "target_type": _LATIN}],
        samples_by_source={"sku": ["ABC", "12"]},
        **common,
    )
    assert not any(issue["severity"] == "block" for issue in ascii), ascii
    contract = create_migration_risk_contract(
        column="sku",
        source_type="VARCHAR(20)",
        destination_type=_LATIN,
        approved_by="qa@dataflow.app",
        reason="Hold characters outside the Latin-1 code page",
        execution_policy="CAST_AND_CONTINUE",
    )
    cleared = validate_mapping_coercions(
        [{
            "source": "sku",
            "target": "sku",
            "target_type": _LATIN,
            "risk_contract": contract.to_dict(),
        }],
        **common,
    )
    assert cleared and all(issue["severity"] == "warn" for issue in cleared), cleared
    changed = validate_mapping_coercions(
        [{
            "source": "qty",
            "target": "qty",
            "target_type": "VARCHAR(20)",
            "risk_contract": contract.to_dict(),
        }],
        source_types={"qty": "INTEGER"},
        target_types={"qty": "VARCHAR(20)"},
        dest_db_type="sqlserver",
        schema_policy="type_locked",
    )
    assert any(issue["severity"] == "block" for issue in changed), changed


def test_decimal_12_3_stays_fixed_point_when_the_destination_has_one():
    from services.decision_kernel.type_invent import refuse_create_new_numeric_collapse
    from services.schema_inference import safe_ddl_logical_type

    assert (
        safe_ddl_logical_type(
            "DECIMAL(12,3)", ["12.500", "not-a-number"], field_name="amt"
        )
        == "DECIMAL(12,3)"
    )
    expected = {
        "mysql": "DECIMAL(12,3)",
        "mariadb": "DECIMAL(12,3)",
        "postgresql": "NUMERIC(12,3)",
        "sqlserver": "DECIMAL(12,3)",
    }
    for db, want in expected.items():
        got = refuse_create_new_numeric_collapse("DECIMAL(12,3)", "TEXT", db)
        assert got.upper().replace(" ", "") == want, (db, got)
    over = refuse_create_new_numeric_collapse("DECIMAL(80,40)", "TEXT", "mysql")
    assert over.upper().replace(" ", "") == "TEXT"
    assert refuse_create_new_numeric_collapse("DECIMAL(12,3)", "TEXT", "sqlite") == "TEXT"
    assert refuse_create_new_numeric_collapse("DECIMAL(12,3)", "TEXT", "") == "TEXT"

    from connectors.generic_sql import _sa_type_for_logical
    from connectors.writer_common import resolve_target_columns
    from sqlalchemy.dialects import mysql as mysql_dialect

    cols, types = resolve_target_columns(
        [{
            "source": "amt",
            "target": "amt",
            "target_type": "TEXT",
            "source_type": "DECIMAL(12,3)",
        }],
        {"amt": "DECIMAL(12,3)"},
        preserve_case=True,
        table_exists=False,
        dest_db="mysql",
    )
    assert cols == ["amt"]
    assert types == ["DECIMAL(12,3)"]
    compiled = str(
        _sa_type_for_logical(types[0], "mysql", "mysql").compile(
            dialect=mysql_dialect.dialect()
        )
    ).upper().replace(" ", "")
    assert compiled in {"DECIMAL(12,3)", "NUMERIC(12,3)"}, compiled
    assert "TEXT" not in compiled
    held = resolve_target_columns(
        [{
            "source": "amt",
            "target": "amt",
            "target_type": "TEXT",
            "source_type": "DECIMAL(12,3)",
            "user_override": True,
        }],
        {"amt": "DECIMAL(12,3)"},
        preserve_case=True,
        table_exists=False,
        dest_db="mysql",
    )
    assert held[1] == ["TEXT"]


def test_quarantine_payload_stores_json_null_not_the_wire_token():
    import json

    from connectors.writer_common import quarantine_cell_wire
    from services.dest_quarantine import (
        project_operator_quarantine_details,
        rejected_details_to_dlq_records,
    )
    from services.value_serializer import DF_MISSING_SENTINEL, SQL_NULL_SENTINEL

    assert quarantine_cell_wire(None) == SQL_NULL_SENTINEL
    stored = {
        "row": 1,
        "column": "note",
        "value": SQL_NULL_SENTINEL,
        "values": {
            "note": SQL_NULL_SENTINEL,
            "sku": "",
            "gone": DF_MISSING_SENTINEL,
        },
    }
    records = rejected_details_to_dlq_records([stored], job_id="j1")
    payload = records[0]["_df_payload"]
    assert SQL_NULL_SENTINEL not in payload
    parsed = json.loads(payload)
    assert parsed["note"] is None
    assert parsed["sku"] == ""
    assert parsed["gone"] == DF_MISSING_SENTINEL
    assert records[0]["_df_value"] is None
    shown = project_operator_quarantine_details([stored])
    assert stored["value"] == SQL_NULL_SENTINEL
    assert shown[0]["value"] is None
    assert shown[0]["values"]["sku"] == ""
    assert shown[0]["values"]["note"] is None

    from src.transfer.models import sanitize_job_for_api

    stored_job = {
        "rejected_details": [dict(stored)],
        "destination_summary": {"rejected_details": [dict(stored)]},
    }
    shown_job = sanitize_job_for_api(stored_job)
    assert stored_job["rejected_details"][0]["value"] == SQL_NULL_SENTINEL
    assert shown_job["rejected_details"][0]["value"] is None
    assert SQL_NULL_SENTINEL not in str(shown_job["destination_summary"]["rejected_details"])
    assert shown_job["destination_summary"]["rejected_details"][0]["values"]["sku"] == ""


def test_dest_quarantine_table_stores_sql_null(tmp_path):
    import sqlite3

    from services.dest_quarantine import write_dest_quarantine
    from services.value_serializer import SQL_NULL_SENTINEL
    from src.transfer.models import EndpointConfig

    dest_path = tmp_path / "dlq.db"
    dest = EndpointConfig(
        kind="database",
        format="sqlite",
        table="orders",
        connection_string=f"sqlite:///{dest_path}",
        database=str(dest_path),
    )
    result = write_dest_quarantine(
        dest,
        [{
            "row": 4,
            "column": "note",
            "target": "note",
            "value": SQL_NULL_SENTINEL,
            "reason": "unfit",
            "policy": "quarantine",
            "values": {"note": SQL_NULL_SENTINEL, "sku": ""},
        }],
        job_id="job-null-wire",
    )
    assert result["ok"] is True, result
    with sqlite3.connect(dest_path) as db:
        value, payload = db.execute(
            "SELECT _df_value, _df_payload FROM orders_df_quarantine"
        ).fetchone()
    assert value is None
    assert SQL_NULL_SENTINEL not in payload
    compact = payload.replace(" ", "")
    assert '"note":null' in compact
    assert '"sku":""' in compact
