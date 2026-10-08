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
