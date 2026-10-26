"""Regression coverage for the remaining QA RT5 / round-2 defects."""

from __future__ import annotations

import sqlite3

from services.compliance_guard import score_compliance_risk
from services.root_cause_engine import _root_id, build_root_causes
from services.schema_fidelity import SourceSchemaCatalog, plan_create_new_fidelity
from services.schema_inference import classify_value
from services.sql_literals import (
    escape_sqlalchemy_literal_colons,
    mask_sql_string_literals,
)
from services.transform_engine import apply_transform
from services.verification_ladder import run_five_layer_verification


def test_percent_token_stays_varchar() -> None:
    assert classify_value("45%") == "VARCHAR"
    assert classify_value("42") == "INTEGER"


def test_mixed_currency_column_infers_decimal() -> None:
    """Every cell parses and one carries a currency mark, so the column is money.

    A lone ``1,234`` still fails closed and cannot pull a column into DECIMAL.
    """
    from services.schema_inference import infer_column

    mixed = infer_column(
        ["$1,000.00", "€1.000,00", "1 000 000.89", "USD 500", "1,234,567"],
        field_name="amount",
    )
    assert mixed["logical_type"] == "DECIMAL"
    ambiguous = infer_column(["1,234", "5,678"], field_name="amount")
    assert ambiguous["logical_type"] != "DECIMAL"


def test_trim_does_not_collapse_internal_whitespace() -> None:
    cleaned, err = apply_transform("  pad  ", "trim")
    assert err is None
    assert cleaned == "pad"
    ident, ident_err = apply_transform("ID-7", "trim_id")
    assert ident_err is None
    assert ident == "ID-7"


def test_inferred_trim_is_not_applied_on_a_normal_load() -> None:
    from connectors.writer_common import _convert_mapped_cell

    value, err = _convert_mapped_cell(
        "  pad  ",
        "trim",
        {"source": "note", "target": "note", "transform": "trim"},
        empty_cells_as_null=False,
        dest_nullability={},
        target="note",
    )
    assert err is None
    assert value == "  pad  "


def test_explicit_trim_still_strips() -> None:
    from connectors.writer_common import _convert_mapped_cell

    value, err = _convert_mapped_cell(
        "  pad  ",
        "trim",
        {"source": "note", "target": "note", "transform": "trim", "user_override": True},
        empty_cells_as_null=False,
        dest_nullability={},
        target="note",
    )
    assert err is None
    assert value == "pad"


def test_base64_error_says_it_decodes() -> None:
    _out, err = apply_transform("hello", "base64")
    assert err is not None
    assert "does not encode plain text" in err


def test_sql_literal_mask_keeps_quotes_and_semicolons() -> None:
    masked, literals = mask_sql_string_literals("SELECT 'a;b''c' FROM t")
    assert ";" not in masked
    assert "''" not in masked
    assert literals == ["'a;b''c'"]


def test_sqlalchemy_colon_inside_a_literal_is_escaped() -> None:
    sql = "SELECT 'call :1 now' AS label, :since AS bound"
    escaped = escape_sqlalchemy_literal_colons(sql)
    assert r"\:1" in escaped
    assert ":since" in escaped
    assert r"\:since" not in escaped


def test_file_identity_column_becomes_primary_key() -> None:
    catalog = SourceSchemaCatalog(
        dialect="csv",
        columns=["CUST_ID", "AMT"],
        column_types={"CUST_ID": "INTEGER", "AMT": "TEXT"},
        nullable={"CUST_ID": True, "AMT": True},
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="sqlite",
        target_columns=["CUST_ID", "AMT"],
        target_types=["INTEGER", "TEXT"],
    )
    assert plan.primary_key == ["CUST_ID"]
    assert "CUST_ID" in plan.not_null_columns
    assert any("PRIMARY KEY" in clause for clause in plan.table_constraints)


def test_database_without_a_key_is_not_given_one() -> None:
    catalog = SourceSchemaCatalog(
        dialect="sqlite",
        columns=["CUST_ID", "AMT"],
        column_types={"CUST_ID": "INTEGER", "AMT": "TEXT"},
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="sqlite",
        target_columns=["CUST_ID", "AMT"],
        target_types=["INTEGER", "TEXT"],
    )
    assert plan.primary_key == []


def test_operator_upsert_key_is_the_destination_primary_key() -> None:
    catalog = SourceSchemaCatalog(
        dialect="sqlite",
        columns=["id", "name"],
        column_types={"id": "INTEGER", "name": "TEXT"},
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="sqlite",
        target_columns=["id", "name"],
        target_types=["INTEGER", "TEXT"],
        operator_key_columns=["id"],
    )
    assert plan.primary_key == ["id"]
    assert "id" in plan.not_null_columns


def test_sqlite_overwrite_publishes_in_one_swap() -> None:
    from connectors.sqlite_writer import write_mapped_rows

    import tempfile
    from pathlib import Path

    path = Path(tempfile.mkdtemp()) / "ow.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE live (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO live VALUES (1, 'old')")
    conn.commit()
    conn.close()
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "name", "target": "name"},
    ]
    result = write_mapped_rows(
        host="",
        port=0,
        database=str(path),
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name="live",
        headers=["id", "name"],
        data_rows=[["2", "new"]],
        mappings=mappings,
        column_types={"id": "INTEGER", "name": "TEXT"},
        atomic_overwrite=True,
        total_chunks=1,
        file_batch_idx=0,
    )
    assert result.ok, result.error
    seen = sqlite3.connect(path)
    rows = seen.execute("SELECT id, name FROM live").fetchall()
    seen.close()
    assert rows == [(2, "new")]


def test_empty_population_checksum_is_not_a_pass() -> None:
    ladder = run_five_layer_verification(
        source_rows=[],
        target_rows=[],
        columns=["id"],
        pk_column="id",
        source_row_count=0,
        target_row_count=0,
    )
    assert ladder["layers"]["L3"]["passed"] is False
    assert ladder["layers"]["L3"]["details"]["reason"] == "empty_population"
    assert ladder["population_checksum_proof"] is False
    assert ladder["passed"] is True


def test_l2_failure_fails_the_ladder_even_when_asked() -> None:
    ladder = run_five_layer_verification(
        source_rows=[{"id": 1, "amount": 5}],
        target_rows=[{"id": 1, "amount": 9}],
        columns=["id", "amount"],
        pk_column="id",
    )
    assert ladder["layers"]["L2"]["passed"] is False
    assert ladder["passed"] is False


def test_personal_name_and_salary_require_review() -> None:
    hr = score_compliance_risk(
        ["full_name", "salary"],
        [{"full_name": "Ada", "salary": "10"}],
    )
    assert hr["requires_review"] is True
    assert "full_name" in hr["high_risk_fields"]
    assert "salary" in hr["high_risk_fields"]
    email = score_compliance_risk(["email"], [{"email": "a@b.co"}])
    assert "email" not in email["high_risk_fields"]
    user = score_compliance_risk(["username"], [{"username": "ada"}])
    assert user["requires_review"] is False


def test_sync_contract_root_ids_differ_by_issue() -> None:
    first = _root_id("sync_contract_incomplete", ["CDC is not supported"], ["g9"])
    second = _root_id("sync_contract_incomplete", ["missing primary key"], ["g9"])
    assert first != second
    assert first != "rc-sync-contract-incomplete-c29ba4643b58"


def test_type_change_is_not_reported_as_mapping_confidence() -> None:
    preflight = {
        "gates": [
            {
                "id": "schema_drift",
                "status": "block",
                "message": "Schema change detected",
                "details": {
                    "schema_evolution": {
                        "breaking": [
                            {
                                "kind": "type_change",
                                "column": "val",
                                "old_type": "TEXT",
                                "new_type": "INTEGER",
                            }
                        ]
                    }
                },
            },
            {
                "id": "g4_mapping_confidence",
                "status": "block",
                "message": "1 mapping(s) below floor 0.85: val",
                "details": {"low_confidence": ["val"]},
            },
        ],
        "blockers": [],
    }
    roots = build_root_causes(preflight)
    kinds = [r.kind for r in roots]
    assert "schema_type_drift" in kinds
    assert "mapping_confidence" not in kinds


def test_unparsed_filter_tail_is_refused() -> None:
    from src.ai.copilot.aggregate_tools import _unparsed_filter_warning

    warning = _unparsed_filter_warning(
        "STS = POSTED' OR '1'='1",
        "' OR '1'='1",
    )
    assert "not run" in warning
    assert _unparsed_filter_warning("status = open and amount > 10", "and") == ""


def test_injection_tail_is_bound_and_or_tail_is_not() -> None:
    from src.ai.copilot.aggregate_tools import _absorb_bound_literal
    from src.ai.copilot.predicates import ParsedFilter

    parsed = ParsedFilter(column="status", op="eq", raw_values=["x"], text="status = x")
    bound = _absorb_bound_literal(
        "status = x'; DROP TABLE w91; --",
        parsed,
        "DROP TABLE w91; --",
    )
    assert bound is not None
    assert bound.raw_values == ["x'; DROP TABLE w91; --"]
    assert (
        _absorb_bound_literal(
            "STS = POSTED' OR '1'='1",
            ParsedFilter(column="STS", op="eq", raw_values=["POSTED"], text="STS = POSTED"),
            "' OR '1'='1",
        )
        is None
    )


def test_column_transforms_stamp_an_override() -> None:
    from src.ai.copilot.transfer_tools import _apply_column_transforms

    rows, refusal = _apply_column_transforms(
        [{"source": "email", "target": "email", "transform": "none"}],
        {"email": "hash_pii"},
    )
    assert refusal == ""
    assert rows[0]["transform"] == "hash_pii"
    assert rows[0]["transform_override"] is True


def test_duplicate_key_message_names_the_value() -> None:
    from connectors.sqlite_writer import _sqlite_duplicate_key_message

    text = _sqlite_duplicate_key_message(
        sqlite3.IntegrityError("UNIQUE constraint failed: live.id"),
        ["id"],
        ["id", "name"],
        [(7, "a")],
    )
    assert "id=7" in text
    assert "duplicate key" in text


def test_offset_text_into_timestamptz_is_not_a_fidelity_collapse() -> None:
    from services.type_coercion import is_lossy_coercion

    offsets = ["2024-01-01T05:00:00+05:30", "2024-06-01T00:00:00Z", "2024-01-01T12:00:00-08:00"]
    assert is_lossy_coercion("VARCHAR", "TIMESTAMPTZ", population=offsets) is False
    assert is_lossy_coercion("TEXT", "TIMESTAMPTZ", population=["2024-01-01 00:00:00"]) is True


def test_source_timezone_upgrades_sqlite_text_wall_clock() -> None:
    from services.timezone_policy import declare_zone_on_schema, stamp_zone_transform

    rows, columns = declare_zone_on_schema(
        [{
            "name": "created",
            "inferred_type": "TEXT",
            "samples": ["2026-10-01 00:00:00", "2026-10-02 00:00:00"],
        }],
        "America/New_York",
    )
    assert columns == {"created"}
    assert rows[0]["inferred_type"] == "TIMESTAMPTZ"
    stamped = stamp_zone_transform(
        [{"source": "created", "target": "created", "transform": "none"}],
        "America/New_York",
        columns,
    )
    assert stamped[0]["transform"] == "assume_timezone:America/New_York"
    offset_rows, offset_cols = declare_zone_on_schema(
        [{
            "name": "tsz",
            "inferred_type": "TEXT",
            "samples": ["2024-01-01T05:00:00+05:30"],
        }],
        "UTC",
    )
    assert offset_cols == set()
    assert offset_rows[0]["inferred_type"] == "TEXT"


def test_mutating_pragma_is_refused_and_table_info_stays() -> None:
    from src.routers.query_router import _is_safe_sql

    assert _is_safe_sql("PRAGMA table_info(users)") is True
    assert _is_safe_sql("PRAGMA writable_schema = ON") is False
    assert _is_safe_sql("PRAGMA journal_mode = WAL") is False


def test_query_error_does_not_echo_the_sql() -> None:
    from services.safe_errors import user_query_error

    error = user_query_error(
        RuntimeError(
            "no such column: missing\n"
            "[SQL: SELECT secret FROM payments WHERE id = 1]\n"
            "(Background on this error at: https://sqlalche.me/e/20/e3q8)"
        ),
        public_message="Query failed",
    )
    assert "no such column: missing" in error.detail
    assert "SELECT secret" not in error.detail
    assert "sqlalche.me" not in error.detail
