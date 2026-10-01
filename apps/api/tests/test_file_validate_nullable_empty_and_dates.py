"""File Validate must match the file writer on blanks and settled dates.

Excel → Postgres CREATE (the Datawrap_Critical_Source_Data path):

* INTEGER → BIGINT is widening. A blank phone cell is SQL NULL on a nullable
  CREATE column, not a fidelity collapse and not "Empty value cannot coerce".
* ``01/15/2024`` settles MDY, so ``11/03/1992`` parses as November 3.
* A lone ``05/06/2024`` stays fail-closed until the operator sets MDY or DMY.
* Proven NOT NULL, unknown physical DDL, and DB→DB (flag off) still block.
"""

from __future__ import annotations

from services.coercion_probe import analyze_coercion
from services.root_cause_engine import _is_fidelity_signal
from services.transform_engine import (
    apply_transform,
    dry_run_sample,
    preview_quarantine_cells,
    reset_active_date_locale,
    set_active_date_locale,
)
from services.type_system import is_lossy_coercion, is_precision_collapse_coercion


def test_integer_to_bigint_is_widening_not_collapse():
    assert is_lossy_coercion("INTEGER", "BIGINT") is False
    assert is_precision_collapse_coercion("INTEGER", "BIGINT") is False
    assert is_lossy_coercion("INTEGER", "BIGINT", dest_db="postgresql", dest_table_exists=False) is False


def test_file_blank_phone_on_nullable_bigint_is_sql_null():
    rows = [{"phone": "5551234567"}, {"phone": ""}, {"phone": ""}, {"phone": ""}]
    report = analyze_coercion(
        sample_rows=rows,
        mappings=[{
            "source": "phone",
            "target": "phone",
            "target_type": "BIGINT",
            "transform": "none",
            "create_new": True,
        }],
        source_types={"phone": "INTEGER"},
        dest_types={"phone": "BIGINT"},
        dest_db_type="postgresql",
        table_exists=False,
        validation_mode="strict",
        empty_cells_as_null=True,
        dest_nullability={"phone": True},
    )
    assert report["has_blocking_failures"] is False
    cols = report.get("columns") or []
    if cols:
        assert cols[0]["failed"] == 0
        assert cols[0]["nulls"] == 3
        assert cols[0].get("fidelity_collapse") is False


def test_blank_phone_still_blocks_when_flag_off_or_not_null():
    mapping = {
        "source": "phone",
        "target": "phone",
        "target_type": "BIGINT",
        "transform": "integer",
        "create_new": True,
    }
    blocked = analyze_coercion(
        sample_rows=[{"phone": ""}],
        mappings=[mapping],
        source_types={"phone": "INTEGER"},
        dest_types={"phone": "BIGINT"},
        dest_db_type="postgresql",
        table_exists=False,
        empty_cells_as_null=False,
    )
    assert blocked["has_blocking_failures"] is True

    not_null = analyze_coercion(
        sample_rows=[{"phone": ""}],
        mappings=[{**mapping, "target_nullable": False}],
        source_types={"phone": "INTEGER"},
        dest_types={"phone": "BIGINT"},
        dest_db_type="postgresql",
        table_exists=False,
        empty_cells_as_null=True,
        dest_nullability={"phone": False},
    )
    assert not_null["has_blocking_failures"] is True
    reason = not_null["columns"][0]["sample_failures"][0]["reason"]
    assert "Empty value cannot coerce" in reason


def test_dry_run_and_preview_treat_file_blanks_as_null():
    headers = ["phone"]
    rows = [["5551234567"], [""], [""]]
    mappings = [{
        "source": "phone",
        "target": "phone",
        "target_type": "BIGINT",
        "transform": "none",
        "create_new": True,
    }]
    ok, errors = dry_run_sample(
        headers=headers,
        sample_rows=rows,
        mappings=mappings,
        column_types={"phone": "INTEGER"},
        empty_cells_as_null=True,
        dest_nullability={"phone": True},
    )
    assert ok, errors
    assert not any("cannot coerce" in e.lower() for e in errors)

    preview = preview_quarantine_cells(
        headers=headers,
        sample_rows=rows,
        mappings=mappings,
        column_types={"phone": "INTEGER"},
        empty_cells_as_null=True,
        dest_nullability={"phone": True},
    )
    assert preview["quarantine_count"] == 0


def test_unambiguous_sibling_settles_mdy_for_dob():
    headers = ["dob"]
    rows = [
        ["01/15/2024"],
        ["11/03/1992"],
        ["12/01/1990"],
    ]
    mappings = [{
        "source": "dob",
        "target": "dob",
        "target_type": "DATE",
        "transform": "date_iso",
    }]
    ok, errors = dry_run_sample(
        headers=headers,
        sample_rows=rows,
        mappings=mappings,
        column_types={"dob": "DATE"},
    )
    assert ok, errors

    preview = preview_quarantine_cells(
        headers=headers,
        sample_rows=rows,
        mappings=mappings,
        column_types={"dob": "DATE"},
    )
    assert preview["quarantine_count"] == 0
    messages = " ".join(str(c.get("message") or "") for c in preview["cells"])
    assert "11/03/1992" not in messages
    assert "12/01/1990" not in messages


def test_lone_ambiguous_date_stays_fail_closed():
    token = set_active_date_locale("")
    try:
        val, err = apply_transform("05/06/2024", "date")
        assert val is None
        assert err and "Invalid date" in err
        assert "MDY" in err and "DMY" in err

        preview = preview_quarantine_cells(
            headers=["dob"],
            sample_rows=[["05/06/2024"]],
            mappings=[{
                "source": "dob",
                "target": "dob",
                "target_type": "DATE",
                "transform": "date",
            }],
            column_types={"dob": "DATE"},
        )
        assert preview["quarantine_count"] == 1
        assert "Invalid date" in preview["cells"][0]["message"]
        assert "MDY" in preview["cells"][0]["message"]
    finally:
        reset_active_date_locale(token)


def test_excel_create_new_phone_and_dob_do_not_block_validate():
    """The live studio case: 13 identity mappings, CREATE BIGINT phone, MDY dob."""
    from services.preflight_service import run_file_preflight

    columns = ["phone", "dob"]
    rows = [
        {"phone": "5551234567", "dob": "01/15/2024"},
        {"phone": "", "dob": "11/03/1992"},
        {"phone": "", "dob": "12/01/1990"},
        {"phone": "5559876543", "dob": "02/28/2020"},
        {"phone": "", "dob": "06/15/2019"},
    ]
    result = run_file_preflight(
        columns=columns,
        column_types={"phone": "INTEGER", "dob": "DATE"},
        row_count=len(rows),
        mappings=[
            {
                "source": "phone",
                "target": "phone",
                "confidence": 0.93,
                "target_type": "BIGINT",
                "transform": "none",
                "create_new": True,
            },
            {
                "source": "dob",
                "target": "dob",
                "confidence": 0.93,
                "target_type": "DATE",
                "transform": "date_iso",
                "create_new": True,
            },
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="file",
        source_format="xlsx",
        sync_mode="full_refresh_append",
        sample_rows=rows,
        confidence_threshold=0.85,
        validation_mode="strict",
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="manual_review",
    )
    blocked = [
        g for g in result.get("gates") or []
        if str(g.get("status") or "").lower() in {"block", "fail", "blocked"}
    ]
    messages = " ".join(str(g.get("message") or "") for g in blocked)
    assert "phone" not in messages.lower(), messages
    assert "11/03/1992" not in messages
    g8 = next(g for g in result.get("gates") or [] if g.get("id") == "g8_reconciliation")
    # Validate receives transform "none". The plan resolves the write cast, so
    # the same gate Execute runs names the blanks instead of a clean identity pass.
    assert int((g8.get("details") or {}).get("file_blank_null_count") or 0) == 3
    assert "3 spreadsheet blanks stored as SQL NULL" in str(g8.get("message") or "")
    assert "fidelity collapse" not in messages.lower()
    kinds = [r.get("kind") if isinstance(r, dict) else getattr(r, "kind", "") for r in (result.get("root_causes") or [])]
    assert "fidelity_collapse" not in kinds, result.get("root_causes")
    assert result.get("date_locale") == "MDY"


def test_one_column_does_not_lend_its_date_order_to_another():
    """01/15/2024 settles dob only. A different column of 05/06 stays ambiguous."""
    from services.preflight_service import run_file_preflight

    rows = [
        {"dob": "01/15/2024", "hired": "05/06/2024"},
        {"dob": "11/03/1992", "hired": "06/07/2024"},
    ]
    result = run_file_preflight(
        columns=["dob", "hired"],
        column_types={"dob": "DATE", "hired": "DATE"},
        row_count=2,
        mappings=[
            {
                "source": "dob",
                "target": "dob",
                "confidence": 0.93,
                "target_type": "DATE",
                "transform": "date_iso",
                "create_new": True,
            },
            {
                "source": "hired",
                "target": "hired",
                "confidence": 0.93,
                "target_type": "DATE",
                "transform": "date_iso",
                "create_new": True,
            },
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="file",
        source_format="xlsx",
        sync_mode="full_refresh_append",
        sample_rows=rows,
        confidence_threshold=0.85,
        validation_mode="strict",
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="manual_review",
    )
    report = result.get("date_locale_report") or {}
    ambiguous = [c.get("column") for c in report.get("ambiguous_columns") or []]
    assert "hired" in ambiguous, report
    assert "dob" not in ambiguous, report
    assert report.get("decision") == "set_locale"
    blob = " ".join(str(g.get("message") or "") for g in result.get("gates") or [])
    assert "11/03/1992" not in blob
    assert "05/06/2024" in blob or "hired" in blob.lower()


def test_connector_cannot_claim_the_file_blank_rule():
    from services.preflight_service import resolve_preflight_source_kind

    assert resolve_preflight_source_kind(
        "file", source_connector_id="conn_pg", source_file_id=""
    ) == "database"
    assert resolve_preflight_source_kind(
        "file", source_connector_id="conn_pg", source_file_id="stale_upload"
    ) == "database"
    assert resolve_preflight_source_kind(
        "file", source_connector_id="", source_file_id="upload_1"
    ) == "file"
    assert resolve_preflight_source_kind(
        "", source_connector_id="conn_pg"
    ) == "database"
    assert resolve_preflight_source_kind("file") == "file"


def test_database_blank_integer_still_blocks_and_is_not_called_collapse():
    """DB extracts do not turn '' into NULL. The block is nullability, not fidelity."""
    from services.preflight_service import run_file_preflight

    result = run_file_preflight(
        columns=["phone"],
        column_types={"phone": "INTEGER"},
        row_count=1,
        mappings=[{
            "source": "phone",
            "target": "phone",
            "confidence": 0.93,
            "target_type": "BIGINT",
            "transform": "none",
            "create_new": True,
        }],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="postgresql",
        sync_mode="full_refresh_append",
        sample_rows=[{"phone": ""}],
        confidence_threshold=0.85,
        validation_mode="strict",
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="manual_review",
    )
    blob = " ".join(
        str(g.get("message") or "") + " " + str((g.get("details") or {}).get("issues") or "")
        for g in result.get("gates") or []
        if str(g.get("status") or "").lower() in {"block", "fail", "blocked"}
    )
    assert "cannot coerce" in blob.lower() or "empty" in blob.lower(), blob
    kinds = [
        r.get("kind") if isinstance(r, dict) else getattr(r, "kind", "")
        for r in (result.get("root_causes") or [])
    ]
    assert "fidelity_collapse" not in kinds, result.get("root_causes")


def _phone_rows():
    return [
        {"phone": "5551234567"},
        {"phone": ""},
        {"phone": "5550001111"},
        {"phone": ""},
        {"phone": "5552223333"},
        {"phone": ""},
    ]


def _run_phone_preflight(*, source_kind: str, nullable: bool, table_exists: bool):
    from services.preflight_service import run_file_preflight

    return run_file_preflight(
        columns=["phone"],
        column_types={"phone": "INTEGER"},
        row_count=6,
        mappings=[{
            "source": "phone",
            "target": "phone",
            "confidence": 0.93,
            # Execute stamps the write-path cast before Gate-8. Validate often
            # still has transform "none"; both must share the file-blank contract.
            "target_type": "BIGINT",
            "transform": "integer",
            "create_new": not table_exists,
        }],
        destination_connected=True,
        source_connected=True,
        source_kind=source_kind,
        source_format="xlsx" if source_kind == "file" else "postgresql",
        sync_mode="full_refresh_append",
        sample_rows=_phone_rows(),
        confidence_threshold=0.85,
        validation_mode="strict",
        destination_column_types={"phone": "BIGINT"} if table_exists else {},
        destination_column_nullability={"phone": nullable},
        destination_table_exists=table_exists,
        destination_can_create=not table_exists,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="manual_review",
    )


def test_execute_stamped_integer_file_blanks_do_not_fail_gate8():
    """Live Run failure: approved Validate, then Gate-8 rejected blank phones.

    Rows 2, 4, and 6 are empty. The destination BIGINT is nullable, so those
    cells are SQL NULL and the other rows stay writable.
    """
    result = _run_phone_preflight(source_kind="file", nullable=True, table_exists=True)
    blocked = [
        g for g in result.get("gates") or []
        if str(g.get("status") or "").lower() in {"block", "fail", "blocked"}
        and "phone" in str(g.get("message") or "").lower()
    ]
    assert blocked == [], [g.get("message") for g in blocked]
    g8 = next(g for g in result.get("gates") or [] if g.get("id") == "g8_reconciliation")
    assert str(g8.get("status") or "").lower() in {"pass", "passed", "ok"}
    assert int((g8.get("details") or {}).get("file_blank_null_count") or 0) == 3
    kinds = [
        r.get("kind") if isinstance(r, dict) else getattr(r, "kind", "")
        for r in (result.get("root_causes") or [])
    ]
    assert "sample_transform" not in kinds


def test_not_null_phone_blanks_are_inspectable_quarantine_rows():
    """Proven NOT NULL still blocks, and each blank names row, column, and cell."""
    from services.quarantine_from_preflight import quarantine_rows_from_preflight
    from services.value_serializer import SQL_NULL_SENTINEL

    result = _run_phone_preflight(source_kind="file", nullable=False, table_exists=True)
    g8 = next(g for g in result.get("gates") or [] if g.get("id") == "g8_reconciliation")
    assert str(g8.get("status") or "").lower() in {"block", "fail", "blocked"}
    rows = quarantine_rows_from_preflight(result)
    phone = [r for r in rows if r.get("column") == "phone"]
    assert [r.get("row") for r in phone] == [2, 4, 6], phone
    for row in phone:
        assert row["target"] == "phone"
        assert row["value"] == ""
        assert row["value"] != SQL_NULL_SENTINEL
        assert row["values"]["phone"] == ""
        assert "Empty value cannot coerce to integer" in row["reason"]
        assert "phone→phone" not in row["reason"]
    assert quarantine_rows_from_preflight({"gates": [], "blockers": []}) == []


def test_database_integer_blank_still_blocks_after_gate8_file_contract():
    result = _run_phone_preflight(source_kind="database", nullable=True, table_exists=False)
    blob = " ".join(
        str(g.get("message") or "")
        for g in result.get("gates") or []
        if str(g.get("status") or "").lower() in {"block", "fail", "blocked"}
    )
    assert "cannot coerce" in blob.lower() or "empty" in blob.lower(), blob


def test_empty_cell_g3_block_is_not_a_fidelity_root():
    details = {
        "issues": [
            "Column 'phone' → BIGINT: 3 of 10 sampled value(s) are empty "
            "and cannot coerce to integer. This is a nullability problem."
        ],
        "issues_detail": [{
            "source": "phone",
            "source_type": "INTEGER",
            "target_type": "BIGINT",
            "probe_cast_only": True,
            "fidelity_collapse": False,
            "declared_lossy": False,
        }],
    }
    assert _is_fidelity_signal(
        "1 type coercion issue(s)",
        details,
        "g3_schema_contract",
    ) is False

    lossy = {
        "issues": ["Lossy coercion: amount (FLOAT) → amount (INTEGER)"],
        "issues_detail": [{
            "source": "amount",
            "fidelity_collapse": True,
            "declared_lossy": True,
        }],
    }
    assert _is_fidelity_signal(
        "1 type coercion issue(s)",
        lossy,
        "g3_schema_contract",
    ) is True
