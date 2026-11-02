"""D150: a field missing from a document is NULL for that row, not a missing column.

Validate's transform dry-run took its headers from the first sampled row. A
MongoDB collection with a sparse field (``payload_k`` absent from the first
document) therefore failed G5 with "Source column missing: payload_k" — even
after the operator signed the risk contract — although the writer stores NULL
for exactly those rows.
"""

from __future__ import annotations

from services.preflight_service import confidence_threshold_for_mode, run_file_preflight


def _preflight(rows, columns, *, source_format="mongodb"):
    return run_file_preflight(
        columns=columns,
        column_types={"_id": "INTEGER", "name": "TEXT", "payload_k": "TEXT"},
        row_count=len(rows),
        mappings=[{"source": c, "target": c, "confidence": 0.99} for c in columns],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format=source_format,
        sync_mode="full_refresh_overwrite",
        sample_rows=rows,
        confidence_threshold=confidence_threshold_for_mode("strict"),
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="sqlserver",
    )


def _g5(result):
    return next(g for g in result["gates"] if g["id"] == "g5_dry_run")


COLUMNS = ["_id", "name", "payload_k"]


def test_field_absent_from_the_first_document_passes_dry_run():
    rows = [
        {"_id": 1, "name": "a"},
        {"_id": 2, "name": "b", "payload_k": "k2"},
        {"_id": 3, "name": "c", "payload_k": "k3"},
    ]
    g5 = _g5(_preflight(rows, COLUMNS))
    assert g5["status"] == "pass", g5["message"]


def test_declared_document_field_no_sampled_document_carries_is_null():
    rows = [{"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}]
    g5 = _g5(_preflight(rows, COLUMNS))
    assert g5["status"] == "pass", g5["message"]


def test_relational_source_still_reports_a_genuinely_missing_column():
    rows = [{"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}]
    g5 = _g5(_preflight(rows, COLUMNS, source_format="postgresql"))
    assert g5["status"] == "block"
    assert "Source column missing: payload_k" in g5["message"]


def test_integrity_dry_run_headers_are_the_union_of_sampled_keys():
    from services.data_integrity import _check_transform_dry_run

    rows = [{"_id": 1, "name": "a"}, {"_id": 2, "name": "b", "payload_k": "k2"}]
    result = _check_transform_dry_run(
        [{"source": c, "target": c} for c in COLUMNS],
        [],
        {"_id": "INTEGER", "name": "TEXT", "payload_k": "TEXT"},
        rows,
        dest_kind="sqlserver",
        database_extract=True,
    )
    assert not any("Source column missing" in i for i in result["issues"]), result
