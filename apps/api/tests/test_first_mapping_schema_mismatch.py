"""A first mapping with no saved contract is an ADD COLUMN review, not drift.

The destination already has id and the edge columns. customer_email,
order_amt, order_date, and status are missing, so Execute stays blocked
until those ADD COLUMN proposals are reviewed. The copy must not say drift,
source grew, or offer acknowledgement. A later saved contract still does.
"""

from __future__ import annotations

from services.preflight_service import run_file_preflight


EDGE = [
    "id",
    "edge_bool",
    "edge_uuid",
    "edge_json",
    "edge_bytes",
    "edge_uint",
    "edge_float",
    "edge_text",
    "edge_date",
    "edge_ts",
    "edge_num",
    "edge_char",
]
MISSING = ["customer_email", "order_amt", "order_date", "status"]


def _run(*, stored: str = "", previous: list[str] | None = None) -> dict:
    columns = ["id", *MISSING]
    schema = {name: "VARCHAR" for name in columns}
    schema["id"] = "INTEGER"
    mappings = [
        {"source": name, "target": name, "confidence": 1, "transform": "none"}
        for name in columns
    ]
    return run_file_preflight(
        columns=columns,
        column_types=schema,
        row_count=1,
        mappings=mappings,
        sample_rows=[{name: "1" if name == "id" else "x" for name in columns}],
        destination_connected=True,
        destination_can_create=True,
        destination_can_write=True,
        destination_table_exists=True,
        destination_column_types={name: "VARCHAR" for name in EDGE} | {"id": "INTEGER"},
        destination_db_type="postgresql",
        source_connected=True,
        source_kind="file",
        source_format="csv",
        source_filename="sample-orders.csv",
        sync_mode="full_refresh_append",
        schema_policy="manual_review",
        validation_mode="balanced",
        stored_source_fp=stored,
        previous_source_columns=previous,
    )


def test_no_saved_contract_is_add_column_review_not_drift() -> None:
    result = _run()
    assert result["passed"] is False
    gate = next(g for g in result["gates"] if g["id"] == "schema_mismatch")
    message = gate["message"].lower()
    assert "add column" in message
    for name in MISSING:
        assert name in message
    assert "drift" not in message
    assert "source grew" not in message
    assert "acknowledge" not in message
    assert gate["details"]["ack_required"] is False
    assert gate["details"]["remediation_kind"] == "review_add_column"
    blob = str(result.get("blockers")).lower() + str(result.get("schema_drift")).lower()
    assert "acknowledge drift" not in blob
