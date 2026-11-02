"""D237: Gate-8 called a Neo4j node's blank properties "spreadsheet blanks".

The blank-as-SQL-NULL clause hard-coded "spreadsheet". It is only a
spreadsheet for a file/object-store record batch; a connector extract
(graph, document, database) names them source blanks.
"""

from __future__ import annotations

from services.preflight_service import run_file_preflight

ROWS = [
    {"qty": "3", "note": "a"},
    {"qty": "", "note": "b"},
    {"qty": "7", "note": "c"},
]


def _g8(source_kind: str, source_format: str) -> str:
    result = run_file_preflight(
        columns=["qty", "note"],
        column_types={"qty": "INTEGER", "note": "TEXT"},
        row_count=len(ROWS),
        mappings=[
            {"source": "qty", "target": "qty", "confidence": 0.95,
             "target_type": "BIGINT", "transform": "integer", "create_new": True},
            {"source": "note", "target": "note", "confidence": 0.95,
             "target_type": "TEXT", "transform": "none", "create_new": True},
        ],
        destination_connected=True,
        source_connected=True,
        source_kind=source_kind,
        source_format=source_format,
        sync_mode="full_refresh_append",
        sample_rows=ROWS,
        confidence_threshold=0.85,
        validation_mode="balanced",
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="manual_review",
    )
    g8 = next(g for g in result.get("gates") or [] if g.get("id") == "g8_reconciliation")
    return str(g8.get("message") or "")


def test_graph_source_blank_is_not_called_a_spreadsheet_blank():
    msg = _g8("database", "neo4j")
    print("G8", msg)
    assert "stored as SQL NULL" in msg, msg
    assert "spreadsheet" not in msg.lower(), msg
    assert "1 source blank stored as SQL NULL" in msg, msg


def test_file_source_keeps_spreadsheet_wording():
    assert "1 spreadsheet blank stored as SQL NULL" in _g8("file", "csv")
