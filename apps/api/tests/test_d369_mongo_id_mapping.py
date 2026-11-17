"""D369: an unrelated column is never proposed as MongoDB ``_id``.

DATE, INT and TEXT columns beside an existing ``_id`` stay off that key.
An omitted column is not part of read-back. Migration rule 4.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))

from services.reconciliation import sample_compare_rows  # noqa: E402
from services.semantic_mapper import map_columns  # noqa: E402


def test_date_int_and_text_are_not_mapped_onto_id() -> None:
    mappings = map_columns(
        ["id", "d", "qty", "name", "note"],
        ["_id", "id", "loyalty_tier", "app_meta"],
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER", "samples": ["1", "2"]},
            {"name": "d", "inferred_type": "DATE", "samples": ["2026-01-01", "2026-02-02"]},
            {"name": "qty", "inferred_type": "INTEGER", "samples": ["10", "20"]},
            {"name": "name", "inferred_type": "VARCHAR", "samples": ["ada", "beau"]},
            {"name": "note", "inferred_type": "TEXT", "samples": ["", "memo"]},
        ],
        target_schemas=[
            {"name": "_id", "inferred_type": "OBJECTID"},
            {"name": "id", "inferred_type": "INTEGER"},
            {"name": "loyalty_tier", "inferred_type": "VARCHAR"},
            {"name": "app_meta", "inferred_type": "JSON"},
        ],
        destination_db_type="mongodb",
        destination_table_exists=True,
        source_db_type="postgresql",
    )
    onto_id = [m for m in mappings if str(m.get("target")) == "_id"]
    assert onto_id == []
    by_source = {m["source"]: m["target"] for m in mappings}
    assert by_source.get("id") == "id"


def test_source_id_may_map_onto_destination_id() -> None:
    mappings = map_columns(
        ["_id", "name"],
        ["_id", "name"],
        source_schemas=[
            {"name": "_id", "inferred_type": "OBJECTID"},
            {"name": "name", "inferred_type": "VARCHAR"},
        ],
        target_schemas=[
            {"name": "_id", "inferred_type": "OBJECTID"},
            {"name": "name", "inferred_type": "VARCHAR"},
        ],
        destination_db_type="mongodb",
        destination_table_exists=True,
    )
    by_source = {m["source"]: m["target"] for m in mappings}
    assert by_source.get("_id") == "_id"


def test_omitted_column_is_not_compared_on_read_back() -> None:
    result = sample_compare_rows(
        [{"id": "1", "d": None, "name": "ada"}],
        [{"id": "1", "_id": "6afb9c4272091f51d66bf9e5", "name": "ada"}],
        [
            {"source": "id", "target": "id"},
            {"source": "d", "target": "_id", "transform": "omit"},
            {"source": "name", "target": "name"},
        ],
        sort_key="id",
    )
    assert result["passed"] is True
    assert result["mismatches"] == []
    assert result["compared"] == 2
