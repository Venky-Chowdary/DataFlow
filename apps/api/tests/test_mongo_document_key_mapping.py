"""A MongoDB re-run must bind ``id`` to ``id``, never to the document key.

DataFlow's own MongoDB writer creates ``_id`` alongside the operator's ``id``.
On the second sync Map folded the two names onto one identifier, so the source
``id`` tied with the reserved document key, the assignment could pin the key,
and every re-run of a collection DataFlow created demanded manual review.
"""

from services.semantic_mapper import map_columns

SOURCE_COLUMNS = ["id", "name", "amount"]
SOURCE_SCHEMAS = [
    {"name": "id", "type": "BIGINT"},
    {"name": "name", "type": "VARCHAR(120)"},
    {"name": "amount", "type": "DECIMAL(12,2)"},
]
TARGET_COLUMNS = ["_id", "id", "name", "amount"]
TARGET_SCHEMAS = [
    {"name": "_id", "type": "VARCHAR"},
    {"name": "id", "type": "BIGINT"},
    {"name": "name", "type": "VARCHAR"},
    {"name": "amount", "type": "DECIMAL(12,2)"},
]


def _map() -> dict[str, dict]:
    mappings = map_columns(
        SOURCE_COLUMNS,
        TARGET_COLUMNS,
        source_schemas=SOURCE_SCHEMAS,
        target_schemas=TARGET_SCHEMAS,
        destination_db_type="mongodb",
        destination_table_exists=True,
    )
    return {m["source"]: m for m in mappings}


def test_id_binds_to_id_not_the_document_key() -> None:
    assert _map()["id"]["target"] == "id"


def test_id_on_an_existing_collection_needs_no_review() -> None:
    mapping = _map()["id"]
    assert mapping["requires_review"] is False
    assert mapping["confidence"] >= 0.9


def test_document_key_is_left_unmapped() -> None:
    assert "_id" not in {m["target"] for m in _map().values()}


def test_real_fold_collision_still_demands_review() -> None:
    """``UserID`` vs ``userid`` remains ambiguous — the fix is scoped to ``_id``."""
    mappings = map_columns(
        ["user_id"],
        ["UserID", "userid"],
        source_schemas=[{"name": "user_id", "type": "BIGINT"}],
        target_schemas=[
            {"name": "UserID", "type": "BIGINT"},
            {"name": "userid", "type": "BIGINT"},
        ],
        destination_db_type="mysql",
        destination_table_exists=True,
    )
    assert mappings[0]["requires_review"] is True
