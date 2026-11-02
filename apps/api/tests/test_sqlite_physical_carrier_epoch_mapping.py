import sqlite3

from services.mapping_pipeline import run_mapping_pipeline
from services.schema_introspect import introspect_schema
from src.transfer.adapters_introspect import _introspect_table_schema_rich


def _sqlite_destination(tmp_path):
    database = tmp_path / "destination.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE contacts (id INTEGER PRIMARY KEY, created_at TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO contacts (id, created_at) VALUES (?, ?)",
            (1, "1767225500"),
        )
    return database


def _source_schema():
    return [
        {
            "name": "created_at",
            "inferred_type": "INTEGER",
            "native_type": "INTEGER",
            "samples": ["1767225500"],
        }
    ]


def test_existing_sqlite_text_epoch_uses_physical_type_for_mapping(tmp_path):
    database = _sqlite_destination(tmp_path)
    cfg = {"database": str(database)}

    inferred, _, _ = _introspect_table_schema_rich(
        "sqlite", cfg, "contacts", [], strict_namespace=True
    )
    physical, _, _ = _introspect_table_schema_rich(
        "sqlite", cfg, "contacts", [], strict_namespace=True, physical_carriers=True
    )
    column = next(
        column
        for column in introspect_schema(
            "sqlite", database=str(database), table="contacts"
        )["columns"]
        if column["name"] == "created_at"
    )

    assert inferred["created_at"] == "TIMESTAMP"
    assert column["declared_type"] == "TEXT"
    assert column["native_type"] == "TEXT"
    assert column["sample_refined"] is True
    assert physical["created_at"] == "TEXT"

    result = run_mapping_pipeline(
        ["created_at"],
        ["created_at"],
        source_schemas=_source_schema(),
        target_schemas=[{"name": "created_at", "inferred_type": physical["created_at"]}],
        destination_db_type="sqlite",
        destination_table_exists=True,
        source_types_authoritative=True,
        use_llm=False,
    )
    mapping = result["mappings"][0]
    assert mapping["target_type"] == "TEXT"
    assert mapping["transform"] == "none"


def test_first_sqlite_run_keeps_integer_epoch_mapping(tmp_path):
    result = run_mapping_pipeline(
        ["created_at"],
        [],
        source_schemas=_source_schema(),
        destination_db_type="sqlite",
        destination_table_exists=False,
        source_types_authoritative=True,
        use_llm=False,
    )

    mapping = result["mappings"][0]
    assert mapping["target_type"] == "INTEGER"
    assert mapping["transform"] == "none"


def test_sqlite_operator_mapping_override_wins(tmp_path):
    _sqlite_destination(tmp_path)
    result = run_mapping_pipeline(
        ["created_at"],
        ["created_at"],
        source_schemas=_source_schema(),
        target_schemas=[{"name": "created_at", "inferred_type": "TEXT"}],
        destination_db_type="sqlite",
        destination_table_exists=True,
        source_types_authoritative=True,
        prior_mappings=[
            {
                "source": "created_at",
                "target": "created_at",
                "target_type": "TEXT",
                "transform": "datetime",
                "user_override": True,
                "confidence": 1.0,
                "reasoning": "operator-selected datetime transform",
                "assignment_strategy": "manual_override",
            }
        ],
        use_llm=False,
    )

    mapping = result["mappings"][0]
    assert mapping["target_type"] == "TEXT"
    assert mapping["transform"] == "datetime"
