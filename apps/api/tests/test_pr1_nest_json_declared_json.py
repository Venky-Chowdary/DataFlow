"""Document Modeler PR1: a ``nest_json`` output is declared JSON, whatever the sample.

``nest_json`` builds an object from columns. The writer makes it a MongoDB
sub-document (and a JSON column in SQL) only when the column is declared JSON;
otherwise the object is stored as a JSON *string*. The declaration was
re-inferred from the shaped sample: with no shaped rows (Validate or Map on an
empty sample, an empty first batch) the column came back ``VARCHAR`` and Map
stamped it, so the run stored ``meta.tags`` as text. The step's output is an
object by construction, so it is declared JSON the way ``cast_column``
declares its type; a later ``cast_column`` still wins, and a rename carries it.
"""

from __future__ import annotations

import os
import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

PRIOR = {"id": "INTEGER", "tier": "TEXT", "segment": "TEXT", "score": "INTEGER"}
NEST = {"op": "nest_json", "options": {"to": "tags", "fields": ["tier", "segment"], "drop": True}}


def _schema(steps: list[dict], columns: list[str], rows: list[dict]) -> dict[str, str]:
    from services.shape_apply import recipe_output_schema
    from services.shape_models import ShapeRecipe

    recipe = ShapeRecipe.parse({"version": 1, "steps": steps}, source_columns=list(PRIOR))
    return recipe_output_schema(recipe, columns, rows, PRIOR)


def test_nest_json_output_is_json_without_a_shaped_sample():
    assert _schema([NEST], ["id", "tags"], [])["tags"] == "JSON"


def test_nest_json_output_is_json_with_a_sample():
    rows = [{"id": 1, "tags": '{"tier": "gold", "segment": "smb"}'}]
    assert _schema([NEST], ["id", "tags"], rows)["tags"] == "JSON"


def test_nest_json_over_numbers_and_nulls_is_still_json():
    step = {"op": "nest_json", "options": {"to": "tags", "fields": ["score"]}}
    rows = [{"id": 1, "tags": '{"score": null}'}, {"id": 2, "tags": '{"score": 3}'}]
    assert _schema([step], ["id", "tier", "segment", "score", "tags"], rows)["tags"] == "JSON"


def test_a_rename_after_nest_json_carries_json():
    steps = [NEST, {"op": "rename_column", "column": "tags", "options": {"to": "labels"}}]
    assert _schema(steps, ["id", "labels"], [])["labels"] == "JSON"


def test_a_later_cast_still_wins():
    steps = [NEST, {"op": "cast_column", "column": "tags", "options": {"to_type": "text"}}]
    out = _schema(steps, ["id", "tags"], [])["tags"]
    assert out != "JSON", out


def test_untouched_columns_keep_their_declared_type():
    out = _schema([NEST], ["id", "score", "tags"], [])
    assert (out["id"], out["score"]) == ("INTEGER", "INTEGER")


# -- live: the declared type decides object vs string in MongoDB -------------

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")


def _mongo_up() -> bool:
    try:
        socket.create_connection(("localhost", 27017), timeout=1).close()
        import pymongo
        from pymongo.errors import PyMongoError
    except (OSError, ImportError):
        return False
    try:
        pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500).server_info()
    except PyMongoError:
        return False
    return True


@pytest.mark.skipif(not _mongo_up(), reason="no MongoDB on :27017")
def test_empty_sample_schema_writes_a_subdocument():
    """The schema Validate derives with no sample rows is the one the writer binds."""
    import pymongo

    from connectors.mongodb_writer import write_mapped_rows

    declared = _schema([NEST], ["id", "tags"], [])
    db = f"pr1n_{uuid.uuid4().hex[:10]}"
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        res = write_mapped_rows(
            host="localhost", port=27017, database=db, username="", password="", schema="",
            connection_string=MONGO_URI, ssl=False, table_name="c",
            headers=["id", "tags"], data_rows=[["1", '{"tier": "gold", "segment": "smb"}']],
            mappings=[
                {"source": "id", "target": "id", "target_type": declared["id"]},
                {"source": "tags", "target": "meta.tags", "target_type": declared["tags"]},
            ],
            column_types=declared, create_table=True,
        )
        assert res.ok, res.error
        doc = client[db]["c"].find_one({}, {"_id": 0})
        assert doc == {"id": 1, "meta": {"tags": {"tier": "gold", "segment": "smb"}}}, doc
    finally:
        client.drop_database(db)
        client.close()
