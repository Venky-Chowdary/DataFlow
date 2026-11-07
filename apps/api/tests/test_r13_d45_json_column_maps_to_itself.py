"""D45 — a JSON column maps onto the same-name JSON column of an existing table.

QA: a second transfer into a table with ``doc jsonb`` (Snowflake ``VARIANT``)
proposed a new ``doc_text`` column, then blocked on schema drift and on
"overwrite empties doc". Regression guard (true on 3e258e90f after the D158
sample fix): the plan binds doc -> doc at the exact band with no ADD COLUMN,
and live PostgreSQL reruns into the table run 1 created load.
"""

from __future__ import annotations

import pytest

from services.mapping_pipeline import run_mapping_pipeline
from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


@pytest.mark.parametrize(
    ("dest", "doc_name", "doc_type"),
    [("postgresql", "doc", "JSONB"), ("snowflake", "DOC", "VARIANT"), ("mysql", "doc", "JSON")],
)
def test_json_source_binds_to_its_existing_json_column(dest, doc_name, doc_type):
    docs = [{"k": n} for n in range(1, 6)] + [None, {"u": "東京", "e": ""}]
    out = run_mapping_pipeline(
        ["id", "doc"], ["id", doc_name],
        source_schemas=[{"name": "id", "inferred_type": "INTEGER", "samples": ["1", "2"]},
                        {"name": "doc", "inferred_type": "JSONB", "samples": docs}],
        target_schemas=[{"name": "id", "inferred_type": "INTEGER"},
                        {"name": doc_name, "inferred_type": doc_type}],
        destination_db_type=dest, source_db_type="postgresql",
        destination_table_exists=True, sync_mode="full_refresh_overwrite", use_llm=False,
    )
    doc = next(m for m in out["mappings"] if m["source"] == "doc")
    assert doc["target"] == doc_name and not doc.get("create_new"), doc
    assert float(doc["confidence"]) >= 0.85 and not doc["requires_review"], doc


def test_jsonb_reruns_into_the_table_run_one_created(isolated):  # noqa: F811
    table = f"d45_{h.token()}"
    out = f"{table}_jd"
    try:
        h.run_sql(
            "postgresql",
            f"CREATE TABLE {table} (id INT PRIMARY KEY, doc JSONB)",
            f"INSERT INTO {table} SELECT n, jsonb_build_object('k', n) FROM generate_series(1, 30) n",
            f"UPDATE {table} SET doc = NULL WHERE id = 5",
            f"""UPDATE {table} SET doc = '{{"u": "東京", "e": ""}}' WHERE id = 6""",
        )
        for _ in range(2):
            result = h.execute(isolated, h.endpoint("postgresql", table), h.endpoint("postgresql", out),
                               sync_mode="full_refresh_overwrite")
            assert result.success, result.error
        cols = dict(h.run_sql("postgresql", "SELECT column_name, data_type FROM "  # nosec B608
                              f"information_schema.columns WHERE table_name = '{out}'"))
        assert cols == {"id": "bigint", "doc": "jsonb"}, cols
        got = dict(h.run_sql("postgresql", f"SELECT id, doc FROM {out} WHERE id IN (5, 6, 7)"))  # nosec B608
        assert got == {5: None, 6: {"u": "東京", "e": ""}, 7: {"k": 7}}
    finally:
        h.drop("postgresql", table)
        h.drop("postgresql", out)
