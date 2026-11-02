"""D158 (b) / D45 / D144 — a JSON column never gets a phantom ``<col>_text``.

Run 2 of PG -> SQL Server mapped the jsonb ``payload`` to a new
``payload_text`` (ADD COLUMN) beside the ``NVARCHAR(MAX)`` column run 1
created for it; a fresh load into an existing table with a JSON column did
the same (``doc -> doc_text``) and was always blocked.

Two causes in ``semantic_mapper.map_columns``: introspected json/jsonb
samples are dicts and were rendered with ``str()`` (``{'k': 1}``), which no
JSON parse accepts, so the sample demotion pushed the exact-name pair below
the floor; and the "map to a new field" branch then invented ``<col>_text``
although a destination column with the source's own name existed.
"""

from __future__ import annotations

import pytest

from services.semantic_mapper import map_columns
from services.type_coercion import is_lossy_coercion

_DICT_SAMPLES = [{"k": 1}, {"k": 2, "tags": ["a"]}, {"k": 3}]


def _payload(src_type, tgt_type, *, samples, dest_db, source_db):
    out = map_columns(
        ["id", "payload"],
        ["id", "payload"],
        source_schemas=[
            {"name": "id", "inferred_type": "bigint"},
            {"name": "payload", "inferred_type": src_type, "samples": samples},
        ],
        target_schemas=[
            {"name": "id", "inferred_type": "BIGINT"},
            {"name": "payload", "inferred_type": tgt_type},
        ],
        destination_db_type=dest_db,
        destination_table_exists=True,
        source_db_type=source_db,
    )
    return next(m for m in out if m["source"] == "payload")


@pytest.mark.parametrize(
    ("src_type", "tgt_type", "dest_db"),
    [
        ("jsonb", "NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS", "sqlserver"),
        ("jsonb", "nvarchar(max)", "sqlserver"),
        ("jsonb", "NTEXT", "sqlserver"),
        ("jsonb", "jsonb", "postgresql"),
        ("json", "json", "postgresql"),
    ],
)
def test_json_source_with_dict_samples_binds_its_own_json_carrier(src_type, tgt_type, dest_db):
    row = _payload(
        src_type, tgt_type, samples=_DICT_SAMPLES, dest_db=dest_db, source_db="postgresql"
    )
    assert row["target"] == "payload", row
    assert not row.get("create_new"), row
    assert float(row["confidence"]) >= 0.95, row


def test_same_named_destination_column_is_bound_never_reinvented_as_text():
    # Samples no JSON parse accepts still demote the pair below the floor;
    # the same-named column is bound and graded, not shadowed by payload_text.
    row = _payload(
        "jsonb",
        "NVARCHAR(MAX)",
        samples=["{'k': 1}", "{'k': 2}", "{'k': 3}"],
        dest_db="sqlserver",
        source_db="postgresql",
    )
    assert row["target"] == "payload", row
    assert not row.get("create_new"), row
    assert row["assignment_strategy"] == "same_name_existing", row
    assert not row["target"].endswith("_text")


def test_same_name_lookup_is_case_insensitive():
    out = map_columns(
        ["payload"],
        ["PAYLOAD"],
        source_schemas=[
            {"name": "payload", "inferred_type": "jsonb", "samples": ["{'k': 1}", "{'k': 2}"]}
        ],
        target_schemas=[{"name": "PAYLOAD", "inferred_type": "NVARCHAR(MAX)"}],
        destination_db_type="sqlserver",
        destination_table_exists=True,
        source_db_type="postgresql",
    )
    assert [m["target"] for m in out] == ["PAYLOAD"]


def test_ntext_is_an_unbounded_json_carrier_and_bounded_nvarchar_stays_graded():
    assert not is_lossy_coercion("jsonb", "NTEXT", dest_db="sqlserver")
    # A bounded carrier can truncate a document: still a graded risk, bound
    # to the same-named column rather than replaced by a new one.
    assert is_lossy_coercion("jsonb", "NVARCHAR(4000)", dest_db="sqlserver")
    row = _payload(
        "jsonb", "NVARCHAR(4000)", samples=_DICT_SAMPLES, dest_db="sqlserver",
        source_db="postgresql",
    )
    assert row["target"] == "payload", row
    assert row["requires_review"], row
    assert "lossy" in row["reasoning"], row
