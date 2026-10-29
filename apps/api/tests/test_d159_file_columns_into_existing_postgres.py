"""D159 — a file load into an existing Postgres table held exact-name columns.

A CSV digit column profiles INTEGER; the table DataFlow (or the operator)
made for it is TEXT (zip, code, sku). That pair is graded ``preserve`` by the
fidelity SSOT, but ``classify_mapping_confidence`` scored it "weak or
conflicted" (0.55 anchor -> 0.63, review) because ("integer", "text") was not
a listed promotion. Every such exact-name column was held at G4, so a fresh
Map into the existing table was blocked while a rerun of the stored mapping
passed. A lossless widening onto a text or wider numeric column is now a safe
promotion; anything the lossy checks flag is still not promoted.
"""

from __future__ import annotations

import pytest

from services.mapping_pipeline import run_mapping_pipeline


def _map(file_type, dest_type, samples):
    out = run_mapping_pipeline(
        ["col_x"],
        ["col_x"],
        source_schemas=[{"name": "col_x", "inferred_type": file_type, "samples": samples}],
        target_schemas=[{"name": "col_x", "inferred_type": dest_type}],
        destination_db_type="postgresql",
        destination_table_exists=True,
        use_llm=False,
        source_db_type="sftp",
        validation_mode="strict",
        file_format="csv",
    )
    return out["mappings"][0]


@pytest.mark.parametrize(
    ("file_type", "dest_type", "samples"),
    [
        ("INTEGER", "TEXT", ["10001", "94105", "60601"]),
        ("BOOLEAN", "NUMERIC(12,2)", ["true", "false", "true"]),
        ("BOOLEAN", "FLOAT8", ["true", "false", "true"]),
    ],
)
def test_lossless_widening_onto_existing_column_binds_without_review(file_type, dest_type, samples):
    row = _map(file_type, dest_type, samples)
    assert row["target"] == "col_x"
    assert float(row["confidence"]) >= 0.85, row
    assert not row["requires_review"], row


@pytest.mark.parametrize(
    ("file_type", "dest_type", "samples"),
    [
        ("DECIMAL(5,2)", "TEXT", ["1.50", "22.25", "3.00"]),
        ("TIMESTAMP", "DATE", ["2024-01-01 10:00:00", "2024-02-02 11:00:00", "2024-03-03 12:00:00"]),
        ("DECIMAL(5,2)", "INT4", ["1.50", "22.25", "3.00"]),
    ],
)
def test_lossy_pairs_are_still_held_for_review(file_type, dest_type, samples):
    row = _map(file_type, dest_type, samples)
    assert row["requires_review"], row
    assert float(row["confidence"]) < 0.85, row


_LONG = "lorem ipsum dolor sit amet " * 40


@pytest.mark.parametrize("dest_type", ["TEXT", "VARCHAR(255)"])
@pytest.mark.parametrize(
    "samples",
    [
        ["2024-01-01", "2024-02-02", "2024-03-03"],
        ["123", "456", "789"],
        [_LONG, _LONG + "x", _LONG + "yy"],
        ['{"a": 1}', '{"b": [1, 2]}', '{"c": null}'],
    ],
    ids=["date_like", "numeric_like", "long", "json_like"],
)
@pytest.mark.parametrize("name", ["name", "long_txt", "note", "payload"])
def test_text_values_that_look_typed_into_an_existing_text_column_are_not_ambiguous(
    name, samples, dest_type
):
    """Date-looking text in a ``note`` column was inferred a ``datetime`` cast
    (a "custom transform"), which held the exact-name column at G4."""
    out = run_mapping_pipeline(
        [name],
        [name],
        source_schemas=[{"name": name, "inferred_type": "VARCHAR", "samples": samples}],
        target_schemas=[{"name": name, "inferred_type": dest_type}],
        destination_db_type="postgresql",
        destination_table_exists=True,
        use_llm=False,
        source_db_type="sftp",
        validation_mode="strict",
        file_format="csv",
    )
    row = out["mappings"][0]
    assert row["transform"] in {"none", "string", "text", "varchar"}, row
    assert not row["requires_review"], row
    assert float(row["confidence"]) >= 0.85, row


def test_json_text_into_an_existing_jsonb_column_is_confident_but_plain_text_is_not():
    json_row = _map("VARCHAR", "JSONB", ['{"a": 1}', '{"b": [1, 2]}', '{"c": null}'])
    assert float(json_row["confidence"]) >= 0.85 and not json_row["requires_review"], json_row
    plain_row = _map("VARCHAR", "JSONB", ["alpha", "beta", "gamma"])
    assert float(plain_row["confidence"]) < 0.85, plain_row
