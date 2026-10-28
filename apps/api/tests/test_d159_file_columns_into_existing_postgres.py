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
