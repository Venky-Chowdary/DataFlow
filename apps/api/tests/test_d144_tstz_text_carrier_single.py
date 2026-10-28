"""D144 — one carrier for an offset-bearing timestamp stored as text.

An Oracle ``TIMESTAMP WITH TIME ZONE`` onto MySQL lands as ``VARCHAR(64)``
(MySQL keeps no offset). The create-new stamp also carried the engine's
default *string* collation (``COLLATE utf8mb4_bin``) while the displayed
native type and a SQL Server source of the same value carried none: two
carriers for one tstz. A temporal source has no string equality to carry.
"""

from __future__ import annotations

import pytest

from services.collation_carry import apply_dest_native_collation
from services.mapping_pipeline import run_mapping_pipeline


@pytest.mark.parametrize("dest", ["mysql", "mariadb"])
@pytest.mark.parametrize(
    "source_type",
    ["TIMESTAMP(6) WITH TIME ZONE", "DATETIMEOFFSET(6)", "TIMESTAMP", "DATE", "TIME"],
)
def test_temporal_source_text_carrier_takes_no_string_collation(dest, source_type):
    assert (
        apply_dest_native_collation(
            "VARCHAR(64)", dest_dialect=dest, source_dialect="oracle", source_type=source_type
        )
        == "VARCHAR(64)"
    )


def test_string_source_still_carries_its_collation():
    assert apply_dest_native_collation(
        "VARCHAR(64)", dest_dialect="mysql", source_dialect="oracle", source_type="VARCHAR2(64)"
    ).endswith("COLLATE utf8mb4_bin")


@pytest.mark.parametrize("src_db", ["oracle", "mssql"])
def test_plan_target_and_native_type_agree_for_tstz_on_mysql(src_db):
    source_type = "TIMESTAMP(6) WITH TIME ZONE" if src_db == "oracle" else "DATETIMEOFFSET(6)"
    out = run_mapping_pipeline(
        ["ts"],
        [],
        source_schemas=[
            {
                "name": "ts",
                "inferred_type": source_type,
                "samples": ["2024-01-01T10:00:00+05:30", "2024-01-02T10:00:00-04:00"],
            }
        ],
        destination_db_type="mysql",
        destination_table_exists=False,
        use_llm=False,
        source_db_type=src_db,
        source_types_authoritative=True,
    )
    row = out["mapping_proof"]["mappings"][0]
    assert out["mappings"][0]["target_type"] == "VARCHAR(64)"
    assert row["target_type"] == row["dest_native_type"] == "VARCHAR(64)"
