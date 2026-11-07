"""D255: run 2 into DataFlow's own structural carrier is not a custom transform.

Run 1 lands a document ARRAY/STRUCT/MAP in the carrier create-new picks
(SQL Server NVARCHAR(MAX), MySQL JSON, Oracle CLOB, ...). When the only
sample is ``[]`` the pair was graded "custom transform · sparse" (0.70) and
G4 blocked the rerun.
"""

from __future__ import annotations

import pytest

from services.mapping_quality import classify_mapping_confidence

_CARRIERS = [
    ("sqlserver", "NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS"),
    ("sqlserver", "NVARCHAR(MAX)"),
    ("mysql", "JSON"),
    ("mariadb", "JSON"),
    ("oracle", "CLOB"),
    ("postgresql", "JSONB"),
    ("snowflake", "VARIANT"),
    ("redshift", "SUPER"),
]


def _grade(source_type: str, target_type: str, db: str) -> dict:
    return classify_mapping_confidence(
        {
            "source": "payload_arr",
            "target": "payload_arr",
            "source_type": source_type,
            "target_type": target_type,
            "transform": "json",
        },
        source_profile={"samples": [], "null_rate": 1.0},
        destination_db_type=db,
    )


@pytest.mark.parametrize("source_type", ["ARRAY", "STRUCT", "MAP"])
@pytest.mark.parametrize(("db", "target_type"), _CARRIERS)
def test_own_structural_carrier_clears_the_floor(source_type, db, target_type):
    out = _grade(source_type, target_type, db)
    assert out["confidence_class"] != "custom_transform_sparse", out
    assert float(out["anchor"]) >= 0.85, out


def test_a_bounded_text_column_is_still_not_the_carrier():
    out = _grade("ARRAY", "NVARCHAR(100)", "sqlserver")
    assert float(out["anchor"]) < 0.85, out
