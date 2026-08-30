"""A wide number belongs in the destination's DECIMAL column, not a text twin.

Live reproduction: the destination already declared ``wide_num DECIMAL(38,0)``
and the writer inserted into an invented ``wide_num_text LONGTEXT`` instead —
``COUNT(*) = 20000`` with ``COUNT(wide_num) = 0``. The cause was upstream of the
writer: ``infer_type`` keeps a 31-digit run as VARCHAR (so account numbers stay
text), Map read that as "samples do not fit DECIMAL(38,0)", and the ObjectId /
hex repair invented a text column beside a column that holds the value exactly.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.mapping_pipeline import (  # noqa: E402
    _repair_unparseable_numeric_targets as repair_unparseable_numeric_targets,
)
from services.schema_inference import samples_fit_logical_type  # noqa: E402

_WIDE = [
    "1000000000000000000000000000001",
    "1000000000000000000000000000002",
    "1000000000000000000000000000003",
]


def test_wide_numbers_fit_a_declared_decimal_38_0() -> None:
    assert samples_fit_logical_type(_WIDE, "DECIMAL(38,0)") is True
    assert samples_fit_logical_type(_WIDE, "NUMBER(38,0)") is True


def test_wide_numbers_do_not_fit_a_narrower_carrier() -> None:
    # Fail-closed stays fail-closed: these do not fit and must not be claimed to.
    assert samples_fit_logical_type(_WIDE, "DECIMAL(18,0)") is False
    assert samples_fit_logical_type(_WIDE, "BIGINT") is False


def test_scale_and_leading_zeros_are_not_silently_dropped() -> None:
    assert samples_fit_logical_type(["1.005", "2.001"], "DECIMAL(10,2)") is False
    # A leading zero is data — no numeric carrier keeps it.
    assert samples_fit_logical_type(["01234", "98765"], "INT4") is False
    assert samples_fit_logical_type(["1.00", "3.14"], "DECIMAL(10,2)") is True


def _mapping(target_type: str) -> list[dict]:
    return [
        {
            "source": "wide_num",
            "target": "wide_num",
            "target_type": target_type,
            "source_type": "NUMERIC(38,0)",
        }
    ]


def _source_schemas() -> list[dict]:
    return [{"name": "wide_num", "inferred_type": "NUMERIC(38,0)", "samples": _WIDE}]


def _target_schemas(target_type: str) -> list[dict]:
    return [{"name": "wide_num", "inferred_type": target_type}]


def test_existing_decimal_column_is_written_not_shadowed() -> None:
    out = repair_unparseable_numeric_targets(
        _mapping("DECIMAL(38,0)"),
        source_schemas=_source_schemas(),
        target_schemas=_target_schemas("DECIMAL(38,0)"),
        destination_db_type="mysql",
    )
    assert [m["target"] for m in out] == ["wide_num"]
    assert not any(str(m.get("target", "")).endswith("_text") for m in out)
    assert not any(m.get("create_new") for m in out)


def test_overflowing_numbers_are_not_diverted_to_a_text_twin() -> None:
    # DECIMAL(18,0) genuinely cannot hold these, but the remedy is widen /
    # refuse — not a silent text column beside a NULL numeric column.
    out = repair_unparseable_numeric_targets(
        _mapping("DECIMAL(18,0)"),
        source_schemas=_source_schemas(),
        target_schemas=_target_schemas("DECIMAL(18,0)"),
        destination_db_type="mysql",
    )
    assert [m["target"] for m in out] == ["wide_num"]


def test_non_numeric_samples_still_get_a_compatible_new_column() -> None:
    out = repair_unparseable_numeric_targets(
        [
            {
                "source": "_id",
                "target": "id",
                "target_type": "NUMBER(38,0)",
                "source_type": "VARCHAR",
            }
        ],
        source_schemas=[
            {
                "name": "_id",
                "inferred_type": "VARCHAR",
                "samples": ["507f1f77bcf86cd799439011", "507f191e810c19729de860ea"],
            }
        ],
        target_schemas=[{"name": "id", "inferred_type": "NUMBER(38,0)"}],
        destination_db_type="mysql",
    )
    assert out[0]["target"] != "id"
    assert out[0]["create_new"] is True
