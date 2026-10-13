"""QA MX2-10 (critical) — decimal precision guessed from a 50-row sample.

``file_parser`` / object stores infer column types from the first 50 rows.
That envelope was then stamped on create-new destination columns as if it were
the column's declared domain, so every later row that needed one more integer
digit was quarantined (most of the table on real PostgreSQL / MySQL runs).

A sample of an untyped source is evidence of *scale*, never a bound on the
integer part. Create-new carriers for such sources must hold the destination's
full integer capacity at the observed scale (PostgreSQL: exact unbounded
``NUMERIC``), so no unsampled row is quarantined for being larger.
"""

from __future__ import annotations

import pytest

from connectors.writer_common import fits_decimal, parse_decimal_precision_scale
from services.decision_kernel.type_invent import create_new_mapping_target_type
from services.schema_inference import infer_columns_from_rows

# The first 50 rows are small; the rest of the file is not.
_HEAD = [[str(i), f"{i}.25"] for i in range(1, 51)]
_TAIL = [[str(i), f"{i * 1000}.75"] for i in range(51, 5001)]
ROWS = _HEAD + _TAIL


def _inferred_amount_type() -> str:
    cols = infer_columns_from_rows(["id", "amount"], ROWS)
    inferred = next(c for c in cols if c["name"] == "amount")["inferred_type"]
    # The defect's precondition: the file parser stamps the 50-row envelope.
    assert inferred == "DECIMAL(4,2)"
    return inferred


def _samples() -> list[str]:
    return [r[1] for r in _HEAD]


def _holds_every_row(stamp: str, dest_db: str) -> int:
    parsed = parse_decimal_precision_scale(stamp, dest_db=dest_db)
    if parsed is None:  # unbounded exact NUMERIC
        return 0
    p, s = parsed
    return sum(1 for r in ROWS if not fits_decimal(r[1], p, s, dest_db=dest_db))


@pytest.mark.parametrize(
    "source_db",
    ["csv", "excel", "json", "s3", "gcs", "azure_blob", "sftp", "file", "google_sheets"],
)
@pytest.mark.parametrize("dest_db", ["postgresql", "mysql", "sqlserver", "snowflake"])
def test_sampled_file_decimal_never_quarantines_unsampled_rows(source_db, dest_db):
    inferred = _inferred_amount_type()
    stamp = create_new_mapping_target_type(
        inferred, dest_db, samples=_samples(), source_db=source_db
    )
    unfit = _holds_every_row(stamp, dest_db)
    assert unfit == 0, (
        f"{source_db}→{dest_db}: stamp {stamp!r} from a 50-row sample "
        f"would quarantine {unfit} of {len(ROWS)} rows (MX2-10)"
    )


def test_postgres_gets_exact_unbounded_numeric_no_scale_pad():
    stamp = create_new_mapping_target_type(
        _inferred_amount_type(), "postgresql", samples=_samples(), source_db="csv"
    )
    assert stamp.upper() == "NUMERIC"


@pytest.mark.parametrize("dest_db", ["mysql", "sqlserver"])
def test_bounded_engines_keep_observed_scale_and_full_integer_capacity(dest_db):
    stamp = create_new_mapping_target_type(
        _inferred_amount_type(), dest_db, samples=_samples(), source_db="s3"
    )
    assert parse_decimal_precision_scale(stamp, dest_db=dest_db) == (38, 2)


def test_declared_relational_precision_is_still_mirrored():
    """A PostgreSQL NUMERIC(10,2) *is* a domain — keep the source contract."""
    stamp = create_new_mapping_target_type(
        "NUMERIC(10,2)", "mysql", samples=["1.25", "2.50"], source_db="postgresql"
    )
    assert parse_decimal_precision_scale(stamp, dest_db="mysql") == (10, 2)


def test_unknown_scale_without_evidence_does_not_invent_zero():
    """A document source with no samples and no typmod must not become
    DECIMAL(38,0) — that would quarantine every fractional value."""
    stamp = create_new_mapping_target_type(
        "DECIMAL", "mysql", samples=None, source_db="mongodb"
    )
    parsed = parse_decimal_precision_scale(stamp, dest_db="mysql")
    assert parsed is None or parsed[1] > 0


@pytest.mark.parametrize(
    "dest_db,expected", [("postgresql", "NUMERIC"), ("mysql", "DECIMAL(38,2)")]
)
def test_map_pipeline_stamps_capacity_carrier_end_to_end(dest_db, expected):
    """Map is the stamp CREATE TABLE uses — prove it, not just the helper."""
    from services.mapping_pipeline import run_mapping_pipeline

    cols = infer_columns_from_rows(["id", "amount"], _HEAD)
    out = run_mapping_pipeline(
        source_columns=["id", "amount"],
        target_columns=[],
        source_schemas=[
            {"name": c["name"], "inferred_type": c["inferred_type"], "samples": c["samples"]}
            for c in cols
        ],
        target_schemas=[],
        source_samples={c["name"]: [r[i] for r in _HEAD] for i, c in enumerate(cols)},
        destination_db_type=dest_db,
        source_db_type="csv",
        destination_table_exists=False,
        sync_mode="full_refresh_overwrite",
    )
    stamped = {m["source"]: m.get("target_type") for m in out["mappings"]}
    assert stamped["amount"] == expected
    assert stamped["id"] == "BIGINT"


def test_ieee_residue_keeps_float_carrier():
    stamp = create_new_mapping_target_type(
        "DECIMAL",
        "mysql",
        samples=["0.1000000000000000055511151231257827", "0.2", "0.30000000000000004"],
        source_db="csv",
    )
    assert "DOUBLE" in stamp.upper() or "FLOAT" in stamp.upper()
