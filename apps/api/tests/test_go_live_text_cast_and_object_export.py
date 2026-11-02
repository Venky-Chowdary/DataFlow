"""DEF-TSTZ-NAME-INFER, DEF-B-032, DEF-SCHEMALESS-DST-CONTRACT.

A declared text column whose values carry an offset stays text when the
destination column is already text. A catalog TIMESTAMPTZ written into TEXT
on a typed engine still needs a contract. Create-new object export and Kafka
JSON do not report a fidelity collapse for a typed relational source.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.mapping_pipeline import run_mapping_pipeline  # noqa: E402
from services.preflight_service import run_file_preflight  # noqa: E402
from services.root_cause_engine import apply_root_causes_to_preflight  # noqa: E402
from services.type_system import is_lossy_coercion  # noqa: E402


def _plan(cols, types, *, dest, exists, target_types=None, samples=None, authoritative=False):
    schemas = [
        {"name": name, "native_type": types[name], "inferred_type": types[name]}
        for name in cols
    ]
    target = None
    if target_types is not None:
        target = [
            {"name": name, "native_type": target_types[name], "inferred_type": target_types[name]}
            for name in cols
        ]
    return run_mapping_pipeline(
        cols,
        cols if target is not None else [],
        source_schemas=schemas,
        target_schemas=target,
        source_samples=samples,
        destination_db_type=dest,
        source_db_type="postgresql",
        destination_table_exists=exists,
        source_types_authoritative=authoritative,
        use_llm=False,
        sync_mode="full_refresh_overwrite",
    )


def test_text_cast_into_an_existing_text_column_is_not_retimed() -> None:
    cols = ["created_at"]
    types = {"created_at": "TEXT"}
    samples = {"created_at": ["2024-03-01 12:00:00+00", "2024-06-01 08:00:00+05:30"]}
    plan = _plan(
        cols,
        types,
        dest="postgresql",
        exists=True,
        target_types={"created_at": "TEXT"},
        samples=samples,
        authoritative=False,
    )
    mapping = plan["mappings"][0]
    assert mapping["source_type"] == "TEXT"
    assert mapping["target_type"] == "TEXT"
    assert mapping["fidelity"] == "preserve"
    preflight = apply_root_causes_to_preflight(
        run_file_preflight(
            columns=cols,
            column_types=types,
            row_count=2,
            mappings=plan["mappings"],
            sample_rows=[{"created_at": "2024-03-01 12:00:00+00"}],
            destination_db_type="postgresql",
            destination_table_exists=True,
            destination_column_types={"created_at": "TEXT"},
            destination_can_write=True,
            destination_can_create=True,
            destination_connected=True,
            source_kind="postgresql",
            sync_mode="full_refresh_append",
        )
    )
    kinds = [
        (row.get("kind") if isinstance(row, dict) else row.kind)
        for row in (preflight.get("root_causes") or [])
    ]
    assert "fidelity_collapse" not in kinds


def test_catalog_timestamptz_into_postgres_text_still_needs_a_contract() -> None:
    assert is_lossy_coercion("TIMESTAMPTZ", "TEXT", dest_db="postgresql") is True
    cols = ["created_at"]
    types = {"created_at": "TIMESTAMPTZ"}
    plan = _plan(
        cols,
        types,
        dest="postgresql",
        exists=True,
        target_types={"created_at": "TEXT"},
        samples={"created_at": ["2024-03-01 12:00:00+00:00"]},
        authoritative=True,
    )
    mapping = plan["mappings"][0]
    assert mapping["source_type"] == "TIMESTAMPTZ"
    assert mapping["fidelity"] == "lossy_cast"


_EXPORT_TYPES = {
    "id": "INTEGER",
    "big_val": "BIGINT",
    "amount": "NUMERIC(12,2)",
    "name": "TEXT",
    "is_active": "BOOLEAN",
    "ts": "TIMESTAMP",
    "created_at": "TIMESTAMPTZ",
    "status": "VARCHAR(32)",
}
_EXPORT_SAMPLE = [{
    "id": 1,
    "big_val": 9000000001,
    "amount": "12.50",
    "name": "Ada",
    "is_active": True,
    "ts": "2024-03-01 12:00:00",
    "created_at": "2024-03-01 12:00:00+00:00",
    "status": "open",
}]


def _export_preflight(dest: str, *, can_create: bool):
    cols = list(_EXPORT_TYPES)
    plan = _plan(cols, _EXPORT_TYPES, dest=dest, exists=False, authoritative=True)
    preflight = apply_root_causes_to_preflight(
        run_file_preflight(
            columns=cols,
            column_types=_EXPORT_TYPES,
            row_count=200,
            mappings=plan["mappings"],
            sample_rows=_EXPORT_SAMPLE,
            destination_db_type=dest,
            destination_table_exists=False,
            destination_can_write=True,
            destination_can_create=can_create,
            destination_connected=True,
            source_kind="postgresql",
            sync_mode="full_refresh_overwrite",
            source_table="qa.sales",
            destination_table="exports/sales.parquet",
        )
    )
    return plan, preflight


def test_create_new_object_export_does_not_collapse_the_relational_columns() -> None:
    plan, preflight = _export_preflight("minio", can_create=False)
    for mapping in plan["mappings"]:
        assert mapping["fidelity"] == "preserve", mapping
    assert preflight.get("passed") is True
    kinds = [
        (row.get("kind") if isinstance(row, dict) else row.kind)
        for row in (preflight.get("root_causes") or [])
    ]
    assert "fidelity_collapse" not in kinds


def test_nightly_file_schedule_columns_do_not_collapse_on_create_new_minio() -> None:
    """DEF-C-044: id, status, qty, price, region must not demand a contract."""
    types = {
        "id": "INTEGER",
        "status": "VARCHAR(32)",
        "qty": "INTEGER",
        "price": "NUMERIC(12,2)",
        "region": "VARCHAR(16)",
    }
    cols = list(types)
    plan = _plan(cols, types, dest="minio", exists=False, authoritative=True)
    preflight = apply_root_causes_to_preflight(
        run_file_preflight(
            columns=cols,
            column_types=types,
            row_count=200,
            mappings=plan["mappings"],
            sample_rows=[{"id": 1, "status": "open", "qty": 2, "price": "9.99", "region": "east"}],
            destination_db_type="minio",
            destination_table_exists=False,
            destination_can_write=True,
            destination_can_create=False,
            destination_connected=True,
            source_kind="postgresql",
            sync_mode="full_refresh_overwrite",
            source_table="qa6c7_sales",
            destination_table="qa6c7_export/sales.csv",
        )
    )
    assert preflight.get("passed") is True
    for mapping in plan["mappings"]:
        assert mapping["fidelity"] == "preserve", mapping


def test_kafka_create_new_does_not_collapse_timestamptz_to_text() -> None:
    plan, preflight = _export_preflight("kafka", can_create=True)
    created = next(m for m in plan["mappings"] if m["source"] == "created_at")
    assert created["fidelity"] != "lossy_cast"
    kinds = [
        (row.get("kind") if isinstance(row, dict) else row.kind)
        for row in (preflight.get("root_causes") or [])
    ]
    assert "fidelity_collapse" not in kinds
