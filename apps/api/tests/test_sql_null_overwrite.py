"""DEF-B-001: SQL NULL must not block a nullable SQL→SQL overwrite.

Postgres→MySQL and MySQL→Postgres both failed lenient overwrite with
rc-sample-transform EMPTY_VALUE_NOT_NULLABLE on amount and ts. The column
profile also reported null_rate 0.0 when the sample held seven SQL NULLs.
"""

from __future__ import annotations

from services.coercion_probe import analyze_coercion
from services.mapping_pipeline import run_mapping_pipeline
from services.value_serializer import SQL_NULL_SENTINEL
from src.ai.copilot.query_tools import _analyze_rows


def _rows() -> list[dict]:
    rows = []
    for i in range(10):
        blank = i < 7
        rows.append(
            {
                "id": str(i + 1),
                "amount": "" if blank else "12.50",
                "ts": "" if blank else "2026-01-02 03:04:05",
            }
        )
    return rows


def _mappings() -> list[dict]:
    return [
        {
            "source": "id",
            "target": "id",
            "source_type": "INTEGER",
            "target_type": "INTEGER",
        },
        {
            "source": "amount",
            "target": "amount",
            "source_type": "NUMERIC(12,2)",
            "target_type": "DECIMAL(12,2)",
        },
        {
            "source": "ts",
            "target": "ts",
            "source_type": "TIMESTAMP",
            "target_type": "DATETIME",
        },
    ]


def test_lenient_sql_overwrite_treats_nullable_blanks_as_null() -> None:
    for dest in ("mysql", "postgresql"):
        report = analyze_coercion(
            sample_rows=_rows(),
            mappings=_mappings(),
            source_types={
                "id": "INTEGER",
                "amount": "NUMERIC(12,2)",
                "ts": "TIMESTAMP",
            },
            dest_types={
                "id": "INTEGER",
                "amount": "DECIMAL(12,2)",
                "ts": "DATETIME",
            },
            dest_db_type=dest,
            table_exists=True,
            validation_mode="lenient",
            empty_cells_as_null=False,
            database_extract=True,
            dest_nullability={"id": False, "amount": True, "ts": True},
        )
        assert report["has_blocking_failures"] is False, dest
        blob = str(report)
        assert "EMPTY_VALUE_NOT_NULLABLE" not in blob
        assert "Empty value cannot coerce" not in blob


def test_proven_not_null_still_blocks_a_blank() -> None:
    report = analyze_coercion(
        sample_rows=[{"amount": ""}],
        mappings=[
            {
                "source": "amount",
                "target": "amount",
                "source_type": "NUMERIC(12,2)",
                "target_type": "DECIMAL(12,2)",
                "target_nullable": False,
            }
        ],
        source_types={"amount": "NUMERIC(12,2)"},
        dest_types={"amount": "DECIMAL(12,2)"},
        dest_db_type="mysql",
        table_exists=True,
        validation_mode="lenient",
        empty_cells_as_null=False,
        database_extract=True,
        dest_nullability={"amount": False},
    )
    assert report["has_blocking_failures"] is True
    col = report["columns"][0]
    assert col["failure_class"] == "EMPTY_VALUE_NOT_NULLABLE"


def test_varchar_empty_string_is_not_invented_as_null() -> None:
    report = analyze_coercion(
        sample_rows=[{"note": ""}],
        mappings=[
            {
                "source": "note",
                "target": "amount",
                "source_type": "VARCHAR",
                "target_type": "DECIMAL(12,2)",
            }
        ],
        source_types={"note": "VARCHAR"},
        dest_types={"amount": "DECIMAL(12,2)"},
        dest_db_type="postgresql",
        table_exists=True,
        validation_mode="lenient",
        empty_cells_as_null=False,
        database_extract=True,
        dest_nullability={"amount": True},
    )
    assert report["has_blocking_failures"] is True


def test_reader_sentinel_stays_a_null() -> None:
    report = analyze_coercion(
        sample_rows=[{"amount": SQL_NULL_SENTINEL, "ts": SQL_NULL_SENTINEL}],
        mappings=_mappings()[1:],
        source_types={"amount": "NUMERIC(12,2)", "ts": "TIMESTAMP"},
        dest_types={"amount": "DECIMAL(12,2)", "ts": "DATETIME"},
        dest_db_type="mysql",
        table_exists=True,
        validation_mode="strict",
        empty_cells_as_null=False,
        database_extract=True,
        dest_nullability={"amount": True, "ts": True},
    )
    assert report["has_blocking_failures"] is False


def test_column_profile_counts_sql_nulls() -> None:
    samples = [SQL_NULL_SENTINEL] * 7 + ["12.50"]
    result = run_mapping_pipeline(
        ["amount"],
        [],
        source_schemas=[
            {
                "name": "amount",
                "inferred_type": "NUMERIC(12,2)",
                "native_type": "NUMERIC(12,2)",
                "samples": list(samples),
            }
        ],
        source_samples={"amount": list(samples)},
        use_llm=False,
        destination_db_type="mysql",
        destination_table_exists=False,
        source_types_authoritative=True,
        source_db_type="postgresql",
    )
    mapping = result["mappings"][0]
    profile = mapping.get("column_profile") or {}
    assert profile.get("null_rate") == 0.875
    assert "TEXT" not in str(mapping.get("target_type") or "").upper()


def test_analyze_rows_counts_the_sql_null_sentinel() -> None:
    rows = [{"amount": SQL_NULL_SENTINEL} for _ in range(7)]
    rows.append({"amount": "12.50"})
    profile = _analyze_rows(rows, ["amount"])
    col = profile["columns"][0]
    assert col["nulls"] == 7
    assert col["null_rate"] == 0.875
