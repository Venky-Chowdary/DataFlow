"""Create-new Snowflake → Postgres types — measured 100% on this named fixture.

Fixture: TPC-H CUSTOMER-shaped columns (the 150k route that showed
DECIMAL(38,0)→BIGINT and DECIMAL(12,2)→NUMERIC(9,4) on Validate).
"""

from __future__ import annotations

from services.decision_kernel.type_invent import create_new_mapping_target_type
from services.type_system import is_lossy_coercion, is_precision_collapse_coercion

# Named fixture — 8/8 must preserve. "100%" means this matrix, not marketing.
SNOWFLAKE_CUSTOMER_TO_POSTGRES = (
    ("DECIMAL(38,0)", "NUMERIC(38,0)"),  # C_CUSTKEY
    ("NUMBER(38,0)", "NUMERIC(38,0)"),
    ("DECIMAL(38,0)", "NUMERIC(38,0)"),  # C_NATIONKEY
    ("DECIMAL(12,2)", "NUMERIC(12,2)"),  # C_ACCTBAL
    ("NUMBER(12,2)", "NUMERIC(12,2)"),
    ("VARCHAR(25)", "VARCHAR(25)"),  # C_NAME
    ("VARCHAR(40)", "VARCHAR(40)"),  # C_ADDRESS
    ("VARCHAR(15)", "VARCHAR(15)"),  # C_PHONE
)


def test_create_new_tpch_customer_types_are_100_percent_preserve():
    failed: list[str] = []
    for src, expected in SNOWFLAKE_CUSTOMER_TO_POSTGRES:
        stamped = create_new_mapping_target_type(
            src, "postgresql", samples=["1", "2", "150000", "9999.99"], source_db="snowflake"
        )
        collapse = is_precision_collapse_coercion(
            src, stamped, dest_db="postgresql", dest_table_exists=False
        ) or is_lossy_coercion(src, stamped, dest_db="postgresql", dest_table_exists=False)
        if stamped.upper().replace(" ", "") != expected.upper().replace(" ", "") or collapse:
            failed.append(f"{src} → {stamped} (want {expected}, collapse={collapse})")
    assert failed == [], f"{len(failed)}/{len(SNOWFLAKE_CUSTOMER_TO_POSTGRES)} failed: {failed}"
    assert len(failed) == 0
    assert len(SNOWFLAKE_CUSTOMER_TO_POSTGRES) == 8


def test_create_new_refuses_sample_sized_bigint_and_narrow_numeric():
    assert create_new_mapping_target_type(
        "DECIMAL(38,0)", "postgresql", samples=["1", "150000"], source_db="snowflake"
    ) == "NUMERIC(38,0)"
    assert create_new_mapping_target_type(
        "DECIMAL(12,2)", "postgresql", samples=["10.50", "9999.99"], source_db="snowflake"
    ) == "NUMERIC(12,2)"


def test_mapping_pipeline_refuses_frontend_sample_bigint_stamp():
    from services.mapping_pipeline import run_mapping_pipeline

    result = run_mapping_pipeline(
        source_columns=["C_CUSTKEY", "C_ACCTBAL"],
        target_columns=[],
        source_schemas=[
            {"name": "C_CUSTKEY", "inferred_type": "DECIMAL(38,0)", "samples": ["1", "150000"]},
            {"name": "C_ACCTBAL", "inferred_type": "DECIMAL(12,2)", "samples": ["10.50", "9999.99"]},
        ],
        target_schemas=None,
        file_format="snowflake",
        destination_db_type="postgresql",
        source_db_type="snowflake",
        destination_table_exists=False,
        use_llm=False,
        prior_mappings=[
            {
                "source": "C_CUSTKEY",
                "target": "c_custkey",
                "source_type": "DECIMAL(38,0)",
                "target_type": "BIGINT",
                "create_new": True,
                "assignment_strategy": "create_compatible_new",
            },
            {
                "source": "C_ACCTBAL",
                "target": "c_acctbal",
                "source_type": "DECIMAL(12,2)",
                "target_type": "NUMERIC(9,4)",
                "create_new": True,
                "assignment_strategy": "create_compatible_new",
            },
        ],
    )
    by_src = {m["source"]: m for m in result["mappings"]}
    assert by_src["C_CUSTKEY"]["target_type"].upper().replace(" ", "") == "NUMERIC(38,0)"
    assert by_src["C_ACCTBAL"]["target_type"].upper().replace(" ", "") == "NUMERIC(12,2)"


def test_writer_create_new_refuses_sample_bigint_stamp():
    from connectors.writer_common import resolve_target_columns

    cols, types = resolve_target_columns(
        [
            {
                "source": "C_CUSTKEY",
                "target": "c_custkey",
                "source_type": "DECIMAL(38,0)",
                "target_type": "BIGINT",
                "create_new": True,
            },
            {
                "source": "C_ACCTBAL",
                "target": "c_acctbal",
                "source_type": "DECIMAL(12,2)",
                "target_type": "NUMERIC(9,4)",
                "create_new": True,
            },
        ],
        {"C_CUSTKEY": "DECIMAL(38,0)", "C_ACCTBAL": "DECIMAL(12,2)"},
        table_exists=False,
        dest_db="postgresql",
    )
    stamped = dict(zip(cols, types))
    assert stamped["c_custkey"].upper().replace(" ", "") == "NUMERIC(38,0)"
    assert stamped["c_acctbal"].upper().replace(" ", "") == "NUMERIC(12,2)"
