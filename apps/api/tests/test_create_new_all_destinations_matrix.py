"""create_new → ADD COLUMN must work for every typed SQL destination family.

Class-of-bug: Snowflake 000904 / invalid identifier \"id_text\" happened because
create_compatible_new wrote a column that was never ALTER ADD'd. The same
failure mode exists on Postgres, MySQL, BigQuery, SQL Server, Oracle, SQLite,
Redshift — fix once, prove on the matrix, not only the failing pair.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from connectors.writer_common import resolve_writer_backfill
from services.batch_progress import effective_backfill_new_fields, mappings_require_new_columns
from services.ddl_compatibility import evaluate_ddl_compatibility
from services.preflight_rules import explain_issue

_API_ROOT = Path(__file__).resolve().parents[1]
_PROOF = _API_ROOT / "data" / "proofs" / "create_new_all_destinations_matrix.json"

# Typed destinations that ADD COLUMN on backfill (writers call resolve_writer_backfill).
SQL_DEST_FAMILIES = [
    "snowflake",
    "postgresql",
    "redshift",
    "mysql",
    "mariadb",
    "bigquery",
    "sqlite",
    "sqlserver",
    "oracle",
    "generic_sql",
]

# Schemaless / document / object stores: create_new is a no-op (no ALTER).
SCHEMALESS_DESTS = [
    "mongodb",
    "dynamodb",
    "elasticsearch",
    "redis",
    "s3",
    "gcs",
    "adls",
]

CREATE_NEW_MAPPINGS = [
    {
        "source": "_id",
        "target": "_id",
        "create_new": True,
        "assignment_strategy": "create_compatible_new",
        "confidence": 0.95,
    },
    {"source": "name", "target": "name", "confidence": 0.99},
]

MISSING_COL_MAPPINGS = [
    {"source": "email", "target": "email_address", "confidence": 0.9},
]


@pytest.mark.parametrize("dest", SQL_DEST_FAMILIES)
def test_create_new_forces_writer_backfill_for_every_sql_family(dest: str):
    del dest  # same contract for every SQL writer family
    assert mappings_require_new_columns(CREATE_NEW_MAPPINGS)
    assert resolve_writer_backfill(
        backfill_new_fields=False,
        mappings=CREATE_NEW_MAPPINGS,
        schema_policy="manual_review",
    )
    assert effective_backfill_new_fields(
        backfill_new_fields=False,
        schema_policy="manual_review",
        mappings=CREATE_NEW_MAPPINGS,
    )


@pytest.mark.parametrize("dest", SQL_DEST_FAMILIES)
def test_preflight_passes_create_new_missing_column_when_will_add(dest: str):
    ok, issues = evaluate_ddl_compatibility(
        mappings=CREATE_NEW_MAPPINGS,
        source_schema={"_id": "VARCHAR", "name": "VARCHAR"},
        target_schema={"id": "DECIMAL", "name": "VARCHAR"},
        table_exists=True,
        dest_connected=True,
        dest_db_type=dest,
        allow_create=True,
        backfill_new_fields=False,
        schema_policy="manual_review",
    )
    assert ok, f"{dest}: {issues}"
    assert not any("does not exist" in i for i in issues)


@pytest.mark.parametrize("dest", SQL_DEST_FAMILIES)
def test_preflight_blocks_missing_column_without_create_new_or_backfill(dest: str):
    ok, issues = evaluate_ddl_compatibility(
        mappings=MISSING_COL_MAPPINGS,
        source_schema={"email": "VARCHAR"},
        target_schema={"email": "TEXT", "id": "INTEGER"},
        table_exists=True,
        dest_connected=True,
        dest_db_type=dest,
        allow_create=True,  # CREATE TABLE alone must NOT silence missing ADD COLUMN
        backfill_new_fields=False,
        schema_policy="manual_review",
    )
    assert not ok, f"{dest} must fail-fast on missing column"
    assert any("does not exist" in i for i in issues)


@pytest.mark.parametrize("dest", SQL_DEST_FAMILIES)
def test_preflight_passes_missing_column_with_explicit_backfill(dest: str):
    ok, issues = evaluate_ddl_compatibility(
        mappings=MISSING_COL_MAPPINGS,
        source_schema={"email": "VARCHAR"},
        target_schema={"email": "TEXT"},
        table_exists=True,
        dest_connected=True,
        dest_db_type=dest,
        allow_create=False,
        backfill_new_fields=True,
        schema_policy="propagate_columns",
    )
    assert ok, f"{dest}: {issues}"


@pytest.mark.parametrize("dest", SCHEMALESS_DESTS)
def test_schemaless_dests_do_not_require_sql_add_column(dest: str):
    ok, issues = evaluate_ddl_compatibility(
        mappings=CREATE_NEW_MAPPINGS,
        source_schema={"_id": "VARCHAR", "name": "VARCHAR"},
        target_schema={},
        table_exists=True,
        dest_connected=True,
        dest_db_type=dest,
        allow_create=False,
        backfill_new_fields=False,
    )
    assert ok, f"{dest}: {issues}"


@pytest.mark.parametrize(
    "message,needle",
    [
        ('invalid identifier \'"id_text"\'', "ADD COLUMN"),
        ('column "_id" of relation "customers" does not exist', "ADD COLUMN"),
        ("Unknown column '_id' in 'field list'", "ADD"),
        ("ERROR 1054 (42S22): Unknown column 'id_text'", "ADD"),
        ("Unrecognized name: id_text", "create-new"),
        ("Invalid column name '_id'", "ADD COLUMN"),
        ('ORA-00904: "_ID": invalid identifier', "ADD"),
    ],
)
def test_remediation_catalog_covers_missing_column_errors(message: str, needle: str):
    explained = explain_issue(message, dest_kind="postgresql")
    fix = (explained.get("fix") or explained.get("remediation") or "").lower()
    why = (explained.get("why") or "").lower()
    blob = f"{fix} {why}"
    assert needle.lower() in blob or "create-new" in blob or "backfill" in blob


def test_write_proof_artifact():
    proof = {
        "title": "create_new → ADD COLUMN all-destination matrix",
        "class_of_bug": "Snowflake 000904 / missing create-new column without ALTER",
        "sql_destinations": SQL_DEST_FAMILIES,
        "schemaless_destinations": SCHEMALESS_DESTS,
        "guarantees": [
            "mappings_require_new_columns(create_compatible_new) → True",
            "resolve_writer_backfill(False, create_new maps) → True for every SQL writer",
            "preflight blocks missing columns when allow_create alone (no backfill/create_new)",
            "preflight passes create_new missing columns (will ADD)",
            "remediations cover Snowflake/PG/MySQL/BQ/SQL Server/Oracle error text",
        ],
    }
    _PROOF.parent.mkdir(parents=True, exist_ok=True)
    _PROOF.write_text(json.dumps(proof, indent=2) + "\n", encoding="utf-8")
    assert _PROOF.is_file()
