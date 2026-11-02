"""D198 — DataFlow recognises, on every engine, the JSON carrier it created.

Run 2 grades source JSON against the destination type *introspection* reports.
"Is this column DataFlow's own JSON carrier?" was answered by matching that
read-back spelling against a token list (JSON/JSONB/VARIANT/NVARCHAR(MAX)/CLOB…).
Introspection does not hand back the DDL spelling: Oracle's writer creates
JSON as ``CLOB`` and Oracle introspection reads ``CLOB`` back as ``TEXT``, which
is not in the list. JSON -> TEXT then graded as a document collapse, and that one
answer drove the two gates QA had to override on Oracle:

* the type gate / Migration Risk Contract (``is_lossy_coercion`` ->
  ``g3_schema_contract``), and
* ``g4_mapping_confidence`` (the mapper demoted the exact-name pair to
  "ambiguous", confidence 0.555 below the 0.85 floor).

There is no Oracle instance locally, so the Oracle leg is built from what DataFlow
itself produces: the run-1 plan's target type, the CREATE TABLE DataFlow's
writer compiles for Oracle from it, and the inferred type DataFlow's Oracle
introspection derives from the ``ALL_TAB_COLS.DATA_TYPE`` Oracle reports for
that DDL (``CLOB``). Confirming it on a real Oracle is QA's.
"""

from __future__ import annotations

import json
import re
from types import SimpleNamespace

import pytest

sa = pytest.importorskip("sqlalchemy")

_SRC = [
    {"name": "id", "inferred_type": "BIGINT"},
    {"name": "payload", "inferred_type": "JSONB"},
    {"name": "note", "inferred_type": "TEXT"},
    {"name": "updated_at", "inferred_type": "BIGINT"},
]
_SAMPLES = {
    "payload": [
        json.dumps({"a": {"b": [1, 2, {"c": "x"}]}, "n": None}),
        json.dumps([1, "two", None, {"k": []}]),
        json.dumps({"name": "Łukasz 山田 🎉"}, ensure_ascii=False),
        "{}",
        "[]",
        json.dumps({"empty": "", "blank": "   "}),
    ],
    "note": ["plain", "", "   ", "Łukasz 山田"],
    "id": ["1", "2", "3"],
    "updated_at": ["1", "2", "3"],
}


def _pipeline(dest_db: str, target_schemas: list[dict], exists: bool) -> list[dict]:
    from services.mapping_pipeline import run_mapping_pipeline

    out = run_mapping_pipeline(
        [c["name"] for c in _SRC],
        [c["name"] for c in target_schemas],
        source_schemas=_SRC,
        target_schemas=target_schemas,
        source_samples=_SAMPLES,
        validation_mode="strict",
        destination_db_type=dest_db,
        source_db_type="postgresql",
        sync_mode="incremental_upsert",
        destination_table_exists=exists,
        source_types_authoritative=True,
        use_llm=False,
    )
    return list(out.get("mappings") or [])


def _oracle_create_table_types() -> dict[str, str]:
    """Run 1: the column types in the CREATE TABLE DataFlow emits on Oracle."""
    from sqlalchemy.dialects import oracle
    from sqlalchemy.schema import CreateTable

    from connectors.generic_sql import _build_table_for_write

    run1 = _pipeline("oracle", [], exists=False)
    column_types = {m["target"]: m["target_type"] for m in run1 if m.get("target")}
    engine = SimpleNamespace(dialect=oracle.dialect())
    table = _build_table_for_write(
        engine, "D198", None, list(column_types), column_types,
        db_type="oracle", conflict_columns=["id"],
    )
    ddl = str(CreateTable(table).compile(dialect=oracle.dialect()))
    found = dict(re.findall(r'^\s*"(\w+)"\s+([A-Z0-9_]+(?:\([^)]*\))?)', ddl, flags=re.M))
    assert found.get("PAYLOAD") == "CLOB", ddl
    return found


def _oracle_introspected(created: dict[str, str]) -> list[dict]:
    """Run 2: what DataFlow's Oracle introspection reports for that table.

    ``ALL_TAB_COLS.DATA_TYPE`` is ``NUMBER`` (precision 38, scale 0) for
    ``NUMERIC(38, 0)`` and ``CLOB`` for ``CLOB``; ``_introspect_oracle`` folds
    precision/scale into ``NUMBER(38,0)`` and maps through ``_oracle_to_logical``.
    """
    from services.schema_introspect_enterprise import _oracle_to_logical

    out = []
    for name, ddl_type in created.items():
        data_type = "NUMBER(38,0)" if ddl_type.startswith("NUMERIC(38") else ddl_type
        out.append({"name": name, "inferred_type": _oracle_to_logical(data_type)})
    return out


def test_oracle_reads_its_own_json_carrier_back_as_text():
    # The shape of the defect: what DataFlow creates is not what it reads back.
    created = _oracle_create_table_types()
    introspected = {c["name"]: c["inferred_type"] for c in _oracle_introspected(created)}
    assert introspected["PAYLOAD"] == "TEXT"


def test_oracle_run_two_maps_json_onto_its_own_clob_without_review_or_contract():
    created = _oracle_create_table_types()
    run2 = _pipeline("oracle", _oracle_introspected(created), exists=True)
    payload = next(m for m in run2 if m["source"] == "payload")
    assert payload["target"] == "PAYLOAD"
    assert not payload.get("requires_review"), payload
    assert not payload.get("requires_risk_contract"), payload
    assert float(payload.get("confidence") or 0) >= 0.85, payload
    assert payload.get("fidelity") in (None, "", "preserve", "lossless"), payload


def test_oracle_run_two_passes_type_gate_and_mapping_confidence_gate():
    from preflight import ColumnMapping, ColumnSchema, DestinationConfig, SourceConfig, TransferPlan
    from preflight.gates import gate_g3_schema_contract, gate_g4_mapping_confidence
    from preflight.models import GateStatus

    created = _oracle_create_table_types()
    dest_cols = _oracle_introspected(created)
    run2 = _pipeline("oracle", dest_cols, exists=True)
    plan = TransferPlan(
        source=SourceConfig(
            kind="database", db_type="postgresql", connected=True, parseable=True,
            columns=[ColumnSchema(name=c["name"], inferred_type=c["inferred_type"]) for c in _SRC],
        ),
        destination=DestinationConfig(
            kind="database", db_type="oracle", connected=True, can_write=True,
            table_exists=True,
            target_columns=[ColumnSchema(name=c["name"], inferred_type=c["inferred_type"]) for c in dest_cols],
        ),
        mappings=[
            ColumnMapping(
                source=m["source"], target=m["target"],
                confidence=float(m.get("confidence") or 0),
                requires_review=bool(m.get("requires_review")),
                score_gap=float(m.get("score_gap", 1.0) or 1.0),
                target_type=m.get("target_type"),
                fidelity=m.get("fidelity"),
                type_narrowing=bool(m.get("type_narrowing")),
            )
            for m in run2 if m.get("target")
        ],
        validation_mode="strict",
        sync_mode="incremental_upsert",
    )
    ctx = SimpleNamespace(plan=plan, sample_rows=[], coercion_report=lambda: {})
    for gate in (gate_g3_schema_contract, gate_g4_mapping_confidence):
        result = gate(ctx)
        assert result.status != GateStatus.BLOCK, (gate.__name__, result.message, result.details)


@pytest.mark.parametrize("source_type", ["JSONB", "JSON"])
def test_oracle_json_onto_existing_own_clob_is_not_lossy(source_type):
    from services.conversion_contract import classify_conversion
    from services.type_coercion import is_lossy_coercion

    assert not is_lossy_coercion(
        source_type, "TEXT", source_db="postgresql", dest_db="oracle", dest_table_exists=True
    )
    verdict = classify_conversion(source_type, "TEXT", dest_db="oracle")
    assert not verdict["lossy"] and not verdict["requires_risk_contract"], verdict


# --------------------------------------------------------------------------- #
# The mechanism, on every engine: what DataFlow creates for JSON, read back by
# DataFlow's own introspection, is recognised as that engine's JSON carrier.
# --------------------------------------------------------------------------- #

def _introspector(dest_db: str):
    """DataFlow's own catalog-type mapper for each engine's introspection."""
    from services import schema_introspect as si
    from services import schema_introspect_enterprise as se

    return {
        "oracle": se._oracle_to_logical,
        # INFORMATION_SCHEMA spells it lower-case; the reader folds (max) in.
        "sqlserver": lambda t: se._sqlserver_to_logical(t.lower()),
        "postgresql": si._pg_to_logical,
        "redshift": si._pg_to_logical,
        "mysql": si._mysql_to_logical,
        "snowflake": si._sf_to_logical,
        "clickhouse": si._ch_to_logical,
        "bigquery": si._bq_to_logical,
    }[dest_db]


_ENGINES = ["oracle", "sqlserver", "postgresql", "redshift", "mysql", "snowflake", "clickhouse", "bigquery"]


@pytest.mark.parametrize("dest_db", _ENGINES)
def test_every_engine_recognises_the_json_carrier_it_creates_as_read_back(dest_db):
    from services.type_coercion import is_lossy_coercion
    from services.type_system import ddl_type, is_dialect_native_document_wire

    created = ddl_type(dest_db, "JSON")
    read_back = _introspector(dest_db)(created)
    assert read_back, (dest_db, created)
    assert is_dialect_native_document_wire(read_back, dest_db=dest_db), (dest_db, created, read_back)
    assert not is_lossy_coercion(
        "JSONB", read_back, source_db="postgresql", dest_db=dest_db, dest_table_exists=True
    ), (dest_db, read_back)


@pytest.mark.parametrize("dest_db", _ENGINES)
def test_catalog_read_back_is_the_introspectors_own_answer(dest_db):
    from services.schema_introspect import catalog_read_back
    from services.type_system import ddl_type

    created = ddl_type(dest_db, "JSON")
    assert catalog_read_back(dest_db, created) == _introspector(dest_db)(created)


@pytest.mark.parametrize(
    "dest_db,target",
    [
        # A user's own PG TEXT column is not DataFlow's JSON carrier (JSONB is).
        ("postgresql", "TEXT"),
        # SQL Server introspection reads code-page VARCHAR(MAX)/TEXT as TEXT;
        # DataFlow's carrier is NVARCHAR(MAX). Unicode JSON would not survive.
        ("sqlserver", "TEXT"),
        # A bounded Oracle VARCHAR2 is never the document carrier.
        ("oracle", "VARCHAR(4000 CHAR)"),
        ("oracle", "VARCHAR(4000)"),
        ("snowflake", "VARCHAR(100)"),
    ],
)
def test_a_column_that_is_not_the_engines_json_carrier_still_collapses(dest_db, target):
    from services.type_coercion import is_lossy_coercion
    from services.type_system import is_dialect_native_document_wire

    assert not is_dialect_native_document_wire(target, dest_db=dest_db)
    assert is_lossy_coercion(
        "JSONB", target, source_db="postgresql", dest_db=dest_db, dest_table_exists=True
    )
