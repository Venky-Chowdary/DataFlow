"""Wave 94 — one fidelity verdict, one authoritative-source rule.

Two defects motivated these tests:

* Every surface re-derived cast risk on its own, so a column could read
  "preserve" in the Map list and "lossy" in the proof drawer. The engine now
  stamps a single verdict via ``mapping_fidelity``.
* ``source_types_authoritative`` was decided ad hoc per caller, and the flag was
  wired using a field that means "csv" for uploads and "postgresql" for
  connectors — so it silently never fired.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.data_profiler import (  # noqa: E402
    merge_profiler_schema,
    source_types_are_authoritative,
)
from services.mapping_proof import (  # noqa: E402
    mapping_fidelity,
    stamp_mapping_fidelity,
)


class TestAuthoritativeSourceTypes:
    """Declared DDL outranks inference — but only where DDL is real."""

    def test_relational_and_warehouse_sources_are_authoritative(self):
        for engine in ("postgresql", "mysql", "snowflake", "bigquery", "sqlserver"):
            assert source_types_are_authoritative("database", engine) is True, engine

    def test_document_stores_are_not_authoritative(self):
        # Mongo/Dynamo "declared" types were themselves inferred from sampled
        # documents, so statistical inference is the better evidence, not worse.
        for engine in ("mongodb", "dynamodb", "elasticsearch"):
            assert source_types_are_authoritative("database", engine) is False, engine

    def test_file_sources_are_never_authoritative(self):
        for kind, fmt in (("file", "csv"), ("file", "json"), ("file_export", "parquet")):
            assert source_types_are_authoritative(kind, fmt) is False, (kind, fmt)

    def test_database_kind_without_format_is_authoritative(self):
        assert source_types_are_authoritative("database", "") is True

    def test_unknown_kind_defers_to_inference(self):
        assert source_types_are_authoritative("", "") is False

    def test_authoritative_merge_keeps_declared_precision(self):
        # The regression: DECIMAL(12,2) re-inferred as a bare DECIMAL, which is
        # then created as DECIMAL(38,15) downstream.
        merged = merge_profiler_schema(
            {"amount": "DECIMAL(12,2)", "note": "JSONB"},
            {"amount": "DECIMAL", "note": "VARCHAR"},
            authoritative_existing=True,
        )
        assert merged["amount"] == "DECIMAL(12,2)"
        assert merged["note"] == "JSONB"

    def test_authoritative_merge_still_fills_undeclared_columns(self):
        merged = merge_profiler_schema(
            {"amount": "DECIMAL(12,2)"},
            {"amount": "DECIMAL", "extra": "BIGINT"},
            authoritative_existing=True,
        )
        assert merged["extra"] == "BIGINT"


class TestMappingFidelityVerdict:
    """The type path decides first; the transform only classifies the rest."""

    def test_identity_roundtrip_is_preserve(self):
        verdict = mapping_fidelity(
            {"source_type": "INTEGER", "target_type": "INTEGER", "transform": "none"}
        )
        assert verdict["verdict"] == "preserve"
        assert verdict["type_narrowing"] is False

    def test_silent_width_truncation_is_lossy_even_with_identity_transform(self):
        # This is the case every client-side heuristic missed: no transform, so
        # the transform name says "preserve" while the write truncates.
        verdict = mapping_fidelity(
            {"source_type": "VARCHAR(255)", "target_type": "VARCHAR(50)", "transform": "none"}
        )
        assert verdict["verdict"] == "lossy_cast"
        assert verdict["type_narrowing"] is True
        assert "VARCHAR(255)" in verdict["reason"]

    def test_safe_parse_is_cast_not_lossy(self):
        # DECIMAL(12,2) → DECIMAL(12,2) loses nothing; only unparseable values
        # are quarantined, so this must not be scored the same as truncation.
        verdict = mapping_fidelity(
            {
                "source_type": "DECIMAL(12,2)",
                "target_type": "DECIMAL(12,2)",
                "transform": "decimal",
            }
        )
        assert verdict["verdict"] == "cast"
        assert verdict["type_narrowing"] is False

    def test_decimal_scale_narrowing_is_lossy(self):
        verdict = mapping_fidelity(
            {
                "source_type": "DECIMAL(20,6)",
                "target_type": "DECIMAL(10,2)",
                "transform": "decimal",
            }
        )
        assert verdict["verdict"] == "lossy_cast"
        assert verdict["type_narrowing"] is True

    def test_value_rewriting_transform_is_mutate(self):
        verdict = mapping_fidelity(
            {"source_type": "VARCHAR", "target_type": "VARCHAR", "transform": "hash_pii"}
        )
        assert verdict["verdict"] == "mutate"
        assert verdict["type_narrowing"] is False

    def test_declared_types_outrank_the_collapsed_carrier(self):
        # ddl_carrier_type drops VARCHAR width, so a mapping alone reads
        # "preserve". The declared pair is what the operator actually has.
        mapping = {"source_type": "VARCHAR", "target_type": "VARCHAR", "transform": "none"}
        assert mapping_fidelity(mapping)["verdict"] == "preserve"
        verdict = mapping_fidelity(
            mapping,
            declared_source_type="VARCHAR(500)",
            declared_target_type="VARCHAR(40)",
        )
        assert verdict["verdict"] == "lossy_cast"
        assert verdict["type_narrowing"] is True

    def test_missing_types_do_not_raise(self):
        verdict = mapping_fidelity({})
        assert verdict["verdict"] in {"preserve", "cast", "mutate", "lossy_cast"}


class TestStampMappingFidelity:
    """Every mapping carries the verdict so no surface has to guess."""

    def test_stamp_adds_verdict_to_each_mapping(self):
        stamped = stamp_mapping_fidelity([
            {"source": "id", "target": "id", "source_type": "BIGINT",
             "target_type": "BIGINT", "transform": "none"},
            {"source": "name", "target": "name", "source_type": "VARCHAR(255)",
             "target_type": "VARCHAR(20)", "transform": "none"},
        ])
        assert stamped[0]["fidelity"] == "preserve"
        assert stamped[1]["fidelity"] == "lossy_cast"
        assert stamped[1]["type_narrowing"] is True
        assert stamped[1]["fidelity_reason"]

    def test_stamp_preserves_existing_keys(self):
        stamped = stamp_mapping_fidelity([
            {"source": "id", "target": "id", "confidence": 0.97, "reasoning": "exact"}
        ])
        assert stamped[0]["confidence"] == 0.97
        assert stamped[0]["reasoning"] == "exact"

    def test_pipeline_output_carries_the_verdict(self):
        from services.mapping_pipeline import run_mapping_pipeline

        result = run_mapping_pipeline(
            ["id", "description"],
            ["id", "description"],
            source_schemas=[
                {"name": "id", "inferred_type": "BIGINT", "samples": ["1", "2"]},
                {"name": "description", "inferred_type": "VARCHAR(500)",
                 "samples": ["alpha", "beta"]},
            ],
            target_schemas=[
                {"name": "id", "inferred_type": "BIGINT", "samples": []},
                {"name": "description", "inferred_type": "VARCHAR(40)", "samples": []},
            ],
            use_llm=False,
            destination_table_exists=True,
        )
        by_source = {m["source"]: m for m in result["mappings"]}
        # VARCHAR(500) → VARCHAR(40) truncates with no transform to warn about.
        assert by_source["description"]["fidelity"] == "lossy_cast"
        assert by_source["description"]["type_narrowing"] is True
        # BIGINT → BIGINT gets an integer parse transform: the type path holds,
        # so it must not be scored as data loss.
        assert by_source["id"]["fidelity"] == "cast"
        assert by_source["id"]["type_narrowing"] is False


class TestHubspotStrictPolicyStillConvertsTemporals:
    """``fail`` is the strictest policy — it must not be the least faithful."""

    def test_fail_policy_converts_valid_datetime_to_epoch_millis(self):
        from connectors.hubspot_writer import _normalize_hubspot_temporal_cells

        rejected: list[dict] = []
        out = _normalize_hubspot_temporal_cells(
            [("2024-05-06T14:30:00",)], ["closedate"], ["TIMESTAMPTZ"], rejected, "fail"
        )
        # Previously returned the raw ISO string untouched under `fail`.
        assert out == [("1715005800000",)]
        assert rejected == []

    def test_fail_policy_holds_out_unparseable_value(self):
        from connectors.hubspot_writer import _normalize_hubspot_temporal_cells

        rejected: list[dict] = []
        out = _normalize_hubspot_temporal_cells(
            [("not-a-date",)], ["closedate"], ["TIMESTAMPTZ"], rejected, "fail"
        )
        assert out == []
        assert len(rejected) == 1
        assert "closedate" == rejected[0]["column"]

    def test_date_only_carrier_uses_calendar_wire(self):
        from connectors.hubspot_writer import _normalize_hubspot_temporal_cells

        rejected: list[dict] = []
        out = _normalize_hubspot_temporal_cells(
            [("2024-05-06",)], ["birthday"], ["DATE"], rejected, "fail"
        )
        assert out == [("2024-05-06",)]
        assert rejected == []
