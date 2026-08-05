"""Tests for the Migration Decision Kernel."""

from __future__ import annotations

import pytest

from services.migration_kernel import (
    ColumnModel,
    ConversionClass,
    MigrationKernel,
    SchemaModel,
    TypeCarrier,
)


@pytest.fixture
def kernel() -> MigrationKernel:
    return MigrationKernel()


def test_identity_same_type(kernel: MigrationKernel) -> None:
    verdict = kernel.classify_conversion("VARCHAR(100)", "VARCHAR(100)")
    assert verdict.classification == ConversionClass.IDENTITY
    assert verdict.confidence == 1.0
    assert verdict.execution_policy == "allowed"


def test_widening_integer_to_decimal(kernel: MigrationKernel) -> None:
    verdict = kernel.classify_conversion("INTEGER", "DECIMAL(19,0)", dest_db="postgresql")
    assert verdict.classification == ConversionClass.WIDENING
    assert verdict.execution_policy == "allowed"


def test_lossy_float_to_integer(kernel: MigrationKernel) -> None:
    verdict = kernel.classify_conversion("FLOAT", "INTEGER", dest_db="postgresql")
    assert verdict.classification == ConversionClass.LOSSY
    assert verdict.execution_policy == "quarantine"
    assert "precision" in verdict.evidence.lower() or "mantissa" in verdict.evidence.lower()


def test_string_to_text_is_widening(kernel: MigrationKernel) -> None:
    verdict = kernel.classify_conversion("VARCHAR(50)", "TEXT", dest_db="postgresql")
    assert verdict.classification == ConversionClass.WIDENING


def test_unknown_when_no_safe_path(kernel: MigrationKernel) -> None:
    # geometry → integer has no safe widening rule in the default classification.
    verdict = kernel.classify_conversion("GEOMETRY", "INTEGER", dest_db="postgresql")
    assert verdict.classification in {ConversionClass.LOSSY, ConversionClass.UNKNOWN}


def test_canonicalize_type_extracts_precision_scale(kernel: MigrationKernel) -> None:
    carrier = kernel.canonicalize_type("DECIMAL(18,4)")
    assert carrier.logical == "decimal"
    assert carrier.precision == 18
    assert carrier.scale == 4
    assert carrier.length is None


def test_canonicalize_type_extracts_varchar_width(kernel: MigrationKernel) -> None:
    carrier = kernel.canonicalize_type("VARCHAR(255)")
    assert carrier.logical == "string"
    assert carrier.length == 255


def test_build_decision_is_immutable_and_hashed(kernel: MigrationKernel) -> None:
    source = SchemaModel(
        kind="database",
        format="postgresql",
        name="src",
        columns=(
            ColumnModel(name="id", carrier=TypeCarrier(logical="integer", native="INTEGER")),
            ColumnModel(name="name", carrier=TypeCarrier(logical="string", native="VARCHAR(100)")),
        ),
    )
    destination = SchemaModel(
        kind="database",
        format="postgresql",
        name="dst",
        columns=(
            ColumnModel(name="id", carrier=TypeCarrier(logical="integer", native="INTEGER")),
            ColumnModel(name="name", carrier=TypeCarrier(logical="string", native="VARCHAR(100)")),
        ),
    )

    decision = kernel.build_decision(source, destination, dest_db="postgresql")
    assert decision.decision_id
    assert decision.hash
    assert decision.mapping.confidence == 1.0
    assert not decision.mapping.requires_review
    assert decision.validation.passed
    assert decision.validation.write_permitted

    # Hash is deterministic for the same inputs.
    decision2 = kernel.build_decision(source, destination, dest_db="postgresql")
    assert decision2.hash == decision.hash


def test_build_decision_blocks_lossy_mapping(kernel: MigrationKernel) -> None:
    source = SchemaModel(
        kind="database",
        format="postgresql",
        name="src",
        columns=(
            ColumnModel(name="amount", carrier=TypeCarrier(logical="float", native="FLOAT")),
        ),
    )
    destination = SchemaModel(
        kind="database",
        format="postgresql",
        name="dst",
        columns=(
            ColumnModel(name="amount", carrier=TypeCarrier(logical="integer", native="INTEGER")),
        ),
    )

    decision = kernel.build_decision(source, destination, dest_db="postgresql", validation_mode="strict")
    assert decision.mapping.requires_review
    assert not decision.validation.passed
    assert not decision.validation.write_permitted
    g3 = next(g for g in decision.validation.gates if g.gate_id == "G3")
    assert g3.status == "block"


def test_validation_mode_discovery_never_writes(kernel: MigrationKernel) -> None:
    source = SchemaModel(
        kind="database",
        format="postgresql",
        name="src",
        columns=(
            ColumnModel(name="id", carrier=TypeCarrier(logical="integer", native="INTEGER")),
        ),
    )
    destination = SchemaModel(
        kind="database",
        format="postgresql",
        name="dst",
        columns=(
            ColumnModel(name="id", carrier=TypeCarrier(logical="integer", native="INTEGER")),
        ),
    )

    decision = kernel.build_decision(source, destination, dest_db="postgresql", validation_mode="discovery")
    assert decision.validation.mode == "discovery"
    assert not decision.validation.write_permitted


def test_canonicalize_temporal_preserves_fsp_and_timezone(kernel: MigrationKernel) -> None:
    carrier = kernel.canonicalize_type("TIMESTAMP(6) WITH TIME ZONE")
    assert carrier.logical == "datetime"
    assert carrier.precision == 6
    assert carrier.timezone == "tz"


def test_canonicalize_time_preserves_fsp_and_ntz(kernel: MigrationKernel) -> None:
    carrier = kernel.canonicalize_type("TIME(3)")
    assert carrier.logical == "time"
    assert carrier.precision == 3
    assert carrier.timezone == "ntz"


def test_canonicalize_binary_extracts_length(kernel: MigrationKernel) -> None:
    carrier = kernel.canonicalize_type("VARBINARY(2048)")
    assert carrier.logical == "binary"
    assert carrier.length == 2048


def test_precision_narrowing_is_lossy(kernel: MigrationKernel) -> None:
    verdict = kernel.classify_conversion("DECIMAL(18,4)", "DECIMAL(12,2)", dest_db="postgresql")
    assert verdict.classification == ConversionClass.LOSSY
    assert verdict.execution_policy == "quarantine"


def test_same_logical_different_width_is_representation_change(kernel: MigrationKernel) -> None:
    verdict = kernel.classify_conversion("VARCHAR(50)", "VARCHAR(100)", dest_db="postgresql")
    assert verdict.classification == ConversionClass.REPRESENTATION_CHANGE
    assert verdict.confidence == pytest.approx(0.7)
