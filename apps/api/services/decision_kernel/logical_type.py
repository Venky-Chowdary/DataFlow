"""Typed logical / native carriers — Property 1 referential transparency.

Native SQL spellings and logical invent tokens must not share one ambiguous
string slot. ``INTEGER`` / ``integer`` / ``INT`` all normalize to the same
logical *kind*; only an explicit width (or an unambiguous token like INT4)
selects 32-bit invent. Bare logical integer/float default to 64-bit.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Union


@dataclass(frozen=True)
class NativeType:
    """Dialect-native DDL text for same-family passthrough."""

    db: str
    text: str


@dataclass(frozen=True)
class LogicalType:
    """Width-bearing logical type (DECIMAL already had precision/scale).

    ``width`` for integers is signed bit width (8/16/32/64; unsigned +1).
    ``width`` for floats is IEEE significand bits (11/24/53).
    ``None`` means invent via DDL_TYPES never-narrower default (64 / IEEE-64).
    """

    kind: str
    width: int | None = None
    precision: int | None = None
    scale: int | None = None
    tz: bool | None = None

    def to_carrier(self) -> str:
        """Unambiguous invent/carrier spelling for the string ddl_type path."""
        kind = (self.kind or "").strip().lower()
        if kind == "integer":
            w = self.width
            if w is None or w >= 64:
                return "BIGINT"
            if w <= 8:
                return "TINYINT"
            if w <= 16:
                return "SMALLINT"
            if w <= 24:
                return "MEDIUMINT"
            if w <= 32:
                return "INT4"
            if w == 33:
                return "INT4 UNSIGNED"
            return "BIGINT"
        if kind == "float":
            w = self.width
            if w is None or w >= 53:
                return "DOUBLE"
            if w <= 11:
                return "FLOAT16"
            if w <= 24:
                return "FLOAT32"
            return "DOUBLE"
        if kind == "decimal" and self.precision is not None:
            if self.scale is not None:
                return f"DECIMAL({self.precision},{self.scale})"
            return f"DECIMAL({self.precision})"
        return self.kind


TypeRef = Union[str, LogicalType, NativeType]


def parse_type_ref(raw: str | None) -> LogicalType | None:
    """Parse a free-form type string into a LogicalType when possible.

    Ambiguous tokens (INTEGER/INT/FLOAT in any case) yield ``width=None`` so
    invent defaults to 64-bit — never case-select Int32/Float32.
    """
    # Late import avoids circular load with type_system.
    from services.type_system import (
        LOGICAL_FLOAT,
        LOGICAL_INTEGER,
        float_mantissa_bits,
        integer_bit_width,
        normalize_logical_type,
        strip_identity_qualifier,
    )

    text = strip_identity_qualifier(raw)
    if not text:
        return None
    kind = normalize_logical_type(text)
    if kind == LOGICAL_INTEGER:
        return LogicalType(kind=kind, width=integer_bit_width(text))
    if kind == LOGICAL_FLOAT:
        return LogicalType(kind=kind, width=float_mantissa_bits(text))
    return LogicalType(kind=kind)


def coerce_type_ref(value: TypeRef | None) -> str | None:
    """Collapse TypeRef to a carrier string for legacy ddl_type(str) callers."""
    if value is None:
        return None
    if isinstance(value, NativeType):
        return value.text
    if isinstance(value, LogicalType):
        return value.to_carrier()
    return value


__all__ = [
    "LogicalType",
    "NativeType",
    "TypeRef",
    "coerce_type_ref",
    "parse_type_ref",
]
