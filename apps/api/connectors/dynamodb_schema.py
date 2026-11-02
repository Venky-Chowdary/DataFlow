"""Canonical DynamoDB table-key and secondary-index schema contracts."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

DYNAMO_KEY_SCALARS = frozenset({"S", "N", "B"})
_DYNAMO_NONSCALAR_CODES = frozenset({"BOOL", "NULL", "M", "L", "SS", "NS", "BS"})
_NO_VALUE = object()

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DynamoKeyAttribute:
    name: str
    key_type: str
    attr_type: str


@dataclass(frozen=True)
class KeyContractViolation:
    column: str
    key_role: Literal["HASH", "RANGE", "INDEX"]
    expected_scalar: str
    reason: str
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True)
class DynamoTableSchema:
    keys: tuple[DynamoKeyAttribute, ...]
    index_attributes: Mapping[str, str]

    @property
    def key_types(self) -> dict[str, str]:
        return {key.name: key.attr_type for key in self.keys}

    @property
    def key_names(self) -> tuple[str, ...]:
        return tuple(key.name for key in self.keys)

    def declared_type(self, name: str) -> str | None:
        for key in self.keys:
            if key.name == name:
                return key.attr_type
        return self.index_attributes.get(name)

    def is_unconstrained(self, name: str) -> bool:
        return self.declared_type(name) is None

    @classmethod
    def from_key_schema_rows(
        cls,
        rows: list[Mapping[str, Any]],
        index_attributes: Mapping[str, str] | None = None,
    ) -> DynamoTableSchema:
        definitions: dict[str, str] = {}
        for row in rows or []:
            name = str(row.get("name") or "").strip()
            scalar = str(row.get("scalar") or row.get("attr_type") or "").strip().upper()
            if not name or scalar not in DYNAMO_KEY_SCALARS:
                raise ValueError("DynamoDB key schema row lacks a valid name/scalar")
            definitions[name] = scalar
        keys = _parse_key_rows(
            rows, definitions, context="table", allow_inline_scalar=True
        )
        indexes = {
            str(name): str(scalar).strip().upper()
            for name, scalar in (index_attributes or {}).items()
        }
        if any(not name or scalar not in DYNAMO_KEY_SCALARS for name, scalar in indexes.items()):
            raise ValueError("DynamoDB index key has an invalid name or scalar type")
        table_names = {key.name for key in keys}
        return cls(
            keys=keys,
            index_attributes={name: scalar for name, scalar in indexes.items() if name not in table_names},
        )


def _parse_key_rows(
    rows: Any,
    definitions: Mapping[str, str],
    *,
    context: str,
    allow_inline_scalar: bool = False,
) -> tuple[DynamoKeyAttribute, ...]:
    if not isinstance(rows, (list, tuple)) or not rows:
        raise ValueError(f"DynamoDB {context} KeySchema is missing")
    parsed: list[DynamoKeyAttribute] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError(f"DynamoDB {context} KeySchema contains an invalid entry")
        name = str(row.get("AttributeName") or row.get("name") or "").strip()
        role = str(row.get("KeyType") or row.get("key_type") or "").strip().upper()
        if not name or role not in {"HASH", "RANGE"}:
            raise ValueError(f"DynamoDB {context} KeySchema contains an invalid key")
        scalar = definitions.get(name)
        if not scalar and allow_inline_scalar:
            raw_scalar = row.get("scalar") or row.get("attr_type")
            scalar = str(raw_scalar or "").strip().upper()
        if scalar not in DYNAMO_KEY_SCALARS:
            raise ValueError(
                f"DynamoDB {context} key attribute {name!r} has invalid or missing AttributeType"
            )
        parsed.append(DynamoKeyAttribute(name, role, scalar))
    hashes = [key for key in parsed if key.key_type == "HASH"]
    ranges = [key for key in parsed if key.key_type == "RANGE"]
    if len(hashes) != 1 or len(ranges) > 1 or (ranges and not hashes):
        raise ValueError(f"DynamoDB {context} KeySchema must have one HASH and at most one RANGE")
    if len({key.name for key in parsed}) != len(parsed):
        raise ValueError(f"DynamoDB {context} KeySchema repeats an attribute")
    return tuple(hashes + ranges)


def parse_table_description(info: Mapping[str, Any]) -> DynamoTableSchema:
    """Parse ``describe_table()['Table']`` and fail closed on malformed keys."""
    if not isinstance(info, Mapping):
        raise ValueError("DynamoDB table description is invalid")
    definitions: dict[str, str] = {}
    for definition in info.get("AttributeDefinitions") or []:
        if not isinstance(definition, Mapping):
            continue
        name = str(definition.get("AttributeName") or "").strip()
        if name:
            scalar = str(definition.get("AttributeType") or "").strip().upper()
            if name in definitions and definitions[name] != scalar:
                raise ValueError(
                    f"DynamoDB attribute {name!r} has conflicting AttributeDefinitions"
                )
            definitions[name] = scalar

    keys = _parse_key_rows(info.get("KeySchema"), definitions, context="table")
    table_names = {key.name for key in keys}
    table_hash = next(key.name for key in keys if key.key_type == "HASH")
    indexes: dict[str, str] = {}
    for field in ("GlobalSecondaryIndexes", "LocalSecondaryIndexes"):
        for index in info.get(field) or []:
            if not isinstance(index, Mapping):
                raise ValueError(f"DynamoDB {field} contains an invalid entry")
            index_keys = _parse_key_rows(
                index.get("KeySchema"), definitions, context="index"
            )
            if field == "LocalSecondaryIndexes":
                index_hash = next(key.name for key in index_keys if key.key_type == "HASH")
                if (
                    len(index_keys) != 2
                    or index_hash != table_hash
                    or not any(key.key_type == "RANGE" for key in index_keys)
                ):
                    raise ValueError(
                        "DynamoDB local secondary index must share the table HASH "
                        "and define one RANGE key"
                    )
            for key in index_keys:
                if key.name in table_names:
                    continue
                previous = indexes.setdefault(key.name, key.attr_type)
                if previous != key.attr_type:
                    raise ValueError(
                        f"DynamoDB index attribute {key.name!r} has conflicting scalar types"
                    )
    return DynamoTableSchema(keys=keys, index_attributes=indexes)


def key_carrier_verdict(
    attr_type: str, logical: str
) -> Literal["compatible", "incompatible", "runtime_validated"]:
    """Classify a declared carrier using the shared canonical type taxonomy."""
    scalar = str(attr_type or "").strip().upper()
    if scalar not in DYNAMO_KEY_SCALARS:
        return "incompatible"
    raw = str(logical or "").strip()
    token = raw.upper()
    if token in DYNAMO_KEY_SCALARS:
        return "compatible" if token == scalar else "incompatible"
    if token in _DYNAMO_NONSCALAR_CODES:
        return "incompatible"
    if not raw:
        return "runtime_validated"

    from services.decimal_identity import is_numeric_catalog_type
    from services.encoding_capacity import is_string_catalog_type
    from services.type_system import (
        LOGICAL_BOOLEAN,
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
        LOGICAL_INTEGER,
        LOGICAL_OBJECTID,
        LOGICAL_TEXT,
        LOGICAL_TIME,
        LOGICAL_UUID,
        LOGICAL_VECTOR,
        is_binary_type,
        is_structural_type,
        normalize_logical_type,
    )

    logical_type = normalize_logical_type(raw)
    if is_structural_type(raw) or logical_type == LOGICAL_VECTOR:
        return "incompatible"
    if is_binary_type(raw):
        return "compatible" if scalar == "B" else "incompatible"
    if logical_type in {LOGICAL_INTEGER, LOGICAL_DECIMAL, LOGICAL_FLOAT} or (
        is_numeric_catalog_type(raw)
    ):
        return "compatible" if scalar in {"S", "N"} else "incompatible"
    if logical_type in {
        LOGICAL_BOOLEAN,
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        LOGICAL_TIME,
        LOGICAL_UUID,
        LOGICAL_OBJECTID,
    }:
        return "compatible" if scalar == "S" else "incompatible"
    if is_string_catalog_type(raw) or logical_type == LOGICAL_TEXT:
        return "compatible"
    return "runtime_validated"


def log_key_refusal(
    *,
    phase: str,
    table: str,
    column: str,
    key_role: str,
    expected_scalar: str,
    reason: str,
    value: Any = _NO_VALUE,
) -> None:
    """Emit a structured refusal without exposing binary or unbounded values."""
    if expected_scalar == "B":
        value_text = "<redacted:binary>"
    elif value is _NO_VALUE:
        value_text = "<n/a>"
    else:
        value_text = repr(value)
        if len(value_text) > 64:
            value_text = value_text[:63] + "…"
    extra = {
        "event": "dynamodb_key_refusal",
        "phase": phase,
        "table": table,
        "column": column,
        "key_role": key_role,
        "expected_scalar": expected_scalar,
        "reason": reason,
        "value": value_text,
    }
    logger.warning(
        "dynamodb_key_refusal phase=%s table=%s column=%s key_role=%s "
        "expected_scalar=%s reason=%s value=%s",
        phase,
        table,
        column,
        key_role,
        expected_scalar,
        reason,
        value_text,
        extra=extra,
    )


def _log_runtime_validated(
    *, column: str, key_role: str, expected_scalar: str, declared_type: str
) -> None:
    logger.info(
        "dynamodb_key_runtime_validated column=%s key_role=%s "
        "expected_scalar=%s declared_type=%s",
        column,
        key_role,
        expected_scalar,
        declared_type,
        extra={
            "event": "dynamodb_key_runtime_validated",
            "column": column,
            "key_role": key_role,
            "expected_scalar": expected_scalar,
            "declared_type": declared_type,
        },
    )


def key_contract_violations(
    schema: DynamoTableSchema,
    *,
    mappings: list[dict],
    column_types: Mapping[str, str] | None,
    conflict_columns: list[str] | None = None,
) -> list[KeyContractViolation]:
    """Return identity and scalar mismatches before DynamoDB receives a write."""
    from services.mapping_constraints import write_mappings

    active = write_mappings(mappings)
    targets = [
        str(mapping.get("target") or "").strip()
        for mapping in active
        if str(mapping.get("target") or "").strip()
    ]
    types = column_types or {}
    violations: list[KeyContractViolation] = []
    for key in schema.keys:
        mapped = [
            mapping
            for mapping in active
            if str(mapping.get("target") or "").strip() == key.name
        ]
        if not mapped:
            violations.append(
                KeyContractViolation(
                    column=key.name,
                    key_role=key.key_type,
                    expected_scalar=key.attr_type,
                    reason="key_not_mapped",
                    message=(
                        f"DynamoDB key attribute {key.name!r} "
                        f"({key.key_type}, {key.attr_type}) is not mapped — "
                        "refuse PutItem without table identity"
                    ),
                )
            )
            continue
        for mapping in mapped:
            source = str(mapping.get("source") or mapping.get("source_column") or "").strip()
            source_type = mapping.get("source_type") or types.get(source) or ""
            source_verdict = key_carrier_verdict(key.attr_type, str(source_type))
            if source_verdict == "incompatible":
                violations.append(
                    KeyContractViolation(
                        column=key.name,
                        key_role=key.key_type,
                        expected_scalar=key.attr_type,
                        reason="incompatible_source_type",
                        message=(
                            f"DynamoDB key attribute {key.name!r} "
                            f"({key.key_type}, {key.attr_type}) cannot carry "
                            f"source type {source_type!r}"
                        ),
                    )
                )
            elif source_verdict == "runtime_validated":
                _log_runtime_validated(
                    column=key.name,
                    key_role=key.key_type,
                    expected_scalar=key.attr_type,
                    declared_type=str(source_type),
                )
            explicit = mapping.get("target_type") or mapping.get("dest_type")
            if explicit:
                target_verdict = key_carrier_verdict(key.attr_type, str(explicit))
            else:
                target_verdict = "compatible"
            if target_verdict == "incompatible":
                violations.append(
                    KeyContractViolation(
                        column=key.name,
                        key_role=key.key_type,
                        expected_scalar=key.attr_type,
                        reason="incompatible_target_type",
                        message=(
                            f"DynamoDB key attribute {key.name!r} "
                            f"({key.key_type}, {key.attr_type}) Map target type "
                            f"{explicit!r} would change the key schema"
                        ),
                    )
                )
            elif target_verdict == "runtime_validated":
                _log_runtime_validated(
                    column=key.name,
                    key_role=key.key_type,
                    expected_scalar=key.attr_type,
                    declared_type=str(explicit),
                )

    for mapping in active:
        target = str(mapping.get("target") or "").strip()
        index_type = schema.index_attributes.get(target)
        explicit = mapping.get("target_type") or mapping.get("dest_type")
        if not index_type or not explicit:
            continue
        index_verdict = key_carrier_verdict(index_type, str(explicit))
        if index_verdict == "incompatible":
            violations.append(
                KeyContractViolation(
                    column=target,
                    key_role="INDEX",
                    expected_scalar=index_type,
                    reason="incompatible_index_target_type",
                    message=(
                        f"DynamoDB index key attribute {target!r} ({index_type}) "
                        f"Map target type {explicit!r} conflicts with its declared scalar"
                    ),
                )
            )
        elif index_verdict == "runtime_validated":
            _log_runtime_validated(
                column=target,
                key_role="INDEX",
                expected_scalar=index_type,
                declared_type=str(explicit),
            )

    if conflict_columns:
        from connectors.writer_common import resolve_conflict_targets

        requested = resolve_conflict_targets(conflict_columns, targets, strict=False)
        if set(requested) != set(schema.key_names):
            hash_key = next(key for key in schema.keys if key.key_type == "HASH")
            violations.append(
                KeyContractViolation(
                    column=hash_key.name,
                    key_role="HASH",
                    expected_scalar=hash_key.attr_type,
                    reason="conflict_columns_mismatch",
                    message=(
                        f"DynamoDB requested identity {requested!r} != table "
                        f"KeySchema {list(schema.key_names)!r} — refuse non-table identity"
                    ),
                )
            )
    return violations


def nonkey_attribute_gaps(
    schema: DynamoTableSchema,
    target_cols: list[str],
    typed: Mapping[str, str] | None,
) -> list[str]:
    """Mapped targets lacking a carrier, using the live-type case-folded lookup."""
    present = {
        str(name).casefold()
        for name, value in (typed or {}).items()
        if str(name).strip() and str(value or "").strip()
    }
    return [
        str(column)
        for column in target_cols or []
        if str(column).strip()
        and str(column).casefold() not in present
    ]
