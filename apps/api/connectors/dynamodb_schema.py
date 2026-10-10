"""Canonical DynamoDB table-key and secondary-index schema contracts."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

DYNAMO_KEY_SCALARS = frozenset({"S", "N", "B"})
_DYNAMO_NONSCALAR_CODES = frozenset({"BOOL", "NULL", "M", "L", "SS", "NS", "BS"})


@dataclass(frozen=True)
class DynamoKeyAttribute:
    name: str
    key_type: str
    attr_type: str


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


def key_carrier_compatible(attr_type: str, logical: str) -> bool:
    """Whether a logical source or explicit Map carrier can feed a Dynamo key."""
    scalar = str(attr_type or "").strip().upper()
    if scalar not in DYNAMO_KEY_SCALARS:
        return False
    raw = str(logical or "").strip().upper()
    base = re.sub(r"\s*\([^)]*\)", "", raw).strip()
    if not base:
        return True
    if base in _DYNAMO_NONSCALAR_CODES:
        return False
    if base in DYNAMO_KEY_SCALARS:
        return base == scalar

    from services.type_system import is_binary_type, is_structural_type

    if is_structural_type(base):
        return False
    if is_binary_type(base) or base in {
        "BINARY", "BLOB", "BYTEA", "VARBINARY", "LONGBLOB", "RAW", "IMAGE",
    }:
        return scalar == "B"

    text_types = {
        "CHAR", "CHARACTER", "CHARACTER VARYING", "CLOB", "NCHAR",
        "ENUM", "NVARCHAR", "NVARCHAR2", "STRING", "TEXT", "UUID",
        "VARCHAR", "VARCHAR2",
    }
    numeric_types = {
        "BIGINT", "BIGSERIAL", "DEC", "DECIMAL", "DECIMAL128", "DOUBLE",
        "DOUBLE PRECISION", "FLOAT", "FLOAT4", "FLOAT8", "FLOAT64", "INT",
        "INT2", "INT4", "INT8", "INT64", "INTEGER", "MONEY", "NUMBER",
        "NUMERIC", "REAL", "SERIAL", "SMALLINT", "SMALLSERIAL", "TINYINT",
        "UINT", "UINT8", "UINT16", "UINT32", "UINT64", "UNSIGNED", "VARINT",
    }
    temporal_types = {
        "DATE", "DATETIME", "DATETIME2", "LOCALTIME", "TIMESTAMP",
        "TIMESTAMP_NTZ", "TIMESTAMPTZ", "TIME", "TIME_TZ",
    }
    if base in text_types:
        return True
    if base in numeric_types:
        return scalar in {"S", "N"}
    if base in temporal_types:
        return scalar == "S"
    if base in {"BOOLEAN", "BOOL"}:
        return scalar == "S"
    return True


def key_contract_violations(
    schema: DynamoTableSchema,
    *,
    mappings: list[dict],
    column_types: Mapping[str, str] | None,
    conflict_columns: list[str] | None = None,
) -> list[str]:
    """Return identity and scalar mismatches before DynamoDB receives a write."""
    from services.mapping_constraints import write_mappings

    active = write_mappings(mappings)
    targets = [
        str(mapping.get("target") or "").strip()
        for mapping in active
        if str(mapping.get("target") or "").strip()
    ]
    types = column_types or {}
    violations: list[str] = []
    for key in schema.keys:
        mapped = [
            mapping
            for mapping in active
            if str(mapping.get("target") or "").strip() == key.name
        ]
        if not mapped:
            violations.append(
                f"DynamoDB key attribute {key.name!r} ({key.key_type}, {key.attr_type}) "
                "is not mapped — refuse PutItem without table identity"
            )
            continue
        for mapping in mapped:
            source = str(mapping.get("source") or mapping.get("source_column") or "").strip()
            source_type = mapping.get("source_type") or types.get(source) or ""
            if not key_carrier_compatible(key.attr_type, str(source_type)):
                violations.append(
                    f"DynamoDB key attribute {key.name!r} ({key.key_type}, {key.attr_type}) "
                    f"cannot carry source type {source_type!r}"
                )
            explicit = mapping.get("target_type") or mapping.get("dest_type")
            if explicit and not key_carrier_compatible(key.attr_type, str(explicit)):
                violations.append(
                    f"DynamoDB key attribute {key.name!r} ({key.key_type}, {key.attr_type}) "
                    f"Map target type {explicit!r} would change the key schema"
                )

    for mapping in active:
        target = str(mapping.get("target") or "").strip()
        index_type = schema.index_attributes.get(target)
        explicit = mapping.get("target_type") or mapping.get("dest_type")
        if index_type and explicit and not key_carrier_compatible(index_type, str(explicit)):
            violations.append(
                f"DynamoDB index key attribute {target!r} ({index_type}) "
                f"Map target type {explicit!r} conflicts with its declared scalar"
            )

    if conflict_columns:
        from connectors.writer_common import resolve_conflict_targets

        requested = resolve_conflict_targets(conflict_columns, targets, strict=False)
        if set(requested) != set(schema.key_names):
            violations.append(
                f"DynamoDB requested identity {requested!r} != table KeySchema "
                f"{list(schema.key_names)!r} — refuse non-table identity"
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
