from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    LongType,
)


class IcebergSchemaEvolutionError(ValueError):
    pass


@dataclass(frozen=True)
class SchemaChange:
    kind: str
    column: str
    from_type: IcebergType | None
    to_type: IcebergType | None
    new_name: str | None = None


@dataclass(frozen=True)
class SchemaPlan:
    changes: tuple[SchemaChange, ...]
    refused: tuple[str, ...]


def _as_iceberg_schema(incoming: pa.Schema | Schema) -> Schema:
    if isinstance(incoming, Schema):
        return incoming
    if isinstance(incoming, pa.Schema):
        return Catalog._convert_schema_if_needed(incoming)
    raise TypeError("incoming must be a pyarrow or Iceberg schema")


def _widening_is_allowed(existing: IcebergType, incoming: IcebergType) -> bool:
    if isinstance(existing, IntegerType) and isinstance(incoming, LongType):
        return True
    if isinstance(existing, FloatType) and isinstance(incoming, DoubleType):
        return True
    return (
        isinstance(existing, DecimalType)
        and isinstance(incoming, DecimalType)
        and existing.scale == incoming.scale
        and incoming.precision > existing.precision
    )


def plan_schema_change(
    existing: Schema,
    incoming: pa.Schema | Schema,
    *,
    rename_columns: Mapping[str, str] | None = None,
    allow_widening: bool = True,
) -> SchemaPlan:
    if rename_columns is not None and not isinstance(rename_columns, Mapping):
        raise ValueError("rename_columns must be a mapping of strings to strings")
    renames = dict(rename_columns or {})
    if any(
        not isinstance(source, str) or not isinstance(target, str)
        for source, target in renames.items()
    ):
        raise ValueError("rename_columns must be a mapping of strings to strings")

    incoming_schema = _as_iceberg_schema(incoming)
    existing_fields = {field.name: field for field in existing.fields}
    incoming_fields = {field.name: field for field in incoming_schema.fields}
    refused: list[str] = []
    rename_changes: list[SchemaChange] = []
    widening_changes: list[SchemaChange] = []
    add_changes: list[SchemaChange] = []
    rename_by_source: dict[str, str] = {}
    reserved_targets: set[str] = set()

    if len(existing_fields) != len(existing.fields):
        refused.append("existing schema has duplicate column names")
    if len(incoming_fields) != len(incoming_schema.fields):
        refused.append("incoming schema has duplicate column names")

    for source, target in renames.items():
        if source == target:
            continue
        reserved_targets.add(target)
        field = existing_fields.get(source)
        if field is None:
            refused.append(f"rename source {source!r} does not exist")
        elif target in existing_fields:
            refused.append(f"rename target {target!r} already exists")
        elif target in rename_by_source.values():
            refused.append(f"rename target {target!r} is specified more than once")
        else:
            rename_by_source[source] = target
            rename_changes.append(
                SchemaChange(
                    "rename",
                    source,
                    field.field_type,
                    field.field_type,
                    target,
                )
            )

    matched_incoming: set[str] = set()
    for field in existing.fields:
        incoming_name = rename_by_source.get(field.name, field.name)
        incoming_field = incoming_fields.get(incoming_name)
        if incoming_field is None:
            continue
        matched_incoming.add(incoming_name)

        if field.required != incoming_field.required:
            refused.append(
                f"requiredness change for {field.name!r}: "
                f"{'required' if field.required else 'optional'} to "
                f"{'required' if incoming_field.required else 'optional'}"
            )

        if field.field_type == incoming_field.field_type:
            continue
        if allow_widening and _widening_is_allowed(
            field.field_type, incoming_field.field_type
        ):
            widening_changes.append(
                SchemaChange(
                    "widen",
                    field.name,
                    field.field_type,
                    incoming_field.field_type,
                )
            )
        else:
            refused.append(
                f"type_locked: keep {field.name}:{field.field_type} "
                f"(incoming {incoming_field.field_type})"
            )

    for field in incoming_schema.fields:
        if field.name in matched_incoming or field.name in existing_fields:
            continue
        if field.name in reserved_targets:
            continue
        if field.required:
            refused.append(f"cannot add required column {field.name!r}")
            continue
        add_changes.append(
            SchemaChange("add", field.name, None, field.field_type)
        )

    return SchemaPlan(
        changes=tuple(widening_changes + rename_changes + add_changes),
        refused=tuple(refused),
    )


def apply_schema_plan(update_schema_ctx: Any, plan: SchemaPlan) -> None:
    if plan.refused:
        raise IcebergSchemaEvolutionError("; ".join(plan.refused))

    for change in plan.changes:
        if change.kind == "add":
            if change.to_type is None:
                raise IcebergSchemaEvolutionError(
                    f"add change for {change.column!r} has no target type"
                )
            update_schema_ctx.add_column(
                (change.column,), change.to_type, required=False
            )
        elif change.kind == "widen":
            if change.to_type is None:
                raise IcebergSchemaEvolutionError(
                    f"widen change for {change.column!r} has no target type"
                )
            update_schema_ctx.update_column(
                (change.column,), field_type=change.to_type
            )
        elif change.kind == "rename":
            if change.new_name is None:
                raise IcebergSchemaEvolutionError(
                    f"rename change for {change.column!r} has no target name"
                )
            update_schema_ctx.rename_column((change.column,), change.new_name)
        else:
            raise IcebergSchemaEvolutionError(
                f"unsupported schema change kind {change.kind!r}"
            )
