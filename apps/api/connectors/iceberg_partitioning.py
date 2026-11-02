from __future__ import annotations

import re
from typing import Any, Mapping

from pyiceberg.partitioning import (
    PARTITION_FIELD_ID_START,
    PartitionField,
    PartitionSpec,
    _PartitionNameGenerator,
    _visit_partition_field,
)
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    TruncateTransform,
    YearTransform,
)


class IcebergPartitionSpecError(ValueError):
    pass


_PARAMETERIZED_TRANSFORM = re.compile(r"^(bucket|truncate)\[(\d+)\]$")


def parse_partition_spec(
    spec: Any,
) -> tuple[tuple[str, Transform[Any, Any]], ...]:
    if not isinstance(spec, list):
        raise ValueError("partition_spec must be a list")

    parsed: list[tuple[str, Transform[Any, Any]]] = []
    seen: set[tuple[str, str]] = set()
    for index, item in enumerate(spec):
        if not isinstance(item, dict) or set(item) != {"column", "transform"}:
            raise ValueError(
                f"partition_spec[{index}] must contain only column and transform"
            )
        column = item["column"]
        transform_name = item["transform"]
        if not isinstance(column, str) or not column.strip():
            raise ValueError(
                f"partition_spec[{index}].column must be a non-empty string"
            )
        if not isinstance(transform_name, str):
            raise ValueError(
                f"partition_spec[{index}].transform must be a string"
            )

        name = transform_name.strip()
        if name == "identity":
            transform: Transform[Any, Any] = IdentityTransform()
        elif name == "year":
            transform = YearTransform()
        elif name == "month":
            transform = MonthTransform()
        elif name == "day":
            transform = DayTransform()
        elif name == "hour":
            transform = HourTransform()
        else:
            match = _PARAMETERIZED_TRANSFORM.fullmatch(name)
            if match is None:
                raise ValueError(
                    f"unsupported Iceberg partition transform {transform_name!r}"
                )
            width = int(match.group(2))
            if width <= 0:
                raise ValueError(
                    f"Iceberg partition transform parameter must be positive: "
                    f"{transform_name!r}"
                )
            if match.group(1) == "bucket":
                transform = BucketTransform(width)
            else:
                transform = TruncateTransform(width)

        key = (column, repr(transform))
        if key in seen:
            raise ValueError(
                f"duplicate Iceberg partition field for {column!r} and {transform}"
            )
        seen.add(key)
        parsed.append((column, transform))
    return tuple(parsed)


def partition_spec_for_schema(
    schema: Schema,
    declared: tuple[tuple[str, Transform[Any, Any]], ...],
    *,
    rename_columns: Mapping[str, str] | None = None,
) -> PartitionSpec:
    source_by_target = {
        target: source for source, target in (rename_columns or {}).items()
    }
    fields: list[PartitionField] = []
    for position, (column, transform) in enumerate(declared):
        source_column = column
        try:
            source_field = schema.find_field(source_column)
        except ValueError:
            source_column = source_by_target.get(column, column)
            try:
                source_field = schema.find_field(source_column)
            except ValueError as exc:
                raise ValueError(
                    f"partition_spec column {column!r} does not exist"
                ) from exc
        if source_field is None:
            raise ValueError(f"partition_spec column {column!r} does not exist")
        if not transform.can_transform(source_field.field_type):
            raise ValueError(
                f"partition transform {transform} cannot transform "
                f"{source_field.field_type} column {column!r}"
            )

        field_id = PARTITION_FIELD_ID_START + position
        unnamed = PartitionField(
            source_id=source_field.field_id,
            field_id=field_id,
            transform=transform,
            name="unassigned_field_name",
        )
        name = _visit_partition_field(schema, unnamed, _PartitionNameGenerator())
        fields.append(
            PartitionField(
                source_id=source_field.field_id,
                field_id=field_id,
                transform=transform,
                name=name,
            )
        )
    return PartitionSpec(*fields)


def partition_spec_matches(
    schema: Schema,
    existing: PartitionSpec,
    declared: tuple[tuple[str, Transform[Any, Any]], ...],
    *,
    rename_columns: Mapping[str, str] | None = None,
) -> bool:
    renamed = rename_columns or {}
    existing_fields: set[tuple[str, str]] = set()
    for field in existing.fields:
        column = schema.find_column_name(field.source_id)
        if column is None:
            return False
        existing_fields.add((renamed.get(column, column), repr(field.transform)))
    desired_fields = {(column, repr(transform)) for column, transform in declared}
    return len(existing_fields) == len(existing.fields) and existing_fields == desired_fields
