"""Shared Arrow type map + cell coerce for Iceberg and object-store Parquet.

One SSOT: Iceberg data files and S3/GCS/ADLS/SFTP ``.parquet`` exports must
not invent a second coerce path. Dialect ``iceberg`` keeps Map≡CREATE decimal
wiring via ``ddl_type("iceberg", …)``. Dialect ``parquet`` honors the
destination carrier already resolved by Studio/Map (no Iceberg DDL overlay).
"""

from __future__ import annotations

import json
from typing import Any

from services.value_serializer import json_default


def logical_to_arrow_type(logical: str, pa: Any, *, dialect: str = "parquet") -> Any:
    """Map a Datawrap logical / DDL carrier → pyarrow type (fail-closed decimals)."""
    from services.type_system import (
        LOGICAL_BINARY,
        LOGICAL_BOOLEAN,
        LOGICAL_DATE,
        LOGICAL_DATETIME,
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
        LOGICAL_INTEGER,
        LOGICAL_TIME,
        normalize_logical_type,
        parse_numeric_precision_scale,
    )

    raw = (logical or "string").strip()
    logical_n = normalize_logical_type(raw)
    if logical_n == LOGICAL_BOOLEAN:
        return pa.bool_()
    if logical_n == LOGICAL_INTEGER:
        return pa.int64()
    if logical_n == LOGICAL_FLOAT:
        raw_u = raw.upper().split("(", 1)[0].strip()
        if raw_u in {"REAL", "FLOAT4", "HALF", "FLOAT16", "FLOAT32", "BINARY_FLOAT", "FLOAT"}:
            return pa.float32()
        return pa.float64()
    if logical_n == LOGICAL_DECIMAL:
        wire = raw
        if dialect == "iceberg":
            from services.decision_kernel import ddl_type

            wire = ddl_type("iceberg", raw)
            if normalize_logical_type(wire) != LOGICAL_DECIMAL:
                return pa.large_string()
        precision, scale = parse_numeric_precision_scale(wire)
        if precision is None:
            return pa.large_string()
        p = int(precision)
        s = int(scale) if scale is not None else 0
        if p < 1 or p > 38 or s < 0 or s > p:
            return pa.large_string()
        return pa.decimal128(p, s)
    if logical_n == LOGICAL_DATE:
        return pa.date32()
    if logical_n == LOGICAL_DATETIME:
        raw_u = raw.upper().replace("_", " ")
        if "TIMESTAMPTZ" in raw_u or "WITH TIME ZONE" in raw_u or "TIMESTAMP TZ" in raw_u:
            return pa.timestamp("us", tz="UTC")
        return pa.timestamp("us")
    if logical_n == LOGICAL_TIME:
        return pa.time64("us")
    if logical_n == LOGICAL_BINARY:
        from services.type_system import parse_binary_carrier_width

        width = parse_binary_carrier_width(raw)
        if width is not None and width > 0:
            return pa.binary(int(width))
        return pa.large_binary()
    return pa.large_string()


def coerce_arrow_cell(
    value: Any,
    arrow_type: Any,
    pa: Any,
    *,
    dialect: str = "parquet",
) -> Any:
    """Coerce a Python cell into the declared Arrow type; raise on hard failure."""
    from datetime import date, datetime, time
    from decimal import Decimal, InvalidOperation

    from services.value_serializer import is_missing_sentinel

    label = "Iceberg" if dialect == "iceberg" else "Parquet"
    if is_missing_sentinel(value):
        raise ValueError(
            "DF_MISSING reached Arrow coerce — sparse CDC must overlay onto "
            "existing rows before building the Arrow batch"
        )
    if value is None:
        return None
    if value == "":
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return ""
        raise ValueError(
            f"empty string cannot coerce to {arrow_type} — "
            "refuse silent NULL invent (quarantine or remap upstream)"
        )
    if pa.types.is_decimal(arrow_type):
        try:
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError(f"cannot cast {value!r} to decimal") from exc
    if pa.types.is_floating(arrow_type):
        from connectors.sql_bind import coerce_float_wire

        if isinstance(value, str) and not str(value).strip():
            raise ValueError(
                "empty string cannot coerce to float — refuse silent NULL invent"
            )
        out = coerce_float_wire(value, ddl_type="FLOAT")
        if out is None:
            return None
        if isinstance(out, float) and (
            out != out or out in {float("inf"), float("-inf")}
        ):
            raise ValueError(
                f"cannot cast non-finite {value!r} to {label} float — refuse invent"
            )
        return out
    if pa.types.is_integer(arrow_type):
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if not value.is_integer():
                raise ValueError(
                    f"cannot coerce non-integral float {value!r} to INTEGER "
                    "without truncation"
                )
            return int(value)
        if isinstance(value, Decimal):
            if value != value.to_integral_value():
                raise ValueError(
                    f"cannot coerce non-integral decimal {value!r} to INTEGER "
                    "without truncation"
                )
            return int(value)
        try:
            return int(value)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"cannot coerce {value!r} to INTEGER without invent"
            ) from exc
    if pa.types.is_boolean(arrow_type):
        from connectors.sql_bind import coerce_boolean_wire

        if isinstance(value, bool):
            return value
        coerced = coerce_boolean_wire(value, as_int=False)
        if not isinstance(coerced, bool):
            raise ValueError(
                f"cannot cast {value!r} to boolean — refuse invent"
            )
        return coerced
    if pa.types.is_timestamp(arrow_type):
        tz = getattr(arrow_type, "tz", None)
        if isinstance(value, datetime):
            if tz and value.tzinfo is None:
                raise ValueError(
                    f"{label} TIMESTAMPTZ refused naive datetime — provide "
                    "offset/Z (refuse silent UTC invent)"
                )
            if not tz and value.tzinfo is not None:
                return value.replace(tzinfo=None)
            return value
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if tz and parsed.tzinfo is None:
            raise ValueError(
                f"{label} TIMESTAMPTZ refused naive datetime — provide "
                "offset/Z (refuse silent UTC invent)"
            )
        if not tz and parsed.tzinfo is not None:
            return parsed.replace(tzinfo=None)
        return parsed
    if pa.types.is_date(arrow_type):
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value)[:10])
    if pa.types.is_time(arrow_type):
        if isinstance(value, time):
            return value
        if isinstance(value, datetime):
            return value.time()
        return time.fromisoformat(str(value))
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        if value is None:
            return None
        from connectors.sql_bind import coerce_binary_wire

        return coerce_binary_wire(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=json_default)
    return str(value)


def write_mapped_rows_parquet(
    mapped_rows: list[tuple],
    target_cols: list[str],
    dest_types: dict[str, str] | None,
    dest: Any,
    *,
    dialect: str = "parquet",
) -> str:
    """Write typed Parquet to a file-like. Raises on coerce failure (never silent NULL)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    dest_types = dest_types or {}
    arrow_types = [
        logical_to_arrow_type(str(dest_types.get(c, "TEXT") or "TEXT"), pa, dialect=dialect)
        for c in target_cols
    ]
    schema = pa.schema([(c, t) for c, t in zip(target_cols, arrow_types)])
    columns: dict[str, list[Any]] = {c: [] for c in target_cols}
    for row in mapped_rows:
        for col, val, at in zip(target_cols, row, arrow_types):
            columns[col].append(coerce_arrow_cell(val, at, pa, dialect=dialect))
    table = pa.table(columns, schema=schema)
    pq.write_table(table, dest, compression="snappy")
    return "application/vnd.apache.parquet"


def mapped_rows_to_parquet_bytes(
    mapped_rows: list[tuple],
    target_cols: list[str],
    dest_types: dict[str, str] | None = None,
    *,
    dialect: str = "parquet",
) -> tuple[bytes, str]:
    """Typed Parquet body + MIME. Raises on coerce failure (never silent NULL)."""
    import io

    buf = io.BytesIO()
    mime = write_mapped_rows_parquet(
        mapped_rows, target_cols, dest_types, buf, dialect=dialect
    )
    return buf.getvalue(), mime
