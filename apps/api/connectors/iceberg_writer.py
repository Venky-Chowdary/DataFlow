"""Apache Iceberg destination writer.

Two paths:

1. Catalog mode (default when ``extra.catalog_type`` / connection string indicate
   REST / Glue / SQL / Nessie): uses ``pyiceberg`` to read/write real Iceberg
   tables. Supports append, overwrite, and MERGE/upsert via ``Table.upsert``.

2. Legacy filesystem CoW mode (bare local path, no catalog): keeps the original
   V2 metadata-file writer for backwards compatibility and environments without
   ``pyiceberg``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from services.value_serializer import json_default

from connectors.writer_common import (
    WriteResult,
    _rejected_row_count,
    apply_write_quarantine_matrix,
    build_mapped_rows_with_details,
    reject_on_strict_policy,
    resolve_conflict_targets,
    resolve_target_columns,
    transform_error_policy,
)

try:
    import pyarrow as pa
    import pyarrow.ipc as pa_ipc
except ImportError:  # pragma: no cover
    pa = None  # type: ignore[assignment]
    pa_ipc = None  # type: ignore[assignment]


def _pyiceberg_available() -> bool:
    try:
        import pyiceberg.catalog  # noqa: F401
        return True
    except Exception:
        return False


def _warehouse_root(host: str, database: str, connection_string: str) -> Path:
    raw = (connection_string or database or host or "").strip()
    if raw.startswith("file://"):
        raw = raw[len("file://") :]
    if not raw:
        raise ValueError("Iceberg warehouse path required (connection_string or database)")
    return Path(raw).expanduser().resolve()


def _logical_to_iceberg_type(logical: str) -> str:
    """Iceberg DDL from Map stamps / logicals — never invent float→double leaves.

    Bare / oversize DECIMAL stamps rematerialize through ``ddl_type`` SSOT so
    CREATE cannot leave bare ``DECIMAL`` (quarantine no-op) or pass through
    ``DECIMAL(40,10)`` that Arrow would silently clamp.
    """
    from services.type_system import (
        LOGICAL_DECIMAL,
        ddl_type,
        materialize_dest_ddl,
        normalize_logical_type,
    )

    raw = (logical or "string").strip()
    # Nested ARRAY/LIST/T[] stamps go through materialize so list<float> spelling
    # and float leaves stay authoritative (no dual ddl_type invent path).
    stamped = materialize_dest_ddl("iceberg", raw)
    # Normalize single-precision aliases to Iceberg's float token.
    bare = stamped.upper().split("(", 1)[0].strip()
    if bare in {"REAL", "FLOAT4", "FLOAT32", "HALF", "FLOAT16", "FLOAT"}:
        return "float"
    # Map≡CREATE decimal honesty: bare → decimal(38,10); oversize → string.
    if normalize_logical_type(raw) == LOGICAL_DECIMAL or normalize_logical_type(
        stamped
    ) == LOGICAL_DECIMAL:
        return ddl_type("iceberg", raw)
    return stamped


def _ensure_iceberg_decimal_carrier(type_str: str) -> str:
    """Map≡CREATE: decimal carriers match ``ddl_type('iceberg', …)`` SSOT.

    Bare ``DECIMAL`` → ``decimal(38,10)`` so shared fit quarantine can parse
    ``(p,s)``. Over Iceberg max precision → ``string`` (fail-closed) — never
    leave a bare or oversize stamp that Arrow would invent/clamp.
    """
    from services.type_system import LOGICAL_DECIMAL, ddl_type, normalize_logical_type

    raw = (type_str or "string").strip()
    if normalize_logical_type(raw) != LOGICAL_DECIMAL:
        return raw
    return ddl_type("iceberg", raw)


def _decimal_target_types_for_iceberg_write(
    target_cols: list[str],
    dest_types: dict[str, str],
    *,
    write_types: dict[str, str] | None = None,
    arrow_schema: Any | None = None,
    pa_mod: Any | None = None,
) -> list[str]:
    """Prefer committed Arrow/Iceberg physical types; else mapped carriers.

    Used by the shared quarantine matrix (decimal / int / fixed(L) / temporal).
    Iceberg ``string`` / ``binary`` are unbounded — string/binary quarantine
    no-ops unless ``fixed(L)`` / DECIMAL(p,s) / int32 are present (spec honesty).
    """
    out: list[str] = []
    for col in target_cols:
        if (
            arrow_schema is not None
            and pa_mod is not None
            and col in getattr(arrow_schema, "names", [])
        ):
            field = arrow_schema.field(col)
            ftype = field.type
            if pa_mod.types.is_decimal(ftype):
                out.append(f"DECIMAL({ftype.precision},{ftype.scale})")
                continue
            if pa_mod.types.is_fixed_size_binary(ftype):
                out.append(f"BINARY({int(ftype.byte_width)})")
                continue
            if pa_mod.types.is_int32(ftype):
                out.append("INT")
                continue
            if pa_mod.types.is_int16(ftype):
                out.append("SMALLINT")
                continue
            if pa_mod.types.is_int64(ftype):
                out.append("BIGINT")
                continue
            if pa_mod.types.is_boolean(ftype):
                out.append("BOOLEAN")
                continue
            if pa_mod.types.is_date(ftype):
                out.append("DATE")
                continue
            if pa_mod.types.is_timestamp(ftype):
                out.append("TIMESTAMPTZ" if getattr(ftype, "tz", None) else "TIMESTAMP_NTZ")
                continue
            if pa_mod.types.is_time(ftype):
                out.append("TIME")
                continue
            if pa_mod.types.is_floating(ftype):
                # Map VARCHAR + physical float — empty must quarantine before Arrow.
                if pa_mod.types.is_float64(ftype):
                    out.append("DOUBLE")
                else:
                    out.append("FLOAT")
                continue
            if pa_mod.types.is_binary(ftype) or pa_mod.types.is_large_binary(ftype):
                out.append("BINARY")
                continue
            if pa_mod.types.is_string(ftype) or pa_mod.types.is_large_string(ftype):
                out.append("STRING")
                continue
        raw = ""
        if write_types and col in write_types:
            raw = str(write_types.get(col) or "")
        if not raw:
            raw = str(dest_types.get(col) or "string")
        # Preserve fixed(L) / BINARY(n) from mapped create-new carriers.
        from services.type_system import (
            LOGICAL_BINARY,
            ddl_type,
            normalize_logical_type,
            parse_binary_carrier_width,
        )

        if normalize_logical_type(raw) == LOGICAL_BINARY:
            width = parse_binary_carrier_width(raw)
            if width is not None:
                # ddl_type legalizes VARBINARY(n) → fixed(n); materialize may
                # pass through illegal Iceberg tokens.
                out.append(ddl_type("iceberg", raw) or f"fixed({width})")
                continue
            out.append("BINARY")
            continue
        out.append(_ensure_iceberg_decimal_carrier(raw))
    return out


def _apply_iceberg_write_quarantine(
    mapped_rows: list[tuple],
    target_cols: list[str],
    target_types: list[str],
    rejected_details: list[dict[str, Any]],
    policy: str,
) -> list[tuple]:
    """Delegate to SSOT matrix — Iceberg string/binary unbounded no-op on width."""
    return apply_write_quarantine_matrix(
        mapped_rows,
        target_cols,
        target_types,
        rejected_details,
        policy,
        dialect_label="Iceberg",
    )


def _iceberg_type_to_logical_carrier(iceberg_type: Any) -> str:
    """Map committed Iceberg field type back to a logical carrier for Parquet writes."""
    if isinstance(iceberg_type, dict):
        kind = str(iceberg_type.get("type") or "").lower()
        if kind == "decimal":
            p = int(iceberg_type.get("precision") or 38)
            s = int(iceberg_type.get("scale") or 0)
            return f"DECIMAL({p},{s})"
        if kind == "fixed":
            length = iceberg_type.get("length") or iceberg_type.get("len")
            try:
                n = int(length)
                if n > 0:
                    return f"BINARY({n})"
            except (TypeError, ValueError):
                pass
            return "BINARY"
        if kind in {"list", "map", "struct"}:
            return "JSON"
        return kind or "string"
    t = str(iceberg_type or "string").lower()
    m_fixed = re.match(r"fixed\s*\[\s*(\d+)\s*\]", t) or re.match(
        r"fixed\s*\(\s*(\d+)\s*\)", t
    )
    if m_fixed:
        return f"BINARY({int(m_fixed.group(1))})"
    mapping = {
        "string": "string",
        "long": "BIGINT",
        "int": "INT",
        "double": "float",
        "float": "float",
        "boolean": "boolean",
        "date": "date",
        "timestamptz": "timestamptz",
        "timestamp": "timestamp_ntz",
        "binary": "binary",
        "uuid": "uuid",
        "time": "time",
    }
    return mapping.get(t, t or "string")


def _write_types_from_schema(
    schema_json: dict[str, Any],
    dest_types: dict[str, str],
) -> dict[str, str]:
    """Physical write types must match committed metadata (type_locked honesty)."""
    out = dict(dest_types)
    for field in schema_json.get("fields") or []:
        name = str(field.get("name") or "")
        if not name:
            continue
        out[name] = _iceberg_type_to_logical_carrier(field.get("type"))
    return out


def _load_metadata(meta_path: Path) -> dict[str, Any] | None:
    if not meta_path.exists():
        return None
    return json.loads(meta_path.read_text(encoding="utf-8"))


def _evolve_schema(
    existing: dict[str, Any] | None,
    columns: list[str],
    column_types: dict[str, str],
) -> tuple[dict[str, Any], list[str]]:
    """Return (schema_json, notes). Additive-only evolution; type_locked conflicts noted."""
    notes: list[str] = []
    if existing is None:
        fields = []
        for i, name in enumerate(columns, start=1):
            fields.append({
                "id": i,
                "name": name,
                "required": False,
                "type": _logical_to_iceberg_type(column_types.get(name, "string")),
            })
        return {
            "type": "struct",
            "schema-id": 0,
            "fields": fields,
        }, notes

    fields = list(existing.get("fields") or [])
    by_name = {f["name"]: f for f in fields}
    next_id = max((int(f.get("id", 0)) for f in fields), default=0) + 1
    for name in columns:
        if name in by_name:
            want = _logical_to_iceberg_type(column_types.get(name, "string"))
            have = by_name[name].get("type")
            if have != want:
                notes.append(f"type_locked: keep {name}:{have} (incoming {want})")
            continue
        fields.append({
            "id": next_id,
            "name": name,
            "required": False,
            "type": _logical_to_iceberg_type(column_types.get(name, "string")),
        })
        notes.append(f"schema_evolve: added column {name}")
        next_id += 1
    schema_id = int(existing.get("schema-id", 0)) + (1 if notes else 0)
    return {"type": "struct", "schema-id": schema_id, "fields": fields}, notes


def _load_existing_rows(table_dir: Path, columns: list[str], current_meta: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Load all rows referenced by current metadata data-files (JSONL/Parquet).

    Fail-closed: missing or unreadable referenced files abort the upsert so we
    never silently drop existing rows (Airbyte/warehouse silent-loss class).
    """
    if not current_meta:
        return []
    rows: list[dict[str, Any]] = []
    for ref in current_meta.get("data-files") or []:
        rel = str(ref.get("path") or "").strip()
        if not rel:
            raise ValueError("Iceberg metadata references a data-file with empty path")
        path = table_dir / rel
        if not path.exists():
            raise ValueError(
                f"Iceberg data-file missing for upsert merge: {rel} "
                "(refuse silent row loss — repair snapshot or rewrite table)"
            )
        if rel.endswith(".parquet"):
            try:
                import pyarrow.parquet as pq

                table = pq.read_table(path)
                for batch in table.to_pylist():
                    rows.append({c: batch.get(c) for c in columns})
            except Exception as exc:
                raise ValueError(
                    f"Iceberg Parquet data-file unreadable for upsert merge: {rel}: {exc}"
                ) from exc
        else:
            try:
                with path.open(encoding="utf-8") as fh:
                    for line_no, line in enumerate(fh, start=1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                        except Exception as exc:
                            raise ValueError(
                                f"Iceberg JSONL data-file corrupt at {rel}:{line_no}: {exc}"
                            ) from exc
                        rows.append({c: obj.get(c) for c in columns})
            except ValueError:
                raise
            except Exception as exc:
                raise ValueError(
                    f"Iceberg JSONL data-file unreadable for upsert merge: {rel}: {exc}"
                ) from exc
    return rows


def _merge_upsert_rows(
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
    *,
    pk_cols: list[str],
    lsn_col: str = "_df_lsn",
) -> list[dict[str, Any]]:
    """PK upsert with LSN guard: keep row with strictly newer LSN; equal → keep existing.

    No LSN on either side → last wins (batch overwrite). Sparse CDC: ``DF_MISSING``
    keys are omitted and never wipe prior column values.
    """
    from connectors.writer_common import compare_lsn
    from services.value_serializer import is_missing_sentinel

    def _key(row: dict[str, Any]) -> tuple:
        return tuple(str(row.get(c, "")) for c in pk_cols)

    def _present(row: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in row.items() if not is_missing_sentinel(v)}

    best: dict[tuple, dict[str, Any]] = {}
    for row in existing:
        best[_key(row)] = dict(row)
    for row in incoming:
        clean = _present(row)
        # Sparse/empty PK must quarantine upstream — refuse invent duplicates here.
        from connectors.writer_common import assert_sparse_upsert_has_pk

        try:
            if any(is_missing_sentinel(v) for v in row.values()):
                assert_sparse_upsert_has_pk(clean, pk_cols)
        except ValueError:
            raise
        key = _key(row)
        prev = best.get(key)
        if prev is None:
            # Sparse insert of an unknown PK would invent NULL for absent columns
            # — refuse, matching the pyiceberg catalog path.
            if any(is_missing_sentinel(v) for v in row.values()):
                raise ValueError(
                    "Iceberg sparse CDC insert of unknown primary key "
                    f"{key!r} refused — would invent NULL for absent fields. "
                    "Require a full row image (no DF_MISSING) or an existing "
                    "destination row to overlay."
                )
            best[key] = clean
            continue
        if lsn_col in clean or lsn_col in prev:
            if compare_lsn(clean.get(lsn_col), prev.get(lsn_col)) > 0:
                merged = dict(prev)
                merged.update(clean)
                best[key] = merged
        else:
            merged = dict(prev)
            merged.update(clean)
            best[key] = merged
    return list(best.values())

def _row_as_dict(columns: list[str], row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return {c: row.get(c) for c in columns}
    return {c: row[i] if i < len(row) else None for i, c in enumerate(columns)}


def _row_tuple(columns: list[str], row: Any) -> tuple:
    """Row as a positional tuple aligned to ``columns`` (dict or sequence)."""
    if isinstance(row, dict):
        return tuple(row.get(c) for c in columns)
    return tuple(row[i] if i < len(row) else None for i in range(len(columns)))


def _logical_to_arrow_type(logical: str, pa: Any) -> Any:
    """Map Datawrap logical / Iceberg DDL carrier → pyarrow type (fail-closed decimals)."""
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
        # Map≡CREATE: honor ddl_type SSOT — bare → (38,10); oversize → string.
        # Never silently clamp DECIMAL(40,10) → decimal128(38,10).
        from services.type_system import ddl_type

        wire = ddl_type("iceberg", raw)
        if normalize_logical_type(wire) != LOGICAL_DECIMAL:
            return pa.large_string()
        precision, scale = parse_numeric_precision_scale(wire)
        if precision is None:
            # SSOT should always parameterize Iceberg decimals; refuse invent.
            return pa.large_string()
        p = int(precision)
        s = int(scale) if scale is not None else 0
        if p < 1 or p > 38 or s < 0 or s > p:
            return pa.large_string()
        return pa.decimal128(p, s)
    if logical_n == LOGICAL_DATE:
        return pa.date32()
    if logical_n == LOGICAL_DATETIME:
        # Prefer timezone-aware when source declared TIMESTAMPTZ.
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
            # Iceberg fixed(L) — exact byte width (spec); never silent truncate.
            return pa.binary(int(width))
        return pa.large_binary()
    # PyIceberg maps Iceberg string -> large_string; use it consistently.
    return pa.large_string()


def _coerce_arrow_cell(value: Any, arrow_type: Any, pa: Any) -> Any:
    """Coerce a Python cell into the declared Arrow type; raise on hard failure."""
    from datetime import date, datetime, time
    from decimal import Decimal, InvalidOperation

    from services.value_serializer import is_missing_sentinel

    if is_missing_sentinel(value):
        raise ValueError(
            "DF_MISSING reached Arrow coerce — sparse CDC must overlay onto "
            "existing rows before building the Arrow batch"
        )
    if value is None:
        return None
    if value == "":
        # Keep empty string for string carriers — never invent NULL from "".
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
                f"cannot cast non-finite {value!r} to Iceberg float — refuse invent"
            )
        return out
    if pa.types.is_integer(arrow_type):
        from decimal import Decimal

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
                    "Iceberg TIMESTAMPTZ refused naive datetime — provide "
                    "offset/Z (refuse silent UTC invent)"
                )
            if not tz and value.tzinfo is not None:
                # NTZ physical type: keep civil digits (Validate owns TZ→NTZ).
                return value.replace(tzinfo=None)
            return value
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if tz and parsed.tzinfo is None:
            raise ValueError(
                "Iceberg TIMESTAMPTZ refused naive datetime — provide "
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

        # Same SSOT as SQL BYTEA/BLOB — refuse silent UTF-8 invent on invalid wire.
        return coerce_binary_wire(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=json_default)
    return str(value)


def _write_data_file(
    data_dir: Path,
    columns: list[str],
    rows: list[Any],
    *,
    column_types: dict[str, str] | None = None,
) -> tuple[str, int, str, list[str]]:
    """Write one data file; returns (relative_path, record_count, checksum, warnings).

    When pyarrow is available, builds an explicit schema from logical types so
    DECIMAL/TIMESTAMPTZ do not collapse to float64/string via inference.
    JSONL fallback is surfaced as an explicit degraded-mode warning (never silent).
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    file_id = uuid.uuid4().hex
    digest = hashlib.sha256()
    dict_rows = [_row_as_dict(columns, r) for r in rows]
    types = column_types or {}
    warnings: list[str] = []

    # Prefer Parquet when pyarrow is available. JSONL is only for missing pyarrow —
    # typed conversion failures must not silently downgrade the physical format.
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        warnings.append(f"parquet_unavailable_jsonl_fallback: {exc}")
    else:
        try:
            arrow_types = [_logical_to_arrow_type(types.get(c, "string"), pa) for c in columns]
            schema = pa.schema([(c, t) for c, t in zip(columns, arrow_types)])
            arrays = []
            for c, at in zip(columns, arrow_types):
                cells = [_coerce_arrow_cell(r.get(c), at, pa) for r in dict_rows]
                arrays.append(pa.array(cells, type=at))
            table = pa.Table.from_arrays(arrays, schema=schema)
            rel = f"data/{file_id}.parquet"
            path = data_dir.parent / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, path)
            digest.update(path.read_bytes())
            return rel, len(dict_rows), digest.hexdigest()[:16], warnings
        except Exception as exc:
            raise ValueError(
                f"Iceberg Parquet type conversion failed; refusing JSONL type downgrade: {exc}"
            ) from exc

    from services.value_serializer import is_missing_sentinel

    for row in dict_rows:
        for k, v in row.items():
            if is_missing_sentinel(v):
                raise ValueError(
                    f"Iceberg JSONL write refused residual DF_MISSING on column {k!r} "
                    "— would serialize the sentinel literally into the data file"
                )

    rel = f"data/{file_id}.jsonl"
    path = data_dir.parent / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in dict_rows:
            line = json.dumps(row, default=json_default)
            fh.write(line + "\n")
            digest.update(line.encode())
    return rel, len(dict_rows), digest.hexdigest()[:16], warnings


def _checksum_arrow_table(pa_table: Any) -> str:
    """Stable deterministic checksum of an Arrow table for reconciliation."""
    out = pa.BufferOutputStream()
    with pa.ipc.new_file(out, pa_table.schema) as writer:
        writer.write_table(pa_table)
    return hashlib.sha256(out.getvalue().to_pybytes()).hexdigest()[:32]


def _pyiceberg_should_use(endpoint: dict[str, Any]) -> bool:
    """Compatibility shim — prefer :func:`resolve_iceberg_write_path`."""
    return resolve_iceberg_write_path(endpoint) == "catalog"


# Iceberg predicate pushdown gets unwieldy past a few hundred terms; scan in
# slices so a 20k-row CDC batch does not build one giant boolean expression.
_PK_SCAN_SLICE = 200


def _pk_predicate_variants(value: Any) -> list[Any]:
    """Expand a PK value into the type variants Iceberg may store.

    CDC / SQL_REDO paths often deliver string keys (``"42"``) while the table
    column is typed as ``long``/``int``. A strict ``In``/``EqualTo`` then
    returns zero rows, the overlay treats the destination as empty, and a
    sparse CDC update invents NULLs (or the LSN guard sees nothing to compare).
    Including every lossless coercion keeps the pushdown honest without a full
    table scan on every batch.
    """
    variants: list[Any] = [value]
    if value is None:
        return variants
    as_str = str(value)
    if as_str not in variants:
        variants.append(as_str)
    if isinstance(value, bool):
        return variants
    if isinstance(value, int):
        return variants
    if isinstance(value, float) and value.is_integer():
        variants.append(int(value))
        return variants
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
            try:
                variants.append(int(text))
            except Exception:
                pass
        else:
            try:
                as_float = float(text)
            except Exception:
                as_float = None
            if as_float is not None and as_float.is_integer():
                variants.append(int(as_float))
    return variants


def _pk_row_filter(pk_cols: list[str], key_tuples: list[tuple]) -> Any:
    """Build a pyiceberg predicate matching exactly these primary keys.

    Single-column keys use ``In``; composite keys use ``Or`` of ``And``
    equality terms. Returns ``None`` when no predicate can be built so the
    caller can fall back to a full scan rather than reading nothing.
    """
    from pyiceberg.expressions import And, EqualTo, In, Or

    if not pk_cols or not key_tuples:
        return None
    if len(pk_cols) == 1:
        values: list[Any] = []
        seen: set[tuple[str, Any]] = set()
        for tup in key_tuples:
            for variant in _pk_predicate_variants(tup[0]):
                marker = (type(variant).__name__, variant)
                if marker in seen:
                    continue
                seen.add(marker)
                values.append(variant)
        return In(pk_cols[0], values)
    terms = []
    for tup in key_tuples:
        # Cross-product of per-column type variants so a string/int mismatch on
        # any part of a composite key still finds the destination row.
        col_variants = [_pk_predicate_variants(tup[i]) for i in range(len(pk_cols))]
        from itertools import product

        for combo in product(*col_variants):
            eqs = [EqualTo(col, combo[i]) for i, col in enumerate(pk_cols)]
            term = eqs[0]
            for eq in eqs[1:]:
                term = And(term, eq)
            terms.append(term)
    combined = terms[0]
    for term in terms[1:]:
        combined = Or(combined, term)
    return combined


def _scan_existing_by_pk(
    tbl: Any, pk_cols: list[str], key_tuples: list[tuple]
) -> dict[tuple, dict[str, Any]]:
    """Read only the destination rows this batch touches, keyed by PK.

    A full ``tbl.scan()`` materialised the entire table into Python dicts for
    every CDC batch (``_df_lsn`` is present on all of them), which does not
    survive a real lakehouse table. Push the batch's key set down as a row
    filter and fall back to a full scan only if the predicate cannot be built
    or returns nothing for a non-empty key set (typical type-mismatch case).
    """
    existing: dict[tuple, dict[str, Any]] = {}

    def _absorb(arrow_table: Any) -> None:
        names = arrow_table.column_names
        columns = {name: arrow_table.column(name).to_pylist() for name in names}
        for idx in range(arrow_table.num_rows):
            row = {name: columns[name][idx] for name in names}
            existing[tuple(str(row.get(c, "")) for c in pk_cols)] = row

    unique_keys = list(dict.fromkeys(key_tuples))
    wanted = {
        tuple("" if v is None else str(v) for v in tup) for tup in unique_keys
    }
    try:
        for start in range(0, len(unique_keys), _PK_SCAN_SLICE):
            chunk = unique_keys[start : start + _PK_SCAN_SLICE]
            row_filter = _pk_row_filter(pk_cols, chunk)
            if row_filter is None:
                raise ValueError("empty predicate")
            _absorb(tbl.scan(row_filter=row_filter).to_arrow())
        found = set(existing.keys())
        if not unique_keys or wanted <= found:
            return existing
        # A clean but incomplete match is the normal "some of these PKs are
        # new" case for sparse CDC — do NOT full-scan the lakehouse for that.
        # A completely empty match with a non-empty key set is almost always a
        # pushdown type mismatch; fall through to the safety net below.
        if found:
            return existing
    except Exception:
        # Unsupported predicate (e.g. non-identity partition transform or an
        # older pyiceberg) — correctness first, read the whole table.
        pass
    existing.clear()
    _absorb(tbl.scan().to_arrow())
    return {k: v for k, v in existing.items() if k in wanted}


def resolve_iceberg_write_path(endpoint: dict[str, Any]) -> str:
    """Decide catalog vs filesystem write path — fail closed on catalog intent.

    A REST/Glue/SQL catalog endpoint must never silently degrade to a local
    Parquet tree when ``pyiceberg`` is missing or the catalog config is
    malformed. That path previously reported ``ok=True`` while writing into a
    directory named after the REST URL — invisible to Spark/Trino/Athena.
    """
    from connectors.driver_guard import platform_driver_unavailable
    from connectors.iceberg_catalog import parse_iceberg_catalog_config

    try:
        cfg = parse_iceberg_catalog_config(endpoint)
    except Exception as exc:
        raise RuntimeError(
            f"Iceberg catalog configuration is invalid; refusing filesystem "
            f"fallback that would invent a local warehouse: {exc}"
        ) from exc

    catalog_type = str(cfg.get("catalog_type") or "filesystem").lower()
    if catalog_type == "filesystem":
        return "filesystem"
    if not _pyiceberg_available():
        raise RuntimeError(platform_driver_unavailable("Apache Iceberg"))
    return "catalog"

def _write_mapped_rows_pyiceberg(
    endpoint: dict[str, Any],
    *,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[..., None] | None,
    create_table: bool,
    error_policy: str | None,
    write_mode: str,
    conflict_columns: list[str] | None,
    sync_mode: str = "",
    file_batch_idx: int = 0,
    destination_column_nullability: dict[str, bool] | None = None,
) -> WriteResult:
    """Write a batch through a real pyiceberg catalog with MERGE/upsert support."""
    from services.sync_cursor import is_overwrite_sync
    if pa is None:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=endpoint.get("table", ""),
            target_schema="",
            checksum="",
            chunks_completed=0,
            error="pyarrow is required for Iceberg catalog writes",
            driver="iceberg",
        )

    try:
        from connectors.iceberg_catalog import (
            ensure_namespace,
            load_catalog,
            parse_iceberg_catalog_config,
        )
        from pyiceberg.exceptions import NoSuchTableError
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=endpoint.get("table", ""),
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=f"Iceberg catalog support unavailable: {exc}",
            driver="iceberg",
        )

    config = parse_iceberg_catalog_config(endpoint)
    table = config["table_name"]
    namespace = config["namespace"]
    target_schema = ".".join(namespace + (table,))

    target_cols, target_types = resolve_target_columns(mappings, column_types, preserve_case=True)
    if conflict_columns:
        try:
            conflict_columns = resolve_conflict_targets(
                conflict_columns, target_cols, strict=True
            )
        except ValueError as exc:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=endpoint.get("table", ""),
                target_schema="",
                checksum="",
                chunks_completed=0,
                error=str(exc),
                driver="iceberg",
            )
    dest_types = {
        target_cols[i]: (
            mappings[i].get("target_type")
            or column_types.get(mappings[i]["source"])
            or (target_types[i] if i < len(target_types) else "string")
        )
        for i in range(len(target_cols))
    }
    policy = transform_error_policy(error_policy)
    mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        error_policy=policy,
        dest_types=dest_types,
        preserve_case=True,
        dest_kind="iceberg",
        destination_pk_columns=list(conflict_columns or []) or None,
        destination_column_nullability=destination_column_nullability,
    )
    _map_abort = reject_on_strict_policy(policy, rejected_details, 'Iceberg', transform_errors)
    if _map_abort:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=target_schema,
            checksum="",
            chunks_completed=0,
            error=_map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_details=rejected_details,
            driver="iceberg",
        )

    if not mapped_rows:
        _empty_abort = reject_on_strict_policy(policy, rejected_details, "Iceberg")
        if _empty_abort:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table,
                target_schema=target_schema,
                checksum="",
                chunks_completed=0,
                error=_empty_abort,
                rejected_details=rejected_details,
                rejected_rows=_rejected_row_count(
                    data_rows, mapped_rows, rejected_details, policy
                ),
                driver="iceberg",
            )
        return WriteResult(
            ok=True,
            rows_written=0,
            table_name=table,
            target_schema=target_schema,
            checksum="",
            chunks_completed=1,
            rejected_details=rejected_details,
            rejected_rows=_rejected_row_count(
                data_rows, mapped_rows, rejected_details, policy
            ),
            driver="iceberg",
        )

    try:
        catalog = load_catalog(endpoint)
        identifier = namespace + (table,)
        tbl = catalog.load_table(identifier)
    except NoSuchTableError:
        if not create_table:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table,
                target_schema=target_schema,
                checksum="",
                chunks_completed=0,
                error=(
                    f"Iceberg table {target_schema} does not exist and "
                    "create_table is disabled"
                ),
                driver="iceberg",
            )
        ensure_namespace(catalog, namespace)
        arrow_types = [_logical_to_arrow_type(dest_types.get(c, "string"), pa) for c in target_cols]
        arrow_schema = pa.schema([(c, t) for c, t in zip(target_cols, arrow_types)])
        tbl = catalog.create_table(identifier, schema=arrow_schema)
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=target_schema,
            checksum="",
            chunks_completed=0,
            error=f"Unable to load or create Iceberg table: {exc}",
            driver="iceberg",
        )

    mode = (write_mode or "append").lower()
    upsert_modes = {"upsert", "merge", "cdc", "incremental_deduped"}
    # Multi-chunk full-refresh overwrite: only the first chunk may replace the
    # destination; later chunks append to the same snapshot. This mirrors the
    # Redis prefix-clear-once contract and avoids losing all but the final chunk.
    if is_overwrite_sync(sync_mode) and mode not in upsert_modes:
        if file_batch_idx in (0, 1):
            if mode == "insert":
                mode = "overwrite"
        else:
            mode = "append"
    elif mode in {"overwrite", "replace"} and file_batch_idx > 1:
        mode = "append"

    try:
        existing_arrow = tbl.schema().as_arrow()
        arrow_types = [_logical_to_arrow_type(dest_types.get(c, "string"), pa) for c in target_cols]
        type_locked_warnings: list[str] = []
        new_fields: list[tuple[str, Any]] = []
        for c, at in zip(target_cols, arrow_types):
            if c not in existing_arrow.names:
                new_fields.append((c, at))
            else:
                existing_type = existing_arrow.field(c).type
                if not existing_type.equals(at):
                    type_locked_warnings.append(
                        f"type_locked: keep {c}:{existing_type} (incoming {at})"
                    )

        if new_fields:
            new_schema = pa.schema(new_fields)
            with tbl.update_schema() as update:
                update.union_by_name(new_schema)
            # Refresh the table so the final schema includes the new columns.
            tbl = catalog.load_table(identifier)
            existing_arrow = tbl.schema().as_arrow()

        final_arrow = existing_arrow
        # Fail-closed quarantine before pa.array (one bad row must not abort the batch).
        quarantine_types = _decimal_target_types_for_iceberg_write(
            target_cols,
            dest_types,
            arrow_schema=final_arrow,
            pa_mod=pa,
        )
        mapped_rows = _apply_iceberg_write_quarantine(
            mapped_rows,
            target_cols,
            quarantine_types,
            rejected_details,
            policy,
        )
        _post_q_abort = reject_on_strict_policy(policy, rejected_details, "Iceberg")
        if _post_q_abort:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table,
                target_schema=target_schema,
                checksum="",
                chunks_completed=0,
                error=_post_q_abort,
                rejected_details=rejected_details,
                driver="iceberg",
            )
        if not mapped_rows:
            _empty_abort = reject_on_strict_policy(policy, rejected_details, "Iceberg")
            if _empty_abort:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table,
                    target_schema=target_schema,
                    checksum="",
                    chunks_completed=0,
                    error=_empty_abort,
                    rejected_details=rejected_details,
                    warnings=type_locked_warnings[:20],
                    driver="iceberg",
                )
            return WriteResult(
                ok=True,
                rows_written=0,
                table_name=table,
                target_schema=target_schema,
                checksum="",
                chunks_completed=1,
                rejected_details=rejected_details,
                rejected_rows=_rejected_row_count(
                    data_rows, mapped_rows, rejected_details, policy
                ),
                warnings=type_locked_warnings[:20],
                driver="iceberg",
            )
        # Sparse CDC + LSN: pyiceberg Table.upsert treats omitted/null as
        # NULL-wipe and has no LSN guard. Fold every upsert batch through a
        # running per-PK map (dense replace / sparse overlay) with should_apply.
        from connectors.writer_common import (
            DF_LSN_COL,
            assert_sparse_upsert_has_pk,
            partition_dense_upsert_rows,
            row_has_missing_sentinel,
            sparse_present_bindings,
        )
        from services.cdc_effectively_once import should_apply_pk_row
        from services.value_serializer import cell_to_string, is_missing_sentinel

        if mode in upsert_modes:
            pk_cols = [c for c in (conflict_columns or []) if c in target_cols]
            if not pk_cols:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table,
                    target_schema=target_schema,
                    checksum="",
                    chunks_completed=0,
                    error="Iceberg upsert requires conflict_columns that match mapped targets",
                    driver="iceberg",
                    rejected_details=rejected_details,
                )
            # Shared dense empty-PK quarantine (SQL_NULL / blank / DF_MISSING keys).
            before_pk = len(mapped_rows)
            mapped_rows = partition_dense_upsert_rows(
                mapped_rows,
                pk_cols,
                target_cols=target_cols,
                rejected_details=rejected_details,
                policy=policy,
            )
            if len(mapped_rows) < before_pk:
                _pk_abort = reject_on_strict_policy(policy, rejected_details, "Iceberg")
                if _pk_abort:
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=table,
                        target_schema=target_schema,
                        checksum="",
                        chunks_completed=0,
                        error=_pk_abort,
                        rejected_details=rejected_details,
                        driver="iceberg",
                    )
                if not mapped_rows:
                    return WriteResult(
                        ok=True,
                        rows_written=0,
                        table_name=table,
                        target_schema=target_schema,
                        checksum="",
                        chunks_completed=1,
                        rejected_details=rejected_details,
                        rejected_rows=_rejected_row_count(
                            data_rows, mapped_rows, rejected_details, policy
                        ),
                        driver="iceberg",
                    )
            # A scan is only needed when the batch carries sparse fields to
            # overlay or an LSN to compare against the destination.
            needs_scan = DF_LSN_COL in target_cols or any(
                row_has_missing_sentinel(_row_tuple(target_cols, r))
                for r in mapped_rows
            )
            existing_by_pk: dict[tuple, dict[str, Any]] = {}
            if needs_scan:
                batch_key_tuples = [
                    tuple(_row_as_dict(target_cols, r).get(c) for c in pk_cols)
                    for r in mapped_rows
                ]
                try:
                    existing_by_pk = _scan_existing_by_pk(
                        tbl, pk_cols, batch_key_tuples
                    )
                except Exception as exc:
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=table,
                        target_schema=target_schema,
                        checksum="",
                        chunks_completed=0,
                        error=(
                            "Iceberg upsert requires a table scan for sparse/LSN "
                            f"guards; scan failed: {exc}"
                        ),
                        driver="iceberg",
                    )
            # Fold in arrival order so same-PK twice in one batch is correct.
            fold_kept: list[tuple] = []
            for row_idx, raw in enumerate(mapped_rows):
                row_dict = _row_as_dict(target_cols, raw)
                row_values = _row_tuple(target_cols, raw)
                if row_has_missing_sentinel(row_values):
                    present = sparse_present_bindings(row_values, target_cols)
                    try:
                        assert_sparse_upsert_has_pk(present, pk_cols)
                    except ValueError as exc:
                        sample = ""
                        try:
                            sample = cell_to_string(
                                next(iter(present.values()), "")
                            )[:120]
                        except Exception:
                            sample = ""
                        rejected_details.append(
                            {
                                "row": row_idx + 1,
                                "column": "*",
                                "value": sample,
                                "reason": str(exc)[:300],
                                "policy": policy,
                            }
                        )
                        continue
                    key = tuple(str(present.get(c, "")) for c in pk_cols)
                    base = existing_by_pk.get(key)
                    if base is None:
                        rejected_details.append(
                            {
                                "row": row_idx + 1,
                                "column": "*",
                                "value": str(key)[:120],
                                "reason": (
                                    "Iceberg sparse CDC insert of unknown primary key "
                                    f"{key!r} refused — would invent NULL for absent "
                                    "fields"
                                ),
                                "policy": policy,
                            }
                        )
                        continue
                    if DF_LSN_COL in present and not should_apply_pk_row(
                        existing_lsn=base.get(DF_LSN_COL),
                        incoming_lsn=present[DF_LSN_COL],
                    ).applied:
                        continue
                    merged = dict(base)
                    merged.update(present)
                    existing_by_pk[key] = merged
                    fold_kept.append(raw)
                else:
                    key = tuple(str(row_dict.get(c, "")) for c in pk_cols)
                    base = existing_by_pk.get(key)
                    if (
                        base is not None
                        and DF_LSN_COL in row_dict
                        and not should_apply_pk_row(
                            existing_lsn=base.get(DF_LSN_COL),
                            incoming_lsn=row_dict.get(DF_LSN_COL),
                        ).applied
                    ):
                        continue
                    existing_by_pk[key] = {
                        **(base or {}),
                        **{k: v for k, v in row_dict.items() if not is_missing_sentinel(v)},
                    }
                    fold_kept.append(raw)
            _fold_abort = reject_on_strict_policy(policy, rejected_details, "Iceberg")
            if _fold_abort:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table,
                    target_schema=target_schema,
                    checksum="",
                    chunks_completed=0,
                    error=_fold_abort,
                    rejected_details=rejected_details,
                    driver="iceberg",
                )
            # Emit one dense row per PK touched by this batch (plus untouched
            # existing rows are left alone via upsert join).
            batch_keys = {
                tuple(str(_row_as_dict(target_cols, r).get(c, "")) for c in pk_cols)
                for r in fold_kept
            }
            mapped_rows = [
                tuple(existing_by_pk[k].get(c) for c in target_cols)
                for k in batch_keys
                if k in existing_by_pk
            ]

        # Append/overwrite are dense Arrow writes — STOP_COLUMN DF_MISSING must
        # materialize to NULL (same as pre-omit semantics), never raise or leak
        # the sentinel string into Parquet. Upsert path overlays sparse first.
        if mode not in upsert_modes:
            from connectors.writer_common import materialize_missing_as_null_for_dense_write

            mapped_rows = materialize_missing_as_null_for_dense_write(mapped_rows)

        dict_rows = [_row_as_dict(target_cols, r) for r in mapped_rows]
        kept_dicts: list[dict[str, Any]] = []
        for row_idx, d in enumerate(dict_rows):
            try:
                for k, v in d.items():
                    if is_missing_sentinel(v):
                        raise ValueError(
                            f"Iceberg write refused residual DF_MISSING on column {k!r} "
                            "— would invent NULL. Sparse overlay must expand first."
                        )
                for field in final_arrow:
                    _coerce_arrow_cell(d.get(field.name), field.type, pa)
                kept_dicts.append(d)
            except ValueError as exc:
                rejected_details.append(
                    {
                        "row": row_idx + 1,
                        "column": "*",
                        "value": "",
                        "reason": str(exc)[:300],
                        "policy": policy,
                    }
                )
        _arrow_abort = reject_on_strict_policy(policy, rejected_details, "Iceberg")
        if _arrow_abort:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table,
                target_schema=target_schema,
                checksum="",
                chunks_completed=0,
                error=_arrow_abort,
                rejected_details=rejected_details,
                driver="iceberg",
            )
        dict_rows = kept_dicts
        if not dict_rows:
            return WriteResult(
                ok=True,
                rows_written=0,
                table_name=table,
                target_schema=target_schema,
                checksum="",
                chunks_completed=1,
                rejected_details=rejected_details,
                rejected_rows=_rejected_row_count(
                    data_rows, [], rejected_details, policy
                ),
                driver="iceberg",
            )
        arrays = []
        for field in final_arrow:
            at = field.type
            cells = [_coerce_arrow_cell(r.get(field.name), at, pa) for r in dict_rows]
            arrays.append(pa.array(cells, type=at))
        pa_table = pa.Table.from_arrays(arrays, schema=final_arrow)
        checksum = _checksum_arrow_table(pa_table)

        if mode in upsert_modes:
            pk_cols = [c for c in (conflict_columns or []) if c in target_cols]
            upsert_result = tbl.upsert(pa_table, join_cols=pk_cols)
            rows_written = upsert_result.rows_updated + upsert_result.rows_inserted
        elif mode in {"overwrite", "replace"}:
            tbl.overwrite(pa_table)
            rows_written = len(pa_table)
        else:
            tbl.append(pa_table)
            rows_written = len(pa_table)
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=target_schema,
            checksum="",
            chunks_completed=0,
            error=f"Iceberg {mode} failed: {exc}",
            driver="iceberg",
        )

    if on_checkpoint:
        on_checkpoint(rows_written, rows_written, 1)

    _final_abort = reject_on_strict_policy(policy, rejected_details, "Iceberg")
    if _final_abort:
        return WriteResult(
            ok=False,
            rows_written=rows_written,
            table_name=table,
            target_schema=target_schema,
            checksum=checksum,
            chunks_completed=1,
            error=_final_abort,
            rejected_details=rejected_details,
            rejected_rows=_rejected_row_count(
                data_rows, mapped_rows, rejected_details, policy
            ),
            warnings=type_locked_warnings[:20],
            driver="iceberg",
        )

    return WriteResult(
        ok=True,
        rows_written=rows_written,
        table_name=table,
        target_schema=target_schema,
        checksum=checksum,
        chunks_completed=1,
        rejected_details=rejected_details,
        rejected_rows=_rejected_row_count(
            data_rows, mapped_rows, rejected_details, policy
        ),
        warnings=type_locked_warnings[:20],
        driver="iceberg",
    )


def test_iceberg(
    *,
    host: str = "",
    port: int = 0,
    database: str = "",
    table: str = "",
    connection_string: str = "",
    api_key: str = "",
    username: str = "",
    password: str = "",
    ssl: bool = False,
    **_kwargs: Any,
) -> tuple[bool, str]:
    """Probe filesystem or real catalog reachability."""
    from connectors.iceberg_catalog import test_iceberg_catalog

    endpoint = {
        "host": host,
        "port": port,
        "database": database,
        "table": table,
        "connection_string": connection_string,
        "api_key": api_key,
        "username": username,
        "password": password,
        "ssl": ssl,
        **_kwargs,
    }
    return test_iceberg_catalog(endpoint)


def _write_mapped_rows_filesystem(
    *,
    host: str = "",
    port: int = 0,
    database: str = "",
    username: str = "",
    password: str = "",
    schema: str = "",
    connection_string: str = "",
    ssl: bool = False,
    table_name: str = "",
    headers: list[str] | None = None,
    data_rows: list[list[str]] | None = None,
    mappings: list[dict] | None = None,
    column_types: dict[str, str] | None = None,
    on_checkpoint: Callable[..., None] | None = None,
    create_table: bool = True,
    error_policy: str | None = None,
    write_mode: str = "append",
    conflict_columns: list[str] | None = None,
    **_kwargs: Any,
) -> WriteResult:
    """Legacy filesystem-only copy-on-write writer."""
    headers = headers or []
    data_rows = data_rows or []
    mappings = mappings or []
    column_types = column_types or {}
    table = (table_name or "events").strip()

    try:
        root = _warehouse_root(host, database, connection_string)
    except Exception as exc:
        return WriteResult(
            ok=False, rows_written=0, table_name=table, target_schema=schema or "",
            checksum="", chunks_completed=0, error=str(exc), driver="iceberg",
        )

    table_dir = root / (schema.strip() if schema else "") / table if schema else root / table
    # Normalize: namespace.table → nested dirs
    if "." in table and not schema:
        parts = table.split(".", 1)
        table_dir = root / parts[0] / parts[1]
        table = parts[1]
    # Deny-create must not invent an empty Iceberg tree (Airbyte-style false provision).
    if not table_dir.exists() and not create_table:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=str(table_dir),
            checksum="",
            chunks_completed=0,
            error="Iceberg table is missing and create_table is disabled",
            driver="iceberg",
        )
    meta_dir = table_dir / "metadata"
    versions = sorted(meta_dir.glob("v*.metadata.json")) if meta_dir.is_dir() else []
    if not create_table and not versions:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=str(table_dir),
            checksum="",
            chunks_completed=0,
            error="Iceberg table metadata is missing and create_table is disabled",
            driver="iceberg",
        )
    meta_dir.mkdir(parents=True, exist_ok=True)

    target_cols, target_types = resolve_target_columns(mappings, column_types, preserve_case=True)
    if conflict_columns:
        try:
            conflict_columns = resolve_conflict_targets(
                conflict_columns, target_cols, strict=True
            )
        except ValueError as exc:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table,
                target_schema=str(table_dir),
                checksum="",
                chunks_completed=0,
                error=str(exc),
                driver="iceberg",
            )
    dest_types = {
        target_cols[i]: (
            mappings[i].get("target_type")
            or column_types.get(mappings[i]["source"])
            or (target_types[i] if i < len(target_types) else "string")
        )
        for i in range(len(target_cols))
    }
    policy = transform_error_policy(error_policy)
    mapped_rows, transform_errors, rejected_details = build_mapped_rows_with_details(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        error_policy=policy,
        dest_types=dest_types,
        preserve_case=True,
        dest_kind="iceberg",
        destination_pk_columns=list(conflict_columns or []) or None,
        destination_column_nullability=_kwargs.get("destination_column_nullability"),
    )
    _map_abort = reject_on_strict_policy(policy, rejected_details, 'Iceberg', transform_errors)
    if _map_abort:
        return WriteResult(
            ok=False, rows_written=0, table_name=table, target_schema=str(table_dir),
            checksum="", chunks_completed=0,
            error=_map_abort or f"Transform errors: {'; '.join(transform_errors[:3])}",
            rejected_details=rejected_details, driver="iceberg",
        )

    # Find current metadata version
    versions = sorted(meta_dir.glob("v*.metadata.json"))
    current_meta = _load_metadata(versions[-1]) if versions else None
    current_schema = (current_meta or {}).get("schemas", [{}])[-1] if current_meta else None
    if current_meta and "schema" in current_meta and not current_schema:
        current_schema = current_meta.get("schema")

    schema_json, evolve_notes = _evolve_schema(current_schema, target_cols, dest_types)
    # Always write Parquet/JSONL using committed field types — never diverge from
    # type_locked metadata (incoming dest_types may differ).
    write_types = _write_types_from_schema(schema_json, dest_types)
    # Fail-closed quarantine against committed schema — never let one overflow
    # row abort the whole Parquet/Arrow batch.
    mapped_rows = _apply_iceberg_write_quarantine(
        mapped_rows,
        target_cols,
        _decimal_target_types_for_iceberg_write(
            target_cols, dest_types, write_types=write_types
        ),
        rejected_details,
        policy,
    )
    _post_q_abort = reject_on_strict_policy(policy, rejected_details, "Iceberg")
    if _post_q_abort:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=str(table_dir),
            checksum="",
            chunks_completed=0,
            error=_post_q_abort,
            rejected_details=rejected_details,
            driver="iceberg",
        )
    file_warnings: list[str] = []
    if write_mode in {"overwrite", "replace"} and current_meta:
        # Drop prior data refs; keep schema evolution
        current_meta = None

    mode = (write_mode or "append").lower()
    upsert_modes = {"upsert", "merge", "cdc", "incremental_deduped"}
    if mode in upsert_modes:
        pk_cols = [c for c in (conflict_columns or []) if c in target_cols]
        if not pk_cols:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table,
                target_schema=str(table_dir),
                checksum="",
                chunks_completed=0,
                error=(
                    "Iceberg upsert/merge requires explicit conflict_columns "
                    "(record key); refusing to invent PK from the first column"
                ),
                rejected_details=rejected_details,
                driver="iceberg",
            )
        existing_rows = _load_existing_rows(table_dir, target_cols, current_meta)
        incoming = [_row_as_dict(target_cols, r) for r in mapped_rows]
        merged = _merge_upsert_rows(existing_rows, incoming, pk_cols=pk_cols)
        rel_path, n_written, checksum, file_warnings = _write_data_file(
            table_dir / "data", target_cols, merged, column_types=write_types
        )
        operation = "overwrite"  # Iceberg CoW upsert lands as overwrite snapshot
        data_files = [{"path": rel_path, "record-count": n_written, "checksum": checksum}]
    else:
        rel_path, n_written, checksum, file_warnings = _write_data_file(
            table_dir / "data", target_cols, mapped_rows, column_types=write_types
        )
        operation = "overwrite" if mode in {"overwrite", "replace"} else "append"
        data_files = list((current_meta or {}).get("data-files") or []) + [
            {"path": rel_path, "record-count": n_written, "checksum": checksum}
        ]
        if mode in {"overwrite", "replace"}:
            data_files = [{"path": rel_path, "record-count": n_written, "checksum": checksum}]

    snapshot_id = int(time.time() * 1000)
    now_ms = snapshot_id

    schemas = list((current_meta or {}).get("schemas") or [])
    if not schemas or schemas[-1].get("schema-id") != schema_json.get("schema-id"):
        schemas.append(schema_json)

    snapshots = list((current_meta or {}).get("snapshots") or [])
    snapshots.append({
        "snapshot-id": snapshot_id,
        "timestamp-ms": now_ms,
        "summary": {
            "operation": operation,
            "added-records": str(n_written),
            "added-data-files": "1",
            "dataflow.checksum": checksum,
            "dataflow.write_mode": mode,
            "dataflow.write_strategy": "copy-on-write",
        },
        "manifest-list": rel_path,
        "schema-id": schema_json.get("schema-id", 0),
    })

    new_version = (int(versions[-1].stem[1:].split(".")[0]) + 1) if versions else 1
    metadata = {
        "format-version": 2,
        "table-uuid": (current_meta or {}).get("table-uuid") or str(uuid.uuid4()),
        "location": str(table_dir),
        "last-updated-ms": now_ms,
        "last-column-id": max((int(f.get("id", 0)) for f in schema_json.get("fields", [])), default=0),
        "schemas": schemas,
        "current-schema-id": schema_json.get("schema-id", 0),
        "schema": schema_json,
        "partition-specs": (current_meta or {}).get("partition-specs") or [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "snapshots": snapshots,
        "current-snapshot-id": snapshot_id,
        "properties": {
            "write.format.default": "parquet" if rel_path.endswith(".parquet") else "jsonl",
            "dataflow.engine": "iceberg_writer",
            "dataflow.evolve": ",".join(evolve_notes) if evolve_notes else "",
            "dataflow.write_mode": mode,
            "dataflow.write_strategy": "copy-on-write",
        },
        "data-files": data_files,
    }

    # Atomic commit: write temp then rename; update version-hint
    meta_path = meta_dir / f"v{new_version}.metadata.json"
    tmp = meta_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(metadata, indent=2, default=json_default), encoding="utf-8")
    os.replace(tmp, meta_path)
    (meta_dir / "version-hint.text").write_text(str(new_version), encoding="utf-8")

    if on_checkpoint:
        on_checkpoint(n_written, n_written, 1)

    _final_abort = reject_on_strict_policy(policy, rejected_details, "Iceberg")
    if _final_abort:
        return WriteResult(
            ok=False,
            rows_written=n_written,
            table_name=table,
            target_schema=str(table_dir),
            checksum=checksum,
            chunks_completed=1,
            error=_final_abort,
            rejected_details=rejected_details,
            rejected_rows=_rejected_row_count(
                data_rows, mapped_rows, rejected_details, policy
            ),
            warnings=(list(evolve_notes) + list(file_warnings))[:20],
            driver="iceberg",
        )

    return WriteResult(
        ok=True,
        rows_written=n_written,
        table_name=table,
        target_schema=str(table_dir),
        checksum=checksum,
        chunks_completed=1,
        rejected_details=rejected_details,
        rejected_rows=_rejected_row_count(
            data_rows, mapped_rows, rejected_details, policy
        ),
        warnings=(list(evolve_notes) + list(file_warnings))[:20],
        driver="iceberg",
    )


def write_mapped_rows(
    *,
    host: str = "",
    port: int = 0,
    database: str = "",
    username: str = "",
    password: str = "",
    schema: str = "",
    connection_string: str = "",
    ssl: bool = False,
    table_name: str = "",
    headers: list[str] | None = None,
    data_rows: list[list[str]] | None = None,
    mappings: list[dict] | None = None,
    column_types: dict[str, str] | None = None,
    on_checkpoint: Callable[..., None] | None = None,
    create_table: bool = True,
    error_policy: str | None = None,
    write_mode: str = "append",
    conflict_columns: list[str] | None = None,
    **_kwargs: Any,
) -> WriteResult:
    """Dispatch between real Iceberg catalog writes and legacy filesystem CoW."""
    headers = headers or []
    data_rows = data_rows or []
    mappings = mappings or []
    column_types = column_types or {}
    sync_mode = str(_kwargs.pop("sync_mode", ""))
    file_batch_idx = int(_kwargs.pop("file_batch_idx", 0) or 0)
    table = (table_name or "events").strip()

    endpoint = {
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "password": password,
        "schema": schema,
        "connection_string": connection_string,
        "ssl": ssl,
        "table": table,
        "table_name": table,
        **_kwargs,
    }

    try:
        write_path = resolve_iceberg_write_path(endpoint)
    except RuntimeError as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema="",
            checksum="",
            chunks_completed=0,
            error=str(exc),
            driver="iceberg",
        )

    if write_path == "catalog":
        return _write_mapped_rows_pyiceberg(
            endpoint,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            on_checkpoint=on_checkpoint,
            create_table=create_table,
            error_policy=error_policy,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            sync_mode=sync_mode,
            file_batch_idx=file_batch_idx,
            destination_column_nullability=_kwargs.get("destination_column_nullability"),
        )

    return _write_mapped_rows_filesystem(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        schema=schema,
        connection_string=connection_string,
        ssl=ssl,
        table_name=table_name,
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        column_types=column_types,
        on_checkpoint=on_checkpoint,
        create_table=create_table,
        error_policy=error_policy,
        write_mode=write_mode,
        conflict_columns=conflict_columns,
        sync_mode=sync_mode,
        file_batch_idx=file_batch_idx,
        **_kwargs,
    )


def _resolve_iceberg_table_dir(cfg: dict[str, Any], table_name: str, schema: str | None) -> Path:
    host = str(cfg.get("host") or "")
    database = str(cfg.get("database") or "")
    connection_string = str(cfg.get("connection_string") or "")
    root = _warehouse_root(host, database, connection_string)
    table = (table_name or "").strip()
    sch = (schema or cfg.get("schema") or "").strip()
    table_dir = root / sch / table if sch else root / table
    if "." in table and not sch:
        parts = table.split(".", 1)
        table_dir = root / parts[0] / parts[1]
    return table_dir


def delete_by_primary_keys(
    cfg: dict[str, Any],
    table_name: str,
    primary_key_column: str,
    keys: list[str],
    schema: str | None = None,
    *,
    incoming_lsn: str | None = None,
    lsn_column: str = "_df_lsn",
) -> int:
    """CDC delete with LSN guard for filesystem CoW and pyiceberg catalogs.

    Stale deletes that would wipe a newer ``_df_lsn`` row are skipped
    (at-least-once redelivery safety). Returns the number of rows removed.
    """
    if not keys:
        return 0
    key_set = {str(k) for k in keys}
    endpoint = {
        **cfg,
        "table": table_name,
        "table_name": table_name,
        "schema": schema or cfg.get("schema") or "",
    }
    try:
        write_path = resolve_iceberg_write_path(endpoint)
    except RuntimeError as exc:
        raise RuntimeError(str(exc)) from exc
    if write_path == "catalog":
        return _delete_pyiceberg(
            endpoint,
            primary_key_column,
            key_set,
            incoming_lsn=incoming_lsn,
            lsn_column=lsn_column,
        )
    return _delete_filesystem(
        cfg,
        table_name,
        primary_key_column,
        key_set,
        schema=schema,
        incoming_lsn=incoming_lsn,
        lsn_column=lsn_column,
    )


def _filter_delete_keys_by_lsn(
    rows: list[dict[str, Any]],
    primary_key_column: str,
    key_set: set[str],
    *,
    incoming_lsn: str | None,
    lsn_column: str,
) -> set[str]:
    from services.cdc_effectively_once import filter_keys_for_lsn_delete

    if not incoming_lsn:
        return set(key_set)
    existing = {
        str(r.get(primary_key_column)): r.get(lsn_column)
        for r in rows
        if str(r.get(primary_key_column)) in key_set
    }
    # Keys absent from table: treat as already deleted (idempotent).
    for k in key_set:
        existing.setdefault(k, None)
    kept = filter_keys_for_lsn_delete(list(key_set), existing, incoming_lsn)
    return {str(k) for k in kept}


def _delete_filesystem(
    cfg: dict[str, Any],
    table_name: str,
    primary_key_column: str,
    key_set: set[str],
    *,
    schema: str | None,
    incoming_lsn: str | None,
    lsn_column: str,
) -> int:
    table_dir = _resolve_iceberg_table_dir(cfg, table_name, schema)
    meta_dir = table_dir / "metadata"
    if not meta_dir.is_dir():
        return 0
    versions = sorted(meta_dir.glob("v*.metadata.json"))
    if not versions:
        return 0
    current_meta = _load_metadata(versions[-1])
    if not current_meta:
        return 0
    schema_json = (current_meta.get("schemas") or [{}])[-1] or current_meta.get("schema") or {}
    columns = [str(f.get("name")) for f in (schema_json.get("fields") or []) if f.get("name")]
    if primary_key_column not in columns:
        columns = list(columns) + [primary_key_column]
    existing = _load_existing_rows(table_dir, columns, current_meta)
    work_keys = _filter_delete_keys_by_lsn(
        existing,
        primary_key_column,
        key_set,
        incoming_lsn=incoming_lsn,
        lsn_column=lsn_column,
    )
    if not work_keys:
        return 0
    kept = [r for r in existing if str(r.get(primary_key_column)) not in work_keys]
    deleted = len(existing) - len(kept)
    write_types = _write_types_from_schema(schema_json, {})
    rel_path, n_written, checksum, _warnings = _write_data_file(
        table_dir / "data",
        columns,
        kept,
        column_types=write_types,
    )
    snapshot_id = int(time.time() * 1000)
    snapshots = list(current_meta.get("snapshots") or [])
    snapshots.append({
        "snapshot-id": snapshot_id,
        "timestamp-ms": snapshot_id,
        "summary": {
            "operation": "overwrite",
            "added-records": str(n_written),
            "added-data-files": "1",
            "dataflow.checksum": checksum,
            "dataflow.write_mode": "cdc_delete",
            "dataflow.write_strategy": "copy-on-write",
            "dataflow.deleted_keys": str(deleted),
        },
        "manifest-list": rel_path,
        "schema-id": schema_json.get("schema-id", 0),
    })
    new_version = int(versions[-1].stem[1:].split(".")[0]) + 1
    metadata = {
        **current_meta,
        "last-updated-ms": snapshot_id,
        "snapshots": snapshots,
        "current-snapshot-id": snapshot_id,
        "data-files": [{"path": rel_path, "record-count": n_written, "checksum": checksum}],
        "properties": {
            **(current_meta.get("properties") or {}),
            "dataflow.write_mode": "cdc_delete",
            "dataflow.write_strategy": "copy-on-write",
        },
    }
    meta_path = meta_dir / f"v{new_version}.metadata.json"
    tmp = meta_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(metadata, indent=2, default=json_default), encoding="utf-8")
    os.replace(tmp, meta_path)
    (meta_dir / "version-hint.text").write_text(str(new_version), encoding="utf-8")
    return deleted


def _delete_pyiceberg(
    endpoint: dict[str, Any],
    primary_key_column: str,
    key_set: set[str],
    *,
    incoming_lsn: str | None,
    lsn_column: str,
) -> int:
    from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config

    config = parse_iceberg_catalog_config(endpoint)
    catalog = load_catalog(endpoint)
    identifier = config.table_identifier
    tbl = catalog.load_table(identifier)
    scanned = tbl.scan().to_arrow()
    rows: list[dict[str, Any]] = []
    for i in range(scanned.num_rows):
        rows.append(
            {name: scanned.column(name)[i].as_py() for name in scanned.column_names}
        )
    work_keys = _filter_delete_keys_by_lsn(
        rows,
        primary_key_column,
        key_set,
        incoming_lsn=incoming_lsn,
        lsn_column=lsn_column,
    )
    if not work_keys:
        return 0
    kept = [r for r in rows if str(r.get(primary_key_column)) not in work_keys]
    deleted = len(rows) - len(kept)
    if deleted == 0:
        return 0
    if pa is None:
        raise RuntimeError("pyarrow required for Iceberg CDC deletes")
    arrays = []
    for name in scanned.column_names:
        field = scanned.schema.field(name)
        cells = [r.get(name) for r in kept]
        arrays.append(pa.array(cells, type=field.type))
    remaining = pa.Table.from_arrays(arrays, schema=scanned.schema)
    tbl.overwrite(remaining)
    return deleted
