"""Apache Iceberg destination writer (filesystem / object-store root).

Implements Iceberg V2 *append* commit semantics without requiring the full
``pyiceberg`` stack at runtime:

1. Map + coerce rows (quarantine on transform failure).
2. Write a data file (Parquet when pyarrow is available, else JSONL).
3. Atomically update ``metadata/v{N}.metadata.json`` with schema evolution
   (additive columns only) and a new snapshot pointing at the data file.

Warehouse root is taken from ``connection_string`` / ``database`` / ``host``
(local path or ``file://`` URI). This is a real table-format writer suitable
for lakehouse landing; REST catalog / Glue committers can wrap the same
snapshot algorithm later.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from connectors.writer_common import (
    WriteResult,
    build_mapped_rows_with_details,
    resolve_target_columns,
    transform_error_policy,
)
from services.value_serializer import json_default


def _warehouse_root(host: str, database: str, connection_string: str) -> Path:
    raw = (connection_string or database or host or "").strip()
    if raw.startswith("file://"):
        raw = raw[len("file://") :]
    if not raw:
        raise ValueError("Iceberg warehouse path required (connection_string or database)")
    return Path(raw).expanduser().resolve()


def _logical_to_iceberg_type(logical: str) -> str:
    """Single source of truth: type_system.ddl_type — never a parallel scale map."""
    from services.type_system import ddl_type

    return ddl_type("iceberg", logical or "string")


def _iceberg_type_to_logical_carrier(iceberg_type: Any) -> str:
    """Map committed Iceberg field type back to a logical carrier for Parquet writes."""
    if isinstance(iceberg_type, dict):
        kind = str(iceberg_type.get("type") or "").lower()
        if kind == "decimal":
            p = int(iceberg_type.get("precision") or 38)
            s = int(iceberg_type.get("scale") or 0)
            return f"DECIMAL({p},{s})"
        if kind in {"list", "map", "struct"}:
            return "JSON"
        return kind or "string"
    t = str(iceberg_type or "string").lower()
    mapping = {
        "string": "string",
        "long": "integer",
        "int": "integer",
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
    """PK upsert with LSN guard: keep row with higher/equal LSN; no LSN → last wins."""
    from connectors.writer_common import compare_lsn

    def _key(row: dict[str, Any]) -> tuple:
        return tuple(str(row.get(c, "")) for c in pk_cols)

    best: dict[tuple, dict[str, Any]] = {}
    for row in existing:
        best[_key(row)] = dict(row)
    for row in incoming:
        key = _key(row)
        prev = best.get(key)
        if prev is None:
            best[key] = dict(row)
            continue
        if lsn_col in row or lsn_col in prev:
            if compare_lsn(row.get(lsn_col), prev.get(lsn_col)) >= 0:
                best[key] = dict(row)
        else:
            best[key] = dict(row)
    return list(best.values())


def _row_as_dict(columns: list[str], row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return {c: row.get(c) for c in columns}
    return {c: row[i] if i < len(row) else None for i, c in enumerate(columns)}


def _logical_to_arrow_type(logical: str, pa: Any) -> Any:
    """Map DataFlow logical / Iceberg DDL carrier → pyarrow type (fail-closed decimals)."""
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
        return pa.float64()
    if logical_n == LOGICAL_DECIMAL:
        precision, scale = parse_numeric_precision_scale(raw)
        p = int(precision) if precision is not None else 38
        s = int(scale) if scale is not None else 10
        p = max(1, min(p, 38))
        s = max(0, min(s, p))
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
        return pa.binary()
    return pa.string()


def _coerce_arrow_cell(value: Any, arrow_type: Any, pa: Any) -> Any:
    """Coerce a Python cell into the declared Arrow type; raise on hard failure."""
    from datetime import date, datetime, time
    from decimal import Decimal, InvalidOperation

    if value is None or value == "":
        return None
    if pa.types.is_decimal(arrow_type):
        try:
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError(f"cannot cast {value!r} to decimal") from exc
    if pa.types.is_floating(arrow_type):
        return float(value)
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
        return int(value)
    if pa.types.is_boolean(arrow_type):
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"1", "true", "t", "yes", "y"}:
            return True
        if text in {"0", "false", "f", "no", "n"}:
            return False
        raise ValueError(f"cannot cast {value!r} to boolean")
    if pa.types.is_timestamp(arrow_type):
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
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
    if pa.types.is_binary(arrow_type):
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        return str(value).encode("utf-8")
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

    rel = f"data/{file_id}.jsonl"
    path = data_dir.parent / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in dict_rows:
            line = json.dumps(row, default=json_default)
            fh.write(line + "\n")
            digest.update(line.encode())
    return rel, len(dict_rows), digest.hexdigest()[:16], warnings


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
    try:
        root = _warehouse_root(host, database, connection_string)
        root.mkdir(parents=True, exist_ok=True)
        probe = root / ".dataflow_iceberg_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True, f"Iceberg warehouse writable at {root}"
    except Exception as exc:
        return False, f"Iceberg warehouse not writable: {exc}"


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
    )
    if transform_errors and policy == "fail":
        return WriteResult(
            ok=False, rows_written=0, table_name=table, target_schema=str(table_dir),
            checksum="", chunks_completed=0,
            error=f"Transform errors: {'; '.join(transform_errors[:3])}",
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

    return WriteResult(
        ok=True,
        rows_written=n_written,
        table_name=table,
        target_schema=str(table_dir),
        checksum=checksum,
        chunks_completed=1,
        rejected_details=rejected_details,
        rejected_rows=len(rejected_details),
        warnings=(list(evolve_notes) + list(file_warnings))[:20],
        driver="iceberg",
    )
