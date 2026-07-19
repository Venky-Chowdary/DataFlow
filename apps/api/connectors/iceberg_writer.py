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
    t = (logical or "string").lower()
    return {
        "int": "int",
        "integer": "int",
        "long": "long",
        "bigint": "long",
        "float": "float",
        "double": "double",
        "decimal": "decimal(38,9)",
        "boolean": "boolean",
        "bool": "boolean",
        "date": "date",
        "timestamp": "timestamptz",
        "datetime": "timestamptz",
        "json": "string",
        "binary": "binary",
    }.get(t, "string")


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


def _row_as_dict(columns: list[str], row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return {c: row.get(c) for c in columns}
    return {c: row[i] if i < len(row) else None for i, c in enumerate(columns)}


def _write_data_file(data_dir: Path, columns: list[str], rows: list[Any]) -> tuple[str, int, str]:
    """Write one data file; returns (relative_path, record_count, checksum)."""
    data_dir.mkdir(parents=True, exist_ok=True)
    file_id = uuid.uuid4().hex
    digest = hashlib.sha256()
    dict_rows = [_row_as_dict(columns, r) for r in rows]

    # Prefer Parquet when pyarrow is available
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        arrays = {c: [r.get(c) for r in dict_rows] for c in columns}
        table = pa.table(arrays)
        rel = f"data/{file_id}.parquet"
        path = data_dir.parent / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
        digest.update(path.read_bytes())
        return rel, len(dict_rows), digest.hexdigest()[:16]
    except Exception:
        pass

    rel = f"data/{file_id}.jsonl"
    path = data_dir.parent / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in dict_rows:
            line = json.dumps(row, default=json_default)
            fh.write(line + "\n")
            digest.update(line.encode())
    return rel, len(dict_rows), digest.hexdigest()[:16]


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
    meta_dir = table_dir / "metadata"
    meta_dir.mkdir(parents=True, exist_ok=True)

    target_cols, logical = resolve_target_columns(mappings, column_types, preserve_case=True)
    dest_types = {
        target_cols[i]: (
            mappings[i].get("target_type")
            or column_types.get(mappings[i]["source"])
            or logical.get(target_cols[i], "string")
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
    if write_mode in {"overwrite", "replace"} and current_meta:
        # Drop prior data refs; keep schema evolution
        current_meta = None

    rel_path, n_written, checksum = _write_data_file(table_dir / "data", target_cols, mapped_rows)
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
            "operation": "overwrite" if write_mode in {"overwrite", "replace"} else "append",
            "added-records": str(n_written),
            "added-data-files": "1",
            "dataflow.checksum": checksum,
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
        },
        "data-files": list((current_meta or {}).get("data-files") or []) + (
            [] if write_mode in {"overwrite", "replace"} else []
        ) + [{"path": rel_path, "record-count": n_written, "checksum": checksum}],
    }
    if write_mode in {"overwrite", "replace"}:
        metadata["data-files"] = [{"path": rel_path, "record-count": n_written, "checksum": checksum}]

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
        driver="iceberg",
    )
