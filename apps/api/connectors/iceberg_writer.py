"""Apache Iceberg destination writer.

Two paths:

1. Catalog mode (default when ``extra.catalog_type`` / connection string indicate
   REST / Glue / SQL / Nessie): uses ``pyiceberg`` to read/write real Iceberg
   tables. Supports append, overwrite, and MERGE/upsert via ``Table.upsert``.

2. Filesystem mode (bare local path, no catalog): V2 metadata-file writer.
   Overwrite/replace stay copy-on-write. Upserts write Iceberg v2
   equality-delete files for updated keys plus a new data file at the
   same snapshot sequence (spec: equality deletes apply only when
   ``data_seq < delete_seq``, so the new image survives). CDC and
   leftover-MERGE deletes write equality-delete files. Dest-engine
   COUNT / leftover listing apply the same MoR kernel.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Sequence

from services.value_serializer import json_default, json_loads_exact

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


def _iceberg_effective_write_mode(
    write_mode: str,
    *,
    sync_mode: str = "",
    file_batch_idx: int = 0,
) -> str:
    """Overwrite sync replaces the snapshot once; later chunks append.

    Iceberg ``drop_table`` is unsupported, so full-refresh overwrite cannot
    clear dest via table_manager. Insert/append of S onto D would duplicate
    keys and leftover MERGE would refuse (unique PK ≠ COUNT). First chunk
    of overwrite sync is snapshot replace — leftover MERGE then sees dest=S.
    """
    from services.sync_cursor import is_overwrite_sync

    mode = (write_mode or "append").lower()
    upsert_modes = {"upsert", "merge", "cdc", "incremental_deduped"}
    idx = int(file_batch_idx or 0)
    if is_overwrite_sync(sync_mode) and mode not in upsert_modes:
        if idx in (0, 1):
            return "overwrite" if mode in {"insert", "append", ""} else mode
        return "append"
    if mode in {"overwrite", "replace"} and idx > 1:
        return "append"
    return mode


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
    from services.decision_kernel import ddl_type, materialize_dest_ddl, normalize_logical_type
    from services.type_system import LOGICAL_DECIMAL

    raw = (logical or "string").strip()
    norm = normalize_logical_type(raw)
    # Bare logical vocabulary → DDL_TYPES SSOT (harness: writer ≡ ddl_type).
    if (
        "(" not in raw
        and "<" not in raw
        and "[" not in raw
        and raw.strip().lower() == norm
    ):
        return ddl_type("iceberg", raw)
    # Nested ARRAY/LIST/T[] stamps go through materialize so list<float> spelling
    # and float leaves stay authoritative (no dual ddl_type invent path).
    stamped = materialize_dest_ddl("iceberg", raw)
    # Normalize single-precision aliases to Iceberg's float token.
    bare = stamped.upper().split("(", 1)[0].strip()
    if bare in {"REAL", "FLOAT4", "FLOAT32", "HALF", "FLOAT16", "FLOAT"}:
        return "float"
    # Map≡CREATE decimal honesty: bare → decimal(38,10); oversize → string.
    if norm == LOGICAL_DECIMAL or normalize_logical_type(stamped) == LOGICAL_DECIMAL:
        return ddl_type("iceberg", raw)
    return stamped


def _ensure_iceberg_decimal_carrier(type_str: str) -> str:
    """Map≡CREATE: decimal carriers match ``ddl_type('iceberg', …)`` SSOT.

    Bare ``DECIMAL`` → ``decimal(38,10)`` so shared fit quarantine can parse
    ``(p,s)``. Over Iceberg max precision → ``string`` (fail-closed) — never
    leave a bare or oversize stamp that Arrow would invent/clamp.
    """
    from services.decision_kernel import ddl_type, normalize_logical_type
    from services.type_system import LOGICAL_DECIMAL

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
        from services.decision_kernel import ddl_type, normalize_logical_type
        from services.type_system import LOGICAL_BINARY, parse_binary_carrier_width

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
    """Map committed Iceberg field type back to a logical carrier for Parquet writes.

    Empty / missing types return ``""`` — never invent ``string`` (that would
    soft-green incomplete metadata past require_physical / rematerialize).
    """
    if isinstance(iceberg_type, dict):
        kind = str(iceberg_type.get("type") or "").strip().lower()
        if not kind:
            return ""
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
        return kind
    if iceberg_type is None:
        return ""
    t = str(iceberg_type).strip().lower()
    if not t:
        return ""
    m_fixed = re.match(r"fixed\s*\[\s*(\d+)\s*\]", t) or re.match(
        r"fixed\s*\(\s*(\d+)\s*\)", t
    )
    if m_fixed:
        return f"BINARY({int(m_fixed.group(1))})"
    mapping = {
        "string": "string",
        "long": "BIGINT",
        "int": "INT",
        # Iceberg double is float64 — never collapse to float32 via "float".
        "double": "DOUBLE",
        "float": "FLOAT",
        "boolean": "boolean",
        "date": "date",
        "timestamptz": "timestamptz",
        "timestamp": "timestamp_ntz",
        "binary": "binary",
        "uuid": "uuid",
        "time": "time",
    }
    return mapping.get(t, t)


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
        carrier = _iceberg_type_to_logical_carrier(field.get("type"))
        # Incomplete metadata must not wipe Studio/Map with invent-empty "".
        if carrier:
            out[name] = carrier
    return out


def _physical_carriers_from_arrow(arrow_schema: Any, pa_mod: Any) -> dict[str, str]:
    """Map committed Arrow fields → logical carriers for rematerialize compare."""
    physical: dict[str, str] = {}
    names = list(getattr(arrow_schema, "names", []) or [])
    for name in names:
        carriers = _decimal_target_types_for_iceberg_write(
            [name],
            {},
            arrow_schema=arrow_schema,
            pa_mod=pa_mod,
        )
        if carriers:
            physical[name] = carriers[0]
            physical.setdefault(name.lower(), carriers[0])
            physical.setdefault(name.upper(), carriers[0])
    return physical


def _iceberg_map_rows(
    *,
    headers: list[str],
    data_rows: list,
    mappings: list,
    target_cols: list[str],
    column_types: dict[str, str] | None,
    dest_types: dict[str, str],
    policy: Any,
    conflict_columns: list[str] | None = None,
    destination_column_nullability: Any = None,
) -> tuple[list[tuple], list[str], list[dict]]:
    """One Map pass against settled dest types. Callers must not Map twice."""
    return build_mapped_rows_with_details(
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


def _iceberg_rematerialize_if_physical_differs(
    *,
    physical: dict[str, str],
    dest_types: dict[str, str],
    target_cols: list[str],
    headers: list[str],
    data_rows: list,
    mappings: list,
    column_types: dict[str, str] | None,
    logical_types: list[str],
    policy: Any,
    conflict_columns: list[str] | None = None,
    destination_column_nullability: Any = None,
    force_remap: bool = False,
) -> tuple[list[tuple], list[str], list[dict], dict[str, str]] | None:
    """Rebuild mapped rows from source when live DDL carriers differ from Map.

    Returns ``(mapped_rows, transform_errors, rejected_details, live_dest_types)``
    or ``None`` when carriers already match (caller keeps Map-built batch).

    ``force_remap`` covers deferred Map under partial Studio (empty batch).

    Additive schema-evolution columns (Map targets not yet on the table) keep
    Map stamps; existing live columns rematerialize without Map VARCHAR invent.
    """
    from connectors.writer_common import rematerialize_live_dest_types

    if not physical:
        return None
    covered_cols: list[str] = []
    covered_physical: dict[str, str] = {}
    for c in target_cols or []:
        if not c:
            continue
        hit = (
            physical.get(c)
            or physical.get(str(c).lower())
            or physical.get(str(c).upper())
        )
        if hit and str(hit).strip():
            covered_cols.append(c)
            covered_physical[c] = str(hit).strip()
    if not covered_cols:
        return None
    live_partial = rematerialize_live_dest_types(
        covered_physical, covered_cols, product="Iceberg"
    )
    if live_partial is None:
        return None
    # Preserve Map stamps for additive columns not yet on the table.
    live_dest_types = dict(dest_types or {})
    live_dest_types.update(live_partial)
    carriers_differ = any(
        str(dest_types.get(c) or "").strip().upper()
        != str(live_dest_types.get(c) or "").strip().upper()
        for c in covered_cols
    )
    if not carriers_differ and not force_remap:
        return None
    # Partial Studio + deferred Map: additive targets not on live physical must
    # carry an explicit Map target_type — never soft-fill from column_types /
    # "string" defaults (create-new refuse parity).
    if force_remap:
        from services.mapping_constraints import write_mappings

        by_tgt: dict[str, dict] = {}
        for mapping in write_mappings(list(mappings or [])):
            tgt = str(mapping.get("target") or "").strip()
            if tgt and tgt not in by_tgt:
                by_tgt[tgt] = mapping
                by_tgt.setdefault(tgt.lower(), mapping)
        for col in target_cols or []:
            if not col:
                continue
            if str(live_dest_types.get(col) or "").strip():
                continue
            mapping = by_tgt.get(col) or by_tgt.get(str(col).lower()) or {}
            stamp = str(mapping.get("target_type") or "").strip()
            if not stamp:
                return None
            live_dest_types[col] = stamp
    mapped_rows, transform_errors, rejected_details = _iceberg_map_rows(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        target_cols=target_cols,
        column_types=column_types,
        dest_types=live_dest_types,
        policy=policy,
        conflict_columns=conflict_columns,
        destination_column_nullability=destination_column_nullability,
    )
    return (
        mapped_rows,
        list(transform_errors or []),
        rejected_details,
        live_dest_types,
    )


def _load_metadata(meta_path: Path) -> dict[str, Any] | None:
    if not meta_path.exists():
        return None
    return json.loads(meta_path.read_text(encoding="utf-8"))


def snapshot_data_files(
    table_dir: Path, current_meta: dict[str, Any] | None
) -> list[tuple[str, Path]]:
    """Current snapshot data-file paths. Missing file raises (fail closed).

    Dest COUNT sums dest-engine population of these files (Parquet footer /
    JSONL line stream). Upsert CoW still materializes rows from the same
    list. Metadata ``record-count`` is writer-stamped and is not dest.
    """
    if not current_meta:
        return []
    out: list[tuple[str, Path]] = []
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
        out.append((rel, path))
    return out


def snapshot_has_delete_files(current_meta: dict[str, Any] | None) -> bool:
    """True when the snapshot lists delete files (MoR).

    Dest COUNT / leftover listing apply Iceberg v2 position/equality and
    v3 deletion vectors through ``iceberg_mor``. Footer sum without MoR
    would lie.
    """
    if not current_meta:
        return False
    deletes = current_meta.get("delete-files") or current_meta.get("delete_files") or []
    return bool(deletes)


def _snapshot_delete_files(current_meta: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not current_meta:
        return []
    refs = current_meta.get("delete-files") or current_meta.get("delete_files") or []
    return [dict(ref) for ref in refs if isinstance(ref, dict)]


def _optional_sequence(ref: dict[str, Any]) -> int | None:
    raw = ref.get("sequence-number")
    if raw is None or raw == "":
        raw = ref.get("sequence_number")
    if raw is None or raw == "":
        return None
    return int(raw)


def _max_iceberg_sequence(
    data_files: Sequence[dict[str, Any]],
    delete_files: Sequence[dict[str, Any]] = (),
) -> int:
    max_seq = 0
    for ref in list(data_files) + list(delete_files):
        seq = _optional_sequence(ref)
        if seq is not None:
            max_seq = max(max_seq, seq)
    return max_seq


def _ensure_data_file_sequences(data_files: list[dict[str, Any]]) -> None:
    """Stamp missing data-file sequence-number to 1 (equality deletes need it)."""
    for ref in data_files:
        if _optional_sequence(ref) is None:
            ref["sequence-number"] = 1


def _equality_ids_for_columns(
    schema_json: dict[str, Any], pk_cols: Sequence[str]
) -> list[int]:
    by_name: dict[str, int] = {}
    for field in schema_json.get("fields") or []:
        name = str(field.get("name") or "").strip()
        if not name:
            continue
        try:
            by_name[name] = int(field.get("id"))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Iceberg schema field {name!r} missing integer id"
            ) from exc
    ids: list[int] = []
    for col in pk_cols:
        if col not in by_name:
            raise ValueError(
                f"Iceberg equality-delete PK {col!r} is not in the snapshot schema"
            )
        ids.append(by_name[col])
    return ids


def _evolve_schema(
    existing: dict[str, Any] | None,
    columns: list[str],
    column_types: dict[str, str],
    *,
    require_types: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    """Return (schema_json, notes). Additive-only evolution; type_locked conflicts noted.

    ``require_types`` (partial Studio): refuse missing carriers instead of inventing
    Iceberg ``string`` for additive columns.
    """
    notes: list[str] = []

    def _carrier_for(name: str) -> str:
        typ = str((column_types or {}).get(name) or "").strip()
        if typ:
            return typ
        if require_types:
            raise ValueError(
                f"Iceberg additive column {name!r} lacks Studio/live type and "
                "Map target_type under partial Studio — refuse string evolve invent. "
                "Stamp the column on Map or re-run destination schema introspect."
            )
        return "string"

    if existing is None:
        fields = []
        for i, name in enumerate(columns, start=1):
            fields.append({
                "id": i,
                "name": name,
                "required": False,
                "type": _logical_to_iceberg_type(_carrier_for(name)),
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
            want = _logical_to_iceberg_type(
                str((column_types or {}).get(name) or "").strip() or "string"
            )
            have = by_name[name].get("type")
            if have != want and str((column_types or {}).get(name) or "").strip():
                notes.append(f"type_locked: keep {name}:{have} (incoming {want})")
            continue
        fields.append({
            "id": next_id,
            "name": name,
            "required": False,
            "type": _logical_to_iceberg_type(_carrier_for(name)),
        })
        notes.append(f"schema_evolve: added column {name}")
        next_id += 1
    schema_id = int(existing.get("schema-id", 0)) + (1 if notes else 0)
    return {"type": "struct", "schema-id": schema_id, "fields": fields}, notes


def _read_snapshot_data_file(
    rel: str, path: Path, columns: Sequence[str]
) -> list[dict[str, Any]]:
    """Fail-closed read of one snapshot data file (JSONL/Parquet)."""
    cols = [str(c) for c in columns]
    rows: list[dict[str, Any]] = []
    if rel.endswith(".parquet") or str(path).endswith(".parquet"):
        try:
            import pyarrow.parquet as pq

            table = pq.read_table(path)
            for batch in table.to_pylist():
                rows.append({c: batch.get(c) for c in cols})
        except Exception as exc:
            raise ValueError(
                f"Iceberg Parquet data-file unreadable for upsert merge: {rel}: {exc}"
            ) from exc
        return rows
    try:
        with path.open(encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json_loads_exact(line)
                except Exception as exc:
                    raise ValueError(
                        f"Iceberg JSONL data-file corrupt at {rel}:{line_no}: {exc}"
                    ) from exc
                rows.append({c: obj.get(c) for c in cols})
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(
            f"Iceberg JSONL data-file unreadable for upsert merge: {rel}: {exc}"
        ) from exc
    return rows


def _project_snapshot_file(
    rel: str, path: Path | None, cols: Sequence[str]
) -> list[dict[str, Any]] | None:
    """MoR project callback. Unreadable → None (unmeasured), never raw resurrect."""
    if path is None:
        return None
    try:
        return _read_snapshot_data_file(rel or str(path), path, cols)
    except ValueError:
        return None


def _load_existing_rows(table_dir: Path, columns: list[str], current_meta: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Load surviving snapshot rows (MoR when delete files exist).

    Fail-closed: missing or unreadable referenced files abort the upsert so we
    never silently drop existing rows (Airbyte/warehouse silent-loss class).
    Delete files must be applied here — a raw data-file scan would resurrect
    CDC / leftover-MERGE keys that dest COUNT already treats as gone.
    Dest COUNT does not use this materialization — it footers the same files
    and applies the same MoR kernel.
    """
    files = snapshot_data_files(table_dir, current_meta)
    if snapshot_has_delete_files(current_meta):
        from connectors.iceberg_mor import filesystem_mor_snapshot_rows

        surviving = filesystem_mor_snapshot_rows(
            table_dir,
            current_meta or {},
            files,
            cols=columns,
            project_file=_project_snapshot_file,
        )
        if surviving is None:
            raise ValueError(
                "Iceberg MoR snapshot unreadable for upsert/delete merge "
                "(refuse silent resurrect of deleted keys)"
            )
        return surviving
    rows: list[dict[str, Any]] = []
    for rel, path in files:
        rows.extend(_read_snapshot_data_file(rel, path, columns))
    return rows


def _upsert_pk_key(row: dict[str, Any], pk_cols: Sequence[str]) -> tuple:
    """Comparable PK tuple. Never stringify None → ``\"None\"``."""
    from connectors.writer_common import _is_nullish_conflict_key

    out: list[str] = []
    for col in pk_cols:
        val = row.get(col)
        if _is_nullish_conflict_key(val):
            out.append("")
        else:
            out.append(str(val))
    return tuple(out)


def _iceberg_present_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Sparse omit Missing; bind reader-null as None, not the extract token."""
    from services.value_serializer import is_missing_sentinel, is_reader_null_cell

    out: dict[str, Any] = {}
    for key, val in row.items():
        if is_missing_sentinel(val):
            continue
        out[key] = None if is_reader_null_cell(val) else val
    return out


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
    from connectors.writer_common import compare_lsn, _is_nullish_conflict_key
    from services.value_serializer import is_missing_sentinel

    best: dict[tuple, dict[str, Any]] = {}
    for row in existing:
        key = _upsert_pk_key(row, pk_cols)
        if any(_is_nullish_conflict_key(row.get(c)) for c in pk_cols):
            continue
        best[key] = dict(row)
    for row in incoming:
        clean = _iceberg_present_fields(row)
        # Sparse/empty PK must quarantine upstream — refuse invent duplicates here.
        from connectors.writer_common import assert_sparse_upsert_has_pk

        try:
            if any(is_missing_sentinel(v) for v in row.values()):
                assert_sparse_upsert_has_pk(clean, pk_cols)
        except ValueError:
            raise
        if any(_is_nullish_conflict_key(clean.get(c)) for c in pk_cols):
            raise ValueError(
                "Iceberg dense upsert has null/empty primary-key column(s) "
                f"{[c for c in pk_cols if _is_nullish_conflict_key(clean.get(c))]}; "
                "refuse NULL=NULL invent duplicates"
            )
        key = _upsert_pk_key(row, pk_cols)
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


def _mor_upsert_delta(
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
    *,
    pk_cols: list[str],
    lsn_col: str = "_df_lsn",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """MoR upsert payload: new data-file rows + equality-delete PK rows.

    Inserts write only a new data image. Updates write an equality delete
    of the surviving PK (old data files keep their bytes) plus the merged
    image. LSN-discarded keys are omitted (no snapshot). ``_merge_upsert_rows``
    still validates sparse/null PK rules and produces the merged images.
    """
    from connectors.writer_common import compare_lsn, _is_nullish_conflict_key
    from services.value_serializer import is_missing_sentinel

    merged = _merge_upsert_rows(
        existing, incoming, pk_cols=pk_cols, lsn_col=lsn_col
    )
    merged_by = {_upsert_pk_key(row, pk_cols): row for row in merged}
    existing_by: dict[tuple, dict[str, Any]] = {}
    for row in existing:
        key = _upsert_pk_key(row, pk_cols)
        if any(_is_nullish_conflict_key(row.get(c)) for c in pk_cols):
            continue
        existing_by[key] = row
    last_incoming: dict[tuple, dict[str, Any]] = {}
    for row in incoming:
        last_incoming[_upsert_pk_key(row, pk_cols)] = row

    new_rows: list[dict[str, Any]] = []
    eq_deletes: list[dict[str, Any]] = []
    for key, row in last_incoming.items():
        applied = merged_by.get(key)
        if applied is None:
            continue
        prev = existing_by.get(key)
        clean = _iceberg_present_fields(row)
        if prev is None:
            new_rows.append(applied)
            continue
        if lsn_col in clean or lsn_col in prev:
            if compare_lsn(clean.get(lsn_col), prev.get(lsn_col)) <= 0:
                continue
        new_rows.append(applied)
        eq_deletes.append({c: prev.get(c) for c in pk_cols})
    return new_rows, eq_deletes

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
    from services.arrow_write import logical_to_arrow_type

    return logical_to_arrow_type(logical, pa, dialect="iceberg")


def _coerce_arrow_cell(value: Any, arrow_type: Any, pa: Any) -> Any:
    """Coerce a Python cell into the declared Arrow type; raise on hard failure."""
    from services.arrow_write import coerce_arrow_cell

    return coerce_arrow_cell(value, arrow_type, pa, dialect="iceberg")


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


def _write_equality_delete_file(
    data_dir: Path,
    columns: list[str],
    rows: list[dict[str, Any]],
    *,
    column_types: dict[str, str] | None = None,
) -> tuple[str, int, str]:
    """Write Iceberg v2 equality-delete parquet (PK columns only).

    Dest COUNT applies these via ``iceberg_mor`` (content=2). JSONL is not
    a legal equality-delete carrier — refuse if pyarrow is missing.
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ValueError(
            "Iceberg equality-delete write requires pyarrow"
        ) from exc
    data_dir.mkdir(parents=True, exist_ok=True)
    file_id = uuid.uuid4().hex
    types = column_types or {}
    dict_rows = [{c: row.get(c) for c in columns} for row in rows]
    arrow_types = [_logical_to_arrow_type(types.get(c, "string"), pa) for c in columns]
    schema = pa.schema([(c, t) for c, t in zip(columns, arrow_types)])
    arrays = []
    for col, at in zip(columns, arrow_types):
        cells = [_coerce_arrow_cell(r.get(col), at, pa) for r in dict_rows]
        arrays.append(pa.array(cells, type=at))
    table = pa.Table.from_arrays(arrays, schema=schema)
    rel = f"data/{file_id}-eq-deletes.parquet"
    path = data_dir.parent / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    digest = hashlib.sha256(path.read_bytes())
    return rel, len(dict_rows), digest.hexdigest()[:16]


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
    Including every lossless write-path coercion keeps the pushdown honest
    without a full table scan on every batch. ``float(text)`` is not lossless
    — Auto ``1.000`` became ``1``.
    """
    from services.value_serializer import (
        is_missing_sentinel,
        is_reader_null_cell,
        present_cell_text,
    )

    if is_missing_sentinel(value):
        return [value]
    if is_reader_null_cell(value):
        return [None]
    variants: list[Any] = [value]
    text = present_cell_text(value)
    if text is not None and text not in variants:
        variants.append(text)
    if isinstance(value, bool):
        return variants
    if isinstance(value, int):
        return variants
    if isinstance(value, float) and value.is_integer():
        variants.append(int(value))
        return variants
    if isinstance(value, str):
        text = value.strip()
        from services.transform_engine import integer_wire_value

        # Write-path integers only. float(text) invented Auto 1.000 → 1
        # and missed $1,234 the leftover long column actually stores.
        whole = integer_wire_value(text)
        if whole is not None:
            as_int = int(whole)
            if as_int not in variants:
                variants.append(as_int)
    return variants


def _pk_lookup_part(value: Any) -> str:
    """One Iceberg leftover PK part on the dest cell wire.

    ``str(True)`` is ``True``; dest and source ``true`` share one token.
    Reader-null / blank stay empty so a sentinel is not probed as a key.
    """
    from connectors.writer_common import _is_nullish_conflict_key
    from services.value_serializer import present_cell_text

    if _is_nullish_conflict_key(value):
        return ""
    text = present_cell_text(value)
    return "" if text is None else text


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
        from connectors.writer_common import _is_nullish_conflict_key

        names = arrow_table.column_names
        columns = {name: arrow_table.column(name).to_pylist() for name in names}
        for idx in range(arrow_table.num_rows):
            row = {name: columns[name][idx] for name in names}
            if any(_is_nullish_conflict_key(row.get(c)) for c in pk_cols):
                continue
            key = tuple(_pk_lookup_part(row.get(c)) for c in pk_cols)
            existing[key] = row

    unique_keys = list(dict.fromkeys(key_tuples))
    wanted = {tuple(_pk_lookup_part(v) for v in tup) for tup in unique_keys}
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
    from connectors.writer_common import resolve_studio_or_map_dest_types

    live_dest = None
    if isinstance(endpoint, dict):
        live_dest = endpoint.get("destination_column_types") or endpoint.get("schema_types")
    dest_types, studio_err = resolve_studio_or_map_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=target_types,
        studio_types=live_dest if isinstance(live_dest, dict) else None,
        product="Iceberg",
        dest_db="iceberg",
    )
    policy = transform_error_policy(error_policy)
    # Map once after live overlay — never Map-then-remap a second concatenated
    # image (SQL warehouse order). Partial Studio defers Map until create-new
    # refuse / live Arrow rematerialize. Honesty: this write still holds the
    # mapped image until Arrow returns; it does not stream finished bundles.
    mapped_rows: list[tuple] = []
    transform_errors: list[str] = []
    rejected_details: list[dict] = []
    # Defer empty / strict abort until after physical load + rematerialize —
    # Map INT/BOOL stamps can empty the batch while live STRING would keep rows.

    table_existed = False
    try:
        catalog = load_catalog(endpoint)
        identifier = namespace + (table,)
        tbl = catalog.load_table(identifier)
        table_existed = True
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
        # Create-new: partial Studio must not soft-bind Map string invent.
        if studio_err:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table,
                target_schema=target_schema,
                checksum="",
                chunks_completed=0,
                error=studio_err,
                driver="iceberg",
            )
        ensure_namespace(catalog, namespace)
        arrow_types = [_logical_to_arrow_type(dest_types.get(c, "string"), pa) for c in target_cols]
        arrow_schema = pa.schema([(c, t) for c, t in zip(target_cols, arrow_types)])
        tbl = catalog.create_table(identifier, schema=arrow_schema)
        table_existed = False
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

    mode = _iceberg_effective_write_mode(
        write_mode, sync_mode=sync_mode, file_batch_idx=file_batch_idx
    )
    upsert_modes = {"upsert", "merge", "cdc", "incremental_deduped"}

    try:
        existing_arrow = tbl.schema().as_arrow()
        # Rematerialize from source when committed Arrow carriers ≠ Map stamps
        # (VARCHAR→int/date/decimal invent cliff — same class as PG/Snowflake).
        if table_existed:
            from connectors.writer_common import require_physical_types_for_existing_table

            physical = _physical_carriers_from_arrow(existing_arrow, pa)
            existing_names = {
                str(n) for n in (getattr(existing_arrow, "names", None) or [])
            }
            # Only gate columns already on the table — additive union_by_name
            # fields are not in Arrow yet and must not trip require_physical.
            mapped_existing = [
                c for c in target_cols if c and c in existing_names
            ]
            # Studio may fill gaps; always require_physical on existing fields
            # (same Mongo/ES bar — never skip when Studio is complete but Arrow sparse).
            if mapped_existing:
                effective = dict(physical)
                if isinstance(live_dest, dict):
                    for c in mapped_existing:
                        if (
                            effective.get(c)
                            or effective.get(str(c).lower())
                            or effective.get(str(c).upper())
                        ):
                            continue
                        st = str(live_dest.get(c) or "").strip()
                        if st:
                            effective[c] = st
                phys_err = require_physical_types_for_existing_table(
                    table_existed=True,
                    physical=effective,
                    dialect_label="Iceberg",
                    target_cols=mapped_existing,
                )
                if phys_err:
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=table,
                        target_schema=target_schema,
                        checksum="",
                        chunks_completed=0,
                        error=phys_err,
                        driver="iceberg",
                    )
                physical = effective
            _force_remap = bool(studio_err)
            remat = _iceberg_rematerialize_if_physical_differs(
                physical=physical,
                dest_types=dest_types,
                target_cols=target_cols,
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                column_types=column_types,
                logical_types=target_types,
                policy=policy,
                conflict_columns=conflict_columns,
                destination_column_nullability=destination_column_nullability,
                force_remap=_force_remap,
            )
            if remat is not None:
                mapped_rows, transform_errors, rejected_details, dest_types = remat
            elif _force_remap:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table,
                    target_schema=target_schema,
                    checksum="",
                    chunks_completed=0,
                    error=(
                        "Iceberg live DDL incomplete for mapped columns — "
                        "refuse Map VARCHAR rematerialize invent. Re-run "
                        "destination schema introspect and retry."
                    ),
                    rejected_details=rejected_details,
                    driver="iceberg",
                )

        if not mapped_rows:
            mapped_rows, transform_errors, rejected_details = _iceberg_map_rows(
                headers=headers,
                data_rows=data_rows,
                mappings=mappings,
                target_cols=target_cols,
                column_types=column_types,
                dest_types=dest_types,
                policy=policy,
                conflict_columns=conflict_columns,
                destination_column_nullability=destination_column_nullability,
            )

        _map_abort = reject_on_strict_policy(
            policy, rejected_details, "Iceberg", transform_errors
        )
        if _map_abort:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table,
                target_schema=target_schema,
                checksum="",
                chunks_completed=0,
                error=_map_abort,
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

        arrow_types: list[Any] = []
        for c in target_cols:
            carrier = str(dest_types.get(c) or "").strip()
            if not carrier:
                if studio_err:
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=table,
                        target_schema=target_schema,
                        checksum="",
                        chunks_completed=0,
                        error=(
                            f"Iceberg additive column {c!r} lacks Studio/live type "
                            "and Map target_type under partial Studio — refuse "
                            "string union_by_name invent. Stamp the column on Map "
                            "or re-run destination schema introspect."
                        ),
                        rejected_details=rejected_details,
                        driver="iceberg",
                    )
                carrier = "string"
            arrow_types.append(_logical_to_arrow_type(carrier, pa))
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
        schema_extra_cols = [n for n in final_arrow.names if n not in set(target_cols)]
        # Overwrite replaces the whole table: partial Map would NULL-wipe dest-only cols.
        if mode in {"overwrite", "replace"} and schema_extra_cols:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table,
                target_schema=target_schema,
                checksum="",
                chunks_completed=0,
                error=(
                    "Iceberg overwrite Map omits destination columns "
                    f"{schema_extra_cols[:12]} — would NULL-wipe them. "
                    "Map every column or use upsert/merge to preserve unmapped fields."
                ),
                driver="iceberg",
                rejected_details=rejected_details,
            )
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
            # A scan is needed for sparse/LSN overlay OR to preserve dest-only
            # columns that pyiceberg upsert would otherwise NULL-wipe.
            needs_scan = (
                bool(schema_extra_cols)
                or DF_LSN_COL in target_cols
                or any(
                    row_has_missing_sentinel(_row_tuple(target_cols, r))
                    for r in mapped_rows
                )
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
                        **_iceberg_present_fields(row_dict),
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
            # Emit one dense row per PK touched by this batch. Include every
            # committed schema field so unmapped dest columns keep prior values
            # (Arrow r.get(missing)→None must not NULL-wipe them on upsert).
            batch_keys = {
                tuple(str(_row_as_dict(target_cols, r).get(c, "")) for c in pk_cols)
                for r in fold_kept
            }
            emit_cols = list(final_arrow.names)
            mapped_rows = [
                tuple(existing_by_pk[k].get(c) for c in emit_cols)
                for k in batch_keys
                if k in existing_by_pk
            ]

        # Append/overwrite are dense Arrow writes — STOP_COLUMN DF_MISSING must
        # materialize to NULL (same as pre-omit semantics), never raise or leak
        # the sentinel string into Parquet. Upsert path overlays sparse first.
        if mode not in upsert_modes:
            from connectors.writer_common import materialize_missing_as_null_for_dense_write

            mapped_rows = materialize_missing_as_null_for_dense_write(mapped_rows)

        arrow_cols = (
            list(final_arrow.names) if mode in upsert_modes else list(target_cols)
        )
        dict_rows = [_row_as_dict(arrow_cols, r) for r in mapped_rows]
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
                    # Append path: only Map columns present; extras stay None for INSERT.
                    # Upsert path: d must carry overlay values for dest-only fields.
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
    write_mode = _iceberg_effective_write_mode(
        write_mode,
        sync_mode=str(_kwargs.get("sync_mode") or ""),
        file_batch_idx=int(_kwargs.get("file_batch_idx") or 0),
    )

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
    from connectors.writer_common import resolve_studio_or_map_dest_types

    live_dest = _kwargs.get("destination_column_types") or _kwargs.get("schema_types")
    dest_types, studio_err = resolve_studio_or_map_dest_types(
        target_cols,
        mappings,
        column_types,
        logical_types=target_types,
        studio_types=live_dest if isinstance(live_dest, dict) else None,
        product="Iceberg",
        dest_db="iceberg",
    )
    policy = transform_error_policy(error_policy)
    # Map once after committed overlay — never Map-then-remap. Honesty: this
    # filesystem path still holds the mapped image until the snapshot write.
    mapped_rows: list[tuple] = []
    transform_errors: list[str] = []
    rejected_details: list[dict] = []
    # Defer strict abort until after committed-schema rematerialize.

    # Find current metadata version
    versions = sorted(meta_dir.glob("v*.metadata.json"))
    current_meta = _load_metadata(versions[-1]) if versions else None
    current_schema = (current_meta or {}).get("schemas", [{}])[-1] if current_meta else None
    if current_meta and "schema" in current_meta and not current_schema:
        current_schema = current_meta.get("schema")

    # Create-new metadata: partial Studio must not soft-bind Map string invent.
    if current_schema is None and studio_err:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=str(table_dir),
            checksum="",
            chunks_completed=0,
            error=studio_err,
            driver="iceberg",
        )

    # Rematerialize against committed carriers BEFORE evolve — never commit
    # additive Iceberg ``string`` invent then clobber Map stamps (partial Studio).
    evolve_notes: list[str] = []
    write_types = dict(dest_types or {})
    if current_schema:
        from connectors.writer_common import require_physical_types_for_existing_table

        committed_physical: dict[str, str] = {}
        for field in current_schema.get("fields") or []:
            name = str(field.get("name") or "")
            if not name:
                continue
            committed_physical[name] = _iceberg_type_to_logical_carrier(
                field.get("type")
            )
        existing_names = set(committed_physical)
        mapped_existing = [c for c in target_cols if c and c in existing_names]
        effective = dict(committed_physical)
        if isinstance(live_dest, dict):
            for c in mapped_existing:
                if (
                    effective.get(c)
                    or effective.get(str(c).lower())
                    or effective.get(str(c).upper())
                ):
                    continue
                st = str(live_dest.get(c) or "").strip()
                if st:
                    effective[c] = st
        if mapped_existing:
            phys_err = require_physical_types_for_existing_table(
                table_existed=True,
                physical=effective,
                dialect_label="Iceberg",
                target_cols=mapped_existing,
            )
            if phys_err:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table,
                    target_schema=str(table_dir),
                    checksum="",
                    chunks_completed=0,
                    error=phys_err,
                    rejected_details=rejected_details,
                    driver="iceberg",
                )
        _force_remap = bool(studio_err)
        remat = _iceberg_rematerialize_if_physical_differs(
            physical=effective if effective else committed_physical,
            dest_types=dest_types,
            target_cols=target_cols,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            logical_types=target_types,
            policy=policy,
            conflict_columns=conflict_columns,
            destination_column_nullability=_kwargs.get("destination_column_nullability"),
            force_remap=_force_remap,
        )
        if remat is not None:
            mapped_rows, transform_errors, rejected_details, dest_types = remat
        elif _force_remap:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table,
                target_schema=str(table_dir),
                checksum="",
                chunks_completed=0,
                error=(
                    "Iceberg live DDL incomplete for mapped columns — "
                    "refuse Map VARCHAR rematerialize invent. Re-run "
                    "destination schema introspect and retry."
                ),
                rejected_details=rejected_details,
                driver="iceberg",
            )

    if not mapped_rows:
        mapped_rows, transform_errors, rejected_details = _iceberg_map_rows(
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            target_cols=target_cols,
            column_types=column_types,
            dest_types=dest_types,
            policy=policy,
            conflict_columns=conflict_columns,
            destination_column_nullability=_kwargs.get("destination_column_nullability"),
        )

    try:
        schema_json, evolve_notes = _evolve_schema(
            current_schema,
            target_cols,
            dest_types,
            require_types=bool(studio_err),
        )
    except ValueError as evolve_exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=str(table_dir),
            checksum="",
            chunks_completed=0,
            error=str(evolve_exc),
            rejected_details=rejected_details,
            driver="iceberg",
        )
    # Always write Parquet/JSONL using committed field types — never diverge from
    # type_locked metadata (incoming dest_types may differ).
    write_types = _write_types_from_schema(schema_json, dest_types)

    _map_abort = reject_on_strict_policy(
        policy, rejected_details, "Iceberg", transform_errors
    )
    if _map_abort:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=str(table_dir),
            checksum="",
            chunks_completed=0,
            error=_map_abort,
            rejected_details=rejected_details,
            driver="iceberg",
        )

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
    schema_cols_all = [
        str(f.get("name") or "")
        for f in (schema_json.get("fields") or [])
        if f.get("name")
    ]
    schema_extra_fs = [c for c in schema_cols_all if c not in set(target_cols)]
    # Parity with catalog: overwrite of a partial Map NULL-wipes dest-only cols.
    if (
        write_mode in {"overwrite", "replace"}
        and current_meta
        and schema_extra_fs
    ):
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table,
            target_schema=str(table_dir),
            checksum="",
            chunks_completed=0,
            error=(
                "Iceberg overwrite Map omits destination columns "
                f"{schema_extra_fs[:12]} — would NULL-wipe them. "
                "Map every column or use upsert/merge to preserve unmapped fields."
            ),
            rejected_details=rejected_details,
            driver="iceberg",
        )
    if write_mode in {"overwrite", "replace"} and current_meta:
        # Drop prior data refs; keep schema evolution
        current_meta = None

    mode = (write_mode or "append").lower()
    upsert_modes = {"upsert", "merge", "cdc", "incremental_deduped"}
    mor_upsert = False
    pending_eq_rows: list[dict[str, Any]] = []
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
        # Shared dense empty-PK quarantine (parity with catalog path).
        from connectors.writer_common import partition_dense_upsert_rows

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
                    target_schema=str(table_dir),
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
                    target_schema=str(table_dir),
                    checksum="",
                    chunks_completed=1,
                    rejected_details=rejected_details,
                    rejected_rows=_rejected_row_count(
                        data_rows, mapped_rows, rejected_details, policy
                    ),
                    driver="iceberg",
                )
        # CoW rewrite must keep every committed schema field — Map-only load
        # would drop dest-only columns from the snapshot.
        schema_cols = [
            str(f.get("name") or "")
            for f in (schema_json.get("fields") or [])
            if f.get("name")
        ]
        load_cols = schema_cols or list(target_cols)
        existing_rows = _load_existing_rows(table_dir, load_cols, current_meta)
        incoming = [_row_as_dict(target_cols, r) for r in mapped_rows]
        try:
            merged = _merge_upsert_rows(existing_rows, incoming, pk_cols=pk_cols)
        except ValueError as exc:
            return WriteResult(
                ok=False,
                rows_written=0,
                table_name=table,
                target_schema=str(table_dir),
                checksum="",
                chunks_completed=0,
                error=str(exc)[:500],
                rejected_details=rejected_details,
                driver="iceberg",
            )
        prior_data_refs = [
            dict(ref)
            for ref in ((current_meta or {}).get("data-files") or [])
            if isinstance(ref, dict) and str(ref.get("path") or "").strip()
        ]
        if prior_data_refs:
            try:
                new_rows, pending_eq_rows = _mor_upsert_delta(
                    existing_rows, incoming, pk_cols=pk_cols
                )
            except ValueError as exc:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table,
                    target_schema=str(table_dir),
                    checksum="",
                    chunks_completed=0,
                    error=str(exc)[:500],
                    rejected_details=rejected_details,
                    driver="iceberg",
                )
            if not new_rows:
                return WriteResult(
                    ok=True,
                    rows_written=0,
                    table_name=table,
                    target_schema=str(table_dir),
                    checksum="",
                    chunks_completed=1,
                    rejected_details=rejected_details,
                    rejected_rows=_rejected_row_count(
                        data_rows, mapped_rows, rejected_details, policy
                    ),
                    driver="iceberg",
                )
            rel_path, n_written, checksum, file_warnings = _write_data_file(
                table_dir / "data",
                load_cols,
                new_rows,
                column_types=write_types,
            )
            mor_upsert = True
            operation = "overwrite"
            data_files = prior_data_refs + [
                {
                    "path": rel_path,
                    "record-count": n_written,
                    "checksum": checksum,
                }
            ]
        else:
            rel_path, n_written, checksum, file_warnings = _write_data_file(
                table_dir / "data", load_cols, merged, column_types=write_types
            )
            operation = "overwrite"
            data_files = [
                {"path": rel_path, "record-count": n_written, "checksum": checksum}
            ]
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

    compact = (
        not mor_upsert
        and (mode in upsert_modes or mode in {"overwrite", "replace"})
    )
    prior_deletes = [] if compact else _snapshot_delete_files(current_meta)
    prior_data = [dict(ref) for ref in data_files[:-1]] if data_files else []
    _ensure_data_file_sequences(prior_data)
    if compact:
        delete_files: list[dict[str, Any]] = []
        write_strategy = "copy-on-write"
        new_seq = 1
    else:
        delete_files = prior_deletes
        write_strategy = "merge-on-read" if (delete_files or pending_eq_rows or mor_upsert) else "copy-on-write"
        new_seq = _max_iceberg_sequence(prior_data, delete_files) + 1
    if mor_upsert and pending_eq_rows:
        try:
            eq_rel, eq_n, eq_ck = _write_equality_delete_file(
                table_dir / "data",
                list(pk_cols),
                pending_eq_rows,
                column_types=write_types,
            )
        except ValueError as exc:
            if "requires pyarrow" not in str(exc):
                raise
            rel_path, n_written, checksum, file_warnings = _write_data_file(
                table_dir / "data", load_cols, merged, column_types=write_types
            )
            data_files = [
                {
                    "path": rel_path,
                    "record-count": n_written,
                    "checksum": checksum,
                    "sequence-number": 1,
                }
            ]
            delete_files = []
            write_strategy = "copy-on-write"
            new_seq = 1
            prior_data = []
            pending_eq_rows = []
            mor_upsert = False
        else:
            delete_files.append(
                {
                    "path": eq_rel,
                    "content": 2,
                    "equality-ids": _equality_ids_for_columns(schema_json, pk_cols),
                    "sequence-number": new_seq,
                    "record-count": eq_n,
                    "checksum": eq_ck,
                }
            )
            write_strategy = "merge-on-read"
    if data_files:
        newest = dict(data_files[-1])
        newest["sequence-number"] = new_seq
        data_files = prior_data + [newest]

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
            "added-delete-files": str(1 if mor_upsert and pending_eq_rows else 0),
            "dataflow.checksum": checksum,
            "dataflow.write_mode": mode,
            "dataflow.write_strategy": write_strategy,
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
            "dataflow.write_strategy": write_strategy,
        },
        "data-files": data_files,
        "delete-files": delete_files,
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


def _iceberg_pk_columns(primary_key_column: str | Sequence[str]) -> list[str]:
    from services.cdc_snapshot_window import _pk_columns

    return _pk_columns(primary_key_column)


def _iceberg_row_pk(row: dict[str, Any], pk_cols: Sequence[str]) -> str | None:
    from services.cdc_snapshot_window import _pk_value

    return _pk_value(row, pk_cols)


def _iceberg_split_key(key: str, width: int) -> list[str]:
    """Split a leftover/CDC address into PK parts. Arity mismatch is fail-closed.

    Composite identity uses the CDC unit separator (same as SQL leftover
    MERGE). A comma-joined ``9,9`` or a single leftover of a 2-col PK is
    not a row identity — returning 0 here would look like an idempotent
    miss and let leftover rows survive.
    """
    from services.cdc_snapshot_window import _PK_SEP

    text = str(key)
    if width <= 1:
        return [text]
    parts = text.split(_PK_SEP)
    if len(parts) != width:
        raise ValueError(
            f"Iceberg composite delete key arity {len(parts)} != {width}"
        )
    return parts


def _iceberg_float_carrier(parsed: Decimal) -> float:
    """Iceberg Float/Double carrier after a successful write-path bind."""
    from services.transform_engine import float_carrier_or_refuse

    return float_carrier_or_refuse(parsed)


def _iceberg_typed_literal(tbl: Any, column: str, raw: Any) -> Any:
    """Bind a leftover/CDC key part to the Iceberg field type.

    Digit strings on a string PK must stay strings (LongLiteral cannot
    convert into string). Numbers, calendars, and booleans use the same
    write-path parsers as ``coerce_arrow_cell`` — ``Decimal(text)`` /
    ``fromisoformat`` / informal ``yes`` invented deletes the writer
    would not store. Fail closed on a missing field or a refused cell.
    """
    from services.arrow_write import _date_from_write_path, _datetime_from_write_path
    from services.transform_engine import apply_transform, decimal_wire_value, integer_wire_value

    field = tbl.schema().find_field(column, case_sensitive=False)
    ftype = getattr(field, "field_type", None)
    if ftype is None:
        raise ValueError(f"Iceberg delete: unknown field {column!r}")
    kind = type(ftype).__name__
    text = str(raw)
    try:
        if kind in {"IntegerType", "LongType"}:
            parsed_int = integer_wire_value(text)
            if parsed_int is None:
                raise ValueError("integer write path refused")
            return parsed_int
        if kind == "BooleanType":
            parsed_bool, err = apply_transform(text, "boolean")
            if parsed_bool is None or err:
                raise ValueError("boolean write path refused")
            return bool(parsed_bool)
        if kind in {"DoubleType", "FloatType"}:
            parsed_num = decimal_wire_value(text)
            if parsed_num is None:
                raise ValueError("float write path refused")
            return _iceberg_float_carrier(parsed_num)
        if kind == "DecimalType":
            parsed_dec = decimal_wire_value(text)
            if parsed_dec is None:
                raise ValueError("decimal write path refused")
            return parsed_dec
        if kind == "DateType":
            return _date_from_write_path(text)
        if kind == "TimestampType":
            parsed_ts = _datetime_from_write_path(text)
            return parsed_ts.replace(tzinfo=None) if parsed_ts.tzinfo is not None else parsed_ts
        if kind == "TimestamptzType":
            return _datetime_from_write_path(text)
        if kind == "UUIDType":
            import uuid as _uuid

            return _uuid.UUID(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Iceberg delete: {column!r} value {text!r} does not bind "
            f"to {kind}"
        ) from exc
    return text


def _iceberg_delete_predicate(tbl: Any, pk_cols: list[str], work_keys: set[str]) -> Any:
    """Dest-engine leftover/CDC delete predicate.

    Single column: ``IN``. Composite: ``OR`` of ``AND`` equalities — the
    same identity SQL leftover MERGE uses. A joined key whose arity does
    not match the PK is fail-closed (never a concatenated column name).
    """
    from pyiceberg.expressions import And, EqualTo, In, Or

    if not pk_cols or not work_keys:
        raise ValueError("Iceberg delete requires PK columns and keys")
    width = len(pk_cols)
    if width == 1:
        col = pk_cols[0]
        return In(col, [_iceberg_typed_literal(tbl, col, k) for k in work_keys])
    terms: list[Any] = []
    for key in work_keys:
        parts = _iceberg_split_key(key, width)
        equals = [
            EqualTo(pk_cols[i], _iceberg_typed_literal(tbl, pk_cols[i], parts[i]))
            for i in range(width)
        ]
        terms.append(And(*equals))
    if len(terms) == 1:
        return terms[0]
    return Or(*terms)


def delete_by_primary_keys(
    cfg: dict[str, Any],
    table_name: str,
    primary_key_column: str | Sequence[str],
    keys: list[str],
    schema: str | None = None,
    *,
    incoming_lsn: str | None = None,
    lsn_column: str = "_df_lsn",
) -> int:
    """CDC / leftover-MERGE delete with LSN guard (filesystem MoR + catalogs).

    ``primary_key_column`` is one column or an ordered composite. Composite
    keys use the CDC unit separator (same as SQL leftover MERGE), never a
    literal ``order_id,line_id`` column. Stale deletes that would wipe a
    newer ``_df_lsn`` row are skipped (at-least-once redelivery). Filesystem
    deletes write Iceberg v2 equality-delete files (content=2) and keep
    existing data files. Returns the number of surviving keys deleted after
    the LSN filter.
    """
    if not keys:
        return 0
    pk_cols = _iceberg_pk_columns(primary_key_column)
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
            pk_cols,
            key_set,
            incoming_lsn=incoming_lsn,
            lsn_column=lsn_column,
        )
    return _delete_filesystem(
        cfg,
        table_name,
        pk_cols,
        key_set,
        schema=schema,
        incoming_lsn=incoming_lsn,
        lsn_column=lsn_column,
    )


def _filter_delete_keys_by_lsn(
    rows: list[dict[str, Any]],
    pk_cols: Sequence[str],
    key_set: set[str],
    *,
    incoming_lsn: str | None,
    lsn_column: str,
) -> set[str]:
    from services.cdc_effectively_once import filter_keys_for_lsn_delete

    if not incoming_lsn:
        return set(key_set)
    existing: dict[str, Any] = {}
    for row in rows:
        pk = _iceberg_row_pk(row, pk_cols)
        if pk is not None and pk in key_set:
            existing[pk] = row.get(lsn_column)
    for key in key_set:
        existing.setdefault(key, None)
    kept = filter_keys_for_lsn_delete(list(key_set), existing, incoming_lsn)
    return {str(k) for k in kept}


def _delete_filesystem(
    cfg: dict[str, Any],
    table_name: str,
    pk_cols: list[str],
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
    for col in pk_cols:
        if col not in columns:
            columns.append(col)
    existing = _load_existing_rows(table_dir, columns, current_meta)
    work_keys = _filter_delete_keys_by_lsn(
        existing,
        pk_cols,
        key_set,
        incoming_lsn=incoming_lsn,
        lsn_column=lsn_column,
    )
    if not work_keys:
        return 0
    width = len(pk_cols)
    for key in work_keys:
        _iceberg_split_key(key, width)
    delete_rows = [
        {c: row.get(c) for c in pk_cols}
        for row in existing
        if (_iceberg_row_pk(row, pk_cols) or "") in work_keys
    ]
    if not delete_rows:
        return 0
    write_types = _write_types_from_schema(schema_json, {})
    try:
        rel_path, n_written, checksum = _write_equality_delete_file(
            table_dir / "data",
            list(pk_cols),
            delete_rows,
            column_types=write_types,
        )
    except ValueError as exc:
        if "requires pyarrow" not in str(exc):
            raise
        return _delete_filesystem_cow(
            table_dir,
            meta_dir,
            versions,
            current_meta,
            schema_json,
            columns,
            existing,
            work_keys,
            pk_cols,
            write_types,
        )
    data_files = [dict(ref) for ref in (current_meta.get("data-files") or []) if isinstance(ref, dict)]
    _ensure_data_file_sequences(data_files)
    delete_files = _snapshot_delete_files(current_meta)
    delete_seq = _max_iceberg_sequence(data_files, delete_files) + 1
    equality_ids = _equality_ids_for_columns(schema_json, pk_cols)
    delete_files.append(
        {
            "path": rel_path,
            "content": 2,
            "equality-ids": equality_ids,
            "sequence-number": delete_seq,
            "record-count": n_written,
            "checksum": checksum,
        }
    )
    snapshot_id = int(time.time() * 1000)
    snapshots = list(current_meta.get("snapshots") or [])
    snapshots.append({
        "snapshot-id": snapshot_id,
        "timestamp-ms": snapshot_id,
        "summary": {
            "operation": "delete",
            "added-delete-files": "1",
            "deleted-records": str(len(delete_rows)),
            "dataflow.checksum": checksum,
            "dataflow.write_mode": "cdc_delete",
            "dataflow.write_strategy": "merge-on-read",
            "dataflow.deleted_keys": str(len(delete_rows)),
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
        "data-files": data_files,
        "delete-files": delete_files,
        "properties": {
            **(current_meta.get("properties") or {}),
            "dataflow.write_mode": "cdc_delete",
            "dataflow.write_strategy": "merge-on-read",
        },
    }
    meta_path = meta_dir / f"v{new_version}.metadata.json"
    tmp = meta_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(metadata, indent=2, default=json_default), encoding="utf-8")
    os.replace(tmp, meta_path)
    (meta_dir / "version-hint.text").write_text(str(new_version), encoding="utf-8")
    return len(delete_rows)


def _delete_filesystem_cow(
    table_dir: Path,
    meta_dir: Path,
    versions: list[Path],
    current_meta: dict[str, Any],
    schema_json: dict[str, Any],
    columns: list[str],
    existing: list[dict[str, Any]],
    work_keys: set[str],
    pk_cols: list[str],
    write_types: dict[str, str],
) -> int:
    """CoW rewrite when equality-delete parquet cannot be written (no pyarrow)."""
    kept = [
        row
        for row in existing
        if (_iceberg_row_pk(row, pk_cols) or "") not in work_keys
    ]
    deleted = len(existing) - len(kept)
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
        "data-files": [
            {
                "path": rel_path,
                "record-count": n_written,
                "checksum": checksum,
                "sequence-number": 1,
            }
        ],
        "delete-files": [],
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
    pk_cols: list[str],
    key_set: set[str],
    *,
    incoming_lsn: str | None,
    lsn_column: str,
) -> int:
    from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config

    config = parse_iceberg_catalog_config(endpoint)
    catalog = load_catalog(endpoint)
    identifier = config["namespace"] + (config["table_name"],)
    tbl = catalog.load_table(identifier)
    work_keys = {str(k) for k in key_set}
    if incoming_lsn:
        # CDC LSN guard projects pk (+ lsn) only. Overwrite leftover MERGE
        # has no LSN and must not materialize scan().to_arrow() of the table.
        select_cols = list(dict.fromkeys([*pk_cols, lsn_column]))
        scanned = tbl.scan().select(*select_cols).to_arrow()
        rows: list[dict[str, Any]] = []
        for i in range(scanned.num_rows):
            rows.append(
                {
                    name: scanned.column(name)[i].as_py()
                    for name in scanned.column_names
                }
            )
        work_keys = _filter_delete_keys_by_lsn(
            rows,
            pk_cols,
            work_keys,
            incoming_lsn=incoming_lsn,
            lsn_column=lsn_column,
        )
        work_keys = {
            pk
            for row in rows
            if (pk := _iceberg_row_pk(row, pk_cols)) is not None and pk in work_keys
        }
    if not work_keys:
        return 0
    try:
        tbl.delete(delete_filter=_iceberg_delete_predicate(tbl, pk_cols, work_keys))
    except Exception:
        if len(pk_cols) != 1:
            raise
        from pyiceberg.types import StringType

        field = tbl.schema().find_field(pk_cols[0], case_sensitive=False)
        ftype = getattr(field, "field_type", None)
        # Quoted-string IN is only valid for string PKs. A numeric In()
        # failure falling through to strings would no-op leftover MERGE.
        if not isinstance(ftype, StringType):
            raise
        quoted = ", ".join("'" + str(k).replace("'", "''") + "'" for k in work_keys)
        tbl.delete(delete_filter=f"{pk_cols[0]} IN ({quoted})")
    return len(work_keys)
