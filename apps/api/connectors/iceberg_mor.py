"""Apache Iceberg v2 Merge-on-Read for dest COUNT and leftover listing.

Spark ``COUNT(*)`` uses manifest ``record-count`` and bails to a full scan
when delete files exist. Arithmetic ``data_footer − delete_record_count``
is a lie for overlapping position deletes and for equality deletes
(Iceberg #14864). Dest-engine population is:

* **Position deletes (content=1):** unique in-range ``(file_path, pos)``.
  Duplicate pos counts once. Out-of-range pos is a no-op. Applies when
  ``data_seq <= delete_seq`` (same-commit position deletes allowed).
* **Equality deletes (content=2):** match every ``equality_ids`` column
  (AND). Null matches null. Applies when ``data_seq < delete_seq``
  (strictly less — a row inserted in the same snapshot is not a phantom
  delete). Missing sequence numbers on equality deletes → unmeasured.
* **Deletion vectors (content=3 / puffin):** Planned / unmeasured.

Path matching: exact, URI suffix on the snapshot relative path, or unique
basename. Ambiguous basename is unmeasured. A missing delete file is
unmeasured (not dest=0). Catalog inspect with delete rows but no readable
parquet stays unmeasured — this module is the filesystem snapshot kernel.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Sequence

logger = logging.getLogger(__name__)

CONTENT_POSITION = 1
CONTENT_EQUALITY = 2
CONTENT_DELETION_VECTOR = 3

_FILE_PATH_NAMES = {"file_path", "file-path", "filepath"}
_POS_NAMES = {"pos", "position"}


class IcebergMorUnmeasured(Exception):
    """Fail-closed: dest COUNT / leftover listing cannot apply MoR honestly."""


@dataclass(frozen=True)
class _PositionDeleteFile:
    sequence_number: int | None
    rows: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class _EqualityDeleteFile:
    sequence_number: int | None
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


@dataclass
class _MorPlan:
    data_seq: dict[str, int | None]
    position: list[_PositionDeleteFile] = field(default_factory=list)
    equality: list[_EqualityDeleteFile] = field(default_factory=list)


def filesystem_mor_count(
    table_dir: Path,
    meta: dict[str, Any],
    data_files: Sequence[tuple[str, Path]],
    *,
    count_data_file: Callable[[Path], int | None],
    project_file: Callable[[Path, Sequence[str]], list[dict[str, Any]] | None],
) -> int | None:
    """Dest COUNT after applying current-snapshot delete files.

    Position-only uses Parquet/JSONL dest-engine population minus unique
    in-range pos — never data-page projection, never delete ``record-count``.
    Equality deletes project equality columns (required by the spec).
    """
    try:
        plan = _load_mor_plan(table_dir, meta, data_files)
        if plan.equality:
            cols = _equality_projection_columns(plan)
            surviving = _project_and_filter(plan, data_files, cols, project_file)
            return None if surviving is None else len(surviving)
        return _position_only_count(plan, data_files, count_data_file)
    except IcebergMorUnmeasured as exc:
        logger.info("iceberg MoR dest COUNT unmeasured: %s", exc)
        return None


def filesystem_mor_snapshot_rows(
    table_dir: Path,
    meta: dict[str, Any],
    data_files: Sequence[tuple[str, Path]],
    *,
    cols: Sequence[str],
    project_file: Callable[[Path, Sequence[str]], list[dict[str, Any]] | None],
) -> list[dict[str, Any]] | None:
    """Projected snapshot rows after MoR. Same population as dest COUNT."""
    wanted = [str(c) for c in cols if str(c).strip()]
    if not wanted:
        return []
    try:
        plan = _load_mor_plan(table_dir, meta, data_files)
        extra = _equality_projection_columns(plan) if plan.equality else []
        project_cols = list(wanted)
        for name in extra:
            if name not in project_cols:
                project_cols.append(name)
        surviving = _project_and_filter(
            plan, data_files, project_cols, project_file
        )
        if surviving is None:
            return None
        return [{col: row.get(col) for col in wanted} for row in surviving]
    except IcebergMorUnmeasured as exc:
        logger.info("iceberg MoR snapshot rows unmeasured: %s", exc)
        return None


def _load_mor_plan(
    table_dir: Path,
    meta: dict[str, Any],
    data_files: Sequence[tuple[str, Path]],
) -> _MorPlan:
    refs = list(meta.get("delete-files") or meta.get("delete_files") or [])
    if not refs:
        raise IcebergMorUnmeasured("no delete files")
    data_seq = _data_sequence_by_rel(meta)
    rels = [rel for rel, _path in data_files]
    plan = _MorPlan(data_seq=data_seq)
    field_names = _schema_field_names(meta)
    for ref in refs:
        if not isinstance(ref, dict):
            raise IcebergMorUnmeasured("delete-files entry is not an object")
        raw_path = str(
            ref.get("path") or ref.get("file_path") or ref.get("file-path") or ""
        ).strip()
        if not raw_path:
            raise IcebergMorUnmeasured("delete-files entry missing path")
        if _is_deletion_vector(ref, raw_path):
            raise IcebergMorUnmeasured("deletion vector / puffin unmeasured")
        resolved = _resolve_snapshot_file(table_dir, raw_path)
        if resolved is None:
            raise IcebergMorUnmeasured(f"delete file missing: {raw_path}")
        content = _content_code(ref)
        seq = _optional_int(
            ref.get("sequence-number")
            or ref.get("sequence_number")
            or ref.get("data-sequence-number")
        )
        names = _parquet_column_names(resolved)
        if names is None:
            raise IcebergMorUnmeasured(f"delete file unreadable: {raw_path}")
        inferred = _infer_content(names)
        if content is None:
            content = inferred
        if content == CONTENT_DELETION_VECTOR:
            raise IcebergMorUnmeasured("deletion vector content=3")
        if content == CONTENT_POSITION:
            if inferred != CONTENT_POSITION:
                raise IcebergMorUnmeasured("position delete schema missing file_path/pos")
            bound = _bind_position_rows(
                _read_position_rows(resolved, names), rels
            )
            plan.position.append(_PositionDeleteFile(seq, bound))
            continue
        if content == CONTENT_EQUALITY:
            eq_cols = _equality_columns(ref, names, field_names)
            if not eq_cols:
                raise IcebergMorUnmeasured("equality delete missing equality_ids")
            if seq is None:
                raise IcebergMorUnmeasured(
                    "equality delete missing sequence-number (data_seq < delete_seq required)"
                )
            for rel, _path in data_files:
                if data_seq.get(rel) is None:
                    raise IcebergMorUnmeasured(
                        "data-file missing sequence-number with equality deletes"
                    )
            plan.equality.append(
                _EqualityDeleteFile(
                    seq, eq_cols, _read_equality_rows(resolved, names, eq_cols)
                )
            )
            continue
        raise IcebergMorUnmeasured(f"unknown delete content {content!r}")
    return plan


def _position_only_count(
    plan: _MorPlan,
    data_files: Sequence[tuple[str, Path]],
    count_data_file: Callable[[Path], int | None],
) -> int:
    deleted = _position_pos_by_rel(plan, [rel for rel, _p in data_files])
    total = 0
    for rel, path in data_files:
        n = count_data_file(path)
        if n is None:
            raise IcebergMorUnmeasured(f"data-file population unmeasured: {rel}")
        dropped = {pos for pos in deleted.get(rel, set()) if 0 <= pos < n}
        total += n - len(dropped)
    return total


def _project_and_filter(
    plan: _MorPlan,
    data_files: Sequence[tuple[str, Path]],
    cols: Sequence[str],
    project_file: Callable[[Path, Sequence[str]], list[dict[str, Any]] | None],
) -> list[dict[str, Any]] | None:
    deleted_pos = _position_pos_by_rel(plan, [rel for rel, _p in data_files])
    out: list[dict[str, Any]] = []
    for rel, path in data_files:
        rows = project_file(path, cols)
        if rows is None:
            return None
        data_seq = plan.data_seq.get(rel)
        gone = deleted_pos.get(rel, set())
        for pos, row in enumerate(rows):
            if pos in gone:
                continue
            if _equality_deletes_row(plan, data_seq, row):
                continue
            out.append(row)
    return out


def _position_pos_by_rel(
    plan: _MorPlan, rels: Sequence[str]
) -> dict[str, set[int]]:
    out: dict[str, set[int]] = {rel: set() for rel in rels}
    for delete in plan.position:
        for rel in rels:
            if not _position_applies(plan.data_seq.get(rel), delete.sequence_number):
                continue
            for bound_rel, pos in delete.rows:
                if bound_rel == rel:
                    out[rel].add(pos)
    return out


def _bind_position_rows(
    rows: Sequence[tuple[str, int]], rels: Sequence[str]
) -> tuple[tuple[str, int], ...]:
    """Resolve file_path onto snapshot rels. Unmatched paths are no-ops."""
    out: list[tuple[str, int]] = []
    for file_path, pos in rows:
        bound = _match_data_rel(file_path, rels)
        if bound is not None:
            out.append((bound, pos))
    return tuple(out)


def _match_data_rel(file_path: str, rels: Sequence[str]) -> str | None:
    """Bind a position-delete file_path onto a current snapshot data-file rel.

    No match is a no-op (the data file was already replaced). Ambiguous
    basename among current snapshot files is unmeasured.
    """
    fp = _norm_path(file_path)
    if not fp:
        return None
    normalized = [_norm_path(rel) for rel in rels]
    exact: list[str] = []
    for rel, norm in zip(rels, normalized):
        if fp == norm or fp.endswith("/" + norm) or norm.endswith("/" + fp):
            exact.append(rel)
    unique_exact = list(dict.fromkeys(exact))
    if len(unique_exact) == 1:
        return unique_exact[0]
    if len(unique_exact) > 1:
        raise IcebergMorUnmeasured(
            f"ambiguous position-delete file_path {file_path!r}"
        )
    base = fp.rsplit("/", 1)[-1]
    hits = [rel for rel, norm in zip(rels, normalized) if norm.rsplit("/", 1)[-1] == base]
    unique_hits = list(dict.fromkeys(hits))
    if len(unique_hits) == 1:
        return unique_hits[0]
    if len(unique_hits) > 1:
        raise IcebergMorUnmeasured(
            f"ambiguous position-delete basename {base!r}"
        )
    return None


def _equality_deletes_row(
    plan: _MorPlan, data_seq: int | None, row: dict[str, Any]
) -> bool:
    for delete in plan.equality:
        applies = _equality_applies(data_seq, delete.sequence_number)
        if applies is None:
            raise IcebergMorUnmeasured("equality sequence incomplete")
        if not applies:
            continue
        probe = tuple(row.get(col) for col in delete.columns)
        for candidate in delete.rows:
            if _equality_tuple_match(probe, candidate):
                return True
    return False


def _equality_tuple_match(left: tuple[Any, ...], right: tuple[Any, ...]) -> bool:
    if len(left) != len(right):
        return False
    return all(_equality_value(a, b) for a, b in zip(left, right))


def _equality_value(left: Any, right: Any) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    if isinstance(left, bool) or isinstance(right, bool):
        return bool(left) is bool(right) and left == right
    if isinstance(left, (bytes, bytearray)) and isinstance(right, (bytes, bytearray)):
        return bytes(left) == bytes(right)
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        if isinstance(left, bool) or isinstance(right, bool):
            return False
        return left == right
    return left == right


def _position_applies(data_seq: int | None, delete_seq: int | None) -> bool:
    if data_seq is None or delete_seq is None:
        return True
    return data_seq <= delete_seq


def _equality_applies(data_seq: int | None, delete_seq: int | None) -> bool | None:
    if data_seq is None or delete_seq is None:
        return None
    return data_seq < delete_seq


def _equality_projection_columns(plan: _MorPlan) -> list[str]:
    seen: list[str] = []
    for delete in plan.equality:
        for col in delete.columns:
            if col not in seen:
                seen.append(col)
    return seen


def _data_sequence_by_rel(meta: dict[str, Any]) -> dict[str, int | None]:
    out: dict[str, int | None] = {}
    for ref in meta.get("data-files") or []:
        if not isinstance(ref, dict):
            continue
        rel = str(ref.get("path") or "").strip()
        if not rel:
            continue
        out[rel] = _optional_int(
            ref.get("sequence-number") or ref.get("sequence_number")
        )
    return out


def _schema_field_names(meta: dict[str, Any]) -> dict[int, str]:
    schema = (meta.get("schemas") or [{}])[-1] or meta.get("schema") or {}
    if not isinstance(schema, dict):
        schema = {}
    out: dict[int, str] = {}
    for field in schema.get("fields") or []:
        if not isinstance(field, dict):
            continue
        fid = field.get("id")
        name = field.get("name")
        if fid is None or not name:
            continue
        try:
            out[int(fid)] = str(name)
        except (TypeError, ValueError):
            continue
    return out


def _content_code(ref: dict[str, Any]) -> int | None:
    raw = ref.get("content")
    if raw is None:
        raw = ref.get("content-type") or ref.get("content_type")
    if raw is None:
        return None
    if isinstance(raw, str):
        token = raw.strip().lower().replace("_", "-").replace(" ", "-")
        if token in {"1", "position", "position-deletes", "position-delete"}:
            return CONTENT_POSITION
        if token in {"2", "equality", "equality-deletes", "equality-delete"}:
            return CONTENT_EQUALITY
        if token in {"3", "deletion-vector", "deletion-vectors", "dv", "puffin"}:
            return CONTENT_DELETION_VECTOR
        raise IcebergMorUnmeasured(f"unknown delete content {raw!r}")
    if isinstance(raw, bool):
        raise IcebergMorUnmeasured("boolean delete content")
    try:
        code = int(raw)
    except (TypeError, ValueError) as exc:
        raise IcebergMorUnmeasured(f"invalid delete content {raw!r}") from exc
    if code in {CONTENT_POSITION, CONTENT_EQUALITY, CONTENT_DELETION_VECTOR}:
        return code
    raise IcebergMorUnmeasured(f"unknown delete content {code}")


def _is_deletion_vector(ref: dict[str, Any], path: str) -> bool:
    fmt = str(
        ref.get("file-format") or ref.get("file_format") or ref.get("format") or ""
    ).strip().lower()
    if fmt in {"puffin", "deletion-vector", "deletion_vector"}:
        return True
    lowered = path.lower()
    return lowered.endswith(".puffin") or lowered.endswith(".puffin.gz")


def _infer_content(names: set[str]) -> int | None:
    lower = {n.lower() for n in names}
    has_fp = bool(lower & _FILE_PATH_NAMES)
    has_pos = bool(lower & _POS_NAMES)
    if has_fp and has_pos:
        return CONTENT_POSITION
    if has_fp or has_pos:
        raise IcebergMorUnmeasured("incomplete position-delete schema")
    if names:
        return CONTENT_EQUALITY
    raise IcebergMorUnmeasured("delete file has no columns")


def _equality_columns(
    ref: dict[str, Any],
    parquet_names: set[str],
    field_names: dict[int, str],
) -> tuple[str, ...]:
    raw_ids = ref.get("equality-ids")
    if raw_ids is None:
        raw_ids = ref.get("equality_ids")
    if raw_ids is None or raw_ids == "":
        skip = _FILE_PATH_NAMES | _POS_NAMES
        cols = tuple(
            n for n in sorted(parquet_names) if n.lower() not in skip
        )
        return cols
    if isinstance(raw_ids, str):
        raise IcebergMorUnmeasured("equality-ids must be a list of field ids")
    try:
        ids = [int(x) for x in raw_ids]
    except (TypeError, ValueError) as exc:
        raise IcebergMorUnmeasured("invalid equality-ids") from exc
    if not ids:
        raise IcebergMorUnmeasured("empty equality-ids")
    cols: list[str] = []
    lower_parquet = {n.lower(): n for n in parquet_names}
    for fid in ids:
        name = field_names.get(fid)
        if name is None:
            raise IcebergMorUnmeasured(f"equality-id {fid} not in snapshot schema")
        actual = name if name in parquet_names else lower_parquet.get(name.lower())
        if actual is None:
            raise IcebergMorUnmeasured(
                f"equality column {name!r} missing from delete file"
            )
        cols.append(actual)
    return tuple(cols)


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise IcebergMorUnmeasured("boolean sequence-number")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise IcebergMorUnmeasured(f"invalid sequence-number {value!r}") from exc


def _norm_path(path: str) -> str:
    return str(path or "").replace("\\", "/").strip().rstrip("/")


def _resolve_snapshot_file(table_dir: Path, rel: str) -> Path | None:
    raw = str(rel or "").strip()
    if raw.startswith("file:"):
        raw = raw.split(":", 1)[1]
        if raw.startswith("//"):
            raw = raw[2:]
    if not raw:
        return None
    candidates = [Path(raw), table_dir / raw]
    for path in candidates:
        if path.is_file():
            return path
    return None


def _parquet_column_names(path: Path) -> set[str] | None:
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return None
    suffix = path.suffix.lower()
    if suffix not in {".parquet", ".parq"}:
        raise IcebergMorUnmeasured(f"delete file is not parquet: {path.name}")
    try:
        pf = pq.ParquetFile(str(path))
        schema = getattr(pf, "schema_arrow", None) or getattr(pf, "schema", None)
        names = getattr(schema, "names", None) or ()
        return {str(n) for n in names}
    except IcebergMorUnmeasured:
        raise
    except Exception as exc:
        logger.info("iceberg MoR delete parquet schema failed: %s", exc)
        return None


def _pick_column(names: set[str], wanted: set[str]) -> str:
    lower = {n.lower(): n for n in names}
    for token in wanted:
        if token in names:
            return token
        hit = lower.get(token)
        if hit is not None:
            return hit
    raise IcebergMorUnmeasured(f"delete parquet missing columns {sorted(wanted)}")


def _read_position_rows(
    path: Path, names: set[str]
) -> tuple[tuple[str, int], ...]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise IcebergMorUnmeasured("pyarrow missing") from exc
    fp_col = _pick_column(names, _FILE_PATH_NAMES)
    pos_col = _pick_column(names, _POS_NAMES)
    try:
        table = pq.ParquetFile(str(path)).read(columns=[fp_col, pos_col])
    except Exception as exc:
        raise IcebergMorUnmeasured(f"position delete unreadable: {path.name}") from exc
    files = table.column(fp_col).to_pylist()
    positions = table.column(pos_col).to_pylist()
    out: list[tuple[str, int]] = []
    for file_path, pos in zip(files, positions):
        if file_path is None or pos is None:
            continue
        try:
            out.append((str(file_path), int(pos)))
        except (TypeError, ValueError) as exc:
            raise IcebergMorUnmeasured("position delete pos is not an integer") from exc
    return tuple(out)


def _read_equality_rows(
    path: Path, names: set[str], columns: Sequence[str]
) -> tuple[tuple[Any, ...], ...]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise IcebergMorUnmeasured("pyarrow missing") from exc
    missing = [c for c in columns if c not in names]
    if missing:
        lower = {n.lower(): n for n in names}
        resolved: list[str] = []
        for col in columns:
            if col in names:
                resolved.append(col)
            elif col.lower() in lower:
                resolved.append(lower[col.lower()])
            else:
                raise IcebergMorUnmeasured(
                    f"equality column {col!r} missing from delete file"
                )
        columns = resolved
    try:
        table = pq.ParquetFile(str(path)).read(columns=list(columns))
    except Exception as exc:
        raise IcebergMorUnmeasured(f"equality delete unreadable: {path.name}") from exc
    pylist = table.to_pylist()
    out: list[tuple[Any, ...]] = []
    for rec in pylist:
        if not isinstance(rec, dict):
            raise IcebergMorUnmeasured("equality delete row is not a struct")
        out.append(tuple(rec.get(col) for col in columns))
    return tuple(out)


def inspect_delete_refs(delete_table: Any) -> list[dict[str, Any]] | None:
    """Parse pyiceberg ``inspect.delete_files()`` into filesystem MoR refs.

    ``None`` means the inspect table cannot be applied honestly (no
    ``file_path`` column — the catalog fake that only stamps
    ``num_rows``). Empty list means no delete files.
    """
    n = int(getattr(delete_table, "num_rows", 0) or 0)
    if n == 0:
        return []
    paths = _inspect_pylist(delete_table, "file_path")
    if paths is None:
        return None
    contents = _inspect_pylist(delete_table, "content")
    formats = _inspect_pylist(delete_table, "file_format")
    eq_ids = _inspect_pylist(delete_table, "equality_ids")
    refs: list[dict[str, Any]] = []
    for i, path in enumerate(paths):
        raw = str(path or "").strip()
        if not raw:
            return None
        ref: dict[str, Any] = {"path": raw, "file_path": raw}
        if contents is not None and i < len(contents) and contents[i] is not None:
            ref["content"] = contents[i]
        if formats is not None and i < len(formats) and formats[i] is not None:
            ref["file-format"] = formats[i]
        if eq_ids is not None and i < len(eq_ids) and eq_ids[i] is not None:
            ref["equality-ids"] = list(eq_ids[i])
        refs.append(ref)
    return refs


def inspect_sequence_by_path(entries_table: Any) -> dict[str, int]:
    """``inspect.entries()`` sequence_number keyed by data_file.file_path."""
    out: dict[str, int] = {}
    if entries_table is None:
        return out
    seqs = _inspect_pylist(entries_table, "sequence_number")
    files = _inspect_pylist(entries_table, "data_file")
    if seqs is None or files is None:
        return out
    for seq, data_file in zip(seqs, files):
        path = ""
        if isinstance(data_file, dict):
            path = str(data_file.get("file_path") or "").strip()
        elif data_file is not None:
            path = str(getattr(data_file, "file_path", "") or "").strip()
        if not path or seq is None:
            continue
        try:
            out[path] = int(seq)
        except (TypeError, ValueError):
            continue
    return out


def _inspect_pylist(table: Any, column: str) -> list[Any] | None:
    getter = getattr(table, "column", None)
    if not callable(getter):
        return None
    try:
        col = getter(column)
        return list(col.to_pylist())
    except Exception:
        return None
