"""Local CSV/TSV identity COPY into PostgreSQL, MySQL, or SQLite.

S3 already GET-then-COPY CSV. Local files were stuck on the row path even
when every mapping was a no-op carry — the same wire, without the GET.
This module is that missing local path.

Full refresh: parse the file once into a mapped CSV (header = dest columns),
then the dest-native bulk load (SQLite ``executemany``, PostgreSQL
``COPY FROM STDIN``, MySQL STRICT ``LOAD DATA``). Dest ``COUNT(*)`` must
equal the mapped source COUNT. Occupied append whose COUNT already
equals the source COUNT is skip-complete; a different COUNT declines
(row path keeps quarantine). Empty files decline so the row path still
raises ``No records found``.

Incremental: the file has no SQL WHERE. Filter with the same
``records_after_watermark`` the row path uses, COPY the delta into
staging (``replace_destination=True``), then the existing
append / ON CONFLICT / ON DUPLICATE apply. Unbounded cursor cells
refuse. Empty delta is a measured no-op.

json / jsonl / yaml / fixed_width / excel parse with the same identity
readers as the row path, then reuse this mapped-CSV dest load. Nested
JSON/YAML cells decline (row path keeps quarantine). Excel nested cells
are already strings on the row path. Windowed ``ReadOptions``
(skip_rows / skip_footer / non-default header_row) decline. gzip is
decompressed, then the same mapped COPY. Legacy ``.xls`` declines.
"""

from __future__ import annotations

import csv
import gzip
import io
import logging
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from services.brand_env import getenv_brand
from services.copy_fast_path import FastPathResult, FastPathUnavailable
from services.copy_incremental import (
    COPY_INCREMENTAL_MODES,
    _apply_staging_to_mysql,
    _apply_staging_to_pg,
    APPEND_PROOF_SCOPE,
    _finish_sqlite_incremental_staging,
    _prepare_sqlite_incremental_dest,
    _require_mapped_cursor,
    _stamp_incremental,
)
from services.copy_pg_mysql import mapping_is_plain_carry, pg_type_is_load_safe
from services.copy_s3_common import s3_iter_delimited_rows
from services.copy_upsert import UPSERT_PROOF_SCOPE, staging_table_name
from services.sync_cursor import (
    is_append_sync,
    is_overwrite_sync,
    records_after_watermark,
    requires_incremental,
)

logger = logging.getLogger(__name__)

_CSV_TYPES = frozenset({"csv", "tsv"})
_JSON_TYPES = frozenset({"json", "jsonl", "ndjson"})
_TABULAR_TYPES = _CSV_TYPES | _JSON_TYPES | frozenset({"yaml", "fixed_width", "excel"})
_SQL_DEST = frozenset({"sqlite", "postgresql", "postgres", "mysql", "mariadb"})
_FILTER_BATCH = 5000


def csv_local_copy_enabled() -> bool:
    raw = (getenv_brand("CSV_LOCAL_COPY", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def csv_local_copy_batch() -> int:
    raw = (getenv_brand("CSV_LOCAL_COPY_BATCH", "5000") or "5000").strip()
    try:
        return max(1, min(int(raw), 20_000))
    except ValueError:
        return 5000


def identity_csv_copy_route(file_type: str, dest_type: str) -> bool:
    """True when local CSV/TSV identity COPY is proven for this pair."""
    src = (file_type or "").strip().lower()
    dest = (dest_type or "").strip().lower()
    return src in _CSV_TYPES and dest in _SQL_DEST


def identity_file_copy_route(file_type: str, dest_type: str) -> bool:
    """True when local tabular identity COPY is proven for this pair.

    csv/tsv are the COPY-native wire. json/jsonl/yaml/fixed_width/excel parse
    with the identity readers, then reuse that wire. Nested JSON/YAML cells
    decline. Excel uses the same ``iter_excel_dicts`` population as the row
    path (blank/formatting rows are not records).
    """
    src = (file_type or "").strip().lower()
    dest = (dest_type or "").strip().lower()
    return src in _TABULAR_TYPES and dest in _SQL_DEST


def _file_kind_token(file_type: str) -> str:
    kind = (file_type or "csv").strip().lower()
    if kind in _CSV_TYPES:
        return "csv"
    if kind in _JSON_TYPES:
        return "json_records"
    if kind == "yaml":
        return "yaml_records"
    if kind == "fixed_width":
        return "fwf_records"
    if kind == "excel":
        return "excel_records"
    return "csv"


def csv_copy_load_method(dest_type: str, sync_mode: str) -> str:
    return file_copy_load_method("csv", dest_type, sync_mode)


def file_copy_load_method(file_type: str, dest_type: str, sync_mode: str) -> str:
    dest = (dest_type or "").strip().lower()
    prefix = _file_kind_token(file_type)
    if dest in {"postgresql", "postgres"}:
        base = f"{prefix}_copy_from_stdin_pg" if prefix != "csv" else "csv_copy_from_stdin_pg"
    elif dest in {"mysql", "mariadb"}:
        base = f"{prefix}_load_data_mysql" if prefix != "csv" else "csv_load_data_mysql"
    else:
        base = (
            f"{prefix}_executemany_sqlite" if prefix != "csv" else "csv_executemany_sqlite"
        )
    mode = (sync_mode or "").strip().lower()
    if mode in COPY_INCREMENTAL_MODES:
        return f"{base}_{mode}"
    return base


def _csv_ext(filename: str) -> str:
    name = str(filename or "").rsplit("/", 1)[-1].lower()
    if name.endswith(".tsv") or name.endswith(".tsv.gz"):
        return "tsv"
    return "csv"


def _empty_to_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if text == "" or text == "\\N":
        return None
    return text


def _csv_cell(value: str | None) -> str:
    return "\\N" if value is None else value


def _read_options_copy_ok(read_options: Any) -> bool:
    if read_options is None:
        return True
    header_row = int(getattr(read_options, "header_row", 1) or 1)
    skip_rows = int(getattr(read_options, "skip_rows", 0) or 0)
    skip_footer = int(getattr(read_options, "skip_footer", 0) or 0)
    if header_row != 1 or skip_rows or skip_footer:
        return False
    return True


def _file_dest_pk(pairs: list[tuple[str, str]], pk_column: str) -> str:
    from services.copy_incremental import mapped_pair

    pk = (pk_column or "").strip()
    if not pk:
        raise FastPathUnavailable(
            "incremental COPY requires exactly one mapped primary key"
        )
    mapped = mapped_pair(pairs, pk)
    if mapped is None:
        raise FastPathUnavailable(
            "incremental COPY requires exactly one mapped primary key"
        )
    return mapped[1]


@contextmanager
def _csv_text(
    content: bytes | str | os.PathLike,
    *,
    encoding: str,
) -> Iterator[Any]:
    binary: Any
    closer = None
    if isinstance(content, (bytes, bytearray)):
        raw = bytes(content)
        if raw[:2] == b"\x1f\x8b":
            binary = gzip.GzipFile(fileobj=io.BytesIO(raw))
            closer = binary
        else:
            binary = io.BytesIO(raw)
    else:
        path = os.fspath(content)
        handle = open(path, "rb")
        head = handle.read(2)
        handle.seek(0)
        if head == b"\x1f\x8b":
            binary = gzip.GzipFile(fileobj=handle)
            closer = binary
        else:
            binary = handle
            closer = handle
    text = io.TextIOWrapper(binary, encoding=encoding, errors="strict", newline="")
    try:
        yield text
    finally:
        try:
            text.close()
        except Exception:
            logger.debug("CSV text close skipped", exc_info=True)
        if closer is not None:
            try:
                closer.close()
            except Exception:
                logger.debug("CSV binary close skipped", exc_info=True)


@contextmanager
def _open_binary(content: bytes | str | os.PathLike) -> Iterator[Any]:
    closer = None
    if isinstance(content, (bytes, bytearray)):
        raw = bytes(content)
        if raw[:2] == b"\x1f\x8b":
            handle: Any = gzip.GzipFile(fileobj=io.BytesIO(raw))
            closer = handle
        else:
            handle = io.BytesIO(raw)
    else:
        path = os.fspath(content)
        raw_handle = open(path, "rb")
        head = raw_handle.read(2)
        raw_handle.seek(0)
        if head == b"\x1f\x8b":
            handle = gzip.GzipFile(fileobj=raw_handle)
            closer = handle
        else:
            handle = raw_handle
            closer = raw_handle
    try:
        yield handle
    finally:
        if closer is not None:
            try:
                closer.close()
            except Exception:
                logger.debug("COPY binary close skipped", exc_info=True)


def _identity_cell(value: Any, *, column: str) -> str | None:
    if isinstance(value, (dict, list, tuple, set, frozenset)):
        raise FastPathUnavailable(
            f"nested value in {column!r} is not COPY-identity "
            "(row path keeps quarantine)"
        )
    if value is None:
        return None
    if isinstance(value, str):
        return _empty_to_none(value)
    from services.value_serializer import present_cell_text

    return present_cell_text(value)


def _project_record(
    raw: dict[str, Any], source_cols: list[str]
) -> dict[str, str | None]:
    index = {str(k).lower(): k for k in raw.keys()}
    out: dict[str, str | None] = {}
    for col in source_cols:
        key = index.get(col.lower())
        if key is None:
            out[col] = None
            continue
        out[col] = _identity_cell(raw.get(key), column=col)
    return out


def _iter_json_records(
    content: bytes | str | os.PathLike,
    source_cols: list[str],
) -> Iterator[dict[str, str | None]]:
    from services.json_tabular import iter_json_record_dicts

    for batch in iter_json_record_dicts(_open_binary, content, chunk_size=_FILTER_BATCH):
        for raw in batch:
            if not isinstance(raw, dict):
                raise FastPathUnavailable("JSON COPY requires an array of objects")
            yield _project_record(raw, source_cols)


def _iter_jsonl_records(
    content: bytes | str | os.PathLike,
    source_cols: list[str],
    read_options: Any = None,
) -> Iterator[dict[str, str | None]]:
    from services.value_serializer import json_loads_exact

    encoding = str(getattr(read_options, "encoding", "") or "").strip() or "utf-8"
    with _csv_text(content, encoding=encoding) as handle:
        for line_no, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            obj = json_loads_exact(text)
            if not isinstance(obj, dict):
                raise FastPathUnavailable(
                    f"JSONL line {line_no} is not an object (COPY-identity)"
                )
            yield _project_record(obj, source_cols)


def _iter_yaml_records(
    content: bytes | str | os.PathLike,
    source_cols: list[str],
    read_options: Any = None,
) -> Iterator[dict[str, str | None]]:
    from services.yaml_tabular import YAMLTabularError, iter_yaml_dicts

    encoding = str(getattr(read_options, "encoding", "") or "").strip() or "utf-8"
    try:
        for raw in iter_yaml_dicts(content, encoding=encoding):
            if not isinstance(raw, dict):
                continue
            yield _project_record(raw, source_cols)
    except YAMLTabularError as exc:
        raise FastPathUnavailable(str(exc)) from exc


def _iter_fwf_records(
    content: bytes | str | os.PathLike,
    source_cols: list[str],
    read_options: Any = None,
) -> Iterator[dict[str, str | None]]:
    from services.fixed_width_layout import FixedWidthError, iter_fixed_width_dicts

    encoding = str(getattr(read_options, "encoding", "") or "").strip() or "utf-8"
    layout = getattr(read_options, "fixed_width_layout", ()) if read_options else ()
    try:
        for raw in iter_fixed_width_dicts(content, layout, encoding=encoding):
            if not isinstance(raw, dict):
                continue
            yield _project_record(raw, source_cols)
    except FixedWidthError as exc:
        raise FastPathUnavailable(str(exc)) from exc


def _iter_excel_records(
    content: bytes | str | os.PathLike,
    filename: str,
    source_cols: list[str],
    read_options: Any = None,
) -> Iterator[dict[str, str | None]]:
    from services.excel_parser import iter_excel_dicts, require_xlsx
    from services.read_options import ReadOptionsError

    try:
        require_xlsx(filename)
        with _open_binary(content) as handle:
            raw = handle.read()
        for rec in iter_excel_dicts(raw, read_options):
            if not isinstance(rec, dict):
                continue
            yield _project_record(rec, source_cols)
    except (ValueError, ReadOptionsError) as exc:
        raise FastPathUnavailable(str(exc)) from exc


def _iter_source_records(
    content: bytes | str | os.PathLike,
    filename: str,
    pairs: list[tuple[str, str]],
    read_options: Any = None,
    *,
    file_type: str = "",
) -> Iterator[dict[str, str | None]]:
    kind = (file_type or "").strip().lower()
    if not kind:
        from services.file_parser import FileParser

        raw = content if isinstance(content, (bytes, bytearray)) else b""
        kind = FileParser.detect_file_type(filename, raw or None)
    source_cols = [p[0] for p in pairs]
    if kind in _JSON_TYPES and kind != "json":
        yield from _iter_jsonl_records(content, source_cols, read_options)
        return
    if kind == "json":
        yield from _iter_json_records(content, source_cols)
        return
    if kind == "yaml":
        yield from _iter_yaml_records(content, source_cols, read_options)
        return
    if kind == "fixed_width":
        yield from _iter_fwf_records(content, source_cols, read_options)
        return
    if kind == "excel":
        yield from _iter_excel_records(content, filename, source_cols, read_options)
        return
    yield from _iter_csv_source_records(
        content, filename, pairs, read_options
    )


def _iter_csv_source_records(
    content: bytes | str | os.PathLike,
    filename: str,
    pairs: list[tuple[str, str]],
    read_options: Any = None,
) -> Iterator[dict[str, str | None]]:
    from services.csv_profiler import detect_delimiter, detect_encoding

    sample = b""
    if isinstance(content, (bytes, bytearray)):
        raw = bytes(content)
        if raw[:2] == b"\x1f\x8b":
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
                sample = gz.read(8192)
        else:
            sample = raw[:8192]
    else:
        path = os.fspath(content)
        with open(path, "rb") as handle:
            head = handle.read(2)
            handle.seek(0)
            if head == b"\x1f\x8b":
                with gzip.GzipFile(fileobj=handle) as gz:
                    sample = gz.read(8192)
            else:
                sample = handle.read(8192)
    opts_enc = str(getattr(read_options, "encoding", "") or "").strip()
    encoding = opts_enc or detect_encoding(sample)
    opts_delim = str(getattr(read_options, "delimiter", "") or "")
    if opts_delim == "tab":
        opts_delim = "\t"
    sniff = sample.decode(encoding, errors="replace")
    delim = opts_delim or detect_delimiter(sniff) or (
        "\t" if _csv_ext(filename) == "tsv" else ","
    )
    source_cols = [p[0] for p in pairs]
    with _csv_text(content, encoding=encoding) as handle:
        reader = csv.reader(handle, delimiter=delim)
        header = next(reader, None)
        if not header:
            raise FastPathUnavailable("CSV file has no header row")
        index: dict[str, int] = {}
        for i, name in enumerate(header):
            key = str(name or "").strip()
            if not key:
                continue
            if key.lower() in index:
                raise FastPathUnavailable(f"CSV header {key!r} is duplicated")
            index[key.lower()] = i
        missing = [c for c in source_cols if c.lower() not in index]
        if missing:
            raise FastPathUnavailable(f"CSV header missing mapped column {missing[0]!r}")
        positions = [index[c.lower()] for c in source_cols]
        for row in reader:
            if not row or all(str(c).strip() == "" for c in row):
                continue
            record: dict[str, str | None] = {}
            for col, pos in zip(source_cols, positions, strict=True):
                cell = row[pos] if pos < len(row) else ""
                record[col] = _empty_to_none(cell)
            yield record


def _write_mapped_csv(
    content: bytes | str | os.PathLike,
    filename: str,
    pairs: list[tuple[str, str]],
    dest_path: str,
    *,
    read_options: Any = None,
    incremental: bool = False,
    cursor_column: str = "",
    watermark: str | None = None,
    pk_column: str = "",
    file_type: str = "",
) -> int:
    """Write dest-ordered CSV with HEADER. Returns data-row COUNT."""
    kind = (file_type or "").strip().lower()
    delim = "\t" if kind == "tsv" or (not kind and _csv_ext(filename) == "tsv") else ","
    source_cols = [p[0] for p in pairs]
    dest_cols = [p[1] for p in pairs]
    count = 0
    unbounded = 0
    pending: list[dict[str, str | None]] = []

    def _flush(rows: list[dict[str, str | None]], writer: Any) -> int:
        written = 0
        for rec in rows:
            writer.writerow([_csv_cell(rec.get(col)) for col in source_cols])
            written += 1
        return written

    with open(dest_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=delim, lineterminator="\n")
        writer.writerow(dest_cols)
        for record in _iter_source_records(
            content, filename, pairs, read_options, file_type=file_type
        ):
            if incremental:
                pending.append(record)
                if len(pending) >= _FILTER_BATCH:
                    delta, n = records_after_watermark(
                        pending,
                        cursor_column,
                        watermark,
                        primary_key=pk_column,
                    )
                    unbounded += n
                    count += _flush(delta, writer)
                    pending = []
            else:
                count += _flush([record], writer)
        if pending:
            delta, n = records_after_watermark(
                pending,
                cursor_column,
                watermark,
                primary_key=pk_column,
            )
            unbounded += n
            count += _flush(delta, writer)
    if incremental and unbounded:
        raise ValueError(
            f"{unbounded} row(s) carry no value for cursor "
            f"'{cursor_column}' — an incremental read cannot "
            "prove whether they already landed. Fill the cursor "
            "column at the source, or run this sync as full "
            "refresh."
        )
    return count


@contextmanager
def _mapped_csv_file(
    content: bytes | str | os.PathLike,
    filename: str,
    pairs: list[tuple[str, str]],
    *,
    read_options: Any = None,
    incremental: bool = False,
    cursor_column: str = "",
    watermark: str | None = None,
    pk_column: str = "",
    file_type: str = "",
) -> Iterator[tuple[str, int, str]]:
    kind = (file_type or "").strip().lower()
    ext = "tsv" if kind == "tsv" or (not kind and _csv_ext(filename) == "tsv") else "csv"
    fd, path = tempfile.mkstemp(prefix="df-csv-local-", suffix=f".{ext}")
    os.close(fd)
    try:
        count = _write_mapped_csv(
            content,
            filename,
            pairs,
            path,
            read_options=read_options,
            incremental=incremental,
            cursor_column=cursor_column,
            watermark=watermark,
            pk_column=pk_column,
            file_type=file_type,
        )
        yield path, count, ext
    finally:
        try:
            os.unlink(path)
        except OSError:
            logger.debug("mapped CSV tempfile unlink skipped", exc_info=True)


def _snapshot(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    out = {
        "copy_workers": 1,
        "copy_split": "serial",
        "copy_partitions": 1,
        "partitions_skipped": 0,
        "partitions_loaded": 1,
        "shard_mode": "file",
        "csv_read": "local",
    }
    if extra:
        out.update(extra)
    return out


def _count_result(source_count: int, dest_count: int, extra: dict[str, Any] | None = None) -> FastPathResult:
    proof = f"dest_count:{dest_count}"
    return FastPathResult(
        rows_copied=dest_count,
        source_rows=source_count,
        source_checksum=proof,
        target_rows=dest_count,
        target_checksum=proof,
        source_snapshot=_snapshot(extra),
        proof_scope="dest_count_equals_source_snapshot_count",
    )


def _empty_incremental(dest_count_before: int, mode: str, extra: dict[str, Any] | None = None) -> FastPathResult:
    proof = f"dest_count:{dest_count_before}"
    result = FastPathResult(
        rows_copied=0,
        source_rows=0,
        source_checksum=proof,
        target_rows=dest_count_before,
        target_checksum=proof,
        source_snapshot=_snapshot(extra),
        proof_scope=(
            APPEND_PROOF_SCOPE if mode == "incremental_append" else UPSERT_PROOF_SCOPE
        ),
    )
    return _stamp_incremental(
        result,
        watermark="",
        dest_count=dest_count_before,
        dest_count_before=dest_count_before,
        staging_count=0,
        sync_mode=mode,
        proof_scope=result.proof_scope,
    )


def copy_csv_to_sqlite(
    *,
    mapped_path: str,
    ext: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    sqlite_ddls: list[str],
    source_count: int,
    replace_destination: bool,
) -> FastPathResult:
    """Mapped local CSV into SQLite executemany. Dest COUNT(*) is the proof."""
    from services.copy_sqlite_common import (
        sqlite_bind_from_text,
        sqlite_connect,
        sqlite_create_sql,
        sqlite_ident,
        sqlite_pragma_types,
        sqlite_resolved_path,
        sqlite_table_exists,
        sqlite_type_is_copy_safe,
        skip_complete_sqlite,
    )

    if not pairs or len(pairs) != len(sqlite_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    for ddl in sqlite_ddls:
        if not sqlite_type_is_copy_safe(ddl):
            raise FastPathUnavailable(f"dest DDL {ddl} is not SQLite COPY-safe")
    sqlite_resolved_path(dest_cfg)
    if source_count == 0:
        raise FastPathUnavailable("empty CSV stays on the row path")

    target_cols = [p[1] for p in pairs]
    dest_ref = sqlite_ident(dest_table)
    col_sql = ", ".join(sqlite_ident(c) for c in target_cols)
    placeholders = ", ".join(["?"] * len(target_cols))
    insert_sql = f"INSERT INTO {dest_ref} ({col_sql}) VALUES ({placeholders})"  # nosec B608
    converters = [sqlite_bind_from_text(ddl) for ddl in sqlite_ddls]
    delim = "\t" if ext == "tsv" else ","
    batch_size = csv_local_copy_batch()

    dest_conn = sqlite_connect(dest_cfg)
    created_here = False
    try:
        dest_conn.execute("BEGIN IMMEDIATE")
        exists = sqlite_table_exists(dest_conn, dest_table)
        dest_count_before = 0
        if exists:
            dest_count_before = int(
                dest_conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
            )
        dest_occupied = dest_count_before > 0
        if dest_occupied and not replace_destination:
            if dest_count_before == source_count:
                dest_conn.rollback()
                return skip_complete_sqlite(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={"csv_read": "skip", "sqlite_write": "skip"},
                )
            raise FastPathUnavailable(
                "append into occupied SQLite dest stays on the row path "
                "(identity COPY would duplicate)"
            )
        if replace_destination and exists:
            dest_conn.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
            exists = False
        if exists:
            live = sqlite_pragma_types(dest_conn, dest_table)
            live_l = {k.lower(): v for k, v in live.items()}
            for col in target_cols:
                declared = live_l.get(col.lower())
                if declared is None:
                    raise FastPathUnavailable(f"dest column {col!r} absent")
                if not sqlite_type_is_copy_safe(declared):
                    raise FastPathUnavailable(
                        f"dest column {col!r} type {declared} is not SQLite COPY-safe"
                    )
        else:
            dest_conn.execute(sqlite_create_sql(dest_table, pairs, sqlite_ddls))
            created_here = True

        pending: list[tuple[Any, ...]] = []
        inserted = 0
        for cells in s3_iter_delimited_rows(mapped_path, delim):
            if len(cells) != len(converters):
                raise ValueError(
                    f"CSV width {len(cells)} != dest columns {len(converters)}"
                )
            pending.append(
                tuple(conv(cell) for conv, cell in zip(converters, cells, strict=True))
            )
            if len(pending) >= batch_size:
                dest_conn.executemany(insert_sql, pending)
                inserted += len(pending)
                pending.clear()
        if pending:
            dest_conn.executemany(insert_sql, pending)
            inserted += len(pending)
        dest_count = int(
            dest_conn.execute(f"SELECT COUNT(*) FROM {dest_ref}").fetchone()[0]  # nosec B608
        )
        if dest_count != source_count or inserted != source_count:
            dest_conn.rollback()
            raise ValueError(
                "CSV→SQLite COPY refused: dest COUNT(*) "
                f"{dest_count} inserted {inserted} != source COUNT {source_count}"
            )
        dest_conn.commit()
        sqlite_write = "overwrite" if replace_destination and dest_occupied else "insert"
        return _count_result(
            source_count,
            dest_count,
            {"sqlite_write": sqlite_write},
        )
    except Exception:
        try:
            dest_conn.rollback()
        except Exception:
            logger.debug("SQLite dest rollback skipped", exc_info=True)
        if created_here:
            try:
                dest_conn.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("SQLite dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            dest_conn.close()
        except Exception:
            logger.debug("SQLite dest close skipped", exc_info=True)


def copy_csv_to_postgres(
    *,
    mapped_path: str,
    ext: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    pg_ddls: list[str],
    source_count: int,
    replace_destination: bool,
) -> FastPathResult:
    """Mapped local CSV into PostgreSQL COPY FROM STDIN. Dest COUNT(*) is the proof."""
    from services.copy_fast_path import _quote, _table_ref
    from services.copy_mysql_pg import _pg_connect, _pg_create_sql
    from services.copy_s3_common import skip_complete_s3

    if not pairs or len(pairs) != len(pg_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    from connectors.write_resilience import is_public_proxy_host

    if is_public_proxy_host(dest_cfg.get("host") or dest_cfg.get("connection_string") or ""):
        raise FastPathUnavailable("public proxy: COPY FROM STDIN not assumed")
    if source_count == 0:
        raise FastPathUnavailable("empty CSV stays on the row path")

    target_cols = [p[1] for p in pairs]
    dest_schema_n = dest_schema or dest_cfg.get("schema") or "public"
    dest_ref = _table_ref(dest_schema_n, dest_table)
    delim = "E'\\t'" if ext == "tsv" else "','"
    col_list = ", ".join(_quote(c) for c in target_cols)
    copy_sql = (
        f"COPY {dest_ref} ({col_list}) FROM STDIN WITH "  # nosec B608
        f"(FORMAT csv, HEADER true, DELIMITER {delim}, NULL '\\N')"
    )

    dest_conn = _pg_connect(dest_cfg)
    created_here = False
    try:
        dst_cur = dest_conn.cursor()
        dst_cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s LIMIT 1",
            (dest_schema_n, dest_table),
        )
        exists = dst_cur.fetchone() is not None
        dest_occupied = False
        if replace_destination and exists:
            dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
            dest_conn.commit()
            exists = False
        if exists:
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
            dest_count_before = int(dst_cur.fetchone()[0])
            dest_occupied = dest_count_before > 0
            if dest_occupied and dest_count_before == source_count and not replace_destination:
                return skip_complete_s3(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={"csv_read": "skip"},
                )
            if dest_occupied:
                raise FastPathUnavailable(
                    "append into occupied PostgreSQL dest stays on the row path "
                    "(identity COPY would duplicate)"
                )
        else:
            dst_cur.execute(
                _pg_create_sql(dest_schema_n, dest_table, pairs, pg_ddls, [])
            )
            dest_conn.commit()
            created_here = True

        with open(mapped_path, "r", encoding="utf-8") as handle:
            dst_cur.copy_expert(copy_sql, handle)
        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        if dest_count != source_count:
            dest_conn.rollback()
            raise ValueError(
                "CSV→PG COPY refused: dest COUNT(*) "
                f"{dest_count} != source COUNT {source_count}"
            )
        dest_conn.commit()
        return _count_result(source_count, dest_count)
    except Exception:
        try:
            dest_conn.rollback()
        except Exception:
            logger.debug("PostgreSQL dest rollback skipped", exc_info=True)
        if created_here:
            try:
                dest_conn.cursor().execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("PG dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            dest_conn.close()
        except Exception:
            logger.debug("PostgreSQL dest close skipped", exc_info=True)


def copy_csv_to_mysql(
    *,
    mapped_path: str,
    ext: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    mysql_ddls: list[str],
    source_count: int,
    replace_destination: bool,
) -> FastPathResult:
    """Mapped local CSV into MySQL STRICT LOAD DATA. Dest COUNT(*) is the proof."""
    from connectors.write_resilience import is_public_proxy_host
    from services.copy_mysql_pg import _mysql_connect, _mysql_ident
    from services.copy_pg_mysql import _mysql_create_sql
    from services.copy_s3_common import skip_complete_s3
    from services.copy_s3_mysql import _load_delimited_into_mysql, _mysql_table_exists

    if not pairs or len(pairs) != len(mysql_ddls):
        raise FastPathUnavailable("column list / DDL mismatch")
    if is_public_proxy_host(dest_cfg.get("host") or dest_cfg.get("connection_string") or ""):
        raise FastPathUnavailable("public proxy: LOAD DATA not assumed")
    if source_count == 0:
        raise FastPathUnavailable("empty CSV stays on the row path")

    target_cols = [p[1] for p in pairs]
    dest_q = _mysql_ident(dest_table)
    dest_conn = _mysql_connect(dest_cfg)
    created_here = False
    dst_cur = dest_conn.cursor()
    try:
        exists = _mysql_table_exists(dst_cur, dest_table)
        dest_occupied = False
        if replace_destination and exists:
            dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
            dest_conn.commit()
            exists = False
        if exists:
            dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
            dest_count_before = int(dst_cur.fetchone()[0])
            dest_occupied = dest_count_before > 0
            if dest_occupied and dest_count_before == source_count and not replace_destination:
                return skip_complete_s3(
                    source_count=source_count,
                    dest_count=dest_count_before,
                    extra_snapshot={"csv_read": "skip", "load_data": "skip"},
                )
            if dest_occupied:
                raise FastPathUnavailable(
                    "append into occupied MySQL dest stays on the row path "
                    "(identity COPY would duplicate)"
                )
        else:
            dst_cur.execute(_mysql_create_sql(dest_table, pairs, mysql_ddls, []))
            dest_conn.commit()
            created_here = True

        _load_delimited_into_mysql(
            dest_conn,
            dst_cur,
            path=mapped_path,
            table_q=dest_q,
            columns=target_cols,
            ext=ext,
        )
        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
        dest_count = int(dst_cur.fetchone()[0])
        if dest_count != source_count:
            dest_conn.rollback()
            raise ValueError(
                "CSV→MySQL COPY refused: dest COUNT(*) "
                f"{dest_count} != source COUNT {source_count}"
            )
        dest_conn.commit()
        return _count_result(source_count, dest_count, {"mysql_write": "load_data"})
    except Exception:
        try:
            dest_conn.rollback()
        except Exception:
            logger.debug("MySQL dest rollback skipped", exc_info=True)
        if created_here:
            try:
                dest_conn.cursor().execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
                dest_conn.commit()
            except Exception:
                logger.debug("MySQL dest drop after copy failure skipped", exc_info=True)
        raise
    finally:
        try:
            dest_conn.close()
        except Exception:
            logger.debug("MySQL dest close skipped", exc_info=True)


def _sqlite_existing_count(dest_cfg: dict[str, Any], dest_table: str) -> int:
    from services.copy_sqlite_common import sqlite_connect, sqlite_ident, sqlite_table_exists

    conn = sqlite_connect(dest_cfg)
    try:
        if not sqlite_table_exists(conn, dest_table):
            return 0
        dest_q = sqlite_ident(dest_table)
        return int(conn.execute(f"SELECT COUNT(*) FROM {dest_q}").fetchone()[0])  # nosec B608
    finally:
        try:
            conn.close()
        except Exception:
            logger.debug("sqlite existing count close skipped", exc_info=True)


def copy_csv_to_sqlite_incremental(
    *,
    content: bytes | str | os.PathLike,
    filename: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    sqlite_ddls: list[str],
    sync_mode: str,
    cursor_column: str,
    watermark: str | None,
    pk_column: str = "",
    read_options: Any = None,
    file_type: str = "",
) -> FastPathResult:
    mode = (sync_mode or "").strip().lower()
    if mode not in COPY_INCREMENTAL_MODES:
        raise FastPathUnavailable(f"incremental COPY does not cover {sync_mode!r}")
    _src_cursor, dest_cursor, _src_pk, dest_pk_col = _require_mapped_cursor(
        pairs, cursor_column, pk_column
    )
    dest_pk = _file_dest_pk(pairs, pk_column)
    target_cols = [p[1] for p in pairs]
    created_dest = False
    dest_count_before = 0
    with _mapped_csv_file(
        content,
        filename,
        pairs,
        read_options=read_options,
        incremental=True,
        cursor_column=cursor_column,
        watermark=watermark,
        pk_column=pk_column,
        file_type=file_type,
    ) as (path, source_count, ext):
        if source_count == 0:
            return _empty_incremental(_sqlite_existing_count(dest_cfg, dest_table), mode)
        created_dest, dest_count_before = _prepare_sqlite_incremental_dest(
            dest_cfg, dest_table, pairs, sqlite_ddls, dest_pk
        )
        from services.copy_incremental import _sqlite_incremental_idents

        _dest_q, staging, _staging_q = _sqlite_incremental_idents(dest_table)
        result = copy_csv_to_sqlite(
            mapped_path=path,
            ext=ext,
            dest_cfg=dest_cfg,
            dest_table=staging,
            pairs=pairs,
            sqlite_ddls=sqlite_ddls,
            source_count=source_count,
            replace_destination=True,
        )
    return _finish_sqlite_incremental_staging(
        dest_cfg,
        dest_table=dest_table,
        dest_pk=dest_pk,
        dest_cursor=dest_cursor,
        dest_pk_col=dest_pk_col,
        dest_count_before=dest_count_before,
        target_cols=target_cols,
        mode=mode,
        result=result,
        created_dest=created_dest,
    )


def copy_csv_to_postgres_incremental(
    *,
    content: bytes | str | os.PathLike,
    filename: str,
    dest_cfg: dict[str, Any],
    dest_schema: str,
    dest_table: str,
    pairs: list[tuple[str, str]],
    pg_ddls: list[str],
    sync_mode: str,
    cursor_column: str,
    watermark: str | None,
    pk_column: str = "",
    read_options: Any = None,
    file_type: str = "",
) -> FastPathResult:
    from services.copy_fast_path import _table_ref
    from services.copy_mysql_pg import _pg_connect, _pg_create_sql

    mode = (sync_mode or "").strip().lower()
    if mode not in COPY_INCREMENTAL_MODES:
        raise FastPathUnavailable(f"incremental COPY does not cover {sync_mode!r}")
    _src_cursor, dest_cursor, _src_pk, dest_pk_col = _require_mapped_cursor(
        pairs, cursor_column, pk_column
    )
    dest_pk = _file_dest_pk(pairs, pk_column)
    target_cols = [p[1] for p in pairs]
    dest_schema_n = dest_schema or dest_cfg.get("schema") or "public"
    dest_ref = _table_ref(dest_schema_n, dest_table)
    staging = staging_table_name(dest_table)
    staging_ref = _table_ref(dest_schema_n, staging)
    created_dest = False
    dest_count_before = 0
    result: FastPathResult | None = None
    with _mapped_csv_file(
        content,
        filename,
        pairs,
        read_options=read_options,
        incremental=True,
        cursor_column=cursor_column,
        watermark=watermark,
        pk_column=pk_column,
        file_type=file_type,
    ) as (path, source_count, ext):
        dest_conn = _pg_connect(dest_cfg)
        try:
            dest_conn.autocommit = True
            with dest_conn.cursor() as dst_cur:
                dst_cur.execute(
                    "SELECT to_regclass(%s)",
                    (f"{dest_schema_n}.{dest_table}",),
                )
                exists = dst_cur.fetchone()[0] is not None
                if source_count == 0:
                    if exists:
                        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
                        dest_count_before = int(dst_cur.fetchone()[0])
                    return _empty_incremental(dest_count_before, mode)
                if not exists:
                    dst_cur.execute(
                        _pg_create_sql(
                            dest_schema_n, dest_table, pairs, pg_ddls, [dest_pk]
                        )
                    )
                    created_dest = True
                else:
                    dst_cur.execute(f"SELECT COUNT(*) FROM {dest_ref}")  # nosec B608
                    dest_count_before = int(dst_cur.fetchone()[0])
        finally:
            try:
                dest_conn.close()
            except Exception:
                logger.debug("csv→pg incremental dest probe close skipped", exc_info=True)
        result = copy_csv_to_postgres(
            mapped_path=path,
            ext=ext,
            dest_cfg=dest_cfg,
            dest_schema=dest_schema_n,
            dest_table=staging,
            pairs=pairs,
            pg_ddls=pg_ddls,
            source_count=source_count,
            replace_destination=True,
        )

    dest_conn = _pg_connect(dest_cfg)
    if result is None:
        raise RuntimeError("CSV→PG incremental COPY produced no staging result")
    try:
        dest_conn.autocommit = False
        with dest_conn.cursor() as dst_cur:
            out = _apply_staging_to_pg(
                dst_cur,
                dest_ref=dest_ref,
                staging_ref=staging_ref,
                staging_name=staging,
                dest_table=dest_table,
                target_cols=target_cols,
                dest_pk=dest_pk,
                dest_cursor=dest_cursor,
                dest_pk_col=dest_pk_col,
                dest_count_before=dest_count_before,
                mode=mode,
                result=result,
            )
            dest_conn.commit()
            return out
    except Exception:
        dest_conn.rollback()
        try:
            dest_conn.autocommit = True
            with dest_conn.cursor() as dst_cur:
                dst_cur.execute(f"DROP TABLE IF EXISTS {staging_ref}")  # nosec B608
                if created_dest:
                    dst_cur.execute(f"DROP TABLE IF EXISTS {dest_ref}")  # nosec B608
        except Exception:
            logger.debug("csv→pg incremental cleanup skipped", exc_info=True)
        raise
    finally:
        try:
            dest_conn.close()
        except Exception:
            logger.debug("csv→pg incremental dest close skipped", exc_info=True)


def copy_csv_to_mysql_incremental(
    *,
    content: bytes | str | os.PathLike,
    filename: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    pairs: list[tuple[str, str]],
    mysql_ddls: list[str],
    sync_mode: str,
    cursor_column: str,
    watermark: str | None,
    pk_column: str = "",
    read_options: Any = None,
    file_type: str = "",
) -> FastPathResult:
    from services.copy_mysql_pg import _mysql_connect, _mysql_ident
    from services.copy_pg_mysql import _mysql_create_sql

    mode = (sync_mode or "").strip().lower()
    if mode not in COPY_INCREMENTAL_MODES:
        raise FastPathUnavailable(f"incremental COPY does not cover {sync_mode!r}")
    _src_cursor, dest_cursor, _src_pk, dest_pk_col = _require_mapped_cursor(
        pairs, cursor_column, pk_column
    )
    dest_pk = _file_dest_pk(pairs, pk_column)
    target_cols = [p[1] for p in pairs]
    dest_q = _mysql_ident(dest_table)
    staging = staging_table_name(dest_table)
    staging_q = _mysql_ident(staging)
    created_dest = False
    dest_count_before = 0
    result: FastPathResult | None = None
    with _mapped_csv_file(
        content,
        filename,
        pairs,
        read_options=read_options,
        incremental=True,
        cursor_column=cursor_column,
        watermark=watermark,
        pk_column=pk_column,
        file_type=file_type,
    ) as (path, source_count, ext):
        dest_conn = _mysql_connect(dest_cfg)
        try:
            with dest_conn.cursor() as dst_cur:
                dst_cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = DATABASE() AND table_name = %s LIMIT 1",
                    (dest_table,),
                )
                exists = dst_cur.fetchone() is not None
                if source_count == 0:
                    if exists:
                        dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
                        dest_count_before = int(dst_cur.fetchone()[0])
                    return _empty_incremental(dest_count_before, mode)
                if not exists:
                    dst_cur.execute(
                        _mysql_create_sql(dest_table, pairs, mysql_ddls, [dest_pk])
                    )
                    dest_conn.commit()
                    created_dest = True
                else:
                    dst_cur.execute(f"SELECT COUNT(*) FROM {dest_q}")  # nosec B608
                    dest_count_before = int(dst_cur.fetchone()[0])
        finally:
            try:
                dest_conn.close()
            except Exception:
                logger.debug("csv→mysql incremental dest probe close skipped", exc_info=True)
        result = copy_csv_to_mysql(
            mapped_path=path,
            ext=ext,
            dest_cfg=dest_cfg,
            dest_table=staging,
            pairs=pairs,
            mysql_ddls=mysql_ddls,
            source_count=source_count,
            replace_destination=True,
        )

    dest_conn = None
    if result is None:
        raise RuntimeError("CSV→MySQL incremental COPY produced no staging result")
    try:
        dest_conn = _mysql_connect(dest_cfg)
        with dest_conn.cursor() as dst_cur:
            out = _apply_staging_to_mysql(
                dst_cur,
                dest_q=dest_q,
                staging_q=staging_q,
                staging_name=staging,
                dest_table=dest_table,
                target_cols=target_cols,
                dest_pk=dest_pk,
                dest_cursor=dest_cursor,
                dest_pk_col=dest_pk_col,
                dest_count_before=dest_count_before,
                mode=mode,
                result=result,
                quote=_mysql_ident,
            )
            dest_conn.commit()
            return out
    except Exception:
        cleanup = dest_conn or _mysql_connect(dest_cfg)
        try:
            with cleanup.cursor() as dst_cur:
                dst_cur.execute(f"DROP TABLE IF EXISTS {staging_q}")  # nosec B608
                if created_dest:
                    dst_cur.execute(f"DROP TABLE IF EXISTS {dest_q}")  # nosec B608
            cleanup.commit()
        except Exception:
            logger.debug("csv→mysql incremental cleanup skipped", exc_info=True)
        if cleanup is not dest_conn:
            try:
                cleanup.close()
            except Exception:
                logger.debug("csv→mysql incremental cleanup close skipped", exc_info=True)
        raise
    finally:
        if dest_conn is not None:
            try:
                dest_conn.close()
            except Exception:
                logger.debug("csv→mysql incremental dest close skipped", exc_info=True)


def _pairs_and_ddls(
    mappings: list[dict],
    schema: dict[str, str],
    dest_type: str,
) -> tuple[list[tuple[str, str]], list[str]]:
    from connectors.mysql_writer import mysql_type
    from connectors.postgresql_writer import pg_type
    from connectors.sqlite_writer import sqlite_type
    from services.copy_mysql_pg import mysql_type_is_copy_safe
    from services.copy_sqlite_common import sqlite_type_is_copy_safe

    dest = (dest_type or "").strip().lower()
    pairs: list[tuple[str, str]] = []
    ddls: list[str] = []
    for item in mappings:
        source_col = str(item.get("source") or "").strip()
        target_col = str(item.get("target") or "").strip()
        declared = str(
            item.get("type") or schema.get(source_col) or schema.get(target_col) or ""
        )
        if dest in {"postgresql", "postgres"}:
            physical = pg_type(declared) if declared else "TEXT"
            if not pg_type_is_load_safe(physical):
                raise FastPathUnavailable(
                    f"{source_col} type {declared or physical} is not COPY-safe"
                )
            ddls.append(physical)
        elif dest in {"mysql", "mariadb"}:
            physical = mysql_type(declared) if declared else "TEXT"
            if not mysql_type_is_copy_safe(physical):
                raise FastPathUnavailable(
                    f"{source_col} type {declared or physical} is not COPY-safe"
                )
            ddls.append(physical)
        else:
            physical = sqlite_type(declared) if declared else "TEXT"
            if not sqlite_type_is_copy_safe(physical):
                raise FastPathUnavailable(
                    f"{source_col} type {declared or physical} is not COPY-safe"
                )
            ddls.append(physical)
        pairs.append((source_col, target_col))
    return pairs, ddls


def _format_csv_copy(
    *,
    result: FastPathResult,
    dest_type: str,
    dest_table: str,
    filename: str,
    sync_mode: str,
    replace_destination: bool,
    file_type: str = "csv",
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    snapshot = dict(result.source_snapshot or {})
    inc_mode = (sync_mode or "").strip().lower()
    incremental = inc_mode in COPY_INCREMENTAL_MODES
    load_method = file_copy_load_method(
        file_type, dest_type, sync_mode if incremental else ""
    )
    dest_n = (dest_type or "").strip().lower()
    dest_summary: dict[str, Any] = {
        "type": dest_type,
        "table": dest_table,
        "rows_written": result.source_rows,
        "checksum": result.target_checksum,
        "load_method": load_method,
        "source_row_count": result.source_rows,
        "source_row_count_source": "engine_population_in_snapshot",
        # Same pair SQL COPY stamps. Without it Gate-8 re-hashes dest rows
        # and compares that fingerprint to dest_count:N.
        "engine_source_checksum": result.source_checksum,
        "engine_target_checksum": result.target_checksum,
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "sync_mode": sync_mode if incremental else (
            "full_refresh_overwrite" if replace_destination else "full_refresh_append"
        ),
        "proof_scope": result.proof_scope,
        "source_snapshot": snapshot,
        "copy_workers": int(snapshot.get("copy_workers") or 1),
        "copy_partitions": snapshot.get("copy_partitions"),
        "partitions_skipped": snapshot.get("partitions_skipped"),
        "shard_mode": snapshot.get("shard_mode"),
        "copy_split": snapshot.get("copy_split"),
        "csv_read": snapshot.get("csv_read"),
        "copy_fast_path": "used",
    }
    inc_wm = str(snapshot.get("incremental_watermark") or "")
    if incremental:
        dest_summary["incremental_watermark"] = inc_wm
        dest_summary["sync_mode"] = inc_mode
        if inc_mode == "incremental_deduped":
            proof_line = (
                "Proof: staging COUNT(*) equals filtered file rows past the cursor; "
                "dest PK ⋈ staging equals staging; dest COUNT(*) independently reread. "
                "Not a SQL WHERE on the file."
            )
        else:
            proof_line = (
                "Proof: staging COUNT(*) equals filtered file rows past the cursor; "
                "dest COUNT(*) equals dest_before + staging (duplicate PK fails closed). "
                "Not a SQL WHERE on the file."
            )
    else:
        proof_line = (
            "Proof: dest COUNT(*) equals mapped source COUNT. "
            "Not pandas / COPY of an unmapped file. Empty dest is bulk load, not upsert."
        )
    if dest_n in {"postgresql", "postgres"}:
        wire = "COPY FROM STDIN"
    elif dest_n in {"mysql", "mariadb"}:
        wire = "STRICT LOAD DATA"
    else:
        wire = "executemany"
    verb = f"COPY+{inc_mode}" if incremental else wire
    kind = (file_type or "csv").strip().lower() or "csv"
    ddl_log = [
        f"COPY {kind} {filename} → {dest_type} {dest_table} "
        f"({result.source_rows:,} rows, local {verb})",
        proof_line,
    ]
    columns = list((snapshot.get("target_columns") or []))
    return result.rows_copied, ddl_log, dest_summary, columns


def try_copy_local_csv(
    *,
    content: bytes | str | os.PathLike,
    filename: str,
    file_type: str,
    dest_type: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    dest_schema: str,
    mappings: list[dict],
    schema: dict[str, str],
    effective_sync: str,
    cursor_column: str = "",
    watermark: str | None = None,
    pk_column: str = "",
    source_filter: dict[str, Any] | None = None,
    shape_runner: Any = None,
    resumed: bool = False,
    read_options: Any = None,
) -> tuple[int, list[str], dict[str, Any], list[str]] | None:
    """Identity local CSV COPY, or None to keep the row path."""
    if not csv_local_copy_enabled():
        return None
    if not identity_file_copy_route(file_type, dest_type):
        return None
    if shape_runner is not None:
        return None
    if source_filter:
        return None
    if resumed:
        return None
    if not _read_options_copy_ok(read_options):
        logger.info("CSV COPY declined: ReadOptions window is not COPY-identity")
        return None
    ok, reason = mapping_is_plain_carry(mappings)
    if not ok:
        logger.info("CSV COPY declined: %s", reason)
        return None

    dest_n = (dest_type or "").strip().lower()
    incremental = requires_incremental(effective_sync)
    replace_destination = is_overwrite_sync(effective_sync) and not incremental
    if incremental:
        mode = (effective_sync or "").strip().lower()
        if mode not in COPY_INCREMENTAL_MODES:
            logger.info("CSV COPY declined: sync mode %s is not identity incremental", mode)
            return None
        if not (cursor_column or "").strip():
            logger.info("CSV COPY declined: incremental COPY requires a mapped cursor_field")
            return None
    elif not (is_append_sync(effective_sync) or is_overwrite_sync(effective_sync)):
        logger.info(
            "CSV COPY declined: sync mode %s is not identity COPY",
            effective_sync,
        )
        return None

    try:
        pairs, ddls = _pairs_and_ddls(mappings, schema, dest_type)
    except FastPathUnavailable as exc:
        logger.info("CSV COPY declined: %s", exc)
        return None

    try:
        if incremental:
            if dest_n in {"postgresql", "postgres"}:
                result = copy_csv_to_postgres_incremental(
                    content=content,
                    filename=filename,
                    dest_cfg=dest_cfg,
                    dest_schema=dest_schema,
                    dest_table=dest_table,
                    pairs=pairs,
                    pg_ddls=ddls,
                    sync_mode=effective_sync,
                    cursor_column=cursor_column,
                    watermark=watermark,
                    pk_column=pk_column,
                    read_options=read_options,
                    file_type=file_type,
                )
            elif dest_n in {"mysql", "mariadb"}:
                result = copy_csv_to_mysql_incremental(
                    content=content,
                    filename=filename,
                    dest_cfg=dest_cfg,
                    dest_table=dest_table,
                    pairs=pairs,
                    mysql_ddls=ddls,
                    sync_mode=effective_sync,
                    cursor_column=cursor_column,
                    watermark=watermark,
                    pk_column=pk_column,
                    read_options=read_options,
                    file_type=file_type,
                )
            else:
                result = copy_csv_to_sqlite_incremental(
                    content=content,
                    filename=filename,
                    dest_cfg=dest_cfg,
                    dest_table=dest_table,
                    pairs=pairs,
                    sqlite_ddls=ddls,
                    sync_mode=effective_sync,
                    cursor_column=cursor_column,
                    watermark=watermark,
                    pk_column=pk_column,
                    read_options=read_options,
                    file_type=file_type,
                )
        else:
            with _mapped_csv_file(
                content,
                filename,
                pairs,
                read_options=read_options,
                file_type=file_type,
            ) as (path, source_count, ext):
                if dest_n in {"postgresql", "postgres"}:
                    result = copy_csv_to_postgres(
                        mapped_path=path,
                        ext=ext,
                        dest_cfg=dest_cfg,
                        dest_schema=dest_schema,
                        dest_table=dest_table,
                        pairs=pairs,
                        pg_ddls=ddls,
                        source_count=source_count,
                        replace_destination=replace_destination,
                    )
                elif dest_n in {"mysql", "mariadb"}:
                    result = copy_csv_to_mysql(
                        mapped_path=path,
                        ext=ext,
                        dest_cfg=dest_cfg,
                        dest_table=dest_table,
                        pairs=pairs,
                        mysql_ddls=ddls,
                        source_count=source_count,
                        replace_destination=replace_destination,
                    )
                else:
                    result = copy_csv_to_sqlite(
                        mapped_path=path,
                        ext=ext,
                        dest_cfg=dest_cfg,
                        dest_table=dest_table,
                        pairs=pairs,
                        sqlite_ddls=ddls,
                        source_count=source_count,
                        replace_destination=replace_destination,
                    )
    except FastPathUnavailable as exc:
        logger.info("CSV COPY declined: %s", exc)
        return None
    except Exception as exc:
        logger.warning("CSV COPY failed after starting: %s", exc)
        raise

    rows, ddl_log, dest_summary, _cols = _format_csv_copy(
        result=result,
        dest_type=dest_type,
        dest_table=dest_table,
        filename=filename,
        sync_mode=effective_sync,
        replace_destination=replace_destination,
        file_type=file_type,
    )
    columns = [p[1] for p in pairs]
    return rows, ddl_log, dest_summary, columns
