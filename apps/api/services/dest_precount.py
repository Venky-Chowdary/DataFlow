"""Destination cardinality taken *before* the write.

Append into a non-empty table cannot be proven by whole-table digests, so
Gate-8 falls back to cardinality. ``target_rows >= expected_rows`` is not a
proof there: a table that already held 30 rows satisfies it even if the writer
appended nothing. The only honest cardinality proof for append is the delta

    rows_after - rows_before == expected_rows

which requires the count taken before the writer runs. Keyed / CDC
conservation is the same shape:

    dest_delta == inserts - deletes
    dest_delta = COUNT(*)_after - COUNT(*)_before

Counting after the first upsert of a table is dest-after, not dest-before.
This module owns that one query, so every destination family answers it
the same way and ``reconcile()`` / the conservation ledger can tell
"delta proven" apart from "delta unknown" instead of silently reporting
the second as the first.

File/object exports have no SQL engine. ``count_artifact_rows`` is the
same identity against the bytes on disk: re-open the written artifact and
COUNT records. Writer ``rows`` / bytes-landed is Airbyte/Fivetran S3
success — it does not close conservation. Independent artifact COUNT is
cardinality, not Gate-8 cell fidelity.

``None`` means the count is unavailable (unsupported engine, missing table,
unreachable destination, or an unreadable/unsupported artifact); callers
must degrade assurance rather than assume zero.
"""

from __future__ import annotations

import gzip
import json
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.transfer.models import EndpointConfig

logger = logging.getLogger(__name__)

__all__ = [
    "PRECOUNT_KEY",
    "ARTIFACT_COUNT_KEY",
    "DEST_COUNT_SOURCE_KEY",
    "DEST_COUNT_ARTIFACT",
    "destination_row_count",
    "destination_key_hits",
    "precount_destination",
    "precount_table",
    "count_endpoint_rows",
    "count_artifact_rows",
    "stamp_artifact_census",
    "DestBeforeCensus",
]

# Dest-engine IN-list chunk. Partitioning the key set (not overlapping) so
# summed COUNT(DISTINCT) equals the full census.
_KEY_HIT_CHUNK = 400

# Key used to carry the pre-write count on the writer's destination summary.
PRECOUNT_KEY = "target_rows_before"

# Independent record COUNT of a written file/object artifact. Analogous to
# SQL COUNT(*) — never the writer's ``rows`` / ``rows_written``. Cardinality
# of the bytes on disk, not Gate-8 cell fidelity.
ARTIFACT_COUNT_KEY = "artifact_row_count"
DEST_COUNT_SOURCE_KEY = "dest_count_source"
DEST_COUNT_ARTIFACT = "artifact_readback"

_ARTIFACT_FORMATS = frozenset({"csv", "tsv", "json", "jsonl", "parquet"})


def _count(conn: Any, table_ref: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        cur.close()


def destination_row_count(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
) -> int | None:
    """Rows already in the destination table, or ``None`` when unknowable.

    A missing table counts as ``0`` — create-on-first-write is a known empty
    destination, which is a proof, not an unknown.
    """
    table = (table_name or "").strip()
    if not table:
        return None
    try:
        from connectors.sql_identifiers import quote_table_ref

        if db_type == "sqlite":
            import sqlite3

            database = str(cfg.get("database") or "")
            if not database:
                return None
            with sqlite3.connect(database) as conn:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                if not exists:
                    return 0
                return _count(conn, quote_table_ref(table, dialect="sqlite"))

        if db_type in {"postgresql", "redshift"}:
            from connectors.postgresql_conn import get_connection

            conn = get_connection(
                host=str(cfg.get("host") or ""),
                port=int(cfg.get("port") or (5439 if db_type == "redshift" else 5432)),
                database=str(cfg.get("database") or ""),
                username=str(cfg.get("username") or ""),
                password=str(cfg.get("password") or ""),
                connection_string=str(cfg.get("connection_string") or ""),
                ssl=bool(cfg.get("ssl", False)),
            )
            try:
                sch = schema or "public"
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT to_regclass(%s)", (f'"{sch}"."{table}"',)
                    )
                    row = cur.fetchone()
                    if not row or row[0] is None:
                        return 0
                return _count(conn, quote_table_ref(table, sch, dialect="postgresql"))
            finally:
                conn.close()

        if db_type == "mysql":
            from connectors.mysql_conn import get_connection

            conn = get_connection(
                host=str(cfg.get("host") or ""),
                port=int(cfg.get("port") or 3306),
                database=str(cfg.get("database") or ""),
                username=str(cfg.get("username") or ""),
                password=str(cfg.get("password") or ""),
                connection_string=str(cfg.get("connection_string") or ""),
                ssl=bool(cfg.get("ssl", False)),
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = DATABASE() AND table_name = %s",
                        (table,),
                    )
                    row = cur.fetchone()
                    if not row or not int(row[0]):
                        return 0
                return _count(conn, quote_table_ref(table, dialect="mysql"))
            finally:
                conn.close()

        if db_type == "mongodb":
            from pymongo import MongoClient

            from src.transfer.adapters import mongodb_connection_string

            client: MongoClient = MongoClient(
                mongodb_connection_string(cfg), serverSelectionTimeoutMS=5000
            )
            try:
                database = str(cfg.get("database") or "")
                if not database:
                    return None
                coll = client[database][table]
                # Exact, not estimated: an approximate count cannot prove a delta.
                return int(coll.count_documents({}))
            finally:
                client.close()
    except Exception as exc:  # pragma: no cover - destination-specific failure
        logger.warning("Pre-write destination count failed: %s", exc)
        return None
    return None


def destination_key_hits(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    key_columns: list[str],
    keys: list[tuple[Any, ...]],
) -> int | None:
    """How many of these keys dest already holds — dest-engine, not writer ack.

    Upsert/CDC ``records_processed`` counts updates as writes. ``COUNT(*)``
    does not move. The independent split is: keys in this batch that already
    exist on dest (updates) versus keys that do not (inserts). ``None`` means
    the probe could not run; callers must leave keyed conservation unproven.
    """
    cols = [str(c).strip() for c in (key_columns or []) if str(c).strip()]
    table = (table_name or "").strip()
    if not table or not cols:
        return None
    unique: list[tuple[Any, ...]] = []
    seen: set[tuple[Any, ...]] = set()
    for raw in keys or []:
        tup = tuple(raw)
        if len(tup) != len(cols) or any(v is None for v in tup):
            continue
        if tup in seen:
            continue
        seen.add(tup)
        unique.append(tup)
    if not unique:
        return 0
    # Missing / empty dest: no hits, and IN against a missing table would error.
    n = destination_row_count(db_type, cfg, schema=schema, table_name=table)
    if n is None:
        return None
    if n == 0:
        return 0
    try:
        return _key_hits_sql(db_type, cfg, schema=schema, table_name=table, cols=cols, keys=unique)
    except Exception as exc:  # pragma: no cover - destination-specific failure
        logger.warning("Pre-write destination key census failed: %s", exc)
        return None


def _key_hits_sql(
    db_type: str,
    cfg: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    cols: list[str],
    keys: list[tuple[Any, ...]],
) -> int | None:
    from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref

    dialect = "mysql" if db_type == "mysql" else db_type
    qchar = "`" if dialect == "mysql" else '"'
    table_ref = quote_table_ref(
        table_name,
        schema if dialect == "postgresql" else None,
        dialect="postgresql" if dialect == "postgresql" else dialect,
    )
    col_sql = ", ".join(quote_sql_identifier(c, qchar) for c in cols)
    ph = "%s" if dialect in {"postgresql", "mysql"} else "?"
    total = 0
    if dialect == "sqlite":
        import sqlite3

        database = str(cfg.get("database") or "")
        if not database:
            return None
        with sqlite3.connect(database) as conn:
            total = _sum_distinct_hits(conn, table_ref, col_sql, cols, keys, ph)
        return total
    if dialect in {"postgresql", "redshift"}:
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or (5439 if db_type == "redshift" else 5432)),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl", False)),
        )
        try:
            return _sum_distinct_hits(conn, table_ref, col_sql, cols, keys, ph)
        finally:
            conn.close()
    if dialect == "mysql":
        from connectors.mysql_conn import get_connection

        conn = get_connection(
            host=str(cfg.get("host") or ""),
            port=int(cfg.get("port") or 3306),
            database=str(cfg.get("database") or ""),
            username=str(cfg.get("username") or ""),
            password=str(cfg.get("password") or ""),
            connection_string=str(cfg.get("connection_string") or ""),
            ssl=bool(cfg.get("ssl", False)),
        )
        try:
            return _sum_distinct_hits(conn, table_ref, col_sql, cols, keys, ph)
        finally:
            conn.close()
    return None


def _sum_distinct_hits(
    conn: Any,
    table_ref: str,
    col_sql: str,
    cols: list[str],
    keys: list[tuple[Any, ...]],
    ph: str,
) -> int:
    total = 0
    width = len(cols)
    for i in range(0, len(keys), _KEY_HIT_CHUNK):
        chunk = keys[i : i + _KEY_HIT_CHUNK]
        if width == 1:
            in_sql = ", ".join(ph for _ in chunk)
            sql = (
                f"SELECT COUNT(DISTINCT {col_sql}) FROM {table_ref} "  # nosec B608
                f"WHERE {col_sql} IN ({in_sql})"
            )
            params: tuple[Any, ...] = tuple(row[0] for row in chunk)
        else:
            row_ph = "(" + ", ".join(ph for _ in cols) + ")"
            in_sql = ", ".join(row_ph for _ in chunk)
            sql = (
                f"SELECT COUNT(*) FROM ("  # nosec B608
                f"SELECT DISTINCT {col_sql} FROM {table_ref} "
                f"WHERE ({col_sql}) IN ({in_sql})"
                f") _df_key_hits"
            )
            params = tuple(v for row in chunk for v in row)
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            row = cur.fetchone()
            total += int(row[0]) if row and row[0] is not None else 0
        finally:
            cur.close()
    return total


def precount_table(db_type: str, cfg: dict[str, Any], table_name: str) -> int | None:
    """Pre-write count for a table the streaming writers already resolved.

    Streaming paths compute the driver type and destination table themselves and
    call the batch writer directly, so they pass those in rather than
    re-resolving from the endpoint.
    """
    from services.dialect_profiles import schema_from_cfg

    return destination_row_count(
        db_type, cfg, schema=schema_from_cfg(db_type, cfg), table_name=table_name
    )


def precount_destination(
    endpoint: EndpointConfig, cfg: dict[str, Any]
) -> int | None:
    """Pre-write count for a resolved destination endpoint.

    Resolves the driver, schema and table exactly the way the writer will, so
    the delta is measured against the object the rows actually land in.
    """
    from src.transfer.adapters import resolve_dest_table
    from src.transfer.connector_capabilities import resolve_driver_type

    db_type = resolve_driver_type(str(cfg.get("type") or endpoint.format or ""))
    return precount_table(
        db_type, cfg, resolve_dest_table(db_type, endpoint, "dt_import")
    )


def count_endpoint_rows(
    endpoint: EndpointConfig | None,
    *,
    table_name: str | None = None,
) -> int | None:
    """Independent engine COUNT(*) of the object this endpoint currently names.

    Multi-stream jobs remap ``endpoint.table`` per stream. Count while that
    bind is still in place, or pass ``table_name`` after the bind is restored.
    ``None`` means unknowable — never substitute writer acknowledgement.
    """
    if endpoint is None:
        return None
    from src.transfer.adapters import resolve_connector_config, resolve_dest_table
    from src.transfer.connector_capabilities import resolve_driver_type

    try:
        cfg = resolve_connector_config(endpoint)
        db_type = resolve_driver_type(str(cfg.get("type") or endpoint.format or ""))
        name = (table_name or "").strip() or resolve_dest_table(
            db_type, endpoint, "dt_import"
        )
        return precount_table(db_type, cfg, name)
    except Exception as exc:
        logger.warning("Endpoint COUNT(*) failed: %s", exc)
        return None


class DestBeforeCensus:
    """Dest COUNT(*) taken once per named object, before that object is written.

    Append delta and keyed/CDC ``dest_delta`` both require this number.
    A second capture of the same name must not re-query: that would observe
    dest-after and close a false identity. ``None`` stored for a name means
    the probe ran and was unknowable — do not retry after writes have begun.
    """

    def __init__(self) -> None:
        self._before: dict[str, int | None] = {}

    def capture(
        self,
        endpoint: Any,
        *,
        table_name: str,
        aliases: Sequence[str] = (),
    ) -> int | None:
        names: list[str] = []
        for raw in (table_name, *aliases):
            name = str(raw or "").strip()
            if name and name not in names:
                names.append(name)
        if not names:
            return None
        for name in names:
            if name in self._before:
                value = self._before[name]
                for other in names:
                    self._before.setdefault(other, value)
                return value
        value = count_endpoint_rows(endpoint, table_name=names[0])
        for name in names:
            self._before[name] = value
        return value

    def get(self, table_name: str) -> int | None:
        return self._before.get(str(table_name or "").strip())

    def stamp(self, summary: dict[str, Any], table_name: str) -> dict[str, Any]:
        """Copy dest-before onto a stream summary. Never dest-after."""
        key = str(table_name or "").strip()
        if key not in self._before:
            return summary
        value = self._before[key]
        if value is None:
            return summary
        summary[PRECOUNT_KEY] = int(value)
        recon = dict(summary.get("reconciliation") or {})
        recon[PRECOUNT_KEY] = int(value)
        summary["reconciliation"] = recon
        return summary


def _infer_artifact_format(path: Path, fmt: str | None) -> str:
    explicit = str(fmt or "").strip().lower()
    aliases = {"ndjson": "jsonl", "xlsx": "excel", "xls": "excel"}
    if explicit in aliases:
        explicit = aliases[explicit]
    if explicit in _ARTIFACT_FORMATS:
        return explicit
    name = path.name.lower()
    if name.endswith(".gz"):
        name = name[: -len(".gz")]
    if name.endswith(".csv"):
        return "csv"
    if name.endswith(".tsv"):
        return "tsv"
    if name.endswith(".jsonl") or name.endswith(".ndjson"):
        return "jsonl"
    if name.endswith(".json"):
        return "json"
    if name.endswith(".parquet"):
        return "parquet"
    return ""


def _read_artifact_bytes(path: Path) -> bytes | None:
    try:
        if path.name.lower().endswith(".gz"):
            with gzip.open(path, "rb") as handle:
                return handle.read()
        return path.read_bytes()
    except OSError as exc:
        logger.info("artifact bytes unreadable at %s: %s", path, exc)
        return None


def _count_jsonl_bytes(content: bytes) -> int | None:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return None
    count = 0
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return None
        if not isinstance(obj, dict):
            return None
        count += 1
    return count


def _count_json_bytes(content: bytes) -> int | None:
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None
    if isinstance(payload, list):
        return len(payload)
    return None


def _count_parquet_path(path: Path) -> int | None:
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return None
    try:
        metadata = pq.ParquetFile(str(path)).metadata
        if metadata is None:
            return None
        return int(metadata.num_rows)
    except Exception as exc:
        logger.info("parquet artifact count unavailable at %s: %s", path, exc)
        return None


def count_artifact_rows(
    path: str | Path | None,
    *,
    fmt: str | None = None,
) -> int | None:
    """Independent record COUNT of a written file, or ``None`` if unknowable.

    Dest-engine analogue of ``destination_row_count`` for file/object exports.
    Re-opens the bytes on disk. Never returns the writer's ``rows_written``.
    Missing path, remote URI without a local file, unsupported format, or
    unparseable content stay ``None`` — conservation remains unmeasured.
    Empty but well-formed artifacts are measured zero.
    """
    raw = str(path or "").strip()
    if not raw:
        return None
    artifact = Path(raw)
    if not artifact.is_file():
        return None
    kind = _infer_artifact_format(artifact, fmt)
    if kind not in _ARTIFACT_FORMATS:
        return None
    if kind == "parquet":
        return _count_parquet_path(artifact)
    content = _read_artifact_bytes(artifact)
    if content is None:
        return None
    try:
        if kind in {"csv", "tsv"}:
            from services.csv_profiler import count_csv_rows

            return int(count_csv_rows(content))
        if kind == "jsonl":
            return _count_jsonl_bytes(content)
        if kind == "json":
            return _count_json_bytes(content)
    except Exception as exc:
        logger.info("artifact count failed for %s (%s): %s", artifact, kind, exc)
        return None
    return None


def stamp_artifact_census(
    recon: Mapping[str, Any],
    dest_summary: Mapping[str, Any] | None,
    *,
    fmt: str | None = None,
) -> dict[str, Any]:
    """Stamp independent artifact COUNT onto Gate-8. Never writer ack.

    File replace is dest-before 0: the engine opens the artifact ``wb``.
    Cell-fidelity flags (``skipped_readback`` / ``unproven``) stay with the
    caller — this only owns dest cardinality.
    """
    out = dict(recon)
    data = dict(dest_summary or {})
    path = data.get("path") or data.get("export_path")
    resolved_fmt = fmt or data.get("format")
    counted = count_artifact_rows(
        path if isinstance(path, str) else None,
        fmt=str(resolved_fmt or "") or None,
    )
    if counted is None:
        # Writer ``target_rows`` must not survive as dest COUNT.
        out["target_rows"] = None
        return out
    out[ARTIFACT_COUNT_KEY] = counted
    out[DEST_COUNT_SOURCE_KEY] = DEST_COUNT_ARTIFACT
    out["target_rows"] = counted
    out[PRECOUNT_KEY] = 0
    return out
