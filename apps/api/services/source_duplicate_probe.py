"""Source-side duplicate-key probe for preflight.

Database sources can be larger than the sample used by G9, so a 100-row sample
may miss duplicates that later cause a write-batch failure. This module runs a
cheap source-side query (SQL GROUP BY or MongoDB aggregation) to find repeated
values for the resolved identity key before the transfer is approved.

Honesty contract:
- ``status="ran"`` means the probe query completed (findings may be empty = clean).
- ``skipped_unsupported`` / ``error`` must NEVER be stamped as full-selected coverage.
- Callers fail-closed when uniqueness is required but the probe did not run.
- Composite identity uses ``GROUP BY c1, c2, …`` — never invent single-column
  uniqueness from the first composite column alone.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Literal

logger = logging.getLogger(__name__)

ProbeStatus = Literal[
    "ran",
    "skipped_no_pk",
    "skipped_no_source",
    "skipped_unsupported",
    "error",
]

# SQL path covers generic_sql and all SQL-like catalog IDs.
SQLISH_SOURCE_TYPES = frozenset({
    "postgresql", "postgres", "redshift", "cockroachdb", "timescaledb", "supabase",
    "mysql", "mariadb", "singlestore",
    "sqlserver", "mssql", "synapse", "azure_sql_database",
    "oracle", "db2", "sqlite", "duckdb", "generic_sql", "h2",
    "snowflake", "bigquery", "databricks", "clickhouse", "trino", "presto", "questdb",
})

# Sources whose whole payload is readable, so uniqueness is proven by scanning
# it rather than by asking the engine to GROUP BY. These have no query language
# to push the aggregation into, and skipping them failed every uniqueness-required
# sync closed — a file the platform can read end to end is the one case where
# population proof is always available.
OBJECT_PAYLOAD_SOURCE_TYPES = frozenset({
    "s3", "minio", "s3_compatible", "aws_s3",
    "gcs", "google_cloud_storage",
    "adls", "azure_blob_storage", "azure_data_lake", "azure_data_lake_storage",
    "sftp",
})

PROBED_SOURCE_TYPES = (
    SQLISH_SOURCE_TYPES
    | frozenset({"mongodb", "mongodb_atlas"})
    | OBJECT_PAYLOAD_SOURCE_TYPES
)

#: Rows scanned before a payload probe reports partial coverage instead of proof.
_PAYLOAD_SCAN_CAP = 1_000_000
_PAYLOAD_PAGE = 10_000


@dataclass
class SourceDuplicateProbeResult:
    """Structured probe outcome — never invent population proof from a skip."""

    findings: list[dict[str, Any]] = field(default_factory=list)
    status: ProbeStatus = "skipped_no_source"
    message: str = ""
    db_type: str = ""
    primary_key_columns: list[str] = field(default_factory=list)

    @property
    def ran(self) -> bool:
        return self.status == "ran"


def _normalize_pk_columns(
    primary_key: str = "",
    primary_key_columns: list[str] | None = None,
) -> list[str]:
    cols = [str(c).strip() for c in (primary_key_columns or []) if str(c or "").strip()]
    if cols:
        return cols
    raw = str(primary_key or "").strip()
    if not raw:
        return []
    if "," in raw:
        return [p.strip() for p in raw.split(",") if p.strip()]
    return [raw]


def _finding_value(columns: list[str], values: tuple[Any, ...]) -> Any:
    """Scalar for single-col (backward compat); joined label for composite."""
    if len(columns) == 1:
        return values[0] if values else None
    parts = ["" if v is None else str(v) for v in values]
    return "(" + ", ".join(parts) + ")"


def _sql_duplicates(
    cfg: dict[str, Any],
    table: str,
    pk_columns: list[str],
    limit: int = 5,
) -> list[dict[str, Any]]:
    import sqlalchemy as sa

    from connectors.generic_sql import _engine

    engine = _engine(cfg)
    schema = (cfg.get("schema") or "").strip() or None

    # Build a dialect-agnostic query so SQLAlchemy emits the right LIMIT/TOP/FETCH
    # syntax for SQL Server, Oracle, etc.
    tbl = sa.table(table, schema=schema)
    pk_cols = [sa.column(c) for c in pk_columns]
    cnt = sa.func.count().label("_cnt")
    stmt = (
        sa.select(*pk_cols, cnt)
        .select_from(tbl)
        .group_by(*pk_cols)
        .having(cnt > 1)
        .limit(limit)
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()

    out: list[dict[str, Any]] = []
    n = len(pk_columns)
    for row in rows:
        vals = tuple(row[i] if i < len(row) else None for i in range(n))
        count = int(row[n]) if len(row) > n else 1
        out.append(
            {
                "value": _finding_value(pk_columns, vals),
                "values": {pk_columns[i]: vals[i] for i in range(n)},
                "columns": list(pk_columns),
                "count": count,
            }
        )
    return out


def _mongo_duplicates(
    cfg: dict[str, Any],
    collection: str,
    pk_columns: list[str],
    limit: int = 5,
) -> list[dict[str, Any]]:
    from pymongo import MongoClient

    from connectors.mongodb_common import normalize_mongodb_connection_string

    uri = normalize_mongodb_connection_string(
        cfg.get("connection_string", ""),
        database=cfg.get("database", ""),
        host=cfg.get("host", ""),
        port=int(cfg.get("port") or 0),
        username=cfg.get("username", ""),
        password=cfg.get("password", ""),
        ssl=bool(cfg.get("ssl")),
        auth_source=cfg.get("auth_source", ""),
    )
    client: MongoClient = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db_name = cfg.get("database") or cfg.get("auth_source") or "test"
    db = client[db_name]
    coll = db[collection]

    try:
        if len(pk_columns) == 1:
            group_id: Any = f"${pk_columns[0]}"
        else:
            group_id = {c: f"${c}" for c in pk_columns}
        pipeline = [
            {"$group": {"_id": group_id, "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
            {"$limit": limit},
        ]
        rows = list(coll.aggregate(pipeline, maxTimeMS=5000))
    finally:
        client.close()

    out: list[dict[str, Any]] = []
    for r in rows:
        rid = r.get("_id")
        count = int(r.get("count", 1))
        if len(pk_columns) == 1:
            vals = (rid,)
            values = {pk_columns[0]: rid}
        elif isinstance(rid, dict):
            vals = tuple(rid.get(c) for c in pk_columns)
            values = {c: rid.get(c) for c in pk_columns}
        else:
            vals = (rid,)
            values = {pk_columns[0]: rid}
        out.append(
            {
                "value": _finding_value(pk_columns, vals),
                "values": values,
                "columns": list(pk_columns),
                "count": count,
            }
        )
    return out


def _object_payload_duplicates(
    cfg: dict[str, Any],
    db_type: str,
    key: str,
    pk_columns: list[str],
    *,
    limit: int = 5,
) -> tuple[list[dict[str, Any]], int, bool]:
    """Count identity keys across an object payload.

    Returns ``(findings, rows_scanned, complete)``. ``complete`` is False when
    the scan hit :data:`_PAYLOAD_SCAN_CAP` — the caller must then refuse to
    claim population coverage, because an unscanned tail can still hold the
    duplicate that a uniqueness-required sync would collide on.

    The object is paged through the same reader the transfer uses, so the probe
    and the load agree on parsing, encoding and column order rather than
    reaching two different answers about the same bytes.
    """
    from collections import Counter

    from src.transfer.batch_readers import _read_batch_impl

    counts: Counter[tuple[str, ...]] = Counter()
    scanned = 0
    offset = 0
    total: int | None = None
    while True:
        result = _read_batch_impl(
            db_type,
            cfg,
            key,
            None,
            offset,
            _PAYLOAD_PAGE,
            known_total_rows=total,
        )
        batch = result[0] if isinstance(result, tuple) else result
        headers = [str(h) for h in (getattr(batch, "headers", None) or [])]
        rows = list(getattr(batch, "rows", None) or [])
        if total is None:
            total = getattr(batch, "total_rows", None)
        if not rows:
            break
        missing = [c for c in pk_columns if c not in headers]
        if missing:
            # The key is not in the payload at all: that is a mapping question,
            # not a duplicate one, and inventing "unique" here would green a
            # column the file never carried.
            raise ValueError(
                f"identity column(s) {', '.join(missing)} are not present in {key}"
            )
        idx = [headers.index(c) for c in pk_columns]
        for row in rows:
            if isinstance(row, dict):
                vals = tuple(_normalize_key_cell(row.get(c)) for c in pk_columns)
            else:
                vals = tuple(
                    _normalize_key_cell(row[i] if i < len(row) else None) for i in idx
                )
            counts[vals] += 1
        scanned += len(rows)
        offset += len(rows)
        if scanned >= _PAYLOAD_SCAN_CAP:
            return _counter_findings(counts, pk_columns, limit), scanned, False
        if total is not None and offset >= int(total):
            break
        if len(rows) < _PAYLOAD_PAGE:
            break
    return _counter_findings(counts, pk_columns, limit), scanned, True


def _normalize_key_cell(value: Any) -> str:
    """Render one identity cell the way the destination key would compare it.

    ``None`` is kept distinct from the empty string: a NULL key is a
    nullability finding, not a duplicate of a row that carried ``''``.
    """
    if value is None:
        return "\x00NULL"
    return str(value)


def _counter_findings(
    counts: Any, pk_columns: list[str], limit: int
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for vals, count in counts.most_common():
        if count < 2:
            break
        if len(out) >= max(1, int(limit)):
            break
        values = ["" if v == "\x00NULL" else v for v in vals]
        out.append(
            {
                "value": _finding_value(pk_columns, values),
                "values": values,
                "columns": list(pk_columns),
                "count": int(count),
            }
        )
    return out


def probe_source_duplicate_keys_result(
    *,
    source_connector_id: str = "",
    source_config: dict[str, Any] | None = None,
    source_table: str = "",
    source_collection: str = "",
    primary_key: str = "",
    primary_key_columns: list[str] | None = None,
    workspace_id: str | None = None,
    limit: int = 5,
) -> SourceDuplicateProbeResult:
    """Run the source duplicate probe and return an honest status + findings."""
    pk_columns = _normalize_pk_columns(primary_key, primary_key_columns)
    if not pk_columns:
        return SourceDuplicateProbeResult(
            status="skipped_no_pk",
            message="No primary key resolved for uniqueness probe",
        )
    if not source_table and not source_collection:
        return SourceDuplicateProbeResult(
            status="skipped_no_source",
            message="No source table/collection for uniqueness probe",
            primary_key_columns=pk_columns,
        )
    if not source_connector_id and not source_config:
        return SourceDuplicateProbeResult(
            status="skipped_no_source",
            message="No source connector id or inline config for uniqueness probe",
            primary_key_columns=pk_columns,
        )

    cfg: dict[str, Any] | None = None
    db_type = ""
    pk_label = ",".join(pk_columns)
    try:
        if source_connector_id:
            from services.connector_store import get_connector

            conn = get_connector(source_connector_id, workspace_id=workspace_id)
            if conn:
                from services.connector_probe import probe_cfg_from_saved

                cfg = probe_cfg_from_saved(conn)
                db_type = (conn.type or "").lower()

        if cfg is None and source_config:
            cfg = dict(source_config)
            db_type = (
                cfg.get("type")
                or cfg.get("db_type")
                or cfg.get("format")
                or ""
            ).lower()

        if not cfg:
            return SourceDuplicateProbeResult(
                status="skipped_no_source",
                message="Source connector could not be loaded for uniqueness probe",
                db_type=db_type,
                primary_key_columns=pk_columns,
            )

        # Inline endpoint configs use "format" for the database type; normalize to
        # "type" so the generic_sql engine builder can build a URL.
        if db_type:
            cfg = dict(cfg)
            cfg.setdefault("type", db_type)

        # Connector-specific settings arrive nested under "extra" from
        # endpoint_to_dict, while connector readers take them as top-level keys.
        # SFTP host-key trust lives there, and without it this probe opened an
        # unverified connection to the source it is meant to vouch for.
        nested_extra = cfg.get("extra")
        if isinstance(nested_extra, dict) and nested_extra:
            cfg = dict(cfg)
            for extra_key, extra_value in nested_extra.items():
                cfg.setdefault(extra_key, extra_value)

        if db_type in ("mongodb", "mongodb_atlas"):
            coll = source_collection or source_table
            if not coll:
                return SourceDuplicateProbeResult(
                    status="skipped_no_source",
                    message="Mongo source missing collection for uniqueness probe",
                    db_type=db_type,
                    primary_key_columns=pk_columns,
                )
            findings = _mongo_duplicates(cfg, coll, pk_columns, limit=limit)
            return SourceDuplicateProbeResult(
                findings=findings,
                status="ran",
                message=f"Mongo uniqueness probe on {coll}.({pk_label})",
                db_type=db_type,
                primary_key_columns=pk_columns,
            )

        if db_type in OBJECT_PAYLOAD_SOURCE_TYPES:
            obj = source_table or source_collection
            if not obj:
                return SourceDuplicateProbeResult(
                    status="skipped_no_source",
                    message="Object source missing key for uniqueness probe",
                    db_type=db_type,
                    primary_key_columns=pk_columns,
                )
            findings, scanned, complete = _object_payload_duplicates(
                cfg, db_type, obj, pk_columns, limit=limit
            )
            if not complete:
                # A truncated scan proves the duplicates it found but cannot
                # prove their absence, so it must not be stamped as population
                # coverage.
                return SourceDuplicateProbeResult(
                    findings=findings,
                    status="skipped_unsupported",
                    message=(
                        f"{db_type} payload exceeds the {_PAYLOAD_SCAN_CAP:,}-row "
                        f"uniqueness scan cap ({scanned:,} read); uniqueness on "
                        f"({pk_label}) is unproven for the remainder"
                    ),
                    db_type=db_type,
                    primary_key_columns=pk_columns,
                )
            return SourceDuplicateProbeResult(
                findings=findings,
                status="ran",
                message=(
                    f"{db_type} payload uniqueness scan on {obj}.({pk_label}) "
                    f"over {scanned:,} row(s)"
                ),
                db_type=db_type,
                primary_key_columns=pk_columns,
            )

        if db_type not in SQLISH_SOURCE_TYPES:
            msg = f"Duplicate-key probe not implemented for source type {db_type or 'unknown'}"
            logger.debug(msg)
            return SourceDuplicateProbeResult(
                status="skipped_unsupported",
                message=msg,
                db_type=db_type,
                primary_key_columns=pk_columns,
            )

        table = source_table or source_collection
        if not table:
            return SourceDuplicateProbeResult(
                status="skipped_no_source",
                message="SQL source missing table for uniqueness probe",
                db_type=db_type,
                primary_key_columns=pk_columns,
            )
        findings = _sql_duplicates(cfg, table, pk_columns, limit=limit)
        return SourceDuplicateProbeResult(
            findings=findings,
            status="ran",
            message=f"SQL uniqueness probe on {table}.({pk_label})",
            db_type=db_type,
            primary_key_columns=pk_columns,
        )
    except Exception as exc:
        logger.warning("Source duplicate-key probe failed: %s", exc, exc_info=exc)
        return SourceDuplicateProbeResult(
            status="error",
            message=f"Source uniqueness probe failed: {exc}"[:400],
            db_type=db_type,
            primary_key_columns=pk_columns,
        )


def probe_source_duplicate_keys(
    *,
    source_connector_id: str = "",
    source_config: dict[str, Any] | None = None,
    source_table: str = "",
    source_collection: str = "",
    primary_key: str = "",
    primary_key_columns: list[str] | None = None,
    workspace_id: str | None = None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Return findings only — prefer :func:`probe_source_duplicate_keys_result` for SSOT.

    Empty list is ambiguous (clean vs skipped). New callers must use the result API.
    """
    return probe_source_duplicate_keys_result(
        source_connector_id=source_connector_id,
        source_config=source_config,
        source_table=source_table,
        source_collection=source_collection,
        primary_key=primary_key,
        primary_key_columns=primary_key_columns,
        workspace_id=workspace_id,
        limit=limit,
    ).findings
