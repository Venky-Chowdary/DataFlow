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

from services.value_serializer import SQL_NULL_SENTINEL, cell_to_string

logger = logging.getLogger(__name__)

ProbeStatus = Literal[
    "ran",
    "skipped_no_pk",
    "skipped_no_source",
    "skipped_unsupported",
    "skipped_callable",
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
    | frozenset(
        {
            "mongodb",
            "mongodb_atlas",
            "dynamodb",
            "amazon_dynamodb",
            "redis",
            "salesforce",
            "stripe",
        }
    )
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


def _identity_cell(value: Any) -> str:
    """One identity cell on the transfer wire.

    ``str(value)`` invented ``True`` / ``1E+2`` / a Python ``b'...'`` repr and
    a space timestamp. ``None`` used a private ``\\x00NULL`` token, then
    findings collapsed it to ``""`` so Validate could not tell NULL from empty.
    """
    return cell_to_string(value, preserve_sql_null=True)


def _finding_value(columns: list[str], values: tuple[Any, ...]) -> Any:
    """Scalar for single-col (backward compat); joined label for composite."""
    if len(columns) == 1:
        return values[0] if values else None
    parts = [_identity_cell(v) for v in values]
    return "(" + ", ".join(parts) + ")"


def _sql_duplicates(
    cfg: dict[str, Any],
    table: str,
    pk_columns: list[str],
    limit: int = 5,
) -> list[dict[str, Any]]:
    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table
    from services.sql_object_identity import resolve_object_identity

    engine = _engine(cfg)
    schema, table = split_qualified_table(table, (cfg.get("schema") or "").strip() or None)
    # Case-folding engines and Studio ``schema.table`` spellings must address
    # the catalog object, not ``public."public.case_a_src"``.
    ident = resolve_object_identity(engine, table, schema, columns=pk_columns)
    if ident.exists:
        table = sa.sql.quoted_name(ident.table, True)
        schema = sa.sql.quoted_name(ident.schema, True) if ident.schema else None
        pk_columns = [ident.columns.get(c, c) for c in pk_columns]

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
        vals = tuple(
            _identity_cell(row[i] if i < len(row) else None) for i in range(n)
        )
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
            vals = (_identity_cell(rid),)
            values = {pk_columns[0]: vals[0]}
        elif isinstance(rid, dict):
            vals = tuple(_identity_cell(rid.get(c)) for c in pk_columns)
            values = {c: vals[i] for i, c in enumerate(pk_columns)}
        else:
            vals = (_identity_cell(rid),)
            values = {pk_columns[0]: vals[0]}
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


def _salesforce_duplicates(
    cfg: dict[str, Any],
    sobject: str,
    pk_columns: list[str],
    *,
    limit: int = 5,
) -> tuple[list[dict[str, Any]], ProbeStatus, str]:
    """Prove identity uniqueness for a Salesforce object.

    ``Id`` is assigned by the platform and is unique by construction, so an
    identity of ``Id`` needs no query — that is stronger evidence than any
    sample, which is why it is reported as having run rather than skipped.

    Any other field, including one flagged ``externalId``, can repeat, so it is
    counted with a SOQL aggregate. Aggregating in the org rather than reading
    the object back is what keeps this usable against a real tenant, where the
    object may hold millions of rows and OFFSET is capped at 2000.
    """
    from connectors.salesforce import API_VERSION, _access, _validate_api_name
    from connectors.saas_common import request

    if [c.strip().lower() for c in pk_columns] == ["id"]:
        return [], "ran", "Salesforce assigns Id; uniqueness is guaranteed by the platform"

    if len(pk_columns) != 1:
        # SOQL GROUP BY takes multiple fields, but COUNT() over a composite key
        # cannot be read back to a single value without ambiguity here. Say so
        # rather than approve a key this probe did not check.
        return (
            [],
            "skipped_unsupported",
            "Composite identity uniqueness is not probed against Salesforce — "
            f"({', '.join(pk_columns)}) is unproven",
        )

    field = _validate_api_name(pk_columns[0].strip(), "field")
    obj = _validate_api_name(sobject.strip(), "object")
    access_token, url_base = _access(cfg)
    soql = (
        f"SELECT {field}, COUNT(Id) dupes FROM {obj} "  # nosec B608 — identifiers validated above
        f"GROUP BY {field} HAVING COUNT(Id) > 1"
    )
    response = request(
        method="GET",
        url=f"{url_base}/services/data/{API_VERSION}/query",
        token=access_token,
        params={"q": soql},
        timeout=60,
    )
    response.raise_for_status()
    from services.value_serializer import load_http_json

    # Identity cells live in ``records``. Response.json() is stdlib
    # json.loads — a long fraction in an External Id collapses to IEEE
    # before Validate shows the duplicate key.
    body = load_http_json(response)
    records = body.get("records") if isinstance(body, dict) else None
    findings: list[dict[str, Any]] = []
    for record in (records or [])[: max(1, int(limit))]:
        if not isinstance(record, dict):
            continue
        value = _identity_cell(record.get(field))
        findings.append(
            {
                "value": _finding_value(pk_columns, (value,)),
                "values": [value],
                "columns": list(pk_columns),
                "count": int(record.get("dupes") or 0),
            }
        )
    return (
        findings,
        "ran",
        f"Salesforce aggregate uniqueness probe on {obj}.({field})",
    )


def _stripe_duplicates(
    cfg: dict[str, Any],
    object_name: str,
    pk_columns: list[str],
    *,
    limit: int = 5,
) -> tuple[list[dict[str, Any]], ProbeStatus, str]:
    """Prove identity uniqueness for a Stripe object.

    Stripe assigns ``id`` (``cus_…``, ``ch_…``) and the platform guarantees it
    is unique. That is the same class of evidence as Salesforce ``Id`` — stronger
    than a sample, so the probe reports ``ran`` rather than skipped.

    Any other identity is unproven: Stripe list APIs have no GROUP BY.
    """
    wanted = [c.strip().lower() for c in pk_columns]
    if wanted == ["id"]:
        return (
            [],
            "ran",
            (
                f"Stripe assigns id on {object_name or 'object'}; "
                "uniqueness is guaranteed by the platform"
            ),
        )
    return (
        [],
        "skipped_unsupported",
        (
            "Stripe uniqueness is proven only for platform id — "
            f"({', '.join(pk_columns)}) is unproven"
        ),
    )


def _dynamodb_duplicates(
    cfg: dict[str, Any],
    table: str,
    pk_columns: list[str],
    *,
    limit: int = 5,
) -> tuple[list[dict[str, Any]], ProbeStatus, str]:
    """Prove identity uniqueness for a DynamoDB source.

    When the identity is the table's own key, DynamoDB has already enforced it:
    two items cannot share a primary key, so the answer is structural and needs
    no scan. That is stronger evidence than any sample, and it is why this is
    reported as ``ran`` rather than skipped.

    A non-key identity carries no such guarantee — a GSI does not enforce
    uniqueness either — so those are counted across a real scan.
    """
    from collections import Counter

    from connectors.dynamodb_reader import describe_key_schema, read_all_paginated

    key_names = [
        str(k.get("name") or "")
        for k in describe_key_schema(cfg, table)
        if k.get("name")
    ]
    wanted = [c.strip().lower() for c in pk_columns]
    if key_names and wanted == [k.lower() for k in key_names]:
        return (
            [],
            "ran",
            f"DynamoDB enforces uniqueness on the table key ({', '.join(key_names)})",
        )

    batch = read_all_paginated(cfg, table, limit=_PAYLOAD_SCAN_CAP)
    headers = [str(h) for h in (batch.headers or [])]
    missing = [c for c in pk_columns if c not in headers]
    if missing:
        raise ValueError(
            f"identity column(s) {', '.join(missing)} are not present in {table}"
        )
    idx = [headers.index(c) for c in pk_columns]
    counts: Counter[tuple[str, ...]] = Counter()
    for row in batch.rows or []:
        counts[
            tuple(_normalize_key_cell(row[i] if i < len(row) else None) for i in idx)
        ] += 1
    scanned = len(batch.rows or [])
    return (
        _counter_findings(counts, pk_columns, limit),
        "ran",
        f"DynamoDB scan uniqueness probe on {table}.({','.join(pk_columns)}) "
        f"over {scanned:,} item(s)",
    )


def _redis_duplicates(
    cfg: dict[str, Any],
    keyspace: str,
    pk_columns: list[str],
    *,
    limit: int = 5,
) -> tuple[list[dict[str, Any]], ProbeStatus, str]:
    """Prove identity uniqueness for a Redis keyspace source.

    ``redis_key`` is unique by construction — one value per key — so an identity
    that is the key itself is answered structurally, like a DynamoDB table key.
    An identity read out of the stored document carries no such guarantee (two
    keys may hold the same ``id``), so those are counted across a real SCAN of
    the keyspace. Skipping the engine entirely failed every uniqueness-required
    Redis route closed for a probe the keyspace can actually answer.
    """
    from collections import Counter

    from connectors.redis_reader import RedisScanState, read_keys_batch, resolve_key_pattern

    wanted = [c.strip().lower() for c in pk_columns]
    if wanted == ["redis_key"]:
        return (
            [],
            "ran",
            "Redis enforces uniqueness on the key itself (redis_key)",
        )

    pattern = resolve_key_pattern(keyspace)
    state = RedisScanState.from_any(None)
    counts: Counter[tuple[str, ...]] = Counter()
    scanned = 0
    idx: list[int] = []
    while scanned < _PAYLOAD_SCAN_CAP:
        batch, state = read_keys_batch(
            cfg=cfg, pattern=pattern, limit=_PAYLOAD_PAGE, scan_state=state
        )
        rows = list(batch.rows or [])
        if rows and not idx:
            headers = [str(h) for h in (batch.headers or [])]
            missing = [c for c in pk_columns if c not in headers]
            if missing:
                raise ValueError(
                    f"identity column(s) {', '.join(missing)} are not present "
                    f"in Redis keyspace {pattern}"
                )
            idx = [headers.index(c) for c in pk_columns]
        for row in rows:
            counts[
                tuple(_normalize_key_cell(row[i] if i < len(row) else None) for i in idx)
            ] += 1
        scanned += len(rows)
        if not rows or state.exhausted:
            break
    if scanned >= _PAYLOAD_SCAN_CAP and not state.exhausted:
        return (
            _counter_findings(counts, pk_columns, limit),
            "skipped_unsupported",
            (
                f"Redis keyspace exceeds the {_PAYLOAD_SCAN_CAP:,}-key uniqueness "
                f"scan cap ({scanned:,} read); uniqueness on "
                f"({','.join(pk_columns)}) is unproven for the remainder"
            ),
        )
    return (
        _counter_findings(counts, pk_columns, limit),
        "ran",
        f"Redis keyspace uniqueness scan on {pattern}.({','.join(pk_columns)}) "
        f"over {scanned:,} key(s)",
    )


def _normalize_key_cell(value: Any) -> str:
    """Render one identity cell the way the destination key would compare it."""
    return _identity_cell(value)


def _counter_findings(
    counts: Any, pk_columns: list[str], limit: int
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for vals, count in counts.most_common():
        if count < 2:
            break
        if len(out) >= max(1, int(limit)):
            break
        values = [_identity_cell(v) for v in vals]
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
    try:
        from services.procedure_source import is_callable_source

        if is_callable_source(source_config):
            return SourceDuplicateProbeResult(
                status="skipped_callable",
                message=(
                    "Stored-procedure / SQL extract is a result-set snapshot — "
                    "uniqueness GROUP BY is not run against a procedure name."
                ),
                primary_key_columns=_normalize_pk_columns(primary_key, primary_key_columns),
            )
    except Exception:
        pass
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

        if db_type == "salesforce":
            sobject = source_table or source_collection
            if not sobject:
                return SourceDuplicateProbeResult(
                    status="skipped_no_source",
                    message="Salesforce source missing object for uniqueness probe",
                    db_type=db_type,
                    primary_key_columns=pk_columns,
                )
            findings, status, message = _salesforce_duplicates(
                cfg, sobject, pk_columns, limit=limit
            )
            return SourceDuplicateProbeResult(
                findings=findings,
                status=status,
                message=message,
                db_type=db_type,
                primary_key_columns=pk_columns,
            )

        if db_type == "stripe":
            obj = source_table or source_collection
            if not obj:
                return SourceDuplicateProbeResult(
                    status="skipped_no_source",
                    message="Stripe source missing object for uniqueness probe",
                    db_type=db_type,
                    primary_key_columns=pk_columns,
                )
            findings, status, message = _stripe_duplicates(
                cfg, obj, pk_columns, limit=limit
            )
            return SourceDuplicateProbeResult(
                findings=findings,
                status=status,
                message=message,
                db_type=db_type,
                primary_key_columns=pk_columns,
            )

        if db_type in ("dynamodb", "amazon_dynamodb"):
            tbl = source_table or source_collection
            if not tbl:
                return SourceDuplicateProbeResult(
                    status="skipped_no_source",
                    message="DynamoDB source missing table for uniqueness probe",
                    db_type=db_type,
                    primary_key_columns=pk_columns,
                )
            findings, status, message = _dynamodb_duplicates(
                cfg, tbl, pk_columns, limit=limit
            )
            return SourceDuplicateProbeResult(
                findings=findings,
                status=status,
                message=message,
                db_type=db_type,
                primary_key_columns=pk_columns,
            )

        if db_type == "redis":
            keyspace = source_table or source_collection
            if not keyspace:
                return SourceDuplicateProbeResult(
                    status="skipped_no_source",
                    message="Redis source missing keyspace for uniqueness probe",
                    db_type=db_type,
                    primary_key_columns=pk_columns,
                )
            findings, status, message = _redis_duplicates(
                cfg, keyspace, pk_columns, limit=limit
            )
            return SourceDuplicateProbeResult(
                findings=findings,
                status=status,
                message=message,
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
