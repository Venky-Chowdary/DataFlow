"""Destination-side key collision probe for append sync modes.

An append writes rows the destination has never seen. When the destination
table already carries a PRIMARY KEY / UNIQUE constraint on the identity column,
re-appending a key that is already stored is a *deterministic* write failure —
Postgres aborts the whole COPY with ``duplicate key value violates unique
constraint`` and nothing lands. That verdict is knowable before the write: it
needs one bounded ``SELECT key WHERE key IN (…)`` against the destination.

Preflight owning that query is the difference between "Validate greened and
Execute exploded" and "Validate told the operator to switch to upsert/merge".
The source-side twin lives in :mod:`services.source_duplicate_probe`; this
module answers the other half — duplicates *between* the batch and the rows
already at rest.

Honesty contract (identical to the source probe):
- ``status="ran"`` means the query completed; empty findings then mean clean.
- ``skipped_*`` / ``error`` must never be stamped as proof of no collision.
- Overwrite / upsert / merge modes are not probed: they resolve keys by design.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from collections.abc import Mapping
from typing import Any, Literal

from services.source_duplicate_probe import SQLISH_SOURCE_TYPES

logger = logging.getLogger(__name__)

ProbeStatus = Literal[
    "ran",
    "skipped_no_key",
    "skipped_no_values",
    "skipped_no_destination",
    "skipped_unsupported",
    "error",
]

# Sync modes that insert without resolving an existing key. Upsert / merge /
# overwrite all define what happens to a colliding key, so they are exempt.
APPEND_ONLY_SYNC_MODES = frozenset(
    {
        "append",
        "insert",
        "full_refresh_append",
        "incremental_append",
        "incremental_append_only",
    }
)

MAX_PROBE_VALUES = 500

# Watermarks persisted before the canonical separator existed used a pipe.
_LEGACY_SEP = "|"


@dataclass
class DestinationCollisionResult:
    """Structured probe outcome — a skip is never proof of a clean append."""

    findings: list[dict[str, Any]] = field(default_factory=list)
    status: ProbeStatus = "skipped_no_destination"
    message: str = ""
    db_type: str = ""
    key_column: str = ""
    values_probed: int = 0
    # A resumed run re-delivers the interrupted batch on purpose. The writer
    # applies that overlap through the destination key (ON CONFLICT / MERGE),
    # so overlapping keys are expected evidence rather than a write abort.
    idempotent_apply: bool = False
    # An incremental run reads past its watermark, so only that delta can
    # collide. Recorded so an operator can see which rows were actually probed.
    delta_scope: dict[str, Any] = field(default_factory=dict)

    @property
    def ran(self) -> bool:
        return self.status == "ran"


def sync_mode_appends_without_key_resolution(sync_mode: str) -> bool:
    """True when the write inserts rows without resolving an existing key."""
    return (sync_mode or "").strip().lower() in APPEND_ONLY_SYNC_MODES


def destination_enforces_key(
    key_column: str,
    *,
    destination_pk_columns: list[str] | None = None,
    destination_unique_keys: list[dict[str, Any]] | None = None,
) -> bool:
    """True when the destination rejects a duplicate of ``key_column``.

    Only single-column constraints count: a composite key tolerates a repeated
    first column, so treating it as enforced would invent a blocker.
    """
    key = (key_column or "").strip().lower()
    if not key:
        return False
    pk = [str(c).strip().lower() for c in (destination_pk_columns or []) if str(c or "").strip()]
    if pk == [key]:
        return True
    for uk in destination_unique_keys or []:
        cols = [
            str(c).strip().lower()
            for c in (uk.get("columns") or [])
            if str(c or "").strip()
        ]
        if cols == [key]:
            return True
    return False


#: Dialects whose unbounded text type cannot appear in a comparison.
_LOB_TEXT_DIALECTS = frozenset({"oracle", "db2", "ibm_db_sa"})

#: Widest bounded character carrier those dialects accept in a comparison.
_BOUNDED_TEXT_LEN = 4000


def key_comparison_carrier(dialect_name: str, values: list[str]) -> Any:
    """Type to cast an identity key to for an ``IN (…)`` comparison.

    ``Text`` compiles to CLOB on Oracle/DB2 and no comparison operator accepts a
    LOB (ORA-22849), so the probe errored and the append lost its pre-write
    duplicate verdict. A bounded VARCHAR compares everywhere; keys wider than it
    keep ``Text`` rather than compare truncated and invent a collision.
    """
    import sqlalchemy as sa

    if (dialect_name or "").lower() not in _LOB_TEXT_DIALECTS:
        return sa.Text()
    widest = max((len(str(v)) for v in values), default=0)
    return sa.String(_BOUNDED_TEXT_LEN) if widest <= _BOUNDED_TEXT_LEN else sa.Text()


def _sql_existing_keys(
    cfg: dict[str, Any],
    table: str,
    key_column: str,
    values: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from services.sql_object_identity import resolve_object_identity

    engine = _engine(cfg)
    schema = (cfg.get("schema") or "").strip() or None
    # Case-folding engines (Oracle/Snowflake/DB2) render an unquoted lower-case
    # name folded, so the probe hit "table does not exist" and degraded to
    # ``error`` — a skip that is not proof of a clean append. Address the object
    # the catalog actually holds, with its stored column spelling.
    ident = resolve_object_identity(engine, table, schema, columns=[key_column])
    if ident.exists:
        table = sa.sql.quoted_name(ident.table, True)
        schema = sa.sql.quoted_name(ident.schema, True) if ident.schema else None
        key_column = ident.columns.get(key_column, key_column)
    tbl = sa.table(table, schema=schema)
    col = sa.column(sa.sql.quoted_name(key_column, True) if ident.exists else key_column)
    # Compare as text: the batch carries stringified keys while the column may be
    # bigint / uuid / numeric, and an uncast IN () raises "operator does not exist".
    key_carrier = key_comparison_carrier(
        str(getattr(engine.dialect, "name", "")), values
    )
    stmt = (
        sa.select(col)
        .select_from(tbl)
        .where(sa.cast(col, key_carrier).in_(values))
        .limit(limit)
    )
    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [
        {"column": key_column, "value": None if r[0] is None else str(r[0])}
        for r in rows
    ]


def _mongo_existing_keys(
    cfg: dict[str, Any],
    collection: str,
    key_column: str,
    values: list[str],
    limit: int,
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
    try:
        db = client[cfg.get("database") or cfg.get("auth_source") or "test"]
        cursor = db[collection].find(
            {key_column: {"$in": values}}, {key_column: 1}
        ).limit(limit)
        return [
            {"column": key_column, "value": str(doc.get(key_column))}
            for doc in cursor
        ]
    finally:
        client.close()


def rows_a_cursor_read_will_deliver(
    sample_rows: list[dict[str, Any]] | None,
    *,
    cursor_column: str,
    watermark: str | None,
    tiebreak_column: str = "",
) -> list[dict[str, Any]]:
    """Narrow a whole-table sample to the rows an incremental read will return.

    Mirrors the reader's seek predicate exactly — ``(cursor, pk) > (wm, wm_pk)``
    when the watermark carries a tie-break, ``cursor > wm`` otherwise — so a
    pre-write check judges the batch that will be written rather than the table
    it is drawn from. A row whose cursor value is missing is kept: an unreadable
    value must not quietly shrink the batch a checker is looking at.
    """
    from services.keyset_pagination import KEYSET_SEP, encode_keyset_bookmark
    from services.sync_cursor import compare_cursor_values

    rows = list(sample_rows or [])
    if not cursor_column or not watermark:
        return rows
    composite = KEYSET_SEP in str(watermark) or _LEGACY_SEP in str(watermark)
    use_tiebreak = bool(composite and tiebreak_column)
    delta: list[dict[str, Any]] = []
    for row in rows:
        if cursor_column not in row:
            return rows
        cell = row.get(cursor_column)
        if cell is None or str(cell) == "":
            delta.append(row)
            continue
        if use_tiebreak:
            candidate = encode_keyset_bookmark(
                [str(cell), str(row.get(tiebreak_column, ""))]
            )
        else:
            candidate = str(cell)
        if compare_cursor_values(candidate, str(watermark)) > 0:
            delta.append(row)
    return delta


def probe_append_key_collisions(
    *,
    mappings: list[dict[str, Any]] | None,
    source_columns: list[str] | None,
    sample_rows: list[dict[str, Any]] | None,
    sync_mode: str,
    dest_kind: str,
    validation_mode: str,
    destination_config: Mapping[str, Any] | None,
    destination_db_type: str,
    destination_table: str,
    destination_table_exists: bool | None,
    destination_pk_columns: list[str] | None,
    destination_unique_keys: list[dict[str, Any]] | None,
    contract_primary_key: str | None = None,
    stream_contracts: list[dict[str, Any]] | None = None,
    source_table: str = "",
    resume: bool = False,
    incremental_cursor_column: str = "",
    incremental_watermark: str | None = None,
    incremental_tiebreak_column: str = "",
) -> DestinationCollisionResult | None:
    """Resolve the identity key, then probe it — ``None`` when not applicable.

    ``None`` means "this write cannot collide by construction" (create-new
    table, overwrite/upsert semantics, no enforced single-column key), which is
    different from a probe that could not run and must not block.
    """
    if not sync_mode_appends_without_key_resolution(sync_mode):
        return None
    if destination_table_exists is not True:
        return None
    if not (destination_pk_columns or destination_unique_keys):
        return None

    try:
        from services.primary_key import resolve_primary_key_source_columns

        pk_source_cols = resolve_primary_key_source_columns(
            mappings=list(mappings or []),
            source_columns=list(source_columns or []),
            dest_kind=dest_kind,
            validation_mode=validation_mode,
            purpose="uniqueness",
            destination_pk_columns=list(destination_pk_columns or []),
            contract_primary_key=contract_primary_key,
            stream_contracts=list(stream_contracts or []),
            stream_name=str(destination_table or source_table or ""),
        )
    except Exception as exc:
        logger.debug("append collision key resolution failed: %s", exc, exc_info=exc)
        return None
    if len(pk_source_cols) != 1:
        return None

    source_key = pk_source_cols[0]
    target_key = next(
        (
            str(m.get("target") or "")
            for m in (mappings or [])
            if isinstance(m, dict) and str(m.get("source") or "") == source_key
        ),
        source_key,
    )
    if not destination_enforces_key(
        target_key,
        destination_pk_columns=destination_pk_columns,
        destination_unique_keys=destination_unique_keys,
    ):
        return None

    # Probe the rows this run will read, not the whole table. An incremental
    # append past a watermark cannot collide with keys it will never re-read,
    # and probing them refused every run after the first.
    batch_rows = rows_a_cursor_read_will_deliver(
        sample_rows,
        cursor_column=incremental_cursor_column,
        watermark=incremental_watermark,
        tiebreak_column=incremental_tiebreak_column,
    )
    result = probe_destination_key_collisions(
        destination_config=destination_config,
        destination_db_type=destination_db_type,
        destination_table=destination_table,
        key_column=target_key,
        values=[row.get(source_key) for row in batch_rows],
    )
    if incremental_cursor_column and incremental_watermark:
        result.delta_scope = {
            "cursor_column": incremental_cursor_column,
            "watermark": str(incremental_watermark),
            "tiebreak_column": incremental_tiebreak_column,
            "sample_rows": len(sample_rows or []),
            "delta_rows": len(batch_rows),
        }
    # Resume re-reads from the last committed checkpoint, so the overlap with
    # rows already at rest is the interrupted batch, not a new append. The
    # writer resolves it on the enforced key; blocking here would strand a
    # half-loaded destination with no forward path.
    result.idempotent_apply = bool(resume)
    return result


def probe_destination_key_collisions(
    *,
    destination_config: Mapping[str, Any] | None = None,
    destination_db_type: str = "",
    destination_table: str = "",
    key_column: str = "",
    values: list[Any] | None = None,
    limit: int = 5,
) -> DestinationCollisionResult:
    """Find keys already present at the destination for an append batch."""
    key = (key_column or "").strip()
    db_type = (destination_db_type or "").strip().lower()
    if not key:
        return DestinationCollisionResult(
            status="skipped_no_key",
            message="No single-column identity key resolved for collision probe",
            db_type=db_type,
        )
    probe_values = [
        str(v)
        for v in (values or [])
        if v is not None and str(v).strip() != ""
    ][:MAX_PROBE_VALUES]
    if not probe_values:
        return DestinationCollisionResult(
            status="skipped_no_values",
            message="No batch key values available for collision probe",
            db_type=db_type,
            key_column=key,
        )
    if not destination_config or not destination_table:
        return DestinationCollisionResult(
            status="skipped_no_destination",
            message="Destination connection or table unavailable for collision probe",
            db_type=db_type,
            key_column=key,
        )

    cfg = dict(destination_config)
    cfg.setdefault("type", db_type)
    try:
        if db_type in ("mongodb", "mongodb_atlas"):
            findings = _mongo_existing_keys(
                cfg, destination_table, key, probe_values, limit
            )
        elif db_type in SQLISH_SOURCE_TYPES:
            findings = _sql_existing_keys(
                cfg, destination_table, key, probe_values, limit
            )
        else:
            return DestinationCollisionResult(
                status="skipped_unsupported",
                message=(
                    "Append collision probe not implemented for destination type "
                    f"{db_type or 'unknown'}"
                ),
                db_type=db_type,
                key_column=key,
            )
    except Exception as exc:
        logger.warning("Destination collision probe failed: %s", exc, exc_info=exc)
        return DestinationCollisionResult(
            status="error",
            message=f"Destination collision probe skipped: {exc}"[:400],
            db_type=db_type,
            key_column=key,
            values_probed=len(probe_values),
        )

    return DestinationCollisionResult(
        findings=findings,
        status="ran",
        message=(
            f"Append collision probe on {destination_table}.{key} "
            f"({len(probe_values)} batch key(s) checked)"
        ),
        db_type=db_type,
        key_column=key,
        values_probed=len(probe_values),
    )
