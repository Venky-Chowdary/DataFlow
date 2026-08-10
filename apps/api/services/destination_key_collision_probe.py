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


@dataclass
class DestinationCollisionResult:
    """Structured probe outcome — a skip is never proof of a clean append."""

    findings: list[dict[str, Any]] = field(default_factory=list)
    status: ProbeStatus = "skipped_no_destination"
    message: str = ""
    db_type: str = ""
    key_column: str = ""
    values_probed: int = 0

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


def _sql_existing_keys(
    cfg: dict[str, Any],
    table: str,
    key_column: str,
    values: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    import sqlalchemy as sa

    from connectors.generic_sql import _engine

    engine = _engine(cfg)
    schema = (cfg.get("schema") or "").strip() or None
    tbl = sa.table(table, schema=schema)
    col = sa.column(key_column)
    # Compare as text: the batch carries stringified keys while the column may be
    # bigint / uuid / numeric, and an uncast IN () raises "operator does not exist".
    stmt = (
        sa.select(col)
        .select_from(tbl)
        .where(sa.cast(col, sa.Text).in_(values))
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

    return probe_destination_key_collisions(
        destination_config=destination_config,
        destination_db_type=destination_db_type,
        destination_table=destination_table,
        key_column=target_key,
        values=[row.get(source_key) for row in (sample_rows or [])],
    )


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
