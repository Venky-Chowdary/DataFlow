"""Destination-side key collision probe for append sync modes.

An append writes rows the destination has never seen. Re-appending a key, or
a whole mapped row, that is already stored is a duplicate. A PRIMARY KEY or
UNIQUE constraint makes that insert abort. A table without one stores the
second copy. Both are knowable before the write: one bounded comparison of
the batch against the destination.

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

from services.source_duplicate_probe import is_sqlish_source
from services.value_serializer import key_identity_text, present_cell_text

logger = logging.getLogger(__name__)

ProbeStatus = Literal[
    "ran",
    "skipped_no_key",
    "skipped_no_values",
    "skipped_no_destination",
    "skipped_unsupported",
    "error",
    "timed_out",
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

#: Most keys one probe call checks (in MAX_PROBE_VALUES-sized queries).
MAX_DELTA_PROBE_VALUES = 200_000

#: Longest a planning tool spends comparing the append batch with the stored
#: keys (D206). The comparison is an index lookup; the cap only bounds a lock
#: queue or an engine that cannot use the index. Unfinished is "not probed",
#: never clean, and Execute re-probes before its first write.
APPEND_COLLISION_STEP_SECONDS = 8.0


def batch_key_texts(values: list[Any] | None, *, limit: int = MAX_PROBE_VALUES) -> list[str]:
    """Present batch keys on the reader wire. SQL NULL is not a collision token;
    ``''`` and ``'a '`` are exact keys (D53, D70)."""
    out: list[str] = []
    for raw in values or []:
        key = key_identity_text(raw)
        if key is None:
            continue
        out.append(key)
        if len(out) >= limit:
            break
    return out


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
    # That is only true when the destination actually rejects a second copy.
    idempotent_apply: bool = False
    # True when a PRIMARY KEY or UNIQUE constraint would abort the insert.
    # False when the engine would store the second copy. Both are duplicates.
    key_enforced: bool = True
    # An incremental run reads past its watermark, so only that delta can
    # collide. Recorded so an operator can see which rows were actually probed.
    delta_scope: dict[str, Any] = field(default_factory=dict)
    # True when this batch is the interrupted run being delivered again.
    # A heap would store that overlap a second time, so resume still blocks.
    resume_redelivery: bool = False

    @property
    def ran(self) -> bool:
        return self.status == "ran"


def sync_mode_appends_without_key_resolution(sync_mode: str) -> bool:
    """True when the write inserts rows without resolving an existing key."""
    return (sync_mode or "").strip().lower() in APPEND_ONLY_SYNC_MODES


def _folded_names(columns: list[str] | None) -> list[str]:
    return [str(c).strip().lower() for c in (columns or []) if str(c or "").strip()]


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
    return destination_enforces_columns(
        [key_column],
        destination_pk_columns=destination_pk_columns,
        destination_unique_keys=destination_unique_keys,
    )


def destination_enforces_columns(
    columns: list[str],
    *,
    destination_pk_columns: list[str] | None = None,
    destination_unique_keys: list[dict[str, Any]] | None = None,
) -> bool:
    """True when a constraint rejects a second copy of this whole column set.

    Order does not matter. A shorter or longer key is a different constraint:
    repeating the first column of a composite key is not a collision.
    """
    wanted = _folded_names(columns)
    if not wanted:
        return False
    pk = _folded_names(destination_pk_columns)
    if pk and sorted(pk) == sorted(wanted):
        return True
    for uk in destination_unique_keys or []:
        cols = _folded_names(list(uk.get("columns") or []))
        if cols and sorted(cols) == sorted(wanted):
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


def _python_type(sa_type: Any) -> type | None:
    try:
        return sa_type.python_type if sa_type is not None else None
    except (NotImplementedError, AttributeError):
        return None


def _integer_key(text: str) -> int | None:
    """``'7'``, ``' 7'``, ``'+7'`` and ``'7.00'`` land in an integer key as 7."""
    stripped = text.strip()
    whole, dot, frac = stripped.partition(".")
    if dot and (not frac or frac.strip("0")):
        return None
    body = whole[1:] if whole[:1] in "+-" else whole
    return int(whole) if body.isdigit() and body.isascii() else None


def native_key_values(sa_type: Any, dialect_name: str, values: list[str]) -> list[Any] | None:
    """The batch keys in the stored column's own type, or ``None`` to compare as text.

    ``CAST(key AS TEXT) IN (...)`` cannot use the key's index, so every append
    plan read the whole destination: a seq scan whose cost grows with the rows
    already stored, run inside the tool's 45s budget (D206). The engine's own
    comparison on the column is the one its unique index enforces, so it is
    also the right collision test: an integer key ``'007'`` lands as ``7``.
    A value that cannot be the column's type cannot be a stored key and is
    left out. Types without an exact Python twin keep the text comparison.
    """
    py = _python_type(sa_type)
    if py is None or py is bool:
        return None
    if issubclass(py, int):
        return [k for k in (_integer_key(v) for v in values) if k is not None]
    if issubclass(py, str):
        # A LOB is never a key and compares only through a cast on these engines.
        if (dialect_name or "").lower() in _LOB_TEXT_DIALECTS and type(sa_type).__name__.upper() in {
            "CLOB", "NCLOB", "TEXT", "UNICODETEXT", "LONG",
        }:
            return None
        return list(values)
    return None


def _key_predicate(col: Any, sa_type: Any, dialect_name: str, values: list[str]) -> Any:
    import sqlalchemy as sa

    native = native_key_values(sa_type, dialect_name, values)
    if native is not None:
        return col.in_(native) if native else sa.false()
    # Compare as text: the batch carries stringified keys while the column may be
    # uuid / numeric, and an uncast IN () raises "operator does not exist".
    return sa.cast(col, key_comparison_carrier(dialect_name, values)).in_(values)


def _key_equals(col: Any, sa_type: Any, dialect_name: str, value: str) -> Any:
    import sqlalchemy as sa

    native = native_key_values(sa_type, dialect_name, [value])
    if native is not None:
        return col == native[0] if native else sa.false()
    return sa.cast(col, key_comparison_carrier(dialect_name, [value])) == value


def _bounded_destination_read(label: str, cfg: Mapping[str, Any], fn: Any) -> tuple[bool, Any]:
    """Run one destination read under its own step budget inside a tool call.

    Returns ``(finished, value)``; outside a tool call it simply runs.
    """
    from src.ai.copilot.tool_deadline import bounded_step

    run = bounded_step(
        label, fn, seconds=APPEND_COLLISION_STEP_SECONDS, conn=dict(cfg), role="destination"
    )
    if run.error is not None:
        raise run.error
    return (not run.timed_out), run.value


def _timed_out_result(table: str, key: str, db_type: str, probed: int) -> DestinationCollisionResult:
    return DestinationCollisionResult(
        status="timed_out",
        message=(
            f"the destination did not answer the key comparison on {table} within "
            f"{APPEND_COLLISION_STEP_SECONDS:g}s (a lock on the table or a slow server); "
            "not proven either way"
        ),
        db_type=db_type,
        key_column=key,
        values_probed=probed,
    )


def _sql_existing_keys(
    cfg: dict[str, Any],
    table: str,
    key_column: str,
    values: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table
    from services.sql_object_identity import resolve_object_identity

    engine = _engine(cfg)
    schema, table = split_qualified_table(table, (cfg.get("schema") or "").strip() or None)
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
    stmt = (
        sa.select(col)
        .select_from(tbl)
        .where(
            _key_predicate(
                col,
                ident.column_types.get(key_column),
                str(getattr(engine.dialect, "name", "")),
                values,
            )
        )
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
    from connectors.mongodb_common import guarded_mongo_client

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
    client: MongoClient = guarded_mongo_client(uri, serverSelectionTimeoutMS=5000)
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
        text = present_cell_text(cell)
        if text is None:
            delta.append(row)
            continue
        if use_tiebreak:
            candidate = encode_keyset_bookmark(
                [text, present_cell_text(row.get(tiebreak_column)) or ""]
            )
        else:
            candidate = text
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
    table, overwrite/upsert semantics), which is different from a probe that
    could not run and must not block.

    An append of a key the destination enforces aborts the insert, so Validate
    blocks before Execute. A table with no key stores the second copy; that is
    append, so Validate warns and the write proceeds. Upsert, merge, and
    overwrite already say what a colliding key becomes, so they are not probed.
    """
    if not sync_mode_appends_without_key_resolution(sync_mode):
        return None
    if destination_table_exists is not True:
        return None

    pairs = _identity_pairs(
        mappings=mappings,
        source_columns=source_columns,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
        destination_pk_columns=destination_pk_columns,
        contract_primary_key=contract_primary_key,
        stream_contracts=stream_contracts,
        source_table=source_table,
        destination_table=destination_table,
    )
    if not pairs:
        return None

    target_columns = [target for _source, target in pairs]
    enforced = destination_enforces_columns(
        target_columns,
        destination_pk_columns=destination_pk_columns,
        destination_unique_keys=destination_unique_keys,
    )
    # The plan's key list can be the source identity copied forward. A heap
    # then blocked the second append with "enforces uniqueness" and inserted
    # nothing (DEF-B2-001). The live catalog is the enforcement fact for a
    # single column. A composite key is not a single-column reject, so the
    # plan answer stands. A catalog miss returns no names and does not invent
    # a blocker; the engine still rejects a real duplicate at insert time.
    if (
        destination_config
        and destination_table
        and len(target_columns) == 1
        and is_sqlish_source(destination_db_type)
    ):
        live_keys = live_single_column_unique_keys(
            destination_config, destination_table
        )
        wanted = target_columns[0].casefold()
        enforced = any(str(key).casefold() == wanted for key in live_keys)
    # Probe the rows this run will read, not the whole table. An incremental
    # append past a watermark cannot collide with keys it will never re-read,
    # and probing them refused every run after the first.
    batch_rows = rows_a_cursor_read_will_deliver(
        sample_rows,
        cursor_column=incremental_cursor_column,
        watermark=incremental_watermark,
        tiebreak_column=incremental_tiebreak_column,
    )
    if len(pairs) == 1:
        source_key, target_key = pairs[0]
        result = probe_destination_key_collisions(
            destination_config=destination_config,
            destination_db_type=destination_db_type,
            destination_table=destination_table,
            key_column=target_key,
            values=[row.get(source_key) for row in batch_rows],
        )
    else:
        result = probe_destination_tuple_collisions(
            destination_config=destination_config,
            destination_db_type=destination_db_type,
            destination_table=destination_table,
            columns=target_columns,
            tuples=[
                tuple(key_identity_text(row.get(source)) for source, _target in pairs)
                for row in batch_rows[:MAX_PROBE_VALUES]
            ],
        )
    if incremental_cursor_column and incremental_watermark:
        result.delta_scope = {
            "cursor_column": incremental_cursor_column,
            "watermark": str(incremental_watermark),
            "tiebreak_column": incremental_tiebreak_column,
            "sample_rows": len(sample_rows or []),
            "delta_rows": len(batch_rows),
        }
    result.key_enforced = enforced
    # Resume re-reads from the last committed checkpoint. The overlap is safe
    # only when the destination rejects a second copy of the key. A heap table
    # would store that overlap again, so resume does not excuse it.
    result.idempotent_apply = bool(resume and enforced)
    result.resume_redelivery = bool(resume)
    return result


def _mapped_pairs(mappings: list[dict[str, Any]] | None) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for item in mappings or []:
        if not isinstance(item, dict):
            continue
        source = str(item.get("source") or "").strip()
        target = str(item.get("target") or "").strip()
        if source and target:
            pairs.append((source, target))
    return pairs


def _identity_pairs(
    *,
    mappings: list[dict[str, Any]] | None,
    source_columns: list[str] | None,
    dest_kind: str,
    validation_mode: str,
    destination_pk_columns: list[str] | None,
    contract_primary_key: str | None,
    stream_contracts: list[dict[str, Any]] | None,
    source_table: str,
    destination_table: str,
) -> list[tuple[str, str]]:
    """Source/target columns that identify a row, else every mapped column.

    The identity resolver is the same one uniqueness already uses. When it
    names nothing, the mapped row itself is the identity: appending that row
    again is the same data.
    """
    mapped = _mapped_pairs(mappings)
    target_by_source = {source: target for source, target in mapped}
    try:
        from services.primary_key import resolve_primary_key_source_columns

        source_keys = resolve_primary_key_source_columns(
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
        source_keys = []
    if source_keys:
        return [
            (source, target_by_source.get(source, source))
            for source in source_keys
        ]
    return mapped


def probe_destination_key_collisions(
    *,
    destination_config: Mapping[str, Any] | None = None,
    destination_db_type: str = "",
    destination_table: str = "",
    key_column: str = "",
    values: list[Any] | None = None,
    limit: int = MAX_PROBE_VALUES,
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
    # Every key of the batch/delta is probed, MAX_PROBE_VALUES per query: a
    # collision past the first 500 keys used to pass the probe and abort the
    # insert (QA round 11, M3).
    probe_values = batch_key_texts(values, limit=MAX_DELTA_PROBE_VALUES)
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
                cfg, destination_table, key, probe_values[:MAX_PROBE_VALUES], limit
            )
        elif is_sqlish_source(db_type):
            findings = []
            for start in range(0, len(probe_values), MAX_PROBE_VALUES):
                chunk = probe_values[start : start + MAX_PROBE_VALUES]
                finished, found = _bounded_destination_read(
                    f"checking the append batch's {key} values against destination "
                    f"{destination_table}",
                    cfg,
                    lambda chunk=chunk: _sql_existing_keys(cfg, destination_table, key, chunk, limit),
                )
                if not finished:
                    return _timed_out_result(destination_table, key, db_type, len(probe_values))
                findings.extend(found or [])
                if len(findings) >= limit:
                    break
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


def _tuple_label(values: tuple[Any, ...]) -> str:
    return " | ".join("NULL" if value is None else str(value) for value in values)


def probe_destination_tuple_collisions(
    *,
    destination_config: Mapping[str, Any] | None = None,
    destination_db_type: str = "",
    destination_table: str = "",
    columns: list[str] | None = None,
    tuples: list[tuple[Any, ...]] | None = None,
    limit: int = MAX_PROBE_VALUES,
) -> DestinationCollisionResult:
    """Find mapped rows already stored, compared column by column.

    SQL ``IN`` does not match NULL, so each column uses equality or IS NULL.
    A skip is not proof that the batch is new.
    """
    names = [str(column).strip() for column in (columns or []) if str(column).strip()]
    key = ", ".join(names)
    db_type = (destination_db_type or "").strip().lower()
    batch = [tuple(row) for row in (tuples or []) if len(row) == len(names)]
    batch = batch[:MAX_PROBE_VALUES]
    if not names:
        return DestinationCollisionResult(
            status="skipped_no_key",
            message="No mapped columns resolved for collision probe",
            db_type=db_type,
        )
    if not batch:
        return DestinationCollisionResult(
            status="skipped_no_values",
            message="No batch rows available for collision probe",
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
            findings = _mongo_existing_tuples(cfg, destination_table, names, batch, limit)
        elif is_sqlish_source(db_type):
            finished, findings = _bounded_destination_read(
                f"checking the append batch's ({key}) rows against destination "
                f"{destination_table}",
                cfg,
                lambda: _sql_existing_tuples(cfg, destination_table, names, batch, limit),
            )
            if not finished:
                return _timed_out_result(destination_table, key, db_type, len(batch))
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
        logger.warning("Destination row collision probe failed: %s", exc, exc_info=exc)
        return DestinationCollisionResult(
            status="error",
            message=f"Destination collision probe skipped: {exc}"[:400],
            db_type=db_type,
            key_column=key,
            values_probed=len(batch),
        )
    return DestinationCollisionResult(
        findings=findings,
        status="ran",
        message=(
            f"Append collision probe on {destination_table} ({key}) "
            f"({len(batch)} batch row(s) checked)"
        ),
        db_type=db_type,
        key_column=key,
        values_probed=len(batch),
    )


def _sql_existing_tuples(
    cfg: dict[str, Any],
    table: str,
    columns: list[str],
    tuples: list[tuple[Any, ...]],
    limit: int,
) -> list[dict[str, Any]]:
    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table
    from services.sql_object_identity import resolve_object_identity

    engine = _engine(cfg)
    schema, table_name = split_qualified_table(
        table, (cfg.get("schema") or "").strip() or None
    )
    ident = resolve_object_identity(engine, table_name, schema, columns=columns)
    resolved = [
        ident.columns.get(column, column) if ident.exists else column
        for column in columns
    ]
    if ident.exists:
        table_name = sa.sql.quoted_name(ident.table, True)
        schema_name = sa.sql.quoted_name(ident.schema, True) if ident.schema else None
    else:
        schema_name = schema
    tbl = sa.table(table_name, schema=schema_name)
    cols = [
        sa.column(sa.sql.quoted_name(name, True) if ident.exists else name)
        for name in resolved
    ]
    dialect_name = str(getattr(engine.dialect, "name", ""))
    types = [ident.column_types.get(name) for name in resolved]
    clauses = []
    for row in tuples:
        parts = []
        for col, sa_type, value in zip(cols, types, row):
            if value is None:
                parts.append(col.is_(None))
            else:
                # Native equality keeps the key's index usable (D206).
                parts.append(_key_equals(col, sa_type, dialect_name, str(value)))
        clauses.append(sa.and_(*parts))
    stmt = sa.select(*cols).select_from(tbl).where(sa.or_(*clauses)).limit(limit)
    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [
        {"column": ", ".join(resolved), "value": _tuple_label(tuple(row))}
        for row in rows
    ]


def _mongo_existing_tuples(
    cfg: dict[str, Any],
    collection: str,
    columns: list[str],
    tuples: list[tuple[Any, ...]],
    limit: int,
) -> list[dict[str, Any]]:
    from pymongo import MongoClient
    from connectors.mongodb_common import guarded_mongo_client

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
    client: MongoClient = guarded_mongo_client(uri, serverSelectionTimeoutMS=5000)
    try:
        db = client[cfg.get("database") or cfg.get("auth_source") or "test"]
        query = {
            "$or": [
                {column: value for column, value in zip(columns, row)}
                for row in tuples
            ]
        }
        cursor = db[collection].find(query, {column: 1 for column in columns}).limit(limit)
        return [
            {
                "column": ", ".join(columns),
                "value": _tuple_label(tuple(doc.get(column) for column in columns)),
            }
            for doc in cursor
        ]
    finally:
        client.close()


def live_single_column_unique_keys(
    destination_config: Mapping[str, Any],
    destination_table: str,
) -> list[str]:
    """Single-column PRIMARY KEY or UNIQUE names on the live table.

    A composite key does not reject a repeated first column, so it is not an
    enforced append key. An unreadable catalog returns an empty list: the
    database still rejects a real duplicate, and a false empty list must not
    invent a blocker on a heap.
    """
    from sqlalchemy import inspect as sa_inspect

    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table

    cfg = dict(destination_config)
    schema, table_name = split_qualified_table(
        destination_table, (cfg.get("schema") or "").strip() or None
    )
    try:
        engine = _engine(cfg)
        insp = sa_inspect(engine)
        pk = insp.get_pk_constraint(table_name, schema=schema) or {}
    except Exception as exc:  # noqa: BLE001 — catalog miss is not a heap
        logger.warning("append key catalog read failed: %s", exc)
        return []
    names: list[str] = []
    pk_cols = [str(c) for c in (pk.get("constrained_columns") or []) if c]
    if len(pk_cols) == 1:
        names.append(pk_cols[0])
    try:
        uniques = insp.get_unique_constraints(table_name, schema=schema) or []
    except Exception as exc:  # noqa: BLE001 — PK alone is still enforced
        logger.debug("unique constraint catalog read failed: %s", exc)
        uniques = []
    for item in uniques:
        cols = [str(c) for c in (item.get("column_names") or []) if c]
        if len(cols) == 1 and cols[0] not in names:
            names.append(cols[0])
    return names


#: What to do about an append that re-delivers stored keys (one wording for
#: Validate and the writer): the modes that land an updated row, or the policy
#: that keeps the stored row and loads only the new keys.
APPEND_COLLISION_REMEDY = (
    "Use sync mode incremental_deduped / incremental_upsert (or upsert) so an updated row replaces "
    "the stored one, or set the duplicate policy to dedupe_skip to keep the "
    "stored row and load only the new keys."
)


def refuse_enforced_append_before_write(
    *,
    destination_config: Mapping[str, Any] | None,
    destination_db_type: str,
    destination_table: str,
    headers: list[str],
    data_rows: list[Any],
    mappings: list[dict[str, Any]] | None,
) -> str | None:
    """Refuse an insert that the live key would abort, before any row lands.

    Validate is supposed to catch this. When that probe did not run, Execute
    inserted until the engine aborted and reported zero rows over a prefix
    that had already committed.
    """
    if not data_rows or not destination_config or not destination_table:
        return None
    keys = live_single_column_unique_keys(destination_config, destination_table)
    if not keys:
        return None
    target_to_source: dict[str, str] = {}
    for item in mappings or []:
        if not isinstance(item, dict):
            continue
        source = str(item.get("source") or "").strip()
        target = str(item.get("target") or "").strip()
        if source and target:
            target_to_source[target.casefold()] = source
    header_index = {str(name): index for index, name in enumerate(headers or [])}
    for key in keys:
        source = target_to_source.get(str(key).casefold())
        if source is None or source not in header_index:
            continue
        index = header_index[source]
        values = [
            row[index]
            if isinstance(row, (list, tuple)) and index < len(row)
            else None
            for row in data_rows
        ]
        result = probe_destination_key_collisions(
            destination_config=destination_config,
            destination_db_type=destination_db_type,
            destination_table=destination_table,
            key_column=key,
            values=values,
        )
        if result.status == "error":
            return (
                f"Append refused before insert: the destination enforces {key} "
                f"and the collision probe could not run ({result.message}). "
                "No rows from this batch were written."
            )
        if result.findings:
            return (
                f"Append refused before insert: {len(result.findings)} key "
                f"value(s) in this batch are already stored on {key}, which "
                "enforces uniqueness. No rows from this batch were written. "
                + APPEND_COLLISION_REMEDY
            )
    return None
