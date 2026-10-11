"""Sync cursor watermarks — incremental and CDC transfer state.

Prefers MongoDB when a shared backend is available (multi-replica safe via
find_one_and_update). Falls back to atomic JSON file for single-instance /
test mode only.
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from services.atomic_file import write_json_atomic
from services.keyset_pagination import (
    KEYSET_SEP,
    encode_keyset_bookmark,
    incremental_tiebreak_column,
    split_cursor_bookmark,
)
from services.platform_config import data_dir
from services.value_serializer import json_default, present_cell_text

_logger = logging.getLogger(__name__)

STORE_PATH = data_dir() / "sync_cursors.json"

INCREMENTAL_MODES = frozenset({
    "incremental_append",
    "incremental_deduped",
    "cdc",
    # SCD2 tracks history from a watermark; reading the whole source every run
    # would re-open already-closed validity windows.
    "scd2",
})

OVERWRITE_SYNC_MODES = frozenset({
    "full_refresh_overwrite",
    "overwrite",
    "replace",
    "truncate",
    "full_overwrite",
})

APPEND_SYNC_MODES = frozenset({
    "full_refresh_append",
    "append",
    "incremental_append",
    "insert",
    "full_append",
})

#: The one set of sync modes the engine acts on. Every other layer's spelling
#: is an alias onto this set — see :data:`_SYNC_MODE_ALIASES`.
CANONICAL_SYNC_MODES = frozenset({
    "full_refresh_overwrite",
    "full_refresh_append",
    "incremental_append",
    "incremental_deduped",
    "cdc",
    "scd2",
    # Full scan, key-idempotent write, destination-only rows left alone.
    "upsert",
    # `mirror` is upsert *plus* deleting destination rows the source no longer
    # has. It is destructive, so nothing aliases onto it implicitly.
    "mirror",
    "reverse_etl",
})

_SYNC_MODE_ALIASES = {
    "full_append": "full_refresh_append",
    "fullappend": "full_refresh_append",
    "append": "full_refresh_append",
    "insert": "full_refresh_append",
    "overwrite": "full_refresh_overwrite",
    "full_overwrite": "full_refresh_overwrite",
    "fulloverwrite": "full_refresh_overwrite",
    "replace": "full_refresh_overwrite",
    "truncate": "full_refresh_overwrite",
    "trunc": "full_refresh_overwrite",
    # schedule_store spelling. Airbyte's bare "Incremental" is append-mode, and
    # append is also this module's non-destructive default, so a schedule that
    # says "incremental" gets incremental *append* rather than a dedup it did
    # not ask for. Before this alias existed the token fell through unmapped and
    # requires_incremental() returned False — every "incremental" schedule
    # quietly re-read the whole source on each run.
    "incremental": "incremental_append",
    # copilot/transfer_tools spelling. These two fell through as well, so a
    # Pilot-planned "incremental_upsert" wrote plain INSERTs and duplicated
    # rows on every re-run.
    "incremental_upsert": "incremental_deduped",
    "cdc_incremental": "cdc",
    # A bare "upsert"/"merge" says how to write, not what to read, and it must
    # not become `mirror` — mirror also deletes destination rows the source no
    # longer has. Nor `incremental_deduped`, which would demand a cursor field
    # the caller never declared. It stays its own canonical mode.
    "merge": "upsert",
    "dedupe": "incremental_deduped",
    "deduped": "incremental_deduped",
    "incremental_dedup": "incremental_deduped",
    "full_refresh_mirror": "mirror",
    "scd_2": "scd2",
    "slowly_changing_dimension": "scd2",
}


def normalize_sync_mode(mode: str | None, *, default: str = "full_refresh_append") -> str:
    """Canonical sync mode string (UI + API aliases).

    Default is **append** (non-destructive) so omitting sync_mode never silently
    wipes a destination. Overwrite must be explicit.

    Four layers spell these modes differently — ``schedule_store`` says
    ``incremental``, ``transfer_tools`` says ``incremental_upsert`` and
    ``cdc_incremental``, this module says ``incremental_deduped``. Unmapped
    tokens used to be returned verbatim, which meant they matched none of the
    behaviour sets below and silently degraded to full-read + insert. Everything
    now resolves onto :data:`CANONICAL_SYNC_MODES`, and anything still unknown
    is logged rather than passed through unnoticed.
    """
    raw = (mode or "").strip().lower().replace("-", "_").replace(" ", "_")
    if not raw:
        return default
    resolved = _SYNC_MODE_ALIASES.get(raw, raw)
    if resolved not in CANONICAL_SYNC_MODES:
        _logger.warning(
            "Unknown sync_mode %r does not resolve to a canonical mode %s; "
            "it will not trigger incremental reads or upsert writes.",
            mode,
            sorted(CANONICAL_SYNC_MODES),
        )
    return resolved


def is_overwrite_sync(mode: str | None) -> bool:
    """True when destination objects must be dropped/replaced before load."""
    normalized = normalize_sync_mode(mode, default="")
    return normalized in OVERWRITE_SYNC_MODES or normalized == "full_refresh_overwrite"


def is_append_sync(mode: str | None) -> bool:
    normalized = normalize_sync_mode(mode, default="")
    return normalized in APPEND_SYNC_MODES or normalized == "full_refresh_append"


#: Destinations that have no column catalog to bind types to — ever. A Redis
#: keyspace and a Kafka topic are not tables: they report no column types on
#: every run, so waiting for a live shape waits forever.
SCHEMALESS_DESTINATIONS = frozenset({"redis", "kafka"})


def destination_exists_for_shape(
    exists: bool | None,
    *,
    dest_format: str = "",
) -> bool | None:
    """Dest-exists boolean for Map / G15 / auto-map.

    Never collapse ``exists=True`` + empty column catalog to missing. That
    collapse is a *typing* concern (``destination_exists_for_typing``) and
    invents create-new against a table the probe already listed.

    Schemaless dests have no dest-exists contract — treat as create-new for
    shape. Overwrite recreate is a separate branch; do not call this helper
    to decide DROP.
    """
    if (dest_format or "").strip().lower() in SCHEMALESS_DESTINATIONS:
        return False
    return exists


def destination_exists_for_typing(
    mode: str | None,
    exists: bool | None,
    *,
    has_live_column_types: bool = True,
    dest_format: str = "",
) -> bool | None:
    """Is there a live column shape for the write to bind its types to?

    Not a dest-exists / create-new authority. Use
    ``destination_exists_for_shape`` for Map / G15 / auto-map existence.

    Two situations answer no, and both used to be mistaken for "wait for a
    Studio stamp", which leaves every target type pending and makes the
    schema-contract gate refuse a transfer it had approved minutes earlier.

    *Overwrite drops and recreates.* Whatever is there now is not what the rows
    land in, so for typing the destination is create-new every run, not just the
    first. Run one created the table and invented types from the source; run two
    found the table present but carrying no usable column types — correctly,
    since typing against a shape about to be dropped would be wrong — and
    failed. Any schedule on overwrite failed from its second tick onward.

    *A destination with no column catalog has nothing to bind to.* Redis is a
    keyspace, not a table: it reports no column types on any run, so append,
    incremental and upsert all left their targets unstamped while Validate had
    invented ``string``, and Execute refused on the divergence.

    Existence that is *unknown* stays unknown. A probe that could not read the
    destination is not evidence that it has no columns, and inventing create-new
    there would type against a shape nobody looked at.
    """
    if is_overwrite_sync(mode):
        return False
    if (dest_format or "").strip().lower() in SCHEMALESS_DESTINATIONS:
        return False
    if exists is True and not has_live_column_types:
        return False
    return exists


def resolve_effective_sync_mode(
    request_mode: str | None,
    contract_mode: str | None = None,
) -> str:
    """Prefer an explicit contract mode; otherwise the request mode.

    Empty contract modes inherit the request so a defaulted contract cannot
    silently upgrade append → overwrite.
    """
    contract = (contract_mode or "").strip()
    if contract:
        return normalize_sync_mode(contract, default=normalize_sync_mode(request_mode))
    return normalize_sync_mode(request_mode)


def should_drop_destination_for_sync(
    *,
    request_sync_mode: str | None,
    contract_sync_mode: str | None = None,
) -> bool:
    """Gate destructive DROP/TRUNCATE on the effective sync mode only."""
    return is_overwrite_sync(
        resolve_effective_sync_mode(request_sync_mode, contract_sync_mode)
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class SyncContract:
    name: str
    sync_mode: str
    cursor_field: str = ""
    primary_key: str = ""  # single column or comma-separated composite
    schema_policy: str = "manual_review"
    validation_mode: str = "strict"
    #: What the cursor column means in the source, declared by the operator.
    #: A column's name and type cannot establish this: ``created_at`` exists on
    #: a table whose rows are updated in place, and a business date is set from
    #: a calendar, not a clock. See :data:`CURSOR_SEMANTICS`.
    cursor_semantics: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SyncContract:
        # Pilot stores ``primary_key`` as a list of source columns. Stringifying
        # that list made the engine merge on the literal "['id']", so a key
        # preflight had accepted never reached the write.
        raw_pk = data.get("primary_key")
        if raw_pk is None or (isinstance(raw_pk, str) and not str(raw_pk).strip()):
            raw_pk = data.get("primary_keys")
        if isinstance(raw_pk, (list, tuple)):
            primary_key = ",".join(str(x).strip() for x in raw_pk if str(x).strip())
        else:
            primary_key = str(raw_pk or "").strip()
        return cls(
            name=str(data.get("name") or data.get("stream") or "stream"),
            # Empty inherits request sync_mode via resolve_effective_sync_mode —
            # never hard-default to overwrite (that silently wiped append jobs).
            sync_mode=str(data.get("sync_mode") or "").strip(),
            cursor_field=str(data.get("cursor_field") or data.get("cursor") or ""),
            primary_key=primary_key,
            schema_policy=str(data.get("schema_policy") or "manual_review"),
            validation_mode=str(data.get("validation_mode") or "strict"),
            cursor_semantics=str(data.get("cursor_semantics") or "").strip().lower(),
        )

    def primary_key_columns(self) -> list[str]:
        """Return PK columns; supports ``id`` or ``order_id,line_id`` / ``primary_keys``."""
        raw = (self.primary_key or "").replace(";", ",")
        return [p.strip() for p in raw.split(",") if p.strip()]


def resolve_sync_contract(stream_contracts: list[dict[str, Any]] | None) -> SyncContract | None:
    """Pick the first selected stream contract."""
    selected = resolve_selected_sync_contracts(stream_contracts)
    return selected[0] if selected else None


def resolve_selected_sync_contracts(
    stream_contracts: list[dict[str, Any]] | None,
) -> list[SyncContract]:
    """Return every selected stream contract (multi-stream foundation)."""
    out: list[SyncContract] = []
    for raw in stream_contracts or []:
        if raw.get("selected", True):
            out.append(SyncContract.from_dict(raw))
    return out


# Unquoted identifiers on these engines are one object regardless of the
# case the operator typed. Oracle stores QA6B_UA_TSOR for qa6b_ua_tsor.
# A bookmark that kept the typed spelling treated the second run as a new
# route and re-read the whole table.
_BOOKMARK_FOLD_UPPER = frozenset({"oracle", "oracledb", "snowflake", "db2"})


def bookmark_identifier(engine: str, name: str) -> str:
    """Fold an unquoted identifier to the engine's stored case.

    A quoted part (``"qa6b_ua_tsor"``) is a different object and stays as
    written. Other engines keep the operator's spelling: Postgres ``Orders``
    and ``orders`` are not the same table.
    """
    raw = (name or "").strip()
    if not raw:
        return ""
    eng = (engine or "").strip().lower()
    if eng not in _BOOKMARK_FOLD_UPPER:
        return raw
    parts: list[str] = []
    for part in raw.split("."):
        piece = part.strip()
        if len(piece) >= 2 and piece[0] == piece[-1] and piece[0] in {'"', "`"}:
            parts.append(piece)
        else:
            parts.append(piece.upper())
    return ".".join(parts)


def build_cursor_key(
    *,
    source_type: str,
    source_database: str,
    source_object: str,
    dest_type: str,
    dest_database: str,
    dest_object: str,
    stream_name: str = "stream",
    dest_identity: str = "",
) -> str:
    """Route bookmark.

    ``dest_identity`` is empty for a route that has no connector id and no
    host, which keeps the historical key. Two destinations that share a
    database name (MySQL and MariaDB both called ``dataflow``) must not share
    the bookmark: the second load would treat the first destination's
    watermark as its own and skip rows it has never written.
    """
    source_database = bookmark_identifier(source_type, source_database)
    source_object = bookmark_identifier(source_type, source_object)
    dest_database = bookmark_identifier(dest_type, dest_database)
    dest_object = bookmark_identifier(dest_type, dest_object)
    base = (
        f"{source_type}:{source_database}:{source_object}"
        f"→{dest_type}:{dest_database}:{dest_object}:{stream_name}"
    )
    ident = (dest_identity or "").strip()
    if not ident:
        return base
    return f"{base}|{ident}"


def route_endpoint_identity(endpoint: Any) -> str:
    """Stable destination identity for a bookmark.

    Connector id wins. Host and port are the fallback when the route was
    built from a raw connection. A dict's ``id`` is not a connector id —
    procedure tests pass two payloads that differ only by binds, and those
    must keep one bookmark.
    """
    if endpoint is None:
        return ""
    if isinstance(endpoint, dict):
        cid = str(endpoint.get("connector_id") or "").strip()
        host = str(endpoint.get("host") or "").strip()
        port = endpoint.get("port")
    else:
        cid = str(getattr(endpoint, "connector_id", "") or "").strip()
        host = str(getattr(endpoint, "host", "") or "").strip()
        port = getattr(endpoint, "port", None)
    ident = ""
    if cid:
        ident = f"id:{cid}"
    elif host:
        port_s = str(port or "").strip()
        if port_s and port_s not in {"0", "None"}:
            ident = f"host:{host}:{port_s}"
        else:
            ident = f"host:{host}"
    return _stamp_engine_identity(endpoint, ident)


def _endpoint_format(endpoint: Any) -> str:
    if endpoint is None:
        return ""
    if isinstance(endpoint, dict):
        raw = endpoint.get("format") or endpoint.get("type") or endpoint.get("db_type") or ""
    else:
        raw = (
            getattr(endpoint, "format", None)
            or getattr(endpoint, "type", None)
            or ""
        )
    return str(raw or "").strip().lower()


def _stamp_engine_identity(endpoint: Any, ident: str) -> str:
    """MariaDB must not share a MySQL bookmark when the driver alias is mysql.

    The catalog id of MariaDB is ``mysql``. Two connectors on the same
    database name then build one cursor key, and the second destination
    looks reset. ``engine:mariadb`` is the distinguisher when host and
    connector id were not on the endpoint yet.
    """
    fmt = _endpoint_format(endpoint)
    if fmt not in {"mariadb", "maria"}:
        return ident
    tag = "engine:mariadb"
    if not ident:
        return tag
    if tag in ident:
        return ident
    return f"{ident}|{tag}"


_CURSOR_LOCK = threading.Lock()


@dataclass(frozen=True)
class IncrementalReadScope:
    """Which source rows the next run of this route will actually read.

    An incremental run reads past its stored watermark, so every pre-write check
    that reasons about "the rows about to be written" has to reason about that
    subset. Checks that used the whole-table sample instead condemned the second
    run of every incremental append: the keys already at rest are, of course,
    still in the source.
    """

    cursor_column: str = ""
    primary_key: str = ""
    watermark: str | None = None
    cursor_key: str = ""
    #: Column the stored watermark was actually measured on ("" when unrecorded).
    watermark_cursor_column: str = ""
    #: Tie-break column a composite watermark was written with ("" when the
    #: watermark is cursor-only or predates the record).
    watermark_tiebreak_column: str = ""

    @property
    def bounded(self) -> bool:
        """True when a stored watermark narrows the read to a delta."""
        return bool(self.cursor_column and self.watermark)

    @property
    def cursor_column_changed(self) -> bool:
        """Was the stored watermark measured on a different column?

        A watermark is a value *of one column*. Reusing ``id = 250`` to bound a
        read on ``updated_at`` either explodes (``invalid input syntax for type
        timestamp: "250"``) or, when both columns happen to be comparable,
        silently skips rows. Neither may be resolved by guessing.
        """
        stored = (self.watermark_cursor_column or "").strip()
        if not stored or not self.watermark:
            return False
        return stored.lower() != (self.cursor_column or "").strip().lower()


def resolve_incremental_read_scope(
    *,
    sync_mode: str,
    stream_contracts: list[dict[str, Any]] | None,
    source_type: str,
    source_database: str,
    source_object: str,
    dest_type: str,
    dest_database: str,
    dest_object: str,
    source: Any = None,
    destination: Any = None,
    destination_config: Any = None,
) -> IncrementalReadScope:
    """Resolve the cursor state of a route — the read side's own view of it.

    Callers must not rebuild the cursor key themselves: the transfer writes the
    watermark under this key, so a checker that derives a different key sees no
    watermark and silently reverts to whole-table reasoning.
    """
    if not requires_incremental(normalize_sync_mode(sync_mode)):
        return IncrementalReadScope()
    contract = resolve_sync_contract(stream_contracts)
    cursor_column = (contract.cursor_field if contract else "").strip()
    if not cursor_column:
        return IncrementalReadScope()
    object_name = source_object
    if source is not None:
        from services.procedure_source import source_object_for_cursor

        token = source_object_for_cursor(source, fallback="")
        if token:
            object_name = token
    stream_name = contract.name if contract else "stream"
    legacy_key = build_cursor_key(
        source_type=source_type,
        source_database=source_database,
        source_object=object_name,
        dest_type=dest_type,
        dest_database=dest_database,
        dest_object=dest_object,
        stream_name=stream_name,
    )
    owner = route_endpoint_identity(destination)
    if not owner and destination_config is not None:
        owner = route_endpoint_identity(destination_config)
    if destination_config is not None and _endpoint_format(destination) in {"mariadb", "maria"}:
        owner = _stamp_engine_identity(destination, owner)
    cursor_key = (
        build_cursor_key(
            source_type=source_type,
            source_database=source_database,
            source_object=object_name,
            dest_type=dest_type,
            dest_database=dest_database,
            dest_object=dest_object,
            stream_name=stream_name,
            dest_identity=owner,
        )
        if owner
        else legacy_key
    )
    pk_cols = contract.primary_key_columns() if contract else []
    if cursor_key != legacy_key:
        watermark, metadata = resolve_owned_watermark(
            cursor_key, legacy_key, owner=owner
        )
    else:
        watermark, metadata = get_watermark_record(cursor_key)
    metadata = dict(metadata or {})
    tiebreak = incremental_tiebreak_column(source_type, cursor_column, pk_cols)
    stored_tiebreak = str(metadata.get("tiebreak_column") or "").strip()
    # QA ACC-02: a composite watermark is a value of (cursor, tie-break) and can
    # only be decoded by a read that seeks on the column it was written with.
    # Run 1's row path took that column from the source catalog PK; run 2's
    # contract (incremental_append, no PK) named none, so the second read
    # decoded "cursor␟pk" single-column and refused. The stored column wins.
    if stored_tiebreak and watermark_is_composite(watermark):
        tiebreak = stored_tiebreak
    return IncrementalReadScope(
        cursor_column=cursor_column,
        primary_key=tiebreak,
        watermark=watermark,
        cursor_key=cursor_key,
        watermark_cursor_column=str(metadata.get("cursor_column") or ""),
        watermark_tiebreak_column=stored_tiebreak,
    )


def watermark_is_composite(watermark: Any) -> bool:
    """True when a stored watermark carries a tie-break part."""
    from services.keyset_pagination import is_cursor_only_bookmark

    if watermark is None or str(watermark) == "":
        return False
    return not is_cursor_only_bookmark(str(watermark))


def reconcile_cursor_tiebreak(
    scope: IncrementalReadScope | None, computed: str
) -> str:
    """The tie-break this run must seek on, given the stored watermark.

    A composite watermark written with column ``c`` is only decodable on ``c``:
    the stored column wins over whatever this run derived. A legacy composite
    watermark with no recorded column keeps the derived one (catalog PK) — the
    same evidence the run that wrote it used.
    """
    if scope is not None and scope.watermark_tiebreak_column and watermark_is_composite(
        scope.watermark
    ):
        return scope.watermark_tiebreak_column
    return computed


def _mongo_cursors():  # type: ignore[no-untyped-def]
    try:
        from services.mongodb_service import get_mongodb_service
        from services.worker_leases import requires_distributed_backend

        mongo = get_mongodb_service()
        if not mongo or type(mongo).__name__ == "MemoryMongoDBService":
            if requires_distributed_backend():
                return None
            return None
        if getattr(mongo, "client", None):
            db = mongo.get_database()
            if db is not None:
                return db["sync_cursors"]
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    return None


def _load() -> dict[str, Any]:
    if not STORE_PATH.exists():
        return {"cursors": []}
    try:
        return json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"cursors": []}


def _save(data: dict[str, Any]) -> None:
    write_json_atomic(STORE_PATH, data, indent=2, default=json_default)


def _claim_legacy_owner(legacy_key: str, owner: str) -> bool:
    """CAS-claim an unscoped watermark for this destination only.

    The first destination to resolve the old shared key keeps it. A second
    destination sees the owner and does not inherit the bookmark, so it
    re-reads instead of silently skipping rows it never loaded.
    """
    owner = (owner or "").strip()
    if not legacy_key or not owner:
        return False
    coll = _mongo_cursors()
    if coll is not None:
        try:
            doc = coll.find_one_and_update(
                {
                    "key": legacy_key,
                    "$or": [
                        {"metadata.route_owner": {"$exists": False}},
                        {"metadata.route_owner": None},
                        {"metadata.route_owner": ""},
                        {"metadata.route_owner": owner},
                    ],
                },
                {"$set": {"metadata.route_owner": owner}},
            )
            if doc is not None:
                return True
            existing = coll.find_one({"key": legacy_key})
            if not existing:
                return False
            meta = existing.get("metadata") if isinstance(existing.get("metadata"), dict) else {}
            return str(meta.get("route_owner") or "") == owner
        except Exception:
            _logger.exception("Mongo legacy cursor claim failed for %s", legacy_key)

    with _CURSOR_LOCK:
        data = _load()
        for entry in data.get("cursors", []):
            if entry.get("key") != legacy_key:
                continue
            meta = dict(entry.get("metadata") or {})
            current = str(meta.get("route_owner") or "")
            if current and current != owner:
                return False
            meta["route_owner"] = owner
            entry["metadata"] = meta
            _save(data)
            return True
    return False


def resolve_owned_watermark(
    scoped_key: str,
    legacy_key: str,
    *,
    owner: str,
) -> tuple[str | None, dict[str, Any]]:
    """Watermark for one destination, adopting an unscoped bookmark once.

    An empty scoped key does not fall through to the shared bookmark. That
    fall-through is how MariaDB skipped the 20 rows MySQL had already consumed.
    """
    watermark, metadata = get_watermark_record(scoped_key)
    if watermark is not None:
        return watermark, metadata
    if not _claim_legacy_owner(legacy_key, owner):
        return None, {}
    watermark, metadata = get_watermark_record(legacy_key)
    if watermark is None:
        return None, {}
    copied = dict(metadata)
    copied.pop("job_id", None)
    copied["route_owner"] = owner
    copied["adopted_from"] = legacy_key
    set_watermark(scoped_key, watermark, metadata=copied)
    return get_watermark_record(scoped_key)


def get_watermark_record(cursor_key: str) -> tuple[str | None, dict[str, Any]]:
    """Return ``(watermark, metadata)`` — the value and what it was measured on.

    The metadata carries the cursor column, so a route whose cursor was changed
    cannot have the old column's value applied to the new one.
    """
    coll = _mongo_cursors()
    if coll is not None:
        try:
            doc = coll.find_one({"key": cursor_key})
            if doc and doc.get("watermark") is not None:
                meta = doc.get("metadata")
                return str(doc["watermark"]), dict(meta) if isinstance(meta, dict) else {}
            return None, {}
        except Exception:
            _logger.exception("Mongo get_watermark failed for %s", cursor_key)

    for entry in _load().get("cursors", []):
        if entry.get("key") == cursor_key:
            val = entry.get("watermark")
            meta = entry.get("metadata")
            return (
                str(val) if val is not None else None,
                dict(meta) if isinstance(meta, dict) else {},
            )
    return None, {}


def get_watermark(cursor_key: str) -> str | None:
    coll = _mongo_cursors()
    if coll is not None:
        try:
            doc = coll.find_one({"key": cursor_key})
            if doc and doc.get("watermark") is not None:
                return str(doc["watermark"])
            return None
        except Exception:
            _logger.exception("Mongo get_watermark failed for %s", cursor_key)

    for entry in _load().get("cursors", []):
        if entry.get("key") == cursor_key:
            val = entry.get("watermark")
            return str(val) if val is not None else None
    return None


def _stamp_job_watermark(job_id: str, cursor_key: str, watermark: str, cursor_column: str) -> None:
    """Record the advanced watermark on the job that advanced it.

    The schedule finalizer and the operator's run history read the watermark
    from the job document; the cursor store alone is keyed by route, not run.
    """
    from services.mongodb_service import get_mongodb_service

    try:
        get_mongodb_service().update_job_fields(
            job_id,
            {"cursor_value": watermark, "cursor_key": cursor_key, "cursor_column": cursor_column},
        )
    except Exception:
        _logger.exception("Could not stamp watermark on job %s", job_id)


def set_watermark(cursor_key: str, watermark: str, *, metadata: dict[str, Any] | None = None) -> None:
    """Persist watermark with CAS semantics when Mongo is available.

    ``metadata["job_id"]`` names the run that proved the delta at rest; the
    watermark is then also stamped on that job document as ``cursor_value``.
    """
    job_id = str((metadata or {}).get("job_id") or "").strip()
    if job_id:
        _stamp_job_watermark(job_id, cursor_key, watermark, str((metadata or {}).get("cursor_column") or ""))
    coll = _mongo_cursors()
    if coll is not None:
        try:
            now = _now()
            update: dict[str, Any] = {
                "key": cursor_key,
                "watermark": watermark,
                "updated_at": now,
            }
            if metadata:
                update["metadata"] = metadata
            coll.find_one_and_update(
                {"key": cursor_key},
                {
                    "$set": update,
                    "$setOnInsert": {"id": str(uuid.uuid4())},
                },
                upsert=True,
            )
            return
        except Exception:
            _logger.exception("Mongo set_watermark failed for %s; falling back to file", cursor_key)

    data = _load()
    entries = list(data.get("cursors", []))
    updated = False
    for entry in entries:
        if entry.get("key") == cursor_key:
            entry["watermark"] = watermark
            entry["updated_at"] = _now()
            if metadata:
                entry["metadata"] = {**entry.get("metadata", {}), **metadata}
            updated = True
            break
    if not updated:
        entries.append({
            "id": str(uuid.uuid4()),
            "key": cursor_key,
            "watermark": watermark,
            "updated_at": _now(),
            "metadata": metadata or {},
        })
    data["cursors"] = entries[-500:]
    _save(data)


def cursor_keys_for_job(job_id: str) -> list[str]:
    """Cursor keys whose stored metadata names this job.

    ``set_watermark`` records ``metadata.job_id``. Slot release uses that
    when the job document itself has no ``cursor_key``, so dropping the
    slot also drops the watermark the next run would try to resume.
    """
    jid = (job_id or "").strip()
    if not jid:
        return []
    found: list[str] = []

    def _add(value: Any) -> None:
        text = str(value or "").strip()
        if text and text not in found:
            found.append(text)

    coll = _mongo_cursors()
    if coll is not None:
        try:
            for doc in coll.find({"metadata.job_id": jid}, {"key": 1}):
                _add(doc.get("key"))
        except Exception:
            _logger.exception("Mongo cursor_keys_for_job failed for %s", jid)
    try:
        for entry in _load().get("cursors", []):
            if not isinstance(entry, dict):
                continue
            meta = entry.get("metadata")
            if isinstance(meta, dict) and str(meta.get("job_id") or "") == jid:
                _add(entry.get("key"))
    except Exception:
        _logger.exception("File cursor_keys_for_job failed for %s", jid)
    return found


def list_cursor_keys() -> list[str]:
    """Every persisted cursor key, so a reset can be aimed without guessing one.

    Clearing a watermark requires its exact key; an operator who has just reset
    a destination knows the route, not the key string.
    """
    coll = _mongo_cursors()
    if coll is not None:
        try:
            return [str(d.get("key")) for d in coll.find({}, {"key": 1}) if d.get("key")]
        except Exception:
            _logger.exception("Mongo list_cursor_keys failed")
    return [str(e.get("key")) for e in _load().get("cursors", []) if e.get("key")]


def clear_watermark(cursor_key: str) -> dict[str, Any]:
    """Delete a CDC/sync watermark so the next run re-snapshots (when_needed/initial).

    Returns ``{cleared: bool, cursor_key, prior_watermark}``.
    """
    key = (cursor_key or "").strip()
    if not key:
        return {"cleared": False, "cursor_key": "", "prior_watermark": None, "reason": "missing_cursor_key"}

    prior: str | None = get_watermark(key)
    coll = _mongo_cursors()
    if coll is not None:
        try:
            coll.delete_one({"key": key})
        except Exception:
            _logger.exception("Mongo clear_watermark failed for %s; falling back to file", key)

    data = _load()
    entries = [e for e in data.get("cursors", []) if e.get("key") != key]
    if len(entries) != len(data.get("cursors", [])):
        data["cursors"] = entries
        _save(data)

    return {
        "cleared": True,
        "cursor_key": key,
        "prior_watermark": prior,
        "reason": "ok" if prior is not None else "not_found",
    }


def _is_composite(watermark: str) -> bool:
    """A watermark carrying a tie-break part alongside the cursor value.

    The canonical separator is unambiguous. Watermarks persisted before it
    existed used a pipe, which a text cursor value can also contain — they keep
    comparing as composite so a stored watermark does not change meaning, while
    every watermark written from now on is unambiguous.
    """
    return KEYSET_SEP in watermark or "|" in watermark


def incremental_read_narrows(sync_mode: str) -> bool:
    """True when the write reads only rows past the stored watermark.

    SCD2 snapshots the whole source against current destination versions.
    CDC applies a changelog, not a table rescan. Incremental append/deduped
    are the modes whose population-fit walk must match the delta — a full
    table scan false-blocks the second run on historical overflows the
    write will never re-read.
    """
    return normalize_sync_mode(sync_mode, default="") in {
        "incremental_append",
        "incremental_deduped",
    }


#: Source types whose *read call* bounds an incremental page to the delta — a
#: SQL ``WHERE cursor > :watermark`` or the engine's own filtered scan. Every
#: other source hands its whole population over and the cursor contract has to
#: be honoured after the read (:func:`records_after_watermark`); a source that
#: is on neither side of this line silently re-reads everything, which appends
#: the full population again on every schedule tick.
CURSOR_PUSHDOWN_SOURCES = frozenset({
    "postgresql",
    "redshift",
    "mysql",
    "snowflake",
    "mongodb",
    "bigquery",
    "generic_sql",
})


def source_bounds_cursor_reads(source_type: str) -> bool:
    """True when this source's read call itself applies the cursor bound.

    One owner for the question, because the read path and the accounting path
    must answer it identically: a reader that pushes the bound down never hands
    a historical row over, while a key-addressed store (Redis ``SCAN``, Dynamo
    ``Scan``, an ES search) hands the whole keyspace over and the bound has to
    be applied to the page.
    """
    return (source_type or "").strip().lower() in CURSOR_PUSHDOWN_SOURCES


def row_after_watermark(
    rec: Any,
    cursor_column: str,
    watermark: str | None,
    *,
    primary_key: str = "",
) -> bool | None:
    """True if ``rec`` is past ``watermark``, False if already landed, None if unreadable.

    One predicate for the batch bound, the collision probe, and the
    population-fit walk. A missing cursor value is unreadable — callers that
    write must refuse; callers that only judge keep the row so it cannot
    silently leave the batch.
    """
    col = (cursor_column or "").strip()
    if not col:
        return True
    pk = (primary_key or "").strip()
    raw = rec.get(col) if isinstance(rec, dict) else None
    text = present_cell_text(raw)
    if text is None:
        return None
    candidate = text
    if pk and pk != col:
        candidate = encode_keyset_bookmark(
            [candidate, present_cell_text(rec.get(pk) if isinstance(rec, dict) else None) or ""]
        )
    if watermark is None:
        return True
    return compare_cursor_values(candidate, watermark) > 0


def iter_rows_after_watermark(
    rows: Any,
    scope: IncrementalReadScope | None,
    *,
    keep_unreadable: bool = True,
):
    """Yield the rows an incremental write will deliver.

    Historical rows (``cursor <= watermark``) are dropped. Unreadable cursor
    cells stay in the stream when ``keep_unreadable`` so a checker cannot
    shrink the batch it is judging. Pass-through when the scope is not bounded.
    """
    if rows is None:
        return None
    if scope is None or not scope.bounded:
        return rows

    def _gen():
        for rec in rows:
            verdict = row_after_watermark(
                rec,
                scope.cursor_column,
                scope.watermark,
                primary_key=scope.primary_key,
            )
            if verdict is False:
                continue
            if verdict is None and not keep_unreadable:
                continue
            yield rec

    return _gen()


def records_after_watermark(
    records: list[dict[str, Any]],
    cursor_column: str,
    watermark: str | None,
    *,
    primary_key: str = "",
) -> tuple[list[dict[str, Any]], int]:
    """Bound already-parsed records to the delta past ``watermark``.

    A database source is bounded in its WHERE clause, but a file or document
    payload arrives whole, so the same cursor contract has to be honoured after
    the parse — otherwise an "incremental append" of a daily CSV re-appends
    every row the file still contains, which is the duplicate-load operators
    report as data corruption and is exactly what the mode promised not to do.

    Comparison goes through :func:`compare_cursor_values`, so a file delta is
    bounded by the same typed comparator (and the same composite tie-break) the
    database reader uses. Returns ``(delta, unbounded)`` where ``unbounded``
    counts records that carry no cursor value: those cannot be proven new or
    old, and the caller must refuse rather than guess.
    """
    col = (cursor_column or "").strip()
    if not col:
        return list(records), 0
    delta: list[dict[str, Any]] = []
    unbounded = 0
    for rec in records:
        verdict = row_after_watermark(
            rec, cursor_column, watermark, primary_key=primary_key
        )
        if verdict is None:
            unbounded += 1
            continue
        if verdict:
            delta.append(rec)
    return delta, unbounded


def max_cursor_value(
    rows: list[list[str]],
    headers: list[str],
    cursor_column: str,
    cursor_primary_key: str | None = None,
) -> str | None:
    """Find maximum cursor value using typed watermark comparator.

    When ``cursor_primary_key`` is set, returns a composite watermark so peer
    rows sharing a timestamp are not skipped on the next incremental poll. The
    composite is encoded by :func:`encode_keyset_bookmark`, whose separator is
    not a value a source column can hold — a pipe is, so a text cursor value
    containing one used to be indistinguishable from a composite.
    """
    if not cursor_column or not rows:
        return None
    try:
        idx = headers.index(cursor_column)
    except ValueError:
        return None
    pk = (cursor_primary_key or "").strip()
    pk_idx: int | None = None
    if pk and pk != cursor_column:
        try:
            pk_idx = headers.index(pk)
        except ValueError:
            pk_idx = None

    if pk_idx is None:
        values: list[str] = []
        for row in rows:
            if idx >= len(row):
                continue
            text = present_cell_text(row[idx])
            if text is not None:
                values.append(text)
        if not values:
            return None
        from services.cdc_engine import infer_watermark_type, max_watermark

        wm_type = infer_watermark_type(values)
        return max_watermark(values, wm_type)

    best: str | None = None
    for row in rows:
        if idx >= len(row):
            continue
        cursor_text = present_cell_text(row[idx])
        if cursor_text is None:
            continue
        pk_val = row[pk_idx] if pk_idx < len(row) else ""
        cand = encode_keyset_bookmark([cursor_text, pk_val])
        if best is None or compare_cursor_values(cand, best) > 0:
            best = cand
    return best


def checkpoint_watermark(checkpoint: Any) -> str | None:
    """Cursor saved on a job checkpoint, if that record has one.

    CDC payloads use ``watermark``. The checkpoint record uses
    ``cursor_value``. A nested ``cdc.watermark`` is the same position.
    An empty string is a real cursor, not a missing one.
    """
    if checkpoint is None:
        return None
    data: Any = checkpoint
    if not isinstance(data, dict):
        if hasattr(data, "to_dict"):
            try:
                data = data.to_dict()
            except Exception:
                data = None
        if not isinstance(data, dict):
            raw = getattr(checkpoint, "cursor_value", None)
            if raw is None:
                raw = getattr(checkpoint, "watermark", None)
            return None if raw is None else str(raw)
    wm = data.get("watermark")
    if wm is None and isinstance(data.get("cdc"), dict):
        wm = data["cdc"].get("watermark")
    if wm is None:
        wm = data.get("cursor_value")
    if wm is None:
        return None
    return str(wm)


def _bare_table_name(name: str) -> str:
    """Table identity without a schema prefix. ``public.orders`` and ``orders`` match."""
    text = str(name or "").strip()
    if not text:
        return ""
    return text.rsplit(".", 1)[-1].strip()


def _same_table(left: str, right: str) -> bool:
    a = _bare_table_name(left)
    b = _bare_table_name(right)
    return bool(a) and a.lower() == b.lower()


def cursor_owner_table(watermark: Any) -> str | None:
    """Single table named inside a resume token.

    Query-CDC scalars name no table. A comma-separated list is one shared
    route cursor, not a table this token may seek on its own.
    """
    if watermark is None:
        return None
    from urllib.parse import unquote

    from services.cdc_resume_tokens import unwrap_resume_token

    token = watermark if isinstance(watermark, dict) else unwrap_resume_token(watermark)
    if isinstance(token, dict):
        raw = token.get("table")
        if isinstance(raw, list):
            names = [_bare_table_name(str(item)) for item in raw]
            names = [item for item in names if item]
            return names[0] if len(names) == 1 else None
        if raw is None:
            return None
        text = str(raw).strip()
        if not text or "," in text:
            return None
        return _bare_table_name(text) or None
    text = str(token or "")
    table = ""
    for part in text.split("|"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key.strip().lower() == "table" and value.strip():
            table = unquote(value.strip())
    if not table or "," in table:
        return None
    return _bare_table_name(table) or None


def checkpoint_stream_name(checkpoint: Any) -> str | None:
    """Stream a checkpoint record claims, when it claims one."""
    if checkpoint is None:
        return None
    named = getattr(checkpoint, "cdc_stream", None)
    if isinstance(named, str) and named.strip() and "," not in named:
        return _bare_table_name(named) or None
    data: Any = checkpoint
    if not isinstance(data, dict) and hasattr(checkpoint, "to_dict"):
        try:
            data = checkpoint.to_dict()
        except Exception:
            data = None
    if not isinstance(data, dict):
        return None
    for key in ("cdc_stream", "stream", "stream_name"):
        raw = data.get(key)
        if isinstance(raw, str) and raw.strip() and "," not in raw:
            return _bare_table_name(raw) or None
    return None


def _checkpoint_is_shared(checkpoint: Any) -> bool:
    if checkpoint is None:
        return False
    if isinstance(checkpoint, dict):
        return bool(checkpoint.get("cdc_shared_reader"))
    if getattr(checkpoint, "cdc_shared_reader", False):
        return True
    if hasattr(checkpoint, "to_dict"):
        try:
            data = checkpoint.to_dict()
        except Exception:
            return False
        return bool(isinstance(data, dict) and data.get("cdc_shared_reader"))
    return False


def _looks_like_log_resume(watermark: str) -> bool:
    """True when the cursor is a log position, not a query-CDC scalar."""
    from services.cdc_resume_tokens import unwrap_resume_token

    token = unwrap_resume_token(watermark)
    if isinstance(token, dict):
        if str(token.get("phase") or "").strip().lower() in {"snapshot", "streaming"}:
            return True
        return any(token.get(key) for key in ("kind", "lsn", "scn", "file", "gtid", "gtid_set"))
    text = str(token or "").strip().lower()
    return "phase=" in text or "slot=" in text or "lsn=" in text or text.startswith("{")


def resume_watermark(
    stored: str | None,
    checkpoint: Any,
    *,
    stream: str | None = None,
    allow_unnamed: bool = True,
    shared: bool = False,
) -> str | None:
    """Resume cursor. The cursor store wins when it has a value.

    A job checkpoint is throttled and can be older than the store. Copying
    it over the store rewinds the next poll and drops a keyset tie-break.
    When the store is empty, the checkpoint is the only record that a
    previous run already applied rows.

    That record is one cursor. A sequential multi-table run must not hand
    it to every stream. Pass ``stream`` and ``allow_unnamed=False`` so an
    unnamed scalar, or a token that names another table, is left behind
    and that stream snapshots. A single stream may still adopt an unnamed
    checkpoint (``allow_unnamed=True``).

    The shared log reader (``shared=True``) adopts a route token onto its
    one key. A token that names one table is that table's keyset, not the
    route position, unless the checkpoint is marked ``cdc_shared_reader``.
    A route streaming token names no table. A single table must not seek
    it: the next run snapshots (at-least-once upsert) instead of skipping
    the dump.
    """
    if stored is not None:
        return str(stored)
    wm = checkpoint_watermark(checkpoint)
    if wm is None:
        return None
    named = checkpoint_stream_name(checkpoint)
    token_table = cursor_owner_table(wm)
    if named and token_table and not _same_table(named, token_table):
        return None
    owner = named or token_table
    route = _checkpoint_is_shared(checkpoint)
    if shared:
        if route:
            return wm
        # A log token with no table and no stream can be a legacy route
        # cursor. One that names a table belongs to that table.
        if owner:
            return None
        if _looks_like_log_resume(wm):
            return wm
        return None
    if stream and str(stream).strip():
        if owner and _same_table(owner, stream):
            return wm
        if owner:
            return None
        # Shared handoff: phase=streaming, no table. Not this table's cursor.
        if route:
            return None
        if allow_unnamed:
            return wm
        return None
    if route and not owner:
        return None
    return wm


def isolate_stream_checkpoint(checkpoint: Any, stream_name: str) -> Any:
    """Resume record for one table in a multi-table load.

    The job checkpoint is one position. Passing that same object into the
    next table seeks it with the previous table's offset or keyset, and the
    write then stores the new position back onto the shared object. A
    checkpoint is applied only when it names this stream, and only as a
    copy. An unnamed checkpoint is not applied: each table reads from the
    start instead of skipping rows.
    """
    if checkpoint is None:
        return None
    name = str(stream_name or "").strip()
    if not name:
        return None
    owner = checkpoint_stream_name(checkpoint)
    if not owner or not _same_table(owner, name):
        return None
    from services.checkpoint_service import Checkpoint

    if isinstance(checkpoint, Checkpoint):
        return Checkpoint.from_dict(checkpoint.to_dict())
    if isinstance(checkpoint, dict):
        return Checkpoint.from_dict(checkpoint)
    return None


def advance_stored_cursor(
    current: str | None,
    candidate: str | None,
) -> tuple[str | None, bool]:
    """Move a stored cursor forward, including a tie-break the old value lacked.

    A composite candidate is compared on its cursor part, then its tie-break.
    Comparing the whole bookmark as text ranks ``10`` behind ``9`` and would
    leave the stored cursor on the old value, so the next poll re-reads the
    same page. A scalar watermark and a composite with the same cursor advance
    to the composite: the scalar cannot name which peer row was last applied.
    """
    if candidate is None or not str(candidate).strip():
        return current, False
    cand = str(candidate)
    if current is None or not str(current).strip():
        return cand, True
    cur = str(current)
    cand_cur, cand_pk = split_cursor_bookmark(cand, has_tiebreak=_is_composite(cand))
    cur_cur, cur_pk = split_cursor_bookmark(cur, has_tiebreak=_is_composite(cur))
    base = compare_cursor_values(cand_cur, cur_cur)
    if base > 0:
        return cand, True
    if base < 0:
        return cur, False
    if cand_pk and not cur_pk:
        return cand, True
    if cand_pk and cur_pk and compare_cursor_values(cand_pk, cur_pk) > 0:
        return cand, True
    return cur, False


def query_cdc_resume_watermark(
    records: list[dict[str, Any]],
    cursor_field: str,
    tiebreak: str,
    current: str | None,
) -> str | None:
    """Watermark after one query-CDC page.

    When the primary key is not the cursor, the stored value is
    ``(cursor, pk)``. The next poll seeks past that pair. A cursor-only
    value would drop every peer row that shared the page's cursor and was
    not in the page.
    """
    field = (cursor_field or "").strip()
    if not field:
        return current
    pk = (tiebreak or "").strip()
    headers = [field] + ([pk] if pk and pk != field else [])
    matrix: list[list[str]] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        matrix.append(["" if rec.get(col) is None else str(rec.get(col)) for col in headers])
    batch_max = max_cursor_value(matrix, headers, field, pk or None)
    new, advanced = advance_stored_cursor(current, batch_max)
    return new if advanced else current


def compare_cursor_values(a: str | None, b: str | None) -> int:
    """Compare two cursor values using the same typed watermark logic.

    Returns -1 if a < b, 0 if equal, 1 if a > b.  None is treated as less
    than any value. Composite watermarks compare lexicographically, and a
    watermark written before the canonical separator existed still compares.
    """
    if a is None and b is None:
        return 0
    if a is None:
        return -1
    if b is None:
        return 1
    sa, sb = str(a), str(b)
    if _is_composite(sa) and _is_composite(sb):
        a_cur, a_pk = split_cursor_bookmark(sa, has_tiebreak=True)
        b_cur, b_pk = split_cursor_bookmark(sb, has_tiebreak=True)
        base = compare_cursor_values(a_cur, b_cur)
        if base != 0:
            return base
        # The tie-break is a key, typed like the cursor: comparing it as text
        # ranks pk 999 above pk 2000, so a batch's high mark stopped at the
        # lexically largest key and the next run re-read every key above it.
        return compare_cursor_values(a_pk, b_pk)
    from services.cdc_engine import compare_watermarks, infer_watermark_type

    wm_type = infer_watermark_type([sa, sb])
    return compare_watermarks(sa, sb, wm_type)


def requires_incremental(sync_mode: str) -> bool:
    return (sync_mode or "").lower() in INCREMENTAL_MODES


#: Modes whose write must be key-idempotent. ``full_refresh_mirror`` is no
#: longer listed because it normalizes to ``mirror`` before reaching this check.
UPSERT_MODES = frozenset({
    "upsert",
    "incremental_deduped",
    "cdc",
    "mirror",
    "reverse_etl",
    "scd2",
})


def requires_upsert(sync_mode: str) -> bool:
    return normalize_sync_mode(sync_mode, default="") in UPSERT_MODES


def map_source_to_target(column: str, mappings: list[dict[str, Any]]) -> str:
    for m in mappings:
        if str(m.get("source") or "") == column:
            return str(m.get("target") or column)
    return column
