"""Sync cursor watermarks — incremental and CDC transfer state.

Prefers MongoDB when a shared backend is available (multi-replica safe via
find_one_and_update). Falls back to atomic JSON file for single-instance /
test mode only.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from services.atomic_file import write_json_atomic
from services.keyset_pagination import (
    KEYSET_SEP,
    encode_keyset_bookmark,
    split_cursor_bookmark,
)
from services.platform_config import data_dir
from services.value_serializer import json_default

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


def destination_exists_for_typing(mode: str | None, exists: bool | None) -> bool | None:
    """Does the table the write will land in exist *at write time*?

    Overwrite drops and recreates, so whatever is there now is not what the rows
    land in: for typing purposes the destination is create-new every run, not
    just the first.

    Conflating the two made the second run of an overwrite fail on every
    destination. Run one created the table and invented types from the source.
    Run two found the table present but — correctly — carrying no usable column
    types, since typing against a shape about to be dropped would be wrong. The
    mapper read "exists, no columns" as "wait for a Studio stamp", left every
    target type pending, and the schema-contract gate then refused a transfer it
    had approved minutes earlier. Any schedule on overwrite failed from its
    second tick onward.
    """
    if is_overwrite_sync(mode):
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
        pks = data.get("primary_keys")
        if isinstance(pks, list) and pks:
            primary_key = ",".join(str(x).strip() for x in pks if str(x).strip())
        else:
            primary_key = str(data.get("primary_key") or "").strip()
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


def build_cursor_key(
    *,
    source_type: str,
    source_database: str,
    source_object: str,
    dest_type: str,
    dest_database: str,
    dest_object: str,
    stream_name: str = "stream",
) -> str:
    return (
        f"{source_type}:{source_database}:{source_object}"
        f"→{dest_type}:{dest_database}:{dest_object}:{stream_name}"
    )


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

    @property
    def bounded(self) -> bool:
        """True when a stored watermark narrows the read to a delta."""
        return bool(self.cursor_column and self.watermark)


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
    cursor_key = build_cursor_key(
        source_type=source_type,
        source_database=source_database,
        source_object=source_object,
        dest_type=dest_type,
        dest_database=dest_database,
        dest_object=dest_object,
        stream_name=contract.name if contract else "stream",
    )
    pk_cols = contract.primary_key_columns() if contract else []
    tiebreak = next((c for c in pk_cols if c and c != cursor_column), "")
    return IncrementalReadScope(
        cursor_column=cursor_column,
        primary_key=tiebreak,
        watermark=get_watermark(cursor_key),
        cursor_key=cursor_key,
    )


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


def set_watermark(cursor_key: str, watermark: str, *, metadata: dict[str, Any] | None = None) -> None:
    """Persist watermark with CAS semantics when Mongo is available."""
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
        values = [rows[i][idx] for i in range(len(rows)) if idx < len(rows[i]) and rows[i][idx]]
        if not values:
            return None
        from services.cdc_engine import infer_watermark_type, max_watermark

        str_values = [str(v) for v in values]
        wm_type = infer_watermark_type(str_values)
        return max_watermark(str_values, wm_type)

    best: str | None = None
    for row in rows:
        if idx >= len(row) or not row[idx]:
            continue
        pk_val = row[pk_idx] if pk_idx < len(row) else ""
        cand = encode_keyset_bookmark([row[idx], pk_val])
        if best is None or compare_cursor_values(cand, best) > 0:
            best = cand
    return best


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
        if a_pk == b_pk:
            return 0
        return 1 if a_pk > b_pk else -1
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
