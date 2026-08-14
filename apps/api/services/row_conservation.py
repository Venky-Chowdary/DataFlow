"""Independent population conservation: dest COUNT(*), not writer ack.

The identity every cardinality-bearing load must close:

    reader_count == dest_population + hold_outs + skipped

``dest_population`` is an independent destination COUNT(*) — Gate-8's
``target_rows`` when that figure came from dest-engine read-back, not from
the writer's ``records_processed``. Closing the identity with the writer's
own acknowledgement is how AWS DMS reports Full Load success and later
``MISSING_TARGET``: the writer counted rows the dest engine does not hold.
Airbyte ``emitted`` / Fivetran MAR counters have the same circular shape.

Hold-outs are rows that did **not** land:

    hold_outs = max(rejected_rows - coerced_null_rows, 0)

Coerced-null rows *did* land (a cell became NULL). Counting them as both
quarantine and written invents a surplus. Gate-8 already uses this drop
arithmetic; this module is the single named identity so the certificate
cannot drift from it.

How dest_population is chosen:

* **overwrite / replace / empty dest** — dest COUNT(*) is the written
  population. Pre-existing rows were dropped or there were none.
* **file / object export** — dest population is an independent record
  COUNT of the artifact on disk (``artifact_readback``), not writer
  ``rows`` / bytes-landed. File replace is overwrite (dest-before 0).
  Cardinality ≠ cell checksum: Gate-8 stays unproven.
* **append** — only the delta proves anything:
  ``COUNT(*)_after - COUNT(*)_before``. A table that already held 30 rows
  satisfies ``dest >= expected`` even if the writer appended nothing.
  Iceberg snapshot COUNT and object-store record COUNT are dest-before
  the same way SQL COUNT(*) is — missing table/object is 0.
* **upsert / CDC into a non-empty dest** — COUNT(*) is not event
  conservation (updates do not change cardinality). Dest-engine key census
  closes the *cardinality* identity:

      dest_delta == inserts_new_keys - deletes

  where ``inserts_new_keys`` is *live* keys dest did not hold before the
  write, and ``deletes`` is dest-engine hits of *tombstone* keys (a
  tombstone for a key dest does not hold is a no-op — COUNT does not
  move). Writer ``records_processed`` still counts updates; it never
  closes the identity. Without a dest-engine census the ledger stays
  unproven.
* **vector / RAG** — one source row becomes N embedding
  chunks. Physical ``COUNT(*)`` of vectors is **not** dest population
  (2 documents → 5 chunks would invent a surplus). Dest population is
  dest-engine ``COUNT(DISTINCT source_id)`` (pgvector SQL, Milvus
  entity query, Qdrant point scroll):

      reader == COUNT(DISTINCT source_id) + hold_outs + skipped

  Empty dest (dest-before 0) is overwrite on identities. Non-empty dest
  without a this-run source_id key census stays unproven — chunk ``id``
  PK conflict is not source identity. Writer chunk-upsert ack never
  closes. Cardinality ≠ embedding cell checksum: Gate-8 stays unproven.
* **SCD Type 2** — one source identity becomes N history versions.
  Physical ``COUNT(*)`` of versions is **not** dest population (2
  identities → 3 history rows after one attribute change would invent a
  surplus). Dest population is dest-engine ``COUNT(*) WHERE is_current``:

      reader == COUNT(*) WHERE is_current + hold_outs + skipped

  Empty dest (dest-before 0) is first-load overwrite on current
  identities. Dest-before is physical history, not current-before —
  dest Δ must not mix them. Incremental watermarked SCD2 (a changed
  subset) stays unproven: this-run rows are version changes, not the
  current population. A full snapshot whose reader equals current
  closes (historical re-sync analogue). Writer version-upsert ack and
  Gate-8 stuffed ``active_rows`` never close. ``is_current`` is
  temporal, not a tombstone — do not reuse mirror ``_deleted``.
* **complete source PK census (overwrite)** — dest COUNT(*) nets one
  missing source key and one leftover dest key to a false balance (DMS
  ``MISSING_TARGET`` + ``EXTRA_TARGET``). Dest-engine key hits of the
  *complete* unique source PK set split them:

      missing = |S| − |D ∩ S|
      extra   = |D| − |D ∩ S|

  Incremental CDC must not run this split (the batch is not ``S``) and
  must not infer-delete leftover dest keys. A **complete overwrite
  snapshot** may MERGE-delete ``D \\ S`` (dest-engine anti-join, then
  ``delete_by_primary_keys``) so EXTRA_TARGET does not survive as a
  COUNT(*) surplus. That apply is not incremental CDC and not mirror
  ``_deleted``. Mirror already applies inferred soft-deletes on full
  re-sync.
* **mirror (inferred deletes)** — Fivetran-style ``_deleted`` flag:
  physical ``COUNT(*)`` does **not** drop. The identity is the dest-engine
  **active** population:

      reader == COUNT(*) WHERE NOT _deleted + hold_outs + skipped

  ``target_rows`` from Gate-8 on this path is that active count, not the
  physical table size. Using it as dest COUNT(*) would hide leftover
  dest keys (the Fivetran ``_fivetran_deleted`` hole). Physical COUNT
  and this-run inferred deletes are diagnostic when the mirror census
  measured them.

Writer ack is a diagnostic third number. It never closes the identity.
A mismatch against dest COUNT is the DMS hole, reported as a note, not as
proof that the rows landed.

An empty pass (reader 0, hold-outs 0, skipped 0, writer ack 0) is a
*measured* zero — the incremental steady state — not an unmeasured dest.

Multi-stream / multi-table jobs:

    job is closed iff every stream ledger is closed

Airbyte sums ``recordsEmitted`` / ``recordsCommitted`` (writer/state ack).
Fivetran MAR is connection-level. Taking the last stream's dest COUNT(*)
as the job is the same lie as taking writer ack. Dest COUNT(*) is summed
only when every stream closed the same additive kind (all overwrite, or
all mirror active). Mixed or keyed kinds stay per-stream — summing them
invents a fake job-level table.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from services.dest_precount import (
    ARTIFACT_COUNT_KEY,
    CURRENT_ROWS_KEY,
    EXTRA_KEYS_KEY,
    HISTORY_ROWS_KEY,
    IDENTITY_COUNT_KEY,
    LEFTOVER_DELETED_KEY,
    MISSING_KEYS_KEY,
    PRECOUNT_KEY,
    VECTOR_ROWS_KEY,
)
from services.reconcile_coverage import is_unproven_export
from services.sync_cursor import is_append_sync, is_overwrite_sync

DEST_READBACK = "gate8_dest_readback"
DEST_UNMEASURED = "unmeasured"
DEST_EMPTY_PASS = "empty_pass"
DEST_ARTIFACT_READBACK = "artifact_readback"
DEST_IDENTITY_READBACK = "identity_readback"
CENSUS_KEY = "keyed_census"
# Dest population that may close overwrite cardinality. Artifact COUNT is
# dest-engine analogue for a replaced file — not SQL COUNT(*), not writer ack.
# Identity COUNT is dest-engine analogue for chunked vector loads.
_INDEPENDENT_DEST = frozenset(
    {DEST_READBACK, DEST_ARTIFACT_READBACK, DEST_IDENTITY_READBACK}
)

KIND_OVERWRITE = "overwrite"
KIND_APPEND_DELTA = "append_delta"
KIND_KEYED = "keyed"
KIND_EMPTY_PASS = "empty_pass"
KIND_UNMEASURED = "unmeasured"
KIND_MIRROR = "mirror"
KIND_VECTOR = "vector"
KIND_SCD2 = "scd2"
KIND_JOB = "job_rollup"
DEST_ACTIVE_READBACK = "gate8_dest_active_readback"
DEST_CURRENT_READBACK = "current_readback"
DEST_PER_STREAM = "per_stream"
# Dest COUNT(*) / active / identity / current population is additive only
# when every stream closed the *same* population identity. Mixing
# overwrite COUNT(*) with keyed dest-delta invents a fake job-level table.
_SUMMABLE_KINDS = frozenset(
    {KIND_OVERWRITE, KIND_MIRROR, KIND_EMPTY_PASS, KIND_VECTOR, KIND_SCD2}
)


@dataclass(frozen=True)
class KeyCensus:
    """Dest-engine split of a keyed batch: live new keys vs dest-held deletes.

    ``unique_batch_keys`` / ``dest_preexisting`` are *live* keys only.
    Mixing tombstone keys into the live unique set invented inserts for
    deletes of missing keys (COUNT would not rise; the ledger would lie).

    ``tombstones`` is COUNT(DISTINCT tombstone key) dest already holds —
    those are the DELETEs that drop ``COUNT(*)``. A tombstone for a key
    dest does not hold is a no-op, not a delete and not an insert.

    ``events_read`` is the at-least-once log/writer event count (Debezium
    messages, ON CONFLICT rowcount). It never enters ``expected_delta``.
    Dest COUNT(*) moves by keys, not by redelivered events.
    """

    unique_batch_keys: int
    dest_preexisting: int
    tombstones: int = 0
    unique_tombstone_keys: int = 0
    events_read: int | None = None

    @property
    def inserts(self) -> int:
        return max(int(self.unique_batch_keys) - int(self.dest_preexisting), 0)

    @property
    def updates(self) -> int:
        return min(int(self.dest_preexisting), int(self.unique_batch_keys))

    @property
    def deletes(self) -> int:
        return max(int(self.tombstones), 0)

    @property
    def expected_delta(self) -> int:
        """Net dest COUNT(*) change if every non-held-out key applies."""
        return self.inserts - self.deletes

    def to_dict(self) -> dict[str, Any]:
        return {
            "unique_batch_keys": self.unique_batch_keys,
            "dest_preexisting": self.dest_preexisting,
            "tombstones": self.tombstones,
            "unique_tombstone_keys": self.unique_tombstone_keys,
            "events_read": self.events_read,
            "inserts": self.inserts,
            "updates": self.updates,
            "deletes": self.deletes,
            "expected_delta": self.expected_delta,
        }

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> KeyCensus | None:
        data = dict(payload or {})
        if "unique_batch_keys" not in data or "dest_preexisting" not in data:
            return None
        try:
            unique = int(data["unique_batch_keys"])
            preexisting = int(data["dest_preexisting"])
            tombs = int(data.get("tombstones") or 0)
            tomb_keys = int(data.get("unique_tombstone_keys") or 0)
            events_raw = data.get("events_read")
            events = int(events_raw) if events_raw is not None and events_raw != "" else None
        except (TypeError, ValueError):
            return None
        if unique < 0 or preexisting < 0 or tombs < 0 or tomb_keys < 0:
            return None
        if events is not None and events < 0:
            return None
        if preexisting > unique:
            return None
        if tombs > tomb_keys and tomb_keys > 0:
            return None
        return cls(
            unique_batch_keys=unique,
            dest_preexisting=preexisting,
            tombstones=tombs,
            unique_tombstone_keys=max(tomb_keys, tombs),
            events_read=events,
        )


def extract_batch_keys(
    records: Sequence[Mapping[str, Any]] | None,
    key_columns: Sequence[str] | None,
    mappings: Sequence[Mapping[str, Any]] | None = None,
) -> list[tuple[Any, ...]]:
    """Distinct non-null key tuples from a buffered batch (target column names)."""
    cols = [str(c).strip() for c in (key_columns or []) if str(c).strip()]
    if not cols:
        return []
    tgt_to_src: dict[str, str] = {}
    for item in mappings or []:
        tgt = str(item.get("target") or "").strip()
        src = str(item.get("source") or "").strip()
        if tgt and src:
            tgt_to_src[tgt] = src
    unique: list[tuple[Any, ...]] = []
    seen: set[tuple[Any, ...]] = set()
    for rec in records or []:
        row = dict(rec or {})
        tup_vals: list[Any] = []
        skip = False
        for col in cols:
            val = row.get(col)
            if val is None and col in tgt_to_src:
                val = row.get(tgt_to_src[col])
            if val is None:
                skip = True
                break
            tup_vals.append(val)
        if skip:
            continue
        tup = tuple(tup_vals)
        if tup in seen:
            continue
        seen.add(tup)
        unique.append(tup)
    return unique


def _row_key(
    row: Mapping[str, Any],
    cols: Sequence[str],
    tgt_to_src: Mapping[str, str],
) -> tuple[Any, ...] | None:
    tup_vals: list[Any] = []
    for col in cols:
        val = row.get(col)
        if val is None and col in tgt_to_src:
            val = row.get(tgt_to_src[col])
        if val is None:
            return None
        tup_vals.append(val)
    return tuple(tup_vals)


def _mapping_targets(mappings: Sequence[Mapping[str, Any]] | None) -> dict[str, str]:
    tgt_to_src: dict[str, str] = {}
    for item in mappings or []:
        tgt = str(item.get("target") or "").strip()
        src = str(item.get("source") or "").strip()
        if tgt and src:
            tgt_to_src[tgt] = src
    return tgt_to_src


@dataclass(frozen=True)
class KeyPartition:
    """Last-op-wins split of a keyed batch.

    The same PK may appear as UPDATE then DELETE (or DELETE then INSERT)
    inside one batch. Last event owns the key: a recreation is live; a
    trailing tombstone is a delete. Unkeyed rows are omitted — they cannot
    be addressed by DELETE and are not invented as inserts.
    """

    live_records: list[dict[str, Any]]
    live_keys: list[tuple[Any, ...]]
    tombstone_keys: list[tuple[Any, ...]]


def partition_keyed_records(
    records: Sequence[Mapping[str, Any]] | None,
    key_columns: Sequence[str] | None,
    mappings: Sequence[Mapping[str, Any]] | None = None,
) -> KeyPartition:
    """Split live upserts from hard-delete tombstones (last-op-wins per key)."""
    from services.tombstone import is_row_tombstone

    cols = [str(c).strip() for c in (key_columns or []) if str(c).strip()]
    if not cols:
        return KeyPartition(live_records=[], live_keys=[], tombstone_keys=[])
    tgt_to_src = _mapping_targets(mappings)
    live: dict[tuple[Any, ...], dict[str, Any]] = {}
    tombs: dict[tuple[Any, ...], dict[str, Any]] = {}
    order_live: list[tuple[Any, ...]] = []
    order_tomb: list[tuple[Any, ...]] = []
    unkeyed_live: list[dict[str, Any]] = []
    for rec in records or []:
        row = dict(rec or {})
        key = _row_key(row, cols, tgt_to_src)
        if is_row_tombstone(row):
            if key is None:
                # Cannot address a DELETE; never upsert a tombstone as a live row.
                continue
            live.pop(key, None)
            if key not in tombs:
                order_tomb.append(key)
            tombs[key] = row
            continue
        if key is None:
            unkeyed_live.append(row)
            continue
        tombs.pop(key, None)
        if key not in live:
            order_live.append(key)
        live[key] = row
    live_keys = [k for k in order_live if k in live]
    tomb_keys = [k for k in order_tomb if k in tombs]
    return KeyPartition(
        live_records=unkeyed_live + [live[k] for k in live_keys],
        live_keys=live_keys,
        tombstone_keys=tomb_keys,
    )


def coerce_pk_part(value: Any) -> Any:
    """Bind a PK part with integer affinity when the token is an integer.

    CDC ``ChangeBatch.deletes`` are strings. Dest BIGINT columns reject a
    text bind on PostgreSQL (``operator does not exist: bigint = text``).
    Boolean stays boolean — ``bool`` is a subclass of ``int`` in Python.
    """
    if value is None or isinstance(value, bool) or isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return value
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        try:
            return int(text)
        except ValueError:
            return value
    return value


def format_delete_keys(keys: Sequence[tuple[Any, ...]]) -> list[str]:
    """``delete_by_primary_keys`` addressing: one string per row, composite joined."""
    from services.cdc_snapshot_window import _PK_SEP

    out: list[str] = []
    for tup in keys:
        if any(p is None for p in tup):
            continue
        parts = [str(coerce_pk_part(p)) for p in tup]
        out.append(_PK_SEP.join(parts) if len(parts) > 1 else parts[0])
    return out


def parse_delete_keys(
    keys: Sequence[str] | None,
    n_cols: int,
) -> list[tuple[Any, ...]]:
    """Inverse of :func:`format_delete_keys` for CDC ``ChangeBatch.deletes``."""
    from services.cdc_snapshot_window import _PK_SEP

    width = max(int(n_cols or 1), 1)
    out: list[tuple[Any, ...]] = []
    seen: set[tuple[Any, ...]] = set()
    for raw in keys or []:
        text = str(raw)
        if not text:
            continue
        parts = text.split(_PK_SEP) if width > 1 else [text]
        if len(parts) != width:
            continue
        tup = tuple(coerce_pk_part(p) for p in parts)
        if tup in seen:
            continue
        seen.add(tup)
        out.append(tup)
    return out


def apply_hard_deletes(
    *,
    db_type: str,
    cfg: Mapping[str, Any],
    schema: str,
    table_name: str,
    key_columns: Sequence[str],
    keys: Sequence[tuple[Any, ...]],
) -> int:
    """Hard-DELETE dest-held keys. Idempotent 0 if dest already lacks them."""
    cols = [str(c).strip() for c in key_columns if str(c).strip()]
    formatted = format_delete_keys(keys)
    if not cols or not formatted:
        return 0
    from connectors.table_manager import delete_by_primary_keys

    return delete_by_primary_keys(
        db_type=db_type,
        cfg=dict(cfg),
        table_name=table_name,
        primary_key_column=cols if len(cols) > 1 else cols[0],
        keys=formatted,
        schema=schema or None,
    )


def apply_inferred_leftover_deletes(
    *,
    db_type: str,
    cfg: Mapping[str, Any],
    schema: str,
    table_name: str,
    key_columns: Sequence[str],
    keys: Sequence[tuple[Any, ...]] | Sequence[Sequence[Any]],
    complete_snapshot: bool,
) -> int | None:
    """Hard-DELETE dest keys not in complete source set ``S``. Overwrite only.

    Fivetran historical re-sync soft-flags leftovers (``_fivetran_deleted``)
    so COUNT(*) does not drop. Airbyte incremental refuses inferred
    deletes because a batch is not ``S``. DMS EXTRA_TARGET measures
    leftovers and leaves them. The dest-engine identity is:

        leftover = D \\ S
        DELETE leftover
        extra → 0

    ``complete_snapshot=False`` (incremental CDC, resume tail, sample)
    is a hard no-op — that would false-delete almost every dest row.
    Dest without unique PKs, unsupported engines, or an oversized
    census stay ``None`` (unapplied); Gate-8 still *measures* extra.
    Vector destinations own identity COUNT, not this PK anti-join.
    Iceberg applies the same anti-join through dest-engine snapshot
    listing + CoW delete (existing ``delete_by_primary_keys``). MoR /
    deletion vectors are a later encoding of the same leftover set.
    """
    if not complete_snapshot:
        return None
    engine = str(db_type or "").strip().lower()
    if engine in {"pgvector", "pinecone", "qdrant", "weaviate", "milvus", "email"}:
        return None
    cols = [str(c).strip() for c in key_columns if str(c).strip()]
    from services.dest_precount import destination_key_list

    unique_s = _unique_source_keys(keys, len(cols))
    if not cols or unique_s is None:
        return None
    dest_keys = destination_key_list(
        engine,
        dict(cfg),
        schema=str(schema or ""),
        table_name=str(table_name or ""),
        key_columns=cols,
    )
    if dest_keys is None:
        return None
    s_set = {tuple(coerce_pk_part(p) for p in tup) for tup in unique_s}
    leftover = [
        tup
        for tup in dest_keys
        if tuple(coerce_pk_part(p) for p in tup) not in s_set
    ]
    if not leftover:
        return 0
    return apply_hard_deletes(
        db_type=engine,
        cfg=cfg,
        schema=schema,
        table_name=table_name,
        key_columns=cols,
        keys=leftover,
    )


def _unique_source_keys(
    keys: Sequence[tuple[Any, ...]] | Sequence[Sequence[Any]],
    width: int,
) -> list[tuple[Any, ...]] | None:
    """Complete unique ``S``, or ``None`` if duplicates / width mismatch."""
    if width <= 0 or not keys:
        return None
    unique: list[tuple[Any, ...]] = []
    seen: set[tuple[Any, ...]] = set()
    for raw in keys:
        tup = tuple(raw)
        if len(tup) != width or any(v is None for v in tup):
            return None
        if tup in seen:
            return None
        seen.add(tup)
        unique.append(tup)
    return unique or None


def census_from_partition(
    partition: KeyPartition,
    *,
    db_type: str,
    cfg: Mapping[str, Any],
    schema: str,
    table_name: str,
    key_columns: Sequence[str],
    events_read: int | None = None,
) -> KeyCensus | None:
    """Dest-engine live hits + dest-held tombstones. ``None`` if either probe fails."""
    from services.dest_precount import destination_key_hits

    cols = [str(c).strip() for c in key_columns if str(c).strip()]
    live_hits = destination_key_hits(
        db_type,
        dict(cfg),
        schema=schema,
        table_name=table_name,
        key_columns=cols,
        keys=list(partition.live_keys),
    )
    tomb_hits = destination_key_hits(
        db_type,
        dict(cfg),
        schema=schema,
        table_name=table_name,
        key_columns=cols,
        keys=list(partition.tombstone_keys),
    )
    if live_hits is None or tomb_hits is None:
        return None
    return KeyCensus(
        unique_batch_keys=len(partition.live_keys),
        dest_preexisting=int(live_hits),
        tombstones=int(tomb_hits),
        unique_tombstone_keys=len(partition.tombstone_keys),
        events_read=events_read,
    )


def prepare_keyed_upsert(
    records: list[dict[str, Any]],
    *,
    key_columns: Sequence[str] | None,
    mappings: Sequence[Mapping[str, Any]] | None,
    db_type: str,
    cfg: Mapping[str, Any],
    schema: str,
    table_name: str,
    dest_nonempty: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Strip tombstones from the upsert, hard-DELETE dest-held keys, census.

    Census without apply would be a lie: dest COUNT would not drop. Apply
    without stripping would upsert the tombstone back as a live row.
    """
    cols = [str(c).strip() for c in (key_columns or []) if str(c).strip()]
    if not cols:
        return records, None
    partition = partition_keyed_records(records, cols, mappings)
    census: KeyCensus | None = None
    if dest_nonempty or partition.tombstone_keys:
        census = census_from_partition(
            partition,
            db_type=db_type,
            cfg=cfg,
            schema=schema,
            table_name=table_name,
            key_columns=cols,
            events_read=len(records),
        )
    if partition.tombstone_keys:
        apply_hard_deletes(
            db_type=db_type,
            cfg=cfg,
            schema=schema,
            table_name=table_name,
            key_columns=cols,
            keys=partition.tombstone_keys,
        )
    # Last-op-wins per key: at-least-once duplicate events are not a second write.
    live_out = partition.live_records
    payload = census.to_dict() if census is not None else None
    return live_out, payload


def census_change_batch(
    *,
    inserts: Sequence[Mapping[str, Any]] | None,
    updates: Sequence[Mapping[str, Any]] | None,
    deletes: Sequence[str] | None,
    key_columns: Sequence[str],
    db_type: str,
    cfg: Mapping[str, Any],
    schema: str,
    table_name: str,
    mappings: Sequence[Mapping[str, Any]] | None = None,
) -> KeyCensus | None:
    """Dest-engine census for a CDC ``ChangeBatch`` (already split by the reader)."""
    cols = [str(c).strip() for c in key_columns if str(c).strip()]
    live_records = [dict(r) for r in list(inserts or []) + list(updates or [])]
    live_keys = extract_batch_keys(live_records, cols, mappings)
    tomb_keys = parse_delete_keys(deletes, len(cols))
    tomb_set = set(tomb_keys)
    # Apply order is upserts then deletes, so a PK in both lists nets delete.
    live_keys = [k for k in live_keys if k not in tomb_set]
    partition = KeyPartition(
        live_records=live_records,
        live_keys=live_keys,
        tombstone_keys=tomb_keys,
    )
    return census_from_partition(
        partition,
        db_type=db_type,
        cfg=cfg,
        schema=schema,
        table_name=table_name,
        key_columns=cols,
        events_read=len(inserts or []) + len(updates or []) + len(deletes or []),
    )


def hold_outs(rejected_rows: int, coerced_null_rows: int) -> int:
    """Rows the dest engine does not hold.

    Coerced-null cells landed; they are not hold-outs. Gate-8's
    ``dropped_rows = max(rejected - coerced_null, 0)`` is the same number.
    """
    return max(int(rejected_rows or 0) - int(coerced_null_rows or 0), 0)


def _as_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _first_present_int(*values: Any, default: int = 0) -> int:
    """First value that was supplied, including measured zero.

    ``rejected_rows or dest.rejected`` treats a measured 0 as missing and
    picks up a stale dest figure — that inflates hold-outs and invents a
    shortfall.
    """
    for value in values:
        if value is None:
            continue
        parsed = _as_optional_int(value)
        if parsed is not None:
            return parsed
    return default


def dest_count_from_recon(recon: Mapping[str, Any] | None) -> tuple[int | None, str]:
    """Independent dest population, or unmeasured.

    Checksum *claim* and dest *cardinality* are different axes. Streaming
    passes often stamp ``source_checksum_provenance=writer_ack``, so Gate-8
    refuses ``full_checksum`` — correctly, because the source digest is the
    writer's own account. The dest engine was still re-read: ``target_rows``
    and ``target_checksum`` are dest COUNT(*) / dest digest. Using writer-ack
    *phase* to discard that count would refuse every SQLite/PG overwrite
    this host can prove.

    ``target_rows`` is dest COUNT(*) when dest produced a digest or an
    explicit dest row-count phase. File/object exports and "verified by
    writer" with no dest digest stuff the writer's acknowledgement into the
    same field — using that would circularly balance a short write.

    Independent artifact COUNT is a third axis: ``skipped_readback`` still
    means no cell digest, but ``artifact_row_count`` re-opened the file.
    Only that keyed field closes dest population — ``target_rows`` on an
    export report is historically writer ack and never sufficient.

    Vector identity is a fourth axis: Gate-8 ``target_rows`` on a vector
    dest is physical embedding COUNT(*) (chunks) when a SQL engine can
    answer it. ``identity_rows`` is COUNT(DISTINCT source_id). Only the
    keyed identity field closes dest population — checksum + stuffed
    chunk COUNT / collection ``rowCount`` would invent a surplus.

    SCD2 current is a fifth axis: Gate-8 ``target_rows`` on SCD2 is the
    writer's ``_active_checksum`` ``active_rows`` (or physical history).
    ``current_rows`` is COUNT(*) WHERE is_current. Only the keyed current
    field closes dest population — checksum + stuffed active / history
    COUNT would invent a surplus after the first attribute change.
    """
    report = dict(recon or {})
    source = str(report.get("dest_count_source") or "").strip()
    if source == DEST_ARTIFACT_READBACK:
        counted = _as_optional_int(report.get(ARTIFACT_COUNT_KEY))
        if counted is not None and counted >= 0:
            return counted, DEST_ARTIFACT_READBACK
        return None, DEST_UNMEASURED
    if source == DEST_IDENTITY_READBACK:
        counted = _as_optional_int(report.get(IDENTITY_COUNT_KEY))
        if counted is not None and counted >= 0:
            return counted, DEST_IDENTITY_READBACK
        return None, DEST_UNMEASURED
    if source == DEST_CURRENT_READBACK:
        counted = _as_optional_int(report.get(CURRENT_ROWS_KEY))
        if counted is None:
            nested = report.get("scd2")
            if isinstance(nested, Mapping):
                counted = _as_optional_int(nested.get("current_rows"))
        if counted is not None and counted >= 0:
            return counted, DEST_CURRENT_READBACK
        return None, DEST_UNMEASURED
    if source == "skipped_identity_readback":
        return None, DEST_UNMEASURED
    if source == "skipped_current_readback":
        return None, DEST_UNMEASURED
    msg = str(report.get("message") or "").lower()
    if report.get("skipped_readback") is True:
        return None, DEST_UNMEASURED
    if is_unproven_export(report, msg):
        return None, DEST_UNMEASURED
    raw = report.get("target_rows")
    if not isinstance(raw, int) or raw < 0:
        return None, DEST_UNMEASURED
    dest_digest = str(report.get("target_checksum") or "").strip()
    if dest_digest:
        return raw, DEST_READBACK
    phase = str(report.get("phase") or "").lower()
    coverage = str(report.get("coverage") or "").lower()
    if coverage == "row_count" or "row_count" in phase or "row count" in msg:
        return raw, DEST_READBACK
    # No dest digest and no dest row-count phase: target_rows is writer ack.
    return None, DEST_UNMEASURED


def conservation_kind(
    sync_mode: str | None,
    *,
    dest_count_before: int | None,
) -> str:
    """Which cardinality identity this run is allowed to close.

    Upsert/CDC into an *empty* destination is insert-only, so overwrite
    cardinality applies. Into a non-empty destination, COUNT(*) cannot
    prove which keys were applied.

    Mirror is not overwrite and not keyed hard-delete: physical COUNT(*)
    stays while dest-engine ``COUNT(*) WHERE NOT _deleted`` is the
    population identity.

    SCD2 is not overwrite, not keyed, and not mirror: physical history
    COUNT(*) grows on every attribute change. Dest-engine
    ``COUNT(*) WHERE is_current`` is the population identity.
    ``is_current`` is temporal current-version, not a tombstone.
    """
    if _is_mirror_sync(sync_mode):
        return KIND_MIRROR
    if _is_scd2_sync(sync_mode):
        return KIND_SCD2
    if is_overwrite_sync(sync_mode):
        return KIND_OVERWRITE
    if is_append_sync(sync_mode):
        return KIND_APPEND_DELTA
    if dest_count_before == 0:
        return KIND_OVERWRITE
    return KIND_KEYED


def _is_mirror_sync(mode: str | None) -> bool:
    from services.sync_cursor import normalize_sync_mode

    return normalize_sync_mode(mode or "", default="") == "mirror"


def _is_scd2_sync(mode: str | None) -> bool:
    from services.sync_cursor import normalize_sync_mode

    return normalize_sync_mode(mode or "", default="") == "scd2"


def extract_mirror_payload(dest: Mapping[str, Any] | None) -> dict[str, Any]:
    """Nested ``destination_summary.mirror`` or stream-path top-level active census.

    SCD2 stamps ``active_rows`` + ``active_checksum`` for Gate-8 cell
    fidelity of *current* versions. That is temporal ``is_current``, not
    a tombstone. Do not treat it as ``COUNT(*) WHERE NOT _deleted``.
    Stream-path this-run ``soft_deleted`` / ``reactivated`` are dest-engine
    transition counts, not driver rowcount.
    """
    data = dict(dest or {})
    sync = str(data.get("sync_mode") or data.get("mode") or "").lower()
    if _is_scd2_sync(sync) or str(data.get("mode") or "").lower() == "scd2":
        return {}
    if isinstance(data.get("scd2"), dict) and not isinstance(data.get("mirror"), dict):
        return {}
    nested = data.get("mirror")
    if isinstance(nested, dict) and (
        nested.get("mode") == "mirror"
        or nested.get("active_rows") is not None
        or nested.get("soft_deleted") is not None
    ):
        return dict(nested)
    if data.get("active_rows") is not None and (
        data.get("active_checksum") or data.get("soft_delete_column")
    ):
        return {
            "active_rows": data.get("active_rows"),
            "active_checksum": data.get("active_checksum"),
            "soft_delete_column": data.get("soft_delete_column") or "_deleted",
            "soft_deleted": data.get("soft_deleted"),
            "reactivated": data.get("reactivated"),
            "rows_scanned": data.get("rows_scanned"),
            "mode": "mirror",
        }
    return {}


@dataclass(frozen=True)
class ConservationLedger:
    """One closed (or honestly open) population identity."""

    rows_read: int | None
    rows_written: int | None
    rows_quarantined: int
    rows_skipped: int
    rows_coerced_null: int
    writer_ack: int | None
    dest_count: int | None
    dest_count_before: int | None
    unaccounted: int | None
    balanced: bool
    rows_read_source: str
    rows_written_source: str
    conservation_kind: str
    note: str
    writer_ack_delta: int | None = None
    inserts: int | None = None
    updates: int | None = None
    deletes: int | None = None
    dest_delta: int | None = None
    unique_batch_keys: int | None = None
    dest_preexisting: int | None = None
    active_count: int | None = None
    inferred_deletes: int | None = None
    reactivated: int | None = None
    events_read: int | None = None
    identity_count: int | None = None
    vector_rows: int | None = None
    current_count: int | None = None
    history_rows: int | None = None
    missing_keys: int | None = None
    extra_keys: int | None = None
    leftover_deleted: int | None = None
    stream_count: int | None = None
    measured_streams: int | None = None
    summable: bool | None = None
    per_stream: tuple[dict[str, Any], ...] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_quarantined": self.rows_quarantined,
            "rows_skipped": self.rows_skipped,
            "rows_coerced_null": self.rows_coerced_null,
            "writer_ack": self.writer_ack,
            "dest_count": self.dest_count,
            "dest_count_before": self.dest_count_before,
            "unaccounted": self.unaccounted,
            "balanced": self.balanced,
            "rows_read_source": self.rows_read_source,
            "rows_written_source": self.rows_written_source,
            "conservation_kind": self.conservation_kind,
            "note": self.note,
            "writer_ack_delta": self.writer_ack_delta,
            "inserts": self.inserts,
            "updates": self.updates,
            "deletes": self.deletes,
            "dest_delta": self.dest_delta,
            "unique_batch_keys": self.unique_batch_keys,
            "dest_preexisting": self.dest_preexisting,
            "active_count": self.active_count,
            "inferred_deletes": self.inferred_deletes,
            "reactivated": self.reactivated,
            "events_read": self.events_read,
            "identity_count": self.identity_count,
            "vector_rows": self.vector_rows,
            "current_count": self.current_count,
            "history_rows": self.history_rows,
            "missing_keys": self.missing_keys,
            "extra_keys": self.extra_keys,
            "leftover_deleted": self.leftover_deleted,
        }
        if self.stream_count is not None:
            payload["stream_count"] = self.stream_count
        if self.measured_streams is not None:
            payload["measured_streams"] = self.measured_streams
        if self.summable is not None:
            payload["summable"] = self.summable
        if self.per_stream is not None:
            payload["per_stream"] = [dict(item) for item in self.per_stream]
        return payload


def _empty_pass_ledger(
    *,
    quarantined: int,
    skipped: int,
    coerced: int,
    writer_ack: int | None,
    dest_count: int | None,
    dest_count_before: int | None,
) -> ConservationLedger:
    return ConservationLedger(
        rows_read=0,
        rows_written=0,
        rows_quarantined=quarantined,
        rows_skipped=skipped,
        rows_coerced_null=coerced,
        writer_ack=writer_ack,
        dest_count=dest_count,
        dest_count_before=dest_count_before,
        unaccounted=0,
        balanced=True,
        rows_read_source="gate8_source_count",
        rows_written_source=DEST_EMPTY_PASS,
        conservation_kind=KIND_EMPTY_PASS,
        note=(
            "Measured empty pass: nothing was read and nothing was written. "
            "This is the incremental steady state, not an unmeasured run."
        ),
        writer_ack_delta=(0 - writer_ack) if writer_ack is not None else None,
    )


def _account_keyed(
    *,
    read: int,
    dest_count: int | None,
    dest_count_source: str,
    dest_count_before: int | None,
    quarantined: int,
    skipped: int,
    coerced: int,
    ack: int | None,
    census: KeyCensus | None,
) -> ConservationLedger:
    """Close dest COUNT(*) delta against dest-engine new keys, not writer ack."""
    base = dict(
        rows_read=read,
        rows_quarantined=quarantined,
        rows_skipped=skipped,
        rows_coerced_null=coerced,
        writer_ack=ack,
        dest_count=dest_count,
        dest_count_before=dest_count_before,
        rows_read_source="gate8_source_count",
        conservation_kind=KIND_KEYED,
    )
    if census is None:
        return ConservationLedger(
            **base,
            rows_written=None,
            unaccounted=None,
            balanced=False,
            rows_written_source=DEST_UNMEASURED,
            note=(
                "Upsert/CDC into a non-empty destination has no COUNT(*) "
                "identity without a dest-engine key census — updates do not "
                "change cardinality. Writer acknowledgements cannot prove "
                "which keys were new."
            ),
            writer_ack_delta=None,
        )
    if dest_count is None or dest_count_before is None or dest_count_source != DEST_READBACK:
        return ConservationLedger(
            **base,
            rows_written=None,
            unaccounted=None,
            balanced=False,
            rows_written_source=DEST_UNMEASURED,
            inserts=census.inserts,
            updates=census.updates,
            deletes=census.deletes,
            unique_batch_keys=census.unique_batch_keys,
            dest_preexisting=census.dest_preexisting,
            note=(
                "Keyed dest COUNT(*) delta unverified: destination population "
                "was not independently measured after the write."
            ),
            writer_ack_delta=None,
            events_read=census.events_read,
        )
    if quarantined or skipped:
        return ConservationLedger(
            **base,
            rows_written=None,
            unaccounted=None,
            balanced=False,
            rows_written_source=DEST_UNMEASURED,
            inserts=census.inserts,
            updates=census.updates,
            deletes=census.deletes,
            dest_delta=dest_count - dest_count_before,
            unique_batch_keys=census.unique_batch_keys,
            dest_preexisting=census.dest_preexisting,
            note=(
                "Keyed conservation is unproven when rows were quarantined or "
                "skipped: hold-outs are not classified as new vs existing keys, "
                "so dest COUNT(*) delta cannot be compared to the census."
            ),
            writer_ack_delta=None,
            events_read=census.events_read,
        )

    dest_delta = dest_count - dest_count_before
    expected = census.expected_delta
    unaccounted = expected - dest_delta
    ack_delta = (dest_delta - ack) if ack is not None else None
    if unaccounted == 0:
        note = (
            f"Keyed cardinality closed: dest COUNT(*) {dest_count_before} → "
            f"{dest_count} (delta {dest_delta}) equals {census.inserts} new "
            f"key(s) minus {census.deletes} delete(s). {census.updates} "
            "update(s) did not change COUNT(*). Writer acknowledgements "
            "count updates; they are not dest population."
        )
        events = census.events_read
        if events is not None and events > census.unique_batch_keys:
            note += (
                f" At-least-once delivered {events} event(s) for "
                f"{census.unique_batch_keys} live key(s); cardinality is keys, "
                "not log events (Debezium/DMS message counts do not move COUNT(*))."
            )
    elif unaccounted > 0:
        note = (
            f"{unaccounted} expected new dest row(s) did not appear in "
            "independent COUNT(*) after keyed apply. Treat as potential "
            "silent loss — writer ON CONFLICT counts are not evidence."
        )
    else:
        note = (
            f"{abs(unaccounted)} more dest row(s) appeared than the key census "
            "predicted (duplicate dest keys, or inserts the census missed)."
        )
    if ack_delta:
        sign = "more" if ack_delta > 0 else "fewer"
        note += (
            f" Writer acknowledged {ack:,}; dest COUNT(*) delta is "
            f"{dest_delta:,} ({abs(ack_delta):,} {sign} than the writer claimed)."
        )
    return ConservationLedger(
        **base,
        rows_written=dest_delta,
        unaccounted=unaccounted,
        balanced=unaccounted == 0,
        rows_written_source=DEST_READBACK,
        note=note,
        writer_ack_delta=ack_delta,
        inserts=census.inserts,
        updates=census.updates,
        deletes=census.deletes,
        dest_delta=dest_delta,
        unique_batch_keys=census.unique_batch_keys,
        dest_preexisting=census.dest_preexisting,
        events_read=census.events_read,
    )


def _account_mirror(
    *,
    read: int,
    dest_count: int | None,
    dest_count_source: str,
    dest_count_before: int | None,
    quarantined: int,
    skipped: int,
    coerced: int,
    ack: int | None,
    mirror: Mapping[str, Any] | None,
) -> ConservationLedger:
    """Close active dest population, not physical COUNT(*) (which does not drop)."""
    payload = dict(mirror or {})
    active = _as_optional_int(payload.get("active_rows"))
    inferred = _as_optional_int(payload.get("soft_deleted"))
    reactivated = _as_optional_int(payload.get("reactivated"))
    scanned = _as_optional_int(payload.get("rows_scanned"))
    stuffed_active = (
        dest_count is not None
        and active is not None
        and dest_count == active
        and dest_count_source == DEST_READBACK
    )
    physical = scanned
    if physical is None and dest_count is not None and not stuffed_active:
        physical = dest_count

    base = dict(
        rows_read=read,
        rows_quarantined=quarantined,
        rows_skipped=skipped,
        rows_coerced_null=coerced,
        writer_ack=ack,
        dest_count=physical,
        dest_count_before=dest_count_before,
        rows_read_source="gate8_source_count",
        conservation_kind=KIND_MIRROR,
        active_count=active,
        inferred_deletes=inferred,
        reactivated=reactivated,
        deletes=inferred,
    )
    if active is None:
        return ConservationLedger(
            **base,
            rows_written=None,
            unaccounted=None,
            balanced=False,
            rows_written_source=DEST_UNMEASURED,
            note=(
                "Mirror inferred-delete identity is unproven: dest-engine "
                "COUNT(*) WHERE NOT _deleted was not captured. Physical "
                "COUNT(*) does not drop on soft-delete (Fivetran "
                "_fivetran_deleted). Writer ack is not active population."
            ),
            writer_ack_delta=None,
        )

    unaccounted = read - (int(active) + quarantined + skipped)
    ack_delta = (int(active) - ack) if ack is not None else None
    leftover = None
    if physical is not None:
        leftover = max(int(physical) - int(active), 0)
    if unaccounted == 0:
        note = (
            f"Mirror active population closed: dest-engine COUNT(*) WHERE NOT "
            f"_deleted = {active} equals rows read minus hold-outs and skips. "
            "Physical COUNT(*) does not drop — leftover dest keys stay as "
            f"soft-deletes"
            + (f" ({leftover} leftover row(s))" if leftover else "")
            + "."
        )
        if inferred:
            note += f" This run inferred {inferred} delete(s)."
        if reactivated:
            note += f" Reactivated {reactivated} previously deleted key(s)."
    elif unaccounted > 0:
        note = (
            f"{unaccounted} source row(s) are not in dest-engine active "
            "population (COUNT(*) WHERE NOT _deleted), quarantined, or skipped."
        )
    else:
        note = (
            f"{abs(unaccounted)} more active dest row(s) than were read. "
            "Reactivated keys or a dest-engine active census that includes "
            "rows this snapshot did not send."
        )
    if ack_delta:
        sign = "more" if ack_delta > 0 else "fewer"
        note += (
            f" Writer acknowledged {ack:,}; active dest population is "
            f"{active:,} ({abs(ack_delta):,} {sign} than the writer claimed)."
        )
    return ConservationLedger(
        **base,
        rows_written=int(active),
        unaccounted=unaccounted,
        balanced=unaccounted == 0,
        rows_written_source=DEST_ACTIVE_READBACK,
        note=note,
        writer_ack_delta=ack_delta,
    )


def extract_vector_payload(dest: Mapping[str, Any] | None) -> dict[str, Any]:
    """Nested ``vector`` census or top-level identity/vector row fields."""
    data = dict(dest or {})
    nested = data.get("vector")
    if isinstance(nested, dict) and (
        nested.get("identity_rows") is not None
        or nested.get("vector_rows") is not None
    ):
        return dict(nested)
    identity = data.get(IDENTITY_COUNT_KEY)
    physical = data.get(VECTOR_ROWS_KEY)
    if identity is not None or physical is not None:
        return {
            "identity_rows": identity,
            "vector_rows": physical,
        }
    return {}


def extract_keyset_payload(dest: Mapping[str, Any] | None) -> dict[str, Any]:
    """Dest-engine MISSING_TARGET / EXTRA_TARGET split, or empty."""
    data = dict(dest or {})
    nested = data.get("keyset")
    if isinstance(nested, dict) and (
        nested.get(MISSING_KEYS_KEY) is not None
        or nested.get(EXTRA_KEYS_KEY) is not None
        or nested.get(LEFTOVER_DELETED_KEY) is not None
    ):
        return dict(nested)
    missing = data.get(MISSING_KEYS_KEY)
    extra = data.get(EXTRA_KEYS_KEY)
    leftover_deleted = data.get(LEFTOVER_DELETED_KEY)
    if missing is not None or extra is not None or leftover_deleted is not None:
        return {
            MISSING_KEYS_KEY: missing,
            EXTRA_KEYS_KEY: extra,
            "dest_key_hits": data.get("dest_key_hits"),
            "source_key_count": data.get("source_key_count"),
            LEFTOVER_DELETED_KEY: leftover_deleted,
        }
    return {}


def _account_vector(
    *,
    read: int,
    dest_count: int | None,
    dest_count_source: str,
    dest_count_before: int | None,
    quarantined: int,
    skipped: int,
    coerced: int,
    ack: int | None,
    vector: Mapping[str, Any] | None,
) -> ConservationLedger:
    """Close COUNT(DISTINCT source_id), not physical embedding COUNT(*).

    Empty dest (dest-before 0) is overwrite on identities. Non-empty dest
    without a this-run source_id census stays unproven — chunk ``id`` PK
    is not source identity. Writer chunk-upsert ack never closes.
    """
    payload = dict(vector or {})
    identity = dest_count
    physical = _as_optional_int(payload.get("vector_rows"))
    base = dict(
        rows_read=read,
        rows_quarantined=quarantined,
        rows_skipped=skipped,
        rows_coerced_null=coerced,
        writer_ack=ack,
        dest_count=identity,
        dest_count_before=dest_count_before,
        rows_read_source="gate8_source_count",
        conservation_kind=KIND_VECTOR,
        identity_count=identity,
        vector_rows=physical,
        dest_delta=(
            (int(identity) - int(dest_count_before))
            if identity is not None and dest_count_before is not None
            else None
        ),
    )
    if identity is None or dest_count_source != DEST_IDENTITY_READBACK:
        return ConservationLedger(
            **base,
            rows_written=None,
            unaccounted=None,
            balanced=False,
            rows_written_source=DEST_UNMEASURED,
            note=(
                "Vector identity is unproven: dest-engine COUNT(DISTINCT "
                "source_id) was not captured. Physical vector COUNT(*) is "
                "chunk cardinality, not source-row conservation. Writer "
                "chunk-upsert acknowledgement is not destination proof."
            ),
            writer_ack_delta=None,
        )
    if dest_count_before is None:
        return ConservationLedger(
            **base,
            rows_written=int(identity),
            unaccounted=None,
            balanced=False,
            rows_written_source=DEST_IDENTITY_READBACK,
            note=(
                "Vector identity dest-before was not measured, so "
                f"COUNT(DISTINCT source_id)={identity} cannot prove this "
                "run's documents landed versus pre-existing identities. "
                "Physical vector COUNT(*) is diagnostic."
            ),
            writer_ack_delta=(int(identity) - ack) if ack is not None else None,
        )
    if dest_count_before > 0:
        return ConservationLedger(
            **base,
            rows_written=int(identity),
            unaccounted=None,
            balanced=False,
            rows_written_source=DEST_IDENTITY_READBACK,
            note=(
                "Vector identity keyed conservation is unproven: dest already "
                f"held {dest_count_before} source identit(ies). Chunk upsert "
                "conflict is on chunk id, not source_id, so dest Δ of "
                "identities cannot be proven from this-run chunk events. "
                "Empty dest (dest-before 0) closes "
                "reader == COUNT(DISTINCT source_id). Physical vector "
                "COUNT(*) is diagnostic."
            ),
            writer_ack_delta=(int(identity) - ack) if ack is not None else None,
        )

    unaccounted = read - (int(identity) + quarantined + skipped)
    ack_delta = (int(identity) - ack) if ack is not None else None
    if unaccounted == 0:
        note = (
            f"Vector identity closed: dest-engine COUNT(DISTINCT source_id) "
            f"= {identity} equals rows read minus hold-outs and skips. "
            "Physical vector COUNT(*) is chunk cardinality, not source-row "
            "conservation."
        )
        if physical is not None:
            note += f" Destination holds {physical} vector row(s)."
    elif unaccounted > 0:
        note = (
            f"{unaccounted} source row(s) are not in dest-engine "
            "COUNT(DISTINCT source_id), quarantined, or skipped. Treat as "
            "potential silent loss — chunk-upsert acknowledgement is not "
            "evidence they landed."
        )
    else:
        note = (
            f"{abs(unaccounted)} more dest identit(ies) than were read. "
            "Pre-existing source_id values on an empty-dest proof, or "
            "duplicate identity accounting."
        )
    if ack_delta:
        sign = "more" if ack_delta > 0 else "fewer"
        note += (
            f" Writer acknowledged {ack:,} vector row(s); identity COUNT is "
            f"{identity:,} ({abs(ack_delta):,} {sign} than the writer claimed)."
        )
    return ConservationLedger(
        **base,
        rows_written=int(identity),
        unaccounted=unaccounted,
        balanced=unaccounted == 0,
        rows_written_source=DEST_IDENTITY_READBACK,
        note=note,
        writer_ack_delta=ack_delta,
    )


def extract_scd2_payload(dest: Mapping[str, Any] | None) -> dict[str, Any]:
    """Nested ``scd2`` census or top-level current/history row fields."""
    data = dict(dest or {})
    nested = data.get("scd2")
    if isinstance(nested, dict) and (
        nested.get("current_rows") is not None
        or nested.get("history_rows") is not None
        or nested.get(CURRENT_ROWS_KEY) is not None
    ):
        payload = dict(nested)
        if payload.get("current_rows") is None and nested.get(CURRENT_ROWS_KEY) is not None:
            payload["current_rows"] = nested.get(CURRENT_ROWS_KEY)
        if payload.get("history_rows") is None and nested.get(HISTORY_ROWS_KEY) is not None:
            payload["history_rows"] = nested.get(HISTORY_ROWS_KEY)
        return payload
    current = data.get(CURRENT_ROWS_KEY)
    history = data.get(HISTORY_ROWS_KEY)
    if current is not None or history is not None:
        return {
            "current_rows": current,
            "history_rows": history,
        }
    return {}


def _account_scd2(
    *,
    read: int,
    dest_count: int | None,
    dest_count_source: str,
    dest_count_before: int | None,
    quarantined: int,
    skipped: int,
    coerced: int,
    ack: int | None,
    scd2: Mapping[str, Any] | None,
) -> ConservationLedger:
    """Close COUNT(*) WHERE is_current, not physical history COUNT(*).

    Empty dest (dest-before 0) is first-load overwrite on current
    identities. Dest-before is physical history — never mix it with
    current-after as dest Δ. Incremental watermarked SCD2 stays
    unproven. Writer version-upsert ack never closes. ``is_current`` is
    not a tombstone.
    """
    payload = dict(scd2 or {})
    current = dest_count
    history = _as_optional_int(payload.get("history_rows"))
    current_count = _as_optional_int(payload.get("current_rows"))
    if current_count is None:
        current_count = current
    base = dict(
        rows_read=read,
        rows_quarantined=quarantined,
        rows_skipped=skipped,
        rows_coerced_null=coerced,
        writer_ack=ack,
        dest_count=current,
        dest_count_before=dest_count_before,
        rows_read_source="gate8_source_count",
        conservation_kind=KIND_SCD2,
        current_count=current_count,
        history_rows=history,
        dest_delta=None,
        active_count=None,
    )
    if current is None or dest_count_source != DEST_CURRENT_READBACK:
        return ConservationLedger(
            **base,
            rows_written=None,
            unaccounted=None,
            balanced=False,
            rows_written_source=DEST_UNMEASURED,
            note=(
                "SCD2 current-row identity is unproven: dest-engine "
                "COUNT(*) WHERE is_current was not captured. Physical "
                "history COUNT(*) grows on every attribute change (1 "
                "source identity → N versions). Writer version-upsert "
                "acknowledgement and Gate-8 stuffed active_rows are not "
                "destination proof. is_current is temporal, not a "
                "tombstone (_deleted)."
            ),
            writer_ack_delta=None,
        )
    history_note = (
        f" Physical history COUNT(*) is {history} (diagnostic)."
        if history is not None
        else " Physical history COUNT(*) is diagnostic."
    )
    ack_delta = (int(current) - ack) if ack is not None else None
    accounted = int(current) + quarantined + skipped
    unaccounted = read - accounted

    if dest_count_before is None:
        return ConservationLedger(
            **base,
            rows_written=int(current),
            unaccounted=None,
            balanced=False,
            rows_written_source=DEST_CURRENT_READBACK,
            note=(
                "SCD2 dest-before was not measured, so "
                f"COUNT(*) WHERE is_current={current} cannot prove this "
                "run's identities versus pre-existing history. Incremental "
                "watermarked SCD2 must not close reader == current when "
                "the batch is a changed subset."
                + history_note
            ),
            writer_ack_delta=ack_delta,
        )

    if dest_count_before == 0:
        if unaccounted == 0:
            note = (
                "SCD2 current-row identity closed (first load): dest-engine "
                f"COUNT(*) WHERE is_current = {current} equals rows read "
                "minus hold-outs and skips."
                + history_note
                + " Writer version ack is diagnostic."
            )
        elif unaccounted > 0:
            note = (
                f"{unaccounted} source row(s) are not in dest-engine "
                "COUNT(*) WHERE is_current, quarantined, or skipped. Treat "
                "as potential silent loss — version-upsert acknowledgement "
                "is not evidence they landed."
                + history_note
            )
        else:
            note = (
                f"{abs(unaccounted)} more current dest identit(ies) than "
                "were read. Pre-existing current rows on a first-load "
                "proof, or duplicate current accounting."
                + history_note
            )
        if ack_delta:
            sign = "more" if ack_delta > 0 else "fewer"
            note += (
                f" Writer acknowledged {ack:,} version row(s); current "
                f"COUNT is {current:,} ({abs(ack_delta):,} {sign} than "
                "the writer claimed)."
            )
        return ConservationLedger(
            **base,
            rows_written=int(current),
            unaccounted=unaccounted,
            balanced=unaccounted == 0,
            rows_written_source=DEST_CURRENT_READBACK,
            note=note,
            writer_ack_delta=ack_delta,
        )

    # dest-before is physical history of a table that already existed.
    # Close only when this run's reader equals the current population —
    # a full snapshot re-sync of current identities, not a watermarked
    # change batch. Do not invent dest Δ from physical-before vs current-after.
    if unaccounted == 0:
        note = (
            "SCD2 current-row identity closed (full snapshot re-sync): "
            f"dest-engine COUNT(*) WHERE is_current = {current} equals "
            "rows read minus hold-outs and skips. Dest-before was physical "
            "history, not current-before — dest Δ is not mixed."
            + history_note
            + " Writer version ack is diagnostic."
        )
        if ack_delta:
            sign = "more" if ack_delta > 0 else "fewer"
            note += (
                f" Writer acknowledged {ack:,} version row(s); current "
                f"COUNT is {current:,} ({abs(ack_delta):,} {sign} than "
                "the writer claimed)."
            )
        return ConservationLedger(
            **base,
            rows_written=int(current),
            unaccounted=unaccounted,
            balanced=True,
            rows_written_source=DEST_CURRENT_READBACK,
            note=note,
            writer_ack_delta=ack_delta,
        )
    return ConservationLedger(
        **base,
        rows_written=int(current),
        unaccounted=None,
        balanced=False,
        rows_written_source=DEST_CURRENT_READBACK,
        note=(
            "SCD2 incremental identity is unproven: this run read "
            f"{read} row(s) but dest holds {current} current identit(ies). "
            "A watermarked change batch is not the current population "
            "(historical re-sync analogue requires a full snapshot of "
            "current identities). Physical history COUNT(*) and writer "
            "version ack never close."
            + history_note
        ),
        writer_ack_delta=ack_delta,
    )


def account_population(
    *,
    rows_read: int | None,
    dest_count: int | None,
    dest_count_source: str,
    dest_count_before: int | None,
    rejected_rows: int,
    coerced_null_rows: int,
    rows_skipped: int,
    writer_ack: int | None,
    sync_mode: str | None,
    census: KeyCensus | None = None,
    mirror: Mapping[str, Any] | None = None,
    vector: Mapping[str, Any] | None = None,
    keyset: Mapping[str, Any] | None = None,
    scd2: Mapping[str, Any] | None = None,
) -> ConservationLedger:
    """Close ``reader == dest_population + hold_outs + skipped`` or say why not."""
    quarantined = hold_outs(rejected_rows, coerced_null_rows)
    skipped = int(rows_skipped or 0)
    coerced = int(coerced_null_rows or 0)
    kind = conservation_kind(sync_mode, dest_count_before=dest_count_before)
    ack = writer_ack if writer_ack is not None else None
    # File replace is overwrite regardless of the operator's SQL sync-mode
    # label: the engine opens the artifact ``wb``. Append-delta / keyed
    # identities do not apply to a replaced file.
    if dest_count_source == DEST_ARTIFACT_READBACK:
        kind = KIND_OVERWRITE
    # Chunked vector loads are identity conservation regardless of the
    # operator's SQL sync-mode label. Physical embedding COUNT(*) is not dest.
    if dest_count_source == DEST_IDENTITY_READBACK:
        kind = KIND_VECTOR
    # SCD2 current identities regardless of the operator's SQL sync-mode
    # label. Physical history COUNT(*) is not dest.
    if dest_count_source == DEST_CURRENT_READBACK:
        kind = KIND_SCD2

    if rows_read is None:
        return ConservationLedger(
            rows_read=None,
            rows_written=None,
            rows_quarantined=quarantined,
            rows_skipped=skipped,
            rows_coerced_null=coerced,
            writer_ack=ack,
            dest_count=dest_count,
            dest_count_before=dest_count_before,
            unaccounted=None,
            balanced=False,
            rows_read_source=DEST_UNMEASURED,
            rows_written_source=dest_count_source,
            conservation_kind=KIND_UNMEASURED,
            note=(
                "Source row count was not measured for this run, so no "
                "conservation check is possible. Destination COUNT(*) cannot "
                "be compared against rows that were never counted on the read."
            ),
            writer_ack_delta=None,
        )

    read = int(rows_read)
    if kind == KIND_MIRROR:
        # Empty-pass must not fire: leftover dest keys stay as _deleted, so a
        # zero-read snapshot is still a dest-engine active census, not a
        # measured-zero overwrite.
        return _account_mirror(
            read=read,
            dest_count=dest_count,
            dest_count_source=dest_count_source,
            dest_count_before=dest_count_before,
            quarantined=quarantined,
            skipped=skipped,
            coerced=coerced,
            ack=ack,
            mirror=mirror,
        )

    if kind == KIND_VECTOR:
        # Empty-pass must not fire: leftover dest identities stay on a
        # non-empty index, so a zero-read load is still an identity census.
        return _account_vector(
            read=read,
            dest_count=dest_count,
            dest_count_source=dest_count_source,
            dest_count_before=dest_count_before,
            quarantined=quarantined,
            skipped=skipped,
            coerced=coerced,
            ack=ack,
            vector=vector,
        )

    if kind == KIND_SCD2:
        # Empty-pass must not fire: leftover current identities stay as
        # history versions, so a zero-read watermark is still a current census.
        return _account_scd2(
            read=read,
            dest_count=dest_count,
            dest_count_source=dest_count_source,
            dest_count_before=dest_count_before,
            quarantined=quarantined,
            skipped=skipped,
            coerced=coerced,
            ack=ack,
            scd2=scd2,
        )

    if (
        read == 0
        and quarantined == 0
        and skipped == 0
        and int(ack or 0) == 0
    ):
        return _empty_pass_ledger(
            quarantined=quarantined,
            skipped=skipped,
            coerced=coerced,
            writer_ack=ack,
            dest_count=dest_count,
            dest_count_before=dest_count_before,
        )

    if kind == KIND_KEYED:
        return _account_keyed(
            read=read,
            dest_count=dest_count,
            dest_count_source=dest_count_source,
            dest_count_before=dest_count_before,
            quarantined=quarantined,
            skipped=skipped,
            coerced=coerced,
            ack=ack,
            census=census,
        )

    written: int | None
    written_source = dest_count_source
    if dest_count is None or dest_count_source not in _INDEPENDENT_DEST:
        written = None
        written_source = DEST_UNMEASURED
        return ConservationLedger(
            rows_read=read,
            rows_written=None,
            rows_quarantined=quarantined,
            rows_skipped=skipped,
            rows_coerced_null=coerced,
            writer_ack=ack,
            dest_count=dest_count,
            dest_count_before=dest_count_before,
            unaccounted=None,
            balanced=False,
            rows_read_source="gate8_source_count",
            rows_written_source=written_source,
            conservation_kind=kind,
            note=(
                "Destination COUNT(*) was not independently measured, so no "
                "conservation check is possible. Writer acknowledgements "
                "cannot prove rows landed — that is the DMS MISSING_TARGET "
                "after Full Load success hole."
            ),
            writer_ack_delta=None,
        )

    if kind == KIND_APPEND_DELTA:
        if dest_count_source != DEST_READBACK:
            return ConservationLedger(
                rows_read=read,
                rows_written=None,
                rows_quarantined=quarantined,
                rows_skipped=skipped,
                rows_coerced_null=coerced,
                writer_ack=ack,
                dest_count=dest_count,
                dest_count_before=dest_count_before,
                unaccounted=None,
                balanced=False,
                rows_read_source="gate8_source_count",
                rows_written_source=DEST_UNMEASURED,
                conservation_kind=kind,
                note=(
                    "Append delta requires dest-engine COUNT(*) before and after "
                    "the write. Artifact record count cannot prove a SQL append."
                ),
                writer_ack_delta=None,
            )
        if dest_count_before is None:
            return ConservationLedger(
                rows_read=read,
                rows_written=None,
                rows_quarantined=quarantined,
                rows_skipped=skipped,
                rows_coerced_null=coerced,
                writer_ack=ack,
                dest_count=dest_count,
                dest_count_before=None,
                unaccounted=None,
                balanced=False,
                rows_read_source="gate8_source_count",
                rows_written_source=DEST_UNMEASURED,
                conservation_kind=kind,
                note=(
                    "Append delta unverified: destination held an unknown "
                    f"number of rows before this write, so the final COUNT(*) "
                    f"({dest_count}) cannot prove the batch landed."
                ),
                writer_ack_delta=None,
            )
        written = dest_count - int(dest_count_before)
        written_source = DEST_READBACK
    else:
        written = dest_count
        written_source = dest_count_source

    artifact = dest_count_source == DEST_ARTIFACT_READBACK
    dest_phrase = (
        "export artifact (independent record count)"
        if artifact
        else "destination (independent COUNT(*))"
    )
    dest_short = "artifact record count" if artifact else "dest COUNT(*)"
    if kind == KIND_APPEND_DELTA:
        dest_short = "dest Δ"

    unaccounted = read - (written + quarantined + skipped)
    ack_delta = (written - ack) if ack is not None else None
    append_delta = written if kind == KIND_APPEND_DELTA else None

    if kind == KIND_APPEND_DELTA:
        before_s = f"{int(dest_count_before):,}"
        after_s = f"{int(dest_count):,}"
        delta_s = f"{int(written):,}"
        if unaccounted > 0:
            note = (
                f"{unaccounted} source row(s) are not in this run's dest COUNT(*) "
                f"growth ({delta_s}; dest {before_s} → {after_s}), quarantined, "
                "or skipped. Treat as potential silent loss — the writer "
                "acknowledgement is not evidence they landed."
            )
        elif unaccounted < 0:
            note = (
                f"{abs(unaccounted)} more row(s) are in this run's dest COUNT(*) "
                f"growth ({delta_s}; dest {before_s} → {after_s}), quarantined, "
                "or skipped than were read."
            )
        else:
            note = (
                f"This run's dest COUNT(*) growth is {delta_s} "
                f"({before_s} → {after_s}). Every source row is in that delta, "
                "quarantined, or skipped. Pre-existing dest rows remain. "
                "Whole-table checksums are not comparable."
            )
    elif unaccounted > 0:
        note = (
            f"{unaccounted} source row(s) are neither in the {dest_phrase}, "
            "quarantined, nor skipped. Treat as potential silent loss — the "
            "writer acknowledgement is not evidence they landed."
        )
    elif unaccounted < 0:
        note = (
            f"{abs(unaccounted)} more row(s) are in the {dest_phrase}, "
            "quarantined, or skipped than were read. Duplicate writes, "
            "pre-existing rows on overwrite, or double-counted rejects."
        )
    else:
        note = (
            f"Every source row is in the {dest_phrase}, quarantined, or skipped."
        )
    if ack_delta:
        sign = "more" if ack_delta > 0 else "fewer"
        note += (
            f" Writer acknowledged {ack:,}; {dest_short} accounts for "
            f"{written:,} ({abs(ack_delta):,} {sign} than the writer claimed)."
        )
    missing = _as_optional_int((keyset or {}).get(MISSING_KEYS_KEY))
    extra = _as_optional_int((keyset or {}).get(EXTRA_KEYS_KEY))
    leftover_deleted = _as_optional_int((keyset or {}).get(LEFTOVER_DELETED_KEY))
    balanced = unaccounted == 0
    if missing is not None and extra is not None:
        if missing or extra:
            balanced = False
            note += (
                f" Dest-engine keyset: {missing} MISSING_TARGET key(s), "
                f"{extra} EXTRA_TARGET leftover dest key(s). COUNT(*) can net "
                "missing+extra to a false balance — that is the DMS validation "
                "hole after Full Load success. Leftover keys are not inferred "
                "deletes on incremental CDC; a complete overwrite snapshot "
                "MERGE-deletes dest keys not in S."
            )
        else:
            note += (
                " Dest-engine keyset closed: every source key is on dest and "
                "dest holds no extra keys."
            )
    if leftover_deleted:
        note += (
            f" Dest-engine MERGE deleted {leftover_deleted} leftover dest "
            "key(s) not in the complete source snapshot (overwrite only). "
            "Incremental CDC must not infer-delete."
        )
    return ConservationLedger(
        rows_read=read,
        rows_written=written,
        rows_quarantined=quarantined,
        rows_skipped=skipped,
        rows_coerced_null=coerced,
        writer_ack=ack,
        dest_count=dest_count,
        dest_count_before=dest_count_before,
        unaccounted=unaccounted,
        balanced=balanced,
        rows_read_source="gate8_source_count",
        rows_written_source=written_source,
        conservation_kind=kind,
        note=note,
        writer_ack_delta=ack_delta,
        dest_delta=append_delta,
        missing_keys=missing,
        extra_keys=extra,
        leftover_deleted=leftover_deleted,
    )


def account_job(job: Mapping[str, Any]) -> ConservationLedger:
    """Conservation ledger for one job document.

    ``rows_written`` is dest COUNT(*), never ``records_processed``.
    Two or more streams replace last-table dest COUNT with the job rollup:
    the job is closed iff every stream ledger is closed.
    """
    dest = dict(job.get("destination_summary") or {})
    streams = dest.get("streams")
    if streams is None:
        streams = job.get("streams")
    rolled = account_job_streams(streams)
    if rolled is not None:
        return rolled
    recon = dict(job.get("reconciliation") or {})
    dest_count, dest_source = dest_count_from_recon(recon)
    before = _as_optional_int(dest.get(PRECOUNT_KEY))
    if before is None:
        before = _as_optional_int(recon.get(PRECOUNT_KEY))
    ack_raw = job.get("records_processed")
    if ack_raw is None:
        ack_raw = dest.get("rows")
    census = KeyCensus.from_mapping(dest.get(CENSUS_KEY) or recon.get(CENSUS_KEY))
    mirror = extract_mirror_payload(dest)
    if not mirror:
        mirror = extract_mirror_payload(recon)
    vector = extract_vector_payload(dest)
    if not vector:
        vector = extract_vector_payload(recon)
    keyset = dict(extract_keyset_payload(dest))
    for field, value in extract_keyset_payload(recon).items():
        if value is not None or field not in keyset:
            keyset[field] = value
    scd2 = extract_scd2_payload(dest)
    if not scd2:
        scd2 = extract_scd2_payload(recon)
    return account_population(
        rows_read=_as_optional_int(recon.get("source_rows")),
        dest_count=dest_count,
        dest_count_source=dest_source,
        dest_count_before=before,
        rejected_rows=_first_present_int(
            recon.get("rejected_rows"),
            job.get("rejected_rows"),
            dest.get("rejected"),
        ),
        coerced_null_rows=_first_present_int(
            recon.get("coerced_null_rows"),
            job.get("coerced_null_rows"),
        ),
        rows_skipped=_first_present_int(
            recon.get("rows_skipped"),
            dest.get("rows_skipped"),
        ),
        writer_ack=_as_optional_int(ack_raw),
        sync_mode=str(job.get("sync_mode") or dest.get("sync_mode") or ""),
        census=census,
        mirror=mirror or None,
        vector=vector or None,
        keyset=keyset or None,
        scd2=scd2 or None,
    )


def attach_conservation_to_updates(
    status: str,
    updates: dict[str, Any],
    *,
    previous: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Stamp ``row_accounting`` on terminal job updates (mutates ``updates``).

    Same hook shape as ``attach_trust_to_updates`` so every completed job
    carries dest COUNT(*) conservation, not only the certificate export.
    """
    from services.job_trust import is_terminal_status

    if not is_terminal_status(status):
        return updates
    merged: dict[str, Any] = dict(previous or {})
    merged.update(updates)
    merged["status"] = status
    updates["row_accounting"] = account_job(merged).to_dict()
    return updates


def ledger_from_transfer_result(
    result: Any,
    *,
    sync_mode: str = "",
) -> dict[str, Any]:
    """Conservation ledger for a ``TransferResult`` (Studio sync response)."""
    dest = dict(getattr(result, "destination_summary", None) or {})
    recon = dict(getattr(result, "reconciliation", None) or {})
    return account_job(
        {
            "records_processed": getattr(result, "records_transferred", None),
            "sync_mode": sync_mode or dest.get("sync_mode") or getattr(result, "operation", "") or "",
            "reconciliation": recon,
            "destination_summary": dest,
            "rejected_rows": dest.get("rejected_rows") if "rejected_rows" in dest else dest.get("rejected"),
            "coerced_null_rows": dest.get("coerced_null_rows"),
        }
    ).to_dict()


def iter_stream_entries(streams: Any) -> list[tuple[str, dict[str, Any]]]:
    """Normalize ``streams`` as a list or name→health dict."""
    if isinstance(streams, dict):
        out: list[tuple[str, dict[str, Any]]] = []
        for key, value in streams.items():
            health = dict(value) if isinstance(value, Mapping) else {}
            name = str(health.get("name") or key)
            out.append((name, health))
        return out
    if isinstance(streams, list):
        out = []
        for index, item in enumerate(streams):
            if not isinstance(item, Mapping):
                continue
            health = dict(item)
            name = str(health.get("name") or health.get("stream") or index)
            out.append((name, health))
        return out
    return []


def stream_dest_measured(ledger: Mapping[str, Any] | None) -> bool:
    """Whether this stream's dest population was independently measured."""
    data = dict(ledger or {})
    kind = str(data.get("conservation_kind") or "")
    source = str(data.get("rows_written_source") or "")
    if kind in ("", KIND_UNMEASURED) or source in ("", DEST_UNMEASURED):
        return False
    if kind == KIND_MIRROR:
        return data.get("active_count") is not None and source == DEST_ACTIVE_READBACK
    if kind == KIND_VECTOR:
        return data.get("dest_count") is not None and source == DEST_IDENTITY_READBACK
    if kind == KIND_SCD2:
        return data.get("dest_count") is not None and source == DEST_CURRENT_READBACK
    if kind == KIND_EMPTY_PASS:
        return True
    if kind == KIND_JOB:
        if source == DEST_PER_STREAM:
            return bool(data.get("balanced"))
        if source == DEST_ACTIVE_READBACK:
            return data.get("active_count") is not None
        return data.get("dest_count") is not None
    return data.get("dest_count") is not None or data.get("dest_delta") is not None


def _compact_stream_ledger(
    name: str,
    health: Mapping[str, Any],
) -> dict[str, Any]:
    raw = health.get("row_accounting")
    if not isinstance(raw, Mapping):
        return {
            "stream": name,
            "measured": False,
            "balanced": False,
            "conservation_kind": None,
            "dest_count": None,
            "active_count": None,
            "rows_read": None,
            "rows_written": None,
            "rows_quarantined": 0,
            "rows_skipped": 0,
            "writer_ack": _as_optional_int(health.get("records_processed")),
        }
    measured = stream_dest_measured(raw)
    return {
        "stream": name,
        "measured": measured,
        "balanced": bool(raw.get("balanced")) and measured,
        "conservation_kind": str(raw.get("conservation_kind") or "") or None,
        "dest_count": _as_optional_int(raw.get("dest_count")),
        "active_count": _as_optional_int(raw.get("active_count")),
        "rows_read": _as_optional_int(raw.get("rows_read")),
        "rows_written": _as_optional_int(raw.get("rows_written")),
        "rows_quarantined": _first_present_int(raw.get("rows_quarantined")),
        "rows_skipped": _first_present_int(raw.get("rows_skipped")),
        "writer_ack": _as_optional_int(raw.get("writer_ack")),
        "unaccounted": _as_optional_int(raw.get("unaccounted")),
    }


def account_job_streams(streams: Any) -> ConservationLedger | None:
    """Job identity: closed iff every stream ledger is closed.

    Returns None when there are fewer than two streams so the single-table
    path (last dest COUNT(*) of that one object) remains the job.
    """
    entries = iter_stream_entries(streams)
    if len(entries) < 2:
        return None
    per = tuple(_compact_stream_ledger(name, health) for name, health in entries)
    measured_n = sum(1 for item in per if item["measured"])
    all_measured = measured_n == len(per)
    all_balanced = all(item["balanced"] for item in per)
    kinds = {str(item["conservation_kind"]) for item in per if item.get("conservation_kind")}
    only = next(iter(kinds)) if len(kinds) == 1 else None
    summable = bool(all_measured and only in _SUMMABLE_KINDS)

    dest_count: int | None = None
    active_count: int | None = None
    rows_written: int | None = None
    rows_read: int | None = None
    quarantined = 0
    skipped = 0
    written_source = DEST_UNMEASURED
    unaccounted: int | None = None
    if summable and only == KIND_OVERWRITE:
        dest_count = sum(int(item["dest_count"] or 0) for item in per)
        rows_written = dest_count
        rows_read = sum(int(item["rows_read"] or 0) for item in per)
        quarantined = sum(int(item["rows_quarantined"] or 0) for item in per)
        skipped = sum(int(item["rows_skipped"] or 0) for item in per)
        written_source = DEST_READBACK
        unaccounted = rows_read - (rows_written + quarantined + skipped)
    elif summable and only == KIND_MIRROR:
        active_count = sum(int(item["active_count"] or 0) for item in per)
        dest_count = (
            sum(int(item["dest_count"] or 0) for item in per)
            if all(item["dest_count"] is not None for item in per)
            else None
        )
        rows_written = active_count
        rows_read = sum(int(item["rows_read"] or 0) for item in per)
        quarantined = sum(int(item["rows_quarantined"] or 0) for item in per)
        skipped = sum(int(item["rows_skipped"] or 0) for item in per)
        written_source = DEST_ACTIVE_READBACK
        unaccounted = rows_read - (rows_written + quarantined + skipped)
    elif summable and only == KIND_VECTOR:
        dest_count = sum(int(item["dest_count"] or 0) for item in per)
        rows_written = dest_count
        rows_read = sum(int(item["rows_read"] or 0) for item in per)
        quarantined = sum(int(item["rows_quarantined"] or 0) for item in per)
        skipped = sum(int(item["rows_skipped"] or 0) for item in per)
        written_source = DEST_IDENTITY_READBACK
        unaccounted = rows_read - (rows_written + quarantined + skipped)
    elif summable and only == KIND_SCD2:
        dest_count = sum(int(item["dest_count"] or 0) for item in per)
        rows_written = dest_count
        rows_read = sum(int(item["rows_read"] or 0) for item in per)
        quarantined = sum(int(item["rows_quarantined"] or 0) for item in per)
        skipped = sum(int(item["rows_skipped"] or 0) for item in per)
        written_source = DEST_CURRENT_READBACK
        unaccounted = rows_read - (rows_written + quarantined + skipped)
    elif summable and only == KIND_EMPTY_PASS:
        dest_count = 0
        rows_written = 0
        rows_read = 0
        written_source = DEST_EMPTY_PASS
        unaccounted = 0
    elif all_measured:
        written_source = DEST_PER_STREAM

    acks = [item["writer_ack"] for item in per if item.get("writer_ack") is not None]
    writer_ack = sum(int(a) for a in acks) if len(acks) == len(per) else None
    ack_delta = None
    if writer_ack is not None and rows_written is not None:
        ack_delta = rows_written - writer_ack

    balanced = all_measured and all_balanced
    if not all_measured:
        note = (
            f"Job conservation is open: {len(per) - measured_n} of {len(per)} "
            "stream(s) have no dest-engine ledger. Last-table dest COUNT(*) is "
            "not the job. Writer acknowledgements cannot close a multi-stream "
            "job (Airbyte recordsCommitted / Fivetran MAR)."
        )
    elif not all_balanced:
        note = (
            "Job conservation is open: a stream ledger is unbalanced. The job "
            "is closed iff every stream ledger is closed."
        )
    elif summable:
        note = (
            f"Job conservation closed across {len(per)} {only} stream(s). Dest "
            "population is the sum of dest-engine counts of the same kind. "
            "Writer ack is diagnostic."
        )
    else:
        note = (
            f"Every stream ledger is closed ({', '.join(sorted(kinds))}). Dest "
            "COUNT(*) is not summed across mixed or keyed kinds — that would "
            "invent a fake job-level population. Open each stream."
        )
    if ack_delta:
        sign = "more" if ack_delta > 0 else "fewer"
        note += (
            f" Writer acknowledged {writer_ack:,}; dest population accounts "
            f"for {rows_written:,} ({abs(ack_delta):,} {sign} than the writer "
            "claimed)."
        )
    return ConservationLedger(
        rows_read=rows_read,
        rows_written=rows_written,
        rows_quarantined=quarantined,
        rows_skipped=skipped,
        rows_coerced_null=0,
        writer_ack=writer_ack,
        dest_count=dest_count,
        dest_count_before=None,
        unaccounted=unaccounted if summable else None,
        balanced=balanced,
        rows_read_source="gate8_source_count" if rows_read is not None else DEST_UNMEASURED,
        rows_written_source=written_source,
        conservation_kind=KIND_JOB,
        note=note,
        writer_ack_delta=ack_delta,
        active_count=active_count,
        stream_count=len(per),
        measured_streams=measured_n,
        summable=summable,
        per_stream=per,
    )


def stamp_stream_populations(
    summary: dict[str, Any],
    *,
    source: Any | None = None,
    destination: Any | None = None,
    dest_table: str | None = None,
    count_source: bool = True,
) -> dict[str, Any]:
    """Independent source and dest COUNT(*) while this stream is still bound.

    Job-level Gate-8 reads the restored primary endpoint (last table). Each
    stream must be counted against the object it actually wrote.
    """
    from services.dest_precount import count_endpoint_rows

    recon = dict(summary.get("reconciliation") or {})
    if count_source and source is not None:
        src_n = count_endpoint_rows(source)
        if src_n is not None:
            recon["source_rows"] = src_n
    if destination is not None:
        dest_n = count_endpoint_rows(destination, table_name=dest_table)
        if dest_n is not None:
            recon["target_rows"] = dest_n
            recon["phase"] = "post_write_row_count"
            recon["coverage"] = "row_count"
            recon["message"] = (
                "Independent dest-engine COUNT(*) for this stream. "
                "Job-level Gate-8 of the last table is not this stream."
            )
    if recon:
        summary["reconciliation"] = recon
    return summary


def stamp_stream_conservation(
    health: dict[str, Any],
    summary: Mapping[str, Any] | None,
    *,
    records_processed: int | None = None,
    sync_mode: str = "",
    census: KeyCensus | None = None,
) -> dict[str, Any]:
    """Attach this stream's ledger. Never uses writer ack as dest COUNT(*)."""
    payload = dict(summary or {})
    payload.pop("streams", None)
    if census is not None:
        payload[CENSUS_KEY] = census.to_dict()
    ack = records_processed
    if ack is None:
        ack = health.get("records_processed")
    health["row_accounting"] = account_job(
        {
            "records_processed": ack,
            "sync_mode": sync_mode
            or str(health.get("sync_mode") or payload.get("sync_mode") or ""),
            "reconciliation": payload.get("reconciliation") or {},
            "destination_summary": payload,
            "rejected_rows": payload.get("rejected_rows", payload.get("rejected")),
            "coerced_null_rows": payload.get("coerced_null_rows"),
        }
    ).to_dict()
    return health


def record_stream_health(
    stream_health: list[dict[str, Any]] | dict[str, dict[str, Any]],
    *,
    name: str,
    status: str,
    records_processed: int = 0,
    summary: Mapping[str, Any] | None = None,
    extra: Mapping[str, Any] | None = None,
    sync_mode: str = "",
    census: KeyCensus | None = None,
    source: Any | None = None,
    destination: Any | None = None,
    dest_table: str | None = None,
    count_source: bool = True,
) -> dict[str, Any]:
    """Record one stream's health plus that stream's dest-engine ledger."""
    entry: dict[str, Any] = {
        "name": name,
        "status": status,
        "records_processed": int(records_processed or 0),
    }
    if extra:
        entry.update(dict(extra))
    payload = dict(summary or {})
    payload.pop("streams", None)
    if source is not None or destination is not None:
        stamp_stream_populations(
            payload,
            source=source,
            destination=destination,
            dest_table=dest_table,
            count_source=count_source,
        )
    stamp_stream_conservation(
        entry,
        payload,
        records_processed=int(records_processed or 0),
        sync_mode=sync_mode or str(entry.get("sync_mode") or payload.get("sync_mode") or ""),
        census=census,
    )
    if isinstance(stream_health, dict):
        existing = dict(stream_health.get(name) or {})
        existing.update(entry)
        stream_health[name] = existing
        return existing
    stream_health.append(entry)
    return entry


class KeyCensusAccumulator:
    """Per-batch dest hits, reconstructed as a run-level census.

    Each batch is probed *before* it writes. Inserts this run become dest
    hits for a later batch, so summing ``len(unseen live) - hits`` equals
    new keys for the whole stream. ``dest_preexisting`` is unique live keys
    minus those inserts — dest-engine, not writer ON CONFLICT.

    At-least-once redelivery of a key already observed this run is not a
    new insert. ``dest_hits`` must be dest-engine hits among the *unseen*
    live keys of that call (callers probe ``unseen_live``). Counting log
    events as inserts is the Debezium/DMS message-count hole.

    Tombstone dest-hits sum independently, keyed uniquely across batches.
    A key that this run inserted and later deleted stays in the live unique
    set so ``inserts - deletes`` still equals dest COUNT(*) delta.
    """

    def __init__(self) -> None:
        self._seen: set[tuple[Any, ...]] = set()
        self._tomb_seen: set[tuple[Any, ...]] = set()
        self._inserts = 0
        self._tombstones = 0
        self._events = 0
        self._unique_tombstone_fallback = 0
        self._failed = False

    def unseen_live(self, keys: Sequence[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
        return self._unseen(keys, self._seen)

    def unseen_tombstones(self, keys: Sequence[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
        return self._unseen(keys, self._tomb_seen)

    @staticmethod
    def _unseen(
        keys: Sequence[tuple[Any, ...]],
        already: set[tuple[Any, ...]],
    ) -> list[tuple[Any, ...]]:
        out: list[tuple[Any, ...]] = []
        seen_here: set[tuple[Any, ...]] = set()
        for key in keys:
            if key in seen_here or key in already:
                continue
            seen_here.add(key)
            out.append(key)
        return out

    def add_events(self, events: int) -> None:
        self._events += max(int(events or 0), 0)

    def add_batch(self, keys: Sequence[tuple[Any, ...]], dest_hits: int | None) -> None:
        if dest_hits is None:
            self._failed = True
            return
        batch: list[tuple[Any, ...]] = []
        seen_batch: set[tuple[Any, ...]] = set()
        for key in keys:
            if key in seen_batch:
                continue
            seen_batch.add(key)
            batch.append(key)
        unseen = [k for k in batch if k not in self._seen]
        hits = int(dest_hits)
        if hits > len(unseen):
            # Caller probed a wider set than unseen live keys — refuse to invent inserts.
            self._failed = True
            return
        self._inserts += max(len(unseen) - hits, 0)
        self._seen.update(batch)

    def add_tombstones(
        self,
        dest_hits: int | None,
        *,
        unique_keys: int = 0,
        keys: Sequence[tuple[Any, ...]] | None = None,
    ) -> None:
        if dest_hits is None:
            self._failed = True
            return
        hits = max(int(dest_hits), 0)
        if keys is not None:
            unseen = self.unseen_tombstones(keys)
            if hits > len(unseen):
                self._failed = True
                return
            self._tombstones += hits
            for key in keys:
                self._tomb_seen.add(key)
            return
        self._tombstones += hits
        self._unique_tombstone_fallback += max(int(unique_keys), 0)

    def to_census(self) -> KeyCensus | None:
        if self._failed:
            return None
        unique_tombs = len(self._tomb_seen) or self._unique_tombstone_fallback
        if not self._seen and self._tombstones == 0 and unique_tombs == 0:
            return None
        unique = len(self._seen)
        preexisting = unique - self._inserts
        if preexisting < 0:
            return None
        return KeyCensus(
            unique_batch_keys=unique,
            dest_preexisting=preexisting,
            tombstones=self._tombstones,
            unique_tombstone_keys=unique_tombs,
            events_read=self._events or None,
        )


def observe_keyed_batch(
    acc: KeyCensusAccumulator,
    *,
    headers: Sequence[str],
    rows: Sequence[Sequence[Any]],
    mappings: Sequence[Mapping[str, Any]] | None,
    key_columns: Sequence[str] | None,
    db_type: str,
    cfg: Mapping[str, Any],
    schema: str,
    table_name: str,
) -> None:
    """Dest-engine key hits for one stream batch, before that batch writes.

    Tombstone rows are stripped from ``rows`` in place (the stream writer
    already bound the same list) and dest-held keys are hard-DELETEd so
    ``COUNT(*)`` can drop. Census without apply would not be dest population.
    """
    from services.dest_precount import destination_key_hits

    cols = [str(c).strip() for c in (key_columns or []) if str(c).strip()]
    records = [dict(zip(headers, row)) for row in rows]
    acc.add_events(len(records))
    if not cols:
        acc.add_batch([], 0)
        return
    partition = partition_keyed_records(records, cols, mappings)
    if partition.tombstone_keys:
        if not isinstance(rows, list):
            raise TypeError(
                "Keyed tombstone apply requires a mutable batch row list so "
                "deleted keys are not upserted back onto the destination"
            )
        header_list = list(headers)
        rows[:] = [
            [rec.get(h) for h in header_list] for rec in partition.live_records
        ]
    live_probe = acc.unseen_live(partition.live_keys)
    tomb_probe = acc.unseen_tombstones(partition.tombstone_keys)
    live_hits = destination_key_hits(
        db_type,
        dict(cfg),
        schema=schema,
        table_name=table_name,
        key_columns=cols,
        keys=live_probe,
    )
    tomb_hits = destination_key_hits(
        db_type,
        dict(cfg),
        schema=schema,
        table_name=table_name,
        key_columns=cols,
        keys=tomb_probe,
    )
    acc.add_batch(partition.live_keys, live_hits)
    acc.add_tombstones(
        tomb_hits,
        unique_keys=len(partition.tombstone_keys),
        keys=partition.tombstone_keys,
    )
    if partition.tombstone_keys:
        apply_hard_deletes(
            db_type=db_type,
            cfg=cfg,
            schema=schema,
            table_name=table_name,
            key_columns=cols,
            keys=partition.tombstone_keys,
        )


def observe_change_batch(
    acc: KeyCensusAccumulator,
    *,
    inserts: Sequence[Mapping[str, Any]] | None,
    updates: Sequence[Mapping[str, Any]] | None,
    deletes: Sequence[str] | None,
    key_columns: Sequence[str],
    db_type: str,
    cfg: Mapping[str, Any],
    schema: str,
    table_name: str,
    mappings: Sequence[Mapping[str, Any]] | None = None,
) -> None:
    """Fold one CDC ChangeBatch into the run-level key census.

    Same identity as ``observe_keyed_batch``: dest-engine hits on unseen
    keys, last-op delete wins inside the batch (apply upserts then deletes),
    redelivered keys are events not inserts.
    """
    from services.dest_precount import destination_key_hits

    cols = [str(c).strip() for c in key_columns if str(c).strip()]
    insert_list = list(inserts or [])
    update_list = list(updates or [])
    delete_list = list(deletes or [])
    acc.add_events(len(insert_list) + len(update_list) + len(delete_list))
    if not cols:
        acc.add_batch([], 0)
        return
    live_records = [dict(r) for r in insert_list + update_list]
    live_keys = extract_batch_keys(live_records, cols, mappings)
    tomb_keys = parse_delete_keys(delete_list, len(cols))
    tomb_set = set(tomb_keys)
    live_keys = [k for k in live_keys if k not in tomb_set]
    live_probe = acc.unseen_live(live_keys)
    tomb_probe = acc.unseen_tombstones(tomb_keys)
    live_hits = destination_key_hits(
        db_type,
        dict(cfg),
        schema=schema,
        table_name=table_name,
        key_columns=cols,
        keys=live_probe,
    )
    tomb_hits = destination_key_hits(
        db_type,
        dict(cfg),
        schema=schema,
        table_name=table_name,
        key_columns=cols,
        keys=tomb_probe,
    )
    acc.add_batch(live_keys, live_hits)
    acc.add_tombstones(tomb_hits, unique_keys=len(tomb_keys), keys=tomb_keys)
