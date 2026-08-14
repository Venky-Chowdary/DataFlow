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
* **append** — only the delta proves anything:
  ``COUNT(*)_after - COUNT(*)_before``. A table that already held 30 rows
  satisfies ``dest >= expected`` even if the writer appended nothing.
* **upsert / CDC into a non-empty dest** — COUNT(*) is not event
  conservation (updates do not change cardinality). Dest-engine key census
  closes the *cardinality* identity:

      dest_delta == inserts_new_keys - deletes

  where ``inserts_new_keys`` is *live* keys dest did not hold before the
  write, and ``deletes`` is dest-engine hits of *tombstone* keys (a
  tombstone for a key dest does not hold is a no-op — COUNT does not
  move). Writer ``records_processed`` still counts updates; it never
  closes the identity. Without a dest-engine census the ledger stays
  unproven. Soft-delete mirrors (``_deleted`` flag, COUNT stays) are a
  different identity and are not this ledger.

Writer ack is a diagnostic third number. It never closes the identity.
A mismatch against dest COUNT is the DMS hole, reported as a note, not as
proof that the rows landed.

An empty pass (reader 0, hold-outs 0, skipped 0, writer ack 0) is a
*measured* zero — the incremental steady state — not an unmeasured dest.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from services.dest_precount import PRECOUNT_KEY
from services.reconcile_coverage import is_unproven_export
from services.sync_cursor import is_append_sync, is_overwrite_sync

DEST_READBACK = "gate8_dest_readback"
DEST_UNMEASURED = "unmeasured"
DEST_EMPTY_PASS = "empty_pass"
CENSUS_KEY = "keyed_census"

KIND_OVERWRITE = "overwrite"
KIND_APPEND_DELTA = "append_delta"
KIND_KEYED = "keyed"
KIND_EMPTY_PASS = "empty_pass"
KIND_UNMEASURED = "unmeasured"


@dataclass(frozen=True)
class KeyCensus:
    """Dest-engine split of a keyed batch: live new keys vs dest-held deletes.

    ``unique_batch_keys`` / ``dest_preexisting`` are *live* keys only.
    Mixing tombstone keys into the live unique set invented inserts for
    deletes of missing keys (COUNT would not rise; the ledger would lie).

    ``tombstones`` is COUNT(DISTINCT tombstone key) dest already holds —
    those are the DELETEs that drop ``COUNT(*)``. A tombstone for a key
    dest does not hold is a no-op, not a delete and not an insert.
    """

    unique_batch_keys: int
    dest_preexisting: int
    tombstones: int = 0
    unique_tombstone_keys: int = 0

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
        except (TypeError, ValueError):
            return None
        if unique < 0 or preexisting < 0 or tombs < 0 or tomb_keys < 0:
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


def census_from_partition(
    partition: KeyPartition,
    *,
    db_type: str,
    cfg: Mapping[str, Any],
    schema: str,
    table_name: str,
    key_columns: Sequence[str],
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
        live_out = partition.live_records
    else:
        live_out = records
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
    """
    report = dict(recon or {})
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
    """
    if is_overwrite_sync(sync_mode):
        return KIND_OVERWRITE
    if is_append_sync(sync_mode):
        return KIND_APPEND_DELTA
    if dest_count_before == 0:
        return KIND_OVERWRITE
    return KIND_KEYED


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

    def to_dict(self) -> dict[str, Any]:
        return {
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
        }


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
) -> ConservationLedger:
    """Close ``reader == dest_population + hold_outs + skipped`` or say why not."""
    quarantined = hold_outs(rejected_rows, coerced_null_rows)
    skipped = int(rows_skipped or 0)
    coerced = int(coerced_null_rows or 0)
    kind = conservation_kind(sync_mode, dest_count_before=dest_count_before)
    ack = writer_ack if writer_ack is not None else None

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
    if dest_count is None or dest_count_source != DEST_READBACK:
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
        written_source = DEST_READBACK

    unaccounted = read - (written + quarantined + skipped)
    ack_delta = (written - ack) if ack is not None else None

    if unaccounted > 0:
        note = (
            f"{unaccounted} source row(s) are neither on the destination "
            "(independent COUNT(*)), quarantined, nor skipped. Treat as "
            "potential silent loss — the writer acknowledgement is not "
            "evidence they landed."
        )
    elif unaccounted < 0:
        note = (
            f"{abs(unaccounted)} more row(s) are on the destination, "
            "quarantined, or skipped than were read. Duplicate writes, "
            "pre-existing rows on overwrite, or double-counted rejects."
        )
    else:
        note = (
            "Every source row is on the destination (independent COUNT(*)), "
            "quarantined, or skipped."
        )
    if ack_delta:
        sign = "more" if ack_delta > 0 else "fewer"
        note += (
            f" Writer acknowledged {ack:,}; dest COUNT(*) accounts for "
            f"{written:,} ({abs(ack_delta):,} {sign} than the writer claimed)."
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
        balanced=unaccounted == 0,
        rows_read_source="gate8_source_count",
        rows_written_source=written_source,
        conservation_kind=kind,
        note=note,
        writer_ack_delta=ack_delta,
    )


def account_job(job: Mapping[str, Any]) -> ConservationLedger:
    """Conservation ledger for one job document.

    ``rows_written`` is dest COUNT(*), never ``records_processed``.
    """
    recon = dict(job.get("reconciliation") or {})
    dest = dict(job.get("destination_summary") or {})
    dest_count, dest_source = dest_count_from_recon(recon)
    before = _as_optional_int(dest.get(PRECOUNT_KEY))
    if before is None:
        before = _as_optional_int(recon.get(PRECOUNT_KEY))
    ack_raw = job.get("records_processed")
    if ack_raw is None:
        ack_raw = dest.get("rows")
    census = KeyCensus.from_mapping(dest.get(CENSUS_KEY) or recon.get(CENSUS_KEY))
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


class KeyCensusAccumulator:
    """Per-batch dest hits, reconstructed as a run-level census.

    Each batch is probed *before* it writes. Inserts this run become dest
    hits for a later batch, so summing ``len(live) - hits`` equals new keys
    for the whole stream. ``dest_preexisting`` is unique live keys minus
    those inserts — dest-engine, not writer ON CONFLICT.

    Tombstone dest-hits sum independently. A key that this run inserted and
    later deleted stays in the live unique set so ``inserts - deletes``
    still equals dest COUNT(*) delta (insert then delete nets zero).
    """

    def __init__(self) -> None:
        self._seen: set[tuple[Any, ...]] = set()
        self._inserts = 0
        self._tombstones = 0
        self._unique_tombstone_keys = 0
        self._failed = False

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
        self._inserts += max(len(batch) - int(dest_hits), 0)
        self._seen.update(batch)

    def add_tombstones(
        self,
        dest_hits: int | None,
        *,
        unique_keys: int = 0,
    ) -> None:
        if dest_hits is None:
            self._failed = True
            return
        self._tombstones += max(int(dest_hits), 0)
        self._unique_tombstone_keys += max(int(unique_keys), 0)

    def to_census(self) -> KeyCensus | None:
        if self._failed:
            return None
        if not self._seen and self._tombstones == 0 and self._unique_tombstone_keys == 0:
            return None
        unique = len(self._seen)
        preexisting = unique - self._inserts
        if preexisting < 0:
            return None
        return KeyCensus(
            unique_batch_keys=unique,
            dest_preexisting=preexisting,
            tombstones=self._tombstones,
            unique_tombstone_keys=self._unique_tombstone_keys,
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
    live_hits = destination_key_hits(
        db_type,
        dict(cfg),
        schema=schema,
        table_name=table_name,
        key_columns=cols,
        keys=partition.live_keys,
    )
    tomb_hits = destination_key_hits(
        db_type,
        dict(cfg),
        schema=schema,
        table_name=table_name,
        key_columns=cols,
        keys=partition.tombstone_keys,
    )
    acc.add_batch(partition.live_keys, live_hits)
    acc.add_tombstones(
        tomb_hits, unique_keys=len(partition.tombstone_keys)
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
