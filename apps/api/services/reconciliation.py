"""Gate 8 reconciliation — independent target verification."""

from __future__ import annotations

import base64
import hashlib
import heapq
import json
import logging
import os
from services.brand_env import getenv_brand
import re
import struct
import tempfile
import uuid
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from datetime import date as _date
from datetime import datetime as _datetime
from datetime import time as _time
from datetime import timezone
from decimal import Decimal, InvalidOperation, Overflow, localcontext
from functools import lru_cache
from typing import Any, Callable, Final, Iterable, Iterator

from connectors.sql_identifiers import quote_sql_identifier
from services.decision_kernel.findings import (
    typed_cast_incompatible_with_text_sink,
)
from services.readback_projection import project_readback
from services.reconcile_sftp import verify_sftp_object
from services.reconcile_coverage import (
    NO_OP_DEST_UNCHANGED,
    SOURCE_DIGEST_WRITE_PASS,
    SOURCE_DIGEST_WRITER_ACK,
    WRITTEN_BATCH_KEYS,
    append_row_count_report,
    extra_rows_note,
    is_sample_authority,
    is_unproven_export,
    is_writer_ack_only,
    row_count_scope_stamp,
)
from services.transform_engine import (
    _DATE_LIKE_RE,
    _STRICT_BOOL_FALSE,
    _STRICT_BOOL_TRUE,
    _parse_date,
    _parse_datetime,
    apply_transform,
    decimal_wire_value,
    reset_active_number_locale,
    set_active_number_locale,
)
from services.type_system import instant_date_carrier, normalize_logical_type
from services.value_serializer import (
    cell_to_string,
    is_missing_sentinel,
    json_default,
    json_loads_exact,
    load_http_json,
)

# Fingerprinting runs once per cell on both the write and the read-back pass, so
# resolving these names inside the function costs a module lookup per cell.
from connectors.sql_bind import normalize_sql_bind_value
from services.carrier_instant import quantize_instant_for_carrier

logger = logging.getLogger(__name__)

SPILL_THRESHOLD = int(getenv_brand("FINGERPRINT_SPILL_THRESHOLD", "1000000"))

# Quick pre-filter for the expensive Decimal / date normalization in
# normalize_cell.  Most string columns (names, emails, codes) are clearly not
# numbers or dates, so we can skip the write-path parser and the date regex.
_NUMERIC_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
# Currency / grouping marks the write path may still bind (or Auto-refuse).
_NUMERIC_WIRE_MARKS = ("$", "€", "£", "¥")
_NUMERIC_WIRE_CODES = ("USD", "EUR", "GBP")
_DATE_LIKE_CHARS = frozenset("-:/T ")
# RFC 4122 UUID wire — engines differ on case (PG lower, some drivers upper).
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_NULL_SENTINEL = "\x00NULL\x00"


def _looks_like_numeric_wire(text: str) -> bool:
    """True when ``text`` might be a number the write path binds or Auto-refuses.

    ASCII scientific / plain decimals stay on the cheap ``_NUMERIC_RE`` path.
    Locale money and grouped forms (``$1,234.56`` / ``1,234.56``) must also
    reach ``decimal_wire_value`` so Gate-8 matches the dest DECIMAL, without
    running the parser on every name or email.
    """
    if not text:
        return False
    if text[0] in "+-0123456789." and _NUMERIC_RE.match(text):
        return True
    if any(mark in text for mark in _NUMERIC_WIRE_MARKS):
        return True
    upper = text.upper()
    if any(code in upper for code in _NUMERIC_WIRE_CODES):
        return True
    return text[0] in "+-0123456789(" and "," in text


@dataclass(frozen=True, slots=True)
class _TextFoldPlan:
    """How a destination column's text is canonicalized for a checksum.

    Every decision here follows from the column's DDL type alone, so it is
    resolved once per type instead of once per cell — a 1M-row × 10-column
    table asked the same eleven type questions 10M times.
    """

    keep_trailing_spaces: bool
    rstrip_blank_pad: bool
    fold_width: bool
    fold_kana: bool
    fold_variation: bool
    fold_accent: bool
    casefold: bool
    uuid_carrier: bool


@lru_cache(maxsize=8192)
def _text_fold_plan(ddl_type: str) -> _TextFoldPlan:
    """Compile the checksum text rules for one destination DDL type."""
    from services.type_system import (
        is_accent_insensitive_collation,
        is_case_insensitive_collation,
        is_fixed_width_char_carrier,
        is_kana_insensitive_collation,
        is_variation_insensitive_collation,
        is_width_insensitive_collation,
    )

    fixed_width = is_fixed_width_char_carrier(ddl_type)
    logical = normalize_logical_type(ddl_type)
    return _TextFoldPlan(
        keep_trailing_spaces=not fixed_width and logical in {"string", "text"},
        rstrip_blank_pad=fixed_width,
        fold_width=is_width_insensitive_collation(ddl_type),
        fold_kana=is_kana_insensitive_collation(ddl_type),
        fold_variation=is_variation_insensitive_collation(ddl_type),
        fold_accent=is_accent_insensitive_collation(ddl_type),
        casefold=is_case_insensitive_collation(ddl_type),
        uuid_carrier=logical == "uuid"
        or bool(re.search(r"\b(?:uuid|uniqueidentifier|guid)\b", ddl_type, re.I)),
    )


@lru_cache(maxsize=256)
def destination_empty_string_is_null(engine: str | None) -> bool:
    """True when the write destination collapses '' → NULL (Oracle VARCHAR2).

    Fivetran HVR Compare applies **write-location** rules for cross-engine
    ambiguity — Oracle treats zero-length VARCHAR2 as NULL; Postgres/MySQL do not.
    """
    eng = (engine or "").strip().lower()
    if not eng:
        return False
    return eng in {"oracle", "oracledb", "oracle_autonomous"} or eng.startswith("oracle")


@dataclass
class ReconciliationReport:
    passed: bool
    source_rows: int
    target_rows: int
    source_checksum: str
    target_checksum: str
    message: str
    rejected_rows: int = 0
    # Rows kept but with >=1 cell coerced to NULL because a value could not be
    # cast. Data was ALTERED, so this is surfaced even when row counts/checksums
    # match — reconciliation must not claim "100% fidelity" in that case.
    coerced_null_rows: int = 0
    # Rows intentionally not written because they were stale/duplicate without
    # being rejected (e.g. CDC LSN redelivery). They must not be counted as
    # dropped, but they also do not appear in the destination.
    rows_skipped: int = 0
    # Bounded read-back sample (mismatches) for operator drill-down / export.
    sample_compare: dict[str, Any] | None = None
    # Honest post-write phase: verified | writer_ack | failed | skipped | pending
    phase: str = ""
    post_write_pending: bool = False
    preview: bool = False
    # Module 4 honesty: sample authority must never hide checksum mismatch.
    checksum_match: bool | None = None
    population_proof: bool = False
    assurance_level: str = ""
    # Set when the digests cover different populations (see reconcile_coverage).
    checksum_scope: str = ""
    # Pre-write dest COUNT(*) — append identity is dest_after − dest_before.
    target_rows_before: int | None = None

    def to_dict(self) -> dict:
        return stamp_post_write_phase(asdict(self))


def stamp_post_write_phase(report: dict[str, Any]) -> dict[str, Any]:
    """Stamp explicit post-write phase so UIs never confuse writer-ack with Verified.

    Also stamps ``coverage``, ``checksum_match``, ``population_proof``,
    ``assurance_level`` — Gate-8 never invents population RI proof.
      * ``full_checksum`` — independent source↔dest digests match
      * ``sample`` — keyed sample is the authority (checksums missing or diverge)
      * ``writer_ack`` — writer digest only
      * ``none`` / omitted — failed / skipped / pending
    """
    out = dict(report or {})
    if out.get("preview") is True or str(out.get("phase") or "").lower().startswith("pre_write"):
        # Leave preflight simulation alone.
        return out
    if out.get("phase") and not str(out.get("phase")).startswith("post_write"):
        # Unknown custom phase — still normalize pending/preview flags.
        return out

    passed = bool(out.get("passed"))
    scoped = row_count_scope_stamp(out)
    if scoped is not None:
        return scoped
    src = str(out.get("source_checksum") or "").strip()
    tgt = str(out.get("target_checksum") or "").strip()
    msg = str(out.get("message") or "").lower()
    independent_match = bool(src and tgt and src == tgt)
    # Module 4: always stamp checksum honesty; never invent population RI proof.
    out["checksum_match"] = independent_match if (src and tgt) else False
    out["population_proof"] = False

    if str(out.get("assurance_level") or "") == NO_OP_DEST_UNCHANGED:
        # Quiet incremental poll — dest-before equals dest-after. Digests from
        # a prior write-pass must not upgrade this to full_checksum.
        out["phase"] = "post_write_no_op"
        out["post_write_pending"] = False
        out["preview"] = False
        out["coverage"] = NO_OP_DEST_UNCHANGED
        out["assurance_level"] = NO_OP_DEST_UNCHANGED
        out["migration_proven"] = False
        out["population_proof"] = False
        out["checksum_match"] = False
        return out

    if is_unproven_export(out, msg):
        out["phase"] = "post_write_skipped"
        out["post_write_pending"] = False
        out["preview"] = False
        out["coverage"] = "none"
        out["assurance_level"] = "none"
        out["unproven"] = True
        out["skipped_readback"] = True
        out["migration_proven"] = False
        out["checksum_match"] = False
        return out

    if not passed:
        out["phase"] = "post_write_failed"
        out["post_write_pending"] = False
        out["preview"] = False
        out["coverage"] = "none"
        out["assurance_level"] = "none"
        return out

    writer_only = is_writer_ack_only(
        msg, tgt, source_provenance=str(out.get("source_checksum_provenance") or "")
    )
    sample = out.get("sample_compare") if isinstance(out.get("sample_compare"), dict) else {}
    sample_ok = bool(sample.get("passed")) and int(sample.get("compared") or 0) > 0
    # GA: diverging independent digests never become sample-verified success.
    # Sample may only classify when digests are missing (not when they disagree).
    if src and tgt and not independent_match:
        out["phase"] = "post_write_failed"
        out["post_write_pending"] = False
        out["preview"] = False
        out["passed"] = False
        out["coverage"] = "none"
        out["assurance_level"] = "none"
        out["checksum_match"] = False
        return out

    sample_authority = sample_ok and is_sample_authority(
        msg, tgt, writer_only=writer_only
    )
    if passed and sample_authority and not (src and tgt):
        out["phase"] = "post_write_sample_verified"
        out["post_write_pending"] = False
        out["preview"] = False
        out["coverage"] = "sample"
        out["assurance_level"] = "sample"
        return out

    if independent_match and not writer_only:
        from services.signed_proof_pack import apply_fidelity_veto

        vetoed = apply_fidelity_veto(out)
        if vetoed.get("coverage") == "coerced" or vetoed.get("phase") == "post_write_failed":
            return vetoed
        provenance = str(out.get("source_checksum_provenance") or "")
        # Write-pass fingerprints hash remapped cells in-process. Dest may be
        # independently SELECT'd, but the source warehouse was not re-read —
        # Fivetran/HVR Compare would not call that full_checksum / migration_proven.
        if provenance in {SOURCE_DIGEST_WRITE_PASS, SOURCE_DIGEST_WRITER_ACK}:
            rows = out.get("target_rows") or out.get("source_rows") or 0
            out["phase"] = "post_write_write_pass"
            out["post_write_pending"] = False
            out["preview"] = False
            out["coverage"] = "write_pass_dest_readback"
            out["assurance_level"] = "write_pass_dest_readback"
            out["migration_proven"] = False
            out["population_proof"] = False
            if str(out.get("message") or "").lower().startswith("row fidelity verified"):
                out["message"] = (
                    f"Destination read-back matches the write-pass fingerprint "
                    f"({rows} rows). Source warehouse was not independently "
                    "re-read — not migration_proven."
                )
            return out
        out["phase"] = "post_write_verified"
        out["post_write_pending"] = False
        out["preview"] = False
        out["coverage"] = "full_checksum"
        out["assurance_level"] = "full_checksum"
        return out

    if writer_only or (passed and src and not tgt):
        out["phase"] = "post_write_writer_ack"
        out["post_write_pending"] = False
        out["preview"] = False
        out["coverage"] = "writer_ack"
        out["assurance_level"] = "writer_ack"
        return out

    out["phase"] = "post_write_pending"
    out["post_write_pending"] = True
    out["preview"] = False
    out["coverage"] = "none"
    out["assurance_level"] = "none"
    return out


def _get_case_insensitive(rec: dict[str, Any], key: str | None) -> Any:
    if not key:
        return None
    if key in rec:
        return rec[key]
    lower = key.lower()
    for k, v in rec.items():
        if k.lower() == lower:
            return v
    return None


@lru_cache(maxsize=4096)
def _canonical_fingerprint_ddl(engine: str, ddl: str) -> str:
    """Normalize a destination type stamp before it steers a fingerprint.

    The two sides of Gate-8 learn the destination type from different places:
    the writer knows the DDL it emitted (``DATETIME(6)``, ``varchar(255)``)
    while the read-back reads the live catalog (``TIMESTAMP_NTZ(6)``,
    ``VARCHAR(255) COLLATE utf8mb4_0900_ai_ci``). Those name the same physical
    column, so fingerprinting on the raw spelling made an identical population
    hash differently and Gate-8 failed a correct write. Both sides now
    canonicalize through the same materializer. SQL type names are
    case-insensitive, so the canonical form is upper case — ``bigint`` and
    ``BIGINT`` must not steer two different fingerprints.
    """
    if not ddl:
        return ""
    try:
        from services.decision_kernel.types import materialize_dest_ddl

        return str(materialize_dest_ddl(engine, ddl) or ddl).upper()
    except Exception:
        return ddl.upper()


def _column_fingerprint_ddl(
    column: str, eng: str, types: dict[str, str] | None
) -> str:
    """Resolve the canonical DDL that steers one column's fingerprint.

    Everything here depends on the column and the destination catalog, never on
    the cell, so a row loop must resolve it once per column rather than once per
    value. Done per cell it cost more than the canonicalization it was steering.
    """
    ddl = ""
    lookup = types or {}
    if column and lookup:
        ddl = str(lookup.get(column) or lookup.get(column.lower()) or "")
        if not ddl:
            needle = column.lower()
            for k, v in lookup.items():
                if str(k).lower() == needle:
                    ddl = str(v or "")
                    break
    return _canonical_fingerprint_ddl(eng, ddl)


def _fingerprint_cell(
    value: Any,
    *,
    column: str = "",
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> str:
    """Cell fingerprint — bind-aware when destination types are known."""
    eng = (dest_db_type or "").strip().lower()
    ddl = _column_fingerprint_ddl(column, eng, dest_types)
    return _fingerprint_with_ddl(value, ddl, eng)


def _fingerprint_with_ddl(value: Any, ddl: str, eng: str) -> str:
    """Fingerprint one cell against an already-resolved column DDL."""
    if eng:
        try:
            # Defined later in this module; resolved at call time.
            # Pass engine even without DDL so Oracle ''↔NULL write rules apply.
            return fingerprint_for_reconcile(value, ddl_type=ddl, engine=eng)
        except Exception:
            pass
    return normalize_cell(value)


def _iter_fingerprints(
    rows: Iterable[Any],
    columns: list[str] | None = None,
    *,
    sort_key: str | None = None,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
):
    """Yield (row_key, fingerprint) tuples for each row without materializing the full list.

    When ``dest_db_type`` / ``dest_types`` are set, cells are fingerprinted through
    the same write-path bind helpers as Gate-8 sample compare.
    """
    eng = (dest_db_type or "").strip().lower()
    types = dest_types or {}
    # Column facts resolved once for the whole scan. These depend on the
    # destination catalog, not on any cell, and resolving them per value cost
    # more than the canonicalization they steer.
    _ddl_cache: dict[str, str] = {}

    def _ddl_for(col: str) -> str:
        ddl = _ddl_cache.get(col)
        if ddl is None:
            ddl = _column_fingerprint_ddl(col, eng, types)
            _ddl_cache[col] = ddl
        return ddl

    def _fp(val: Any, col: str = "") -> str:
        return _fingerprint_with_ddl(val, _ddl_for(col), eng)

    if columns is not None:
        cols = columns
        sorted_cols = sorted(cols, key=lambda x: x.lower())
        col_index = {c: i for i, c in enumerate(cols)}
        # Pair each emitted column with its lowered label and resolved DDL so the
        # row loop does no per-cell lookup at all.
        emit_plan = [(c, c.lower(), _ddl_for(c)) for c in sorted_cols]
        sort_idx = -1
        if sort_key:
            sort_key_lower = sort_key.lower()
            for i, c in enumerate(cols):
                if c.lower() == sort_key_lower:
                    sort_idx = i
                    break
        for row in rows:
            if isinstance(row, dict):
                parts = [
                    f"{label}={_fingerprint_with_ddl(row.get(c), ddl, eng)}"
                    for c, label, ddl in emit_plan
                ]
                if sort_key:
                    row_key = _fp(row.get(sort_key), sort_key)
                    if row_key is None or row_key == "":
                        for k, v in row.items():
                            if k.lower() == sort_key_lower:
                                row_key = _fp(v, sort_key)
                                break
                else:
                    row_key = ""
            else:
                n = len(row)
                parts = [
                    f"{label}="
                    f"{_fingerprint_with_ddl(row[col_index[c]] if col_index[c] < n else None, ddl, eng)}"
                    for c, label, ddl in emit_plan
                ]
                row_key = (
                    _fp(
                        row[sort_idx] if sort_idx >= 0 and sort_idx < len(row) else None,
                        sort_key or "",
                    )
                    if sort_key
                    else ""
                )
            fingerprint = "\x1f".join(parts)
            yield (row_key, fingerprint)
    else:
        for row in rows:
            if isinstance(row, dict):
                keys = sorted(row.keys(), key=lambda x: x.lower())
                parts = [f"{k.lower()}={_fp(row.get(k), k)}" for k in keys]
                fingerprint = "\x1f".join(parts)
                row_key = (
                    _fp(_get_case_insensitive(row, sort_key), sort_key or "")
                    if sort_key
                    else ""
                )
            else:
                fingerprint = "|".join(sorted(_fp(v) for v in row))
                row_key = ""
            yield (row_key, fingerprint)


# Accumulating fingerprints into a digest lives in its own module (size budget);
# re-exported here because this module is the reconciliation surface callers
# import from.
from services.fingerprint_accumulator import (  # noqa: E402,F401 — re-export
    FingerprintAccumulator,
    fingerprint_checksum,
    _hash_fingerprints,
)


def canonical_checksum(
    rows: list[Any],
    columns: list[str] | None = None,
    *,
    sort_key: str | None = None,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> str:
    """Stable, order-independent checksum that preserves column identity.

    Accepts either a matrix of values (with an explicit column list) or a list
    of dicts. Column names are included in the row fingerprint so that swapped
    columns cannot collide. Column labels are normalized to lowercase so source
    and target casing differences do not produce false mismatches. When no
    columns are provided, the legacy cell-only fallback is used for matrices.

    Pass ``dest_db_type`` / ``dest_types`` so source wire and destination
    read-back share write-path bind fingerprints (bool/JSON parity).
    """
    if not rows:
        return hashlib.sha256(b"").hexdigest()
    return _hash_fingerprints(
        list(
            _iter_fingerprints(
                rows,
                columns,
                sort_key=sort_key,
                dest_db_type=dest_db_type,
                dest_types=dest_types,
            )
        )
    )


def canonical_checksum_from_iter(
    rows: Iterable[Any],
    columns: list[str] | None = None,
    *,
    sort_key: str | None = None,
    limit: int = 0,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> str:
    """Streaming variant of canonical_checksum with optional sample limit.

    Reads rows lazily and hashes fingerprints in sorted order. A limit of 0
    means process all rows.

    Accumulation goes through :class:`FingerprintAccumulator`, which spills
    sorted chunks to disk past a threshold. It used to append every
    ``(row_key, fingerprint)`` pair to a plain list, which made the strict
    reconcile — the default validation mode, and the one that passes
    ``limit=0`` — allocate roughly 250 bytes per destination row. That is about
    5 GB at 20M rows, and the OOM landed in the *verification* step after the
    data had already been written. The digest is unchanged: with no spill the
    accumulator sorts and hashes exactly as before.
    """
    acc = FingerprintAccumulator()
    for i, (row_key, fp) in enumerate(
        _iter_fingerprints(
            rows,
            columns,
            sort_key=sort_key,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
        )
    ):
        if limit and i >= limit:
            break
        acc.add(row_key, fp)
    return acc.digest()


# Digest every checksum path produces for a population with no rows. Both
# sides fold zero fingerprints into SHA-256, so this value is a proof of
# emptiness rather than the absence of a digest.
EMPTY_POPULATION_DIGEST: Final[str] = hashlib.sha256().hexdigest()


def checksum_rows(
    rows: list[Any],
    columns: list[str] | None = None,
    *,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> str:
    """Canonical, order-independent checksum over a matrix or list of dicts."""
    return canonical_checksum(
        rows, columns, dest_db_type=dest_db_type, dest_types=dest_types
    )


def aggregate_checksum(
    records: list[dict[str, Any]],
    columns: list[str] | None = None,
    *,
    sort_key: str | None = None,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> str:
    """Order-independent checksum for reconciliation with column identity."""
    return canonical_checksum(
        records,
        columns,
        sort_key=sort_key,
        dest_db_type=dest_db_type,
        dest_types=dest_types,
    )


def reconcile(
    *,
    source_rows: int,
    target_rows: int,
    source_checksum: str,
    target_checksum: str,
    rejected_rows: int = 0,
    strict_checksum: bool = True,
    allow_extra_rows: bool = False,
    sample_compare: dict[str, Any] | None = None,
    coerced_null_rows: int = 0,
    rows_skipped: int = 0,
    rows_shaped_out: int = 0,
    rows_source_filtered: int = 0,
    target_rows_before: int | None = None,
    checksum_scope: str = "",
) -> ReconciliationReport:
    """Compare source and destination evidence into one Gate-8 verdict.

    ``checksum_scope`` names the population the target digest covers. It is
    :data:`WRITTEN_BATCH_KEYS` when the destination was re-read by written key,
    which is the only comparable digest for an append into a table that already
    held rows — the whole-table count still goes in the report, but the digest
    must not be described as covering it.

    Strict vs balanced does not make incomparable populations comparable.
    Whole-table digest mismatch on append/upsert into a larger sink uses the
    dest-before delta identity (``append_row_count_report``). A keyed-batch
    digest that still disagrees is a real cell failure and still fails Gate-8.
    Sample compare never upgrades that to ``full_checksum``.
    """
    coerced_null_rows = max(int(coerced_null_rows or 0), 0)
    rows_skipped = max(int(rows_skipped or 0), 0)
    rows_shaped_out = max(int(rows_shaped_out or 0), 0)
    rows_source_filtered = max(int(rows_source_filtered or 0), 0)
    # Coerced rows are KEPT in the destination (a cell became NULL), so they do
    # not lower the expected row count — only genuinely DROPPED / held-out rows do.
    # Under quarantine, bad rows are held out of the primary write (rejected >
    # coerced); under coerce_null, rejected == coerced and dropped == 0.
    # Under fail, coerced == 0 so dropped == rejected.
    # Skipped rows are neither dropped nor written (e.g. stale CDC LSN
    # redelivery) and must be excluded from the expected destination count.
    # Rows an approved shaping recipe removed on the read (filtered or diverted)
    # were read and are deliberately absent from the destination. They are a
    # declared effect of the recipe, not a loss and not a quarantine finding, so
    # they lower the expected count exactly like a hold-out does.
    # A declared source row filter removes rows on the read for the same reason:
    # they were counted in the source population and were never candidates for
    # the destination.
    dropped_rows = max(max(rejected_rows, 0) - coerced_null_rows, 0)
    expected_rows = max(
        source_rows
        - dropped_rows
        - rows_skipped
        - rows_shaped_out
        - rows_source_filtered,
        0,
    )
    if (
        not source_checksum
        and expected_rows == 0
        and target_rows == 0
        and target_checksum in {"", EMPTY_POPULATION_DIGEST}
    ):
        # Every source row was held out, so this run's projection is the empty
        # population — a digest that is defined, not missing. The writer had no
        # rows to hash and returned "", which then read as a mismatch against
        # the destination's empty digest and failed an all-quarantined run that
        # behaved exactly as the policy asked.
        source_checksum = target_checksum
    row_count_ok = target_rows == expected_rows or (
        allow_extra_rows and target_rows >= expected_rows
    )
    if not row_count_ok:
        extra_note = (
            f" (target has {target_rows - expected_rows} extra rows)"
            if target_rows > expected_rows
            else ""
        )
        return ReconciliationReport(
            passed=False,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message=(
                f"Row count mismatch: source {source_rows}, rejected {rejected_rows}, "
                f"skipped {rows_skipped}, removed by transform {rows_shaped_out}, "
                f"filtered out {rows_source_filtered}, "
                f"expected target {expected_rows} vs target {target_rows}{extra_note}"
            ),
            rejected_rows=rejected_rows,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
        )

    if sample_compare and not sample_compare.get("passed", True):
        mismatches = sample_compare.get("mismatches") or []
        first = mismatches[0] if mismatches else None
        if isinstance(first, dict):
            detail = (
                f"column {first.get('source')}→{first.get('target')}: "
                f"source={first.get('source_value')!r} dest={first.get('target_value')!r}"
            )
        else:
            detail = first if first else "value mismatch in read-back sample"
        return ReconciliationReport(
            passed=False,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message=f"Read-back sample verification failed: {detail}",
            rejected_rows=rejected_rows,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
            sample_compare=sample_compare,
        )

    if source_checksum != target_checksum:
        # Comparable checksum mismatch always fails Gate-8. Sample compare may
        # attach diagnostics only — never green-pass / override. Incomparable
        # append/upsert into a larger sink is not a checksum: dest-before delta
        # is the identity. Strict does not invent comparability.
        compared = int((sample_compare or {}).get("compared") or 0)
        sample_ok = (
            bool(sample_compare)
            and bool(sample_compare.get("passed", False))
            and compared > 0
        )
        has_extra = allow_extra_rows and target_rows > expected_rows
        extra_note = extra_rows_note(target_rows, expected_rows) if has_extra else ""
        sample_note = ""
        if sample_ok:
            # ``compared`` is a count of cells; naming it rows over-stated the
            # evidence by the column count of the projection.
            sample_rows = int((sample_compare or {}).get("rows_compared") or 0)
            scope = (
                f"{sample_rows:,} row(s) / {compared:,} cell(s)"
                if sample_rows
                else f"{compared:,} cell(s)"
            )
            sample_note = (
                f" Key-aligned sample compared {scope} without value "
                "mismatches — diagnostic only; does NOT override checksum failure."
            )
        elif sample_compare:
            sample_note = (
                " Key-aligned sample compare incomplete or failed — not used to "
                "soften checksum mismatch."
            )
        mode_label = "strict" if strict_checksum else "balanced"
        if has_extra and checksum_scope != WRITTEN_BATCH_KEYS:
            return append_row_count_report(
                source_rows=source_rows,
                target_rows=target_rows,
                target_rows_before=target_rows_before,
                expected_rows=expected_rows,
                source_checksum=source_checksum,
                target_checksum=target_checksum,
                sample_note=sample_note,
                rejected_rows=rejected_rows,
                coerced_null_rows=coerced_null_rows,
                rows_skipped=rows_skipped,
                sample_compare=sample_compare,
            )
        return ReconciliationReport(
            passed=False,
            source_rows=source_rows,
            target_rows=target_rows,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            message=(
                f"Checksum mismatch ({mode_label}): source {source_checksum} vs "
                f"target {target_checksum}.{extra_note}{sample_note} "
                "Sample success cannot override full-table checksum mismatch."
            ),
            rejected_rows=rejected_rows,
            coerced_null_rows=coerced_null_rows,
            rows_skipped=rows_skipped,
            sample_compare=sample_compare,
            checksum_match=False,
            population_proof=False,
            assurance_level="none",
        )
    if coerced_null_rows:
        # Row counts and checksums can still match here because the SAME failed
        # coercion is applied when re-reading the source for its checksum. That
        # does NOT mean the destination holds the original values — it holds
        # NULLs. Be explicit rather than claiming full fidelity.
        message = (
            f"Transfer completed but NOT full fidelity: {coerced_null_rows} row(s) had a value "
            f"coerced to NULL because it could not be cast to the target type"
        )
        if rejected_rows and rejected_rows != coerced_null_rows:
            message += f"; {rejected_rows} row(s) rejected"
    elif checksum_scope == WRITTEN_BATCH_KEYS and target_rows > expected_rows:
        message = (
            f"Row fidelity verified for the {expected_rows} row(s) this run wrote "
            f"— destination digest re-read by written key. Destination holds "
            f"{target_rows} row(s) in total; rows written by earlier runs are "
            "outside this proof."
        )
        if rejected_rows:
            message += f" {rejected_rows} row(s) rejected."
    else:
        message = f"Row fidelity verified — source and target checksums match ({target_rows} rows)"
        if rejected_rows:
            message = f"Transfer verified ({target_rows} rows written, {rejected_rows} rejected)"
    return ReconciliationReport(
        passed=True,
        source_rows=source_rows,
        target_rows=target_rows,
        source_checksum=source_checksum,
        target_checksum=target_checksum,
        message=message,
        rejected_rows=rejected_rows,
        coerced_null_rows=coerced_null_rows,
        rows_skipped=rows_skipped,
        sample_compare=sample_compare,
        checksum_match=True,
        population_proof=False,
        assurance_level="coerced" if coerced_null_rows else "full_checksum",
        checksum_scope=checksum_scope,
        target_rows_before=target_rows_before,
    )


def _iter_fetchmany(cur, batch_size: int = 5000):
    """Yield rows from a DBAPI cursor without loading the full result set."""
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield row


READBACK_ITERSIZE: Final[int] = 5000


@contextmanager
def streaming_readback_cursor(conn: Any, *, engine: str) -> Iterator[Any]:
    """Cursor that streams a whole-table read-back instead of buffering it.

    ``fetchmany`` alone does not stream: psycopg2 and PyMySQL both pull the
    entire result set into client memory when the statement executes, so the
    Gate-8 proof was the largest allocation of the migration — a measured
    2.0 GB RSS to verify 10M rows, and linear from there. A PostgreSQL named
    (server-side) cursor and a PyMySQL ``SSCursor`` hold one batch at a time
    (39 MB for the same read), which is what lets the proof scale past the
    table it is proving.

    Engines whose drivers already stream (SQLite, DuckDB, FreeTDS, Snowflake,
    Oracle) fall through to a plain cursor.
    """
    cur = None
    try:
        if engine in {"postgresql", "redshift", "timescaledb", "cockroachdb"}:
            # Named cursors are WITHOUT HOLD: they need an open transaction,
            # which an autocommit connection does not have.
            if not getattr(conn, "autocommit", False):
                cur = conn.cursor(name=f"df_readback_{uuid.uuid4().hex}")
                cur.itersize = READBACK_ITERSIZE
        elif engine == "mysql":
            import pymysql.cursors

            cur = conn.cursor(pymysql.cursors.SSCursor)
        if cur is None:
            cur = conn.cursor()
        yield cur
    finally:
        if cur is not None:
            # A server-side cursor dies with its transaction; a close failure
            # here must not mask the verdict the read-back just produced.
            with suppress(Exception):
                cur.close()


def dbapi_streaming_rows(
    cur: Any, *, batch_size: int = READBACK_ITERSIZE
) -> tuple[list[str], Iterator[Any]]:
    """Return ``(column_names, rows)`` for an already-executed DBAPI cursor.

    A psycopg2 server-side cursor has no ``description`` until the first block
    is fetched, so the names must be read *after* priming — reading them first
    fingerprints zero columns and yields the digest of an empty table.
    """
    first = cur.fetchmany(batch_size)
    names = [d[0] for d in cur.description] if cur.description else []

    def _rows() -> Iterator[Any]:
        batch = first
        while batch:
            yield from batch
            batch = cur.fetchmany(batch_size)

    return names, _rows()


def sa_streaming_result(
    conn: Any, statement: Any, *, itersize: int = READBACK_ITERSIZE
) -> tuple[list[str], Iterator[Any]]:
    """Return ``(column_names, rows)`` where rows arrive a block at a time.

    Drivers without server-side cursors ignore ``stream_results`` and buffer as
    before — no engine is made worse, and those that can stream stop paying for
    the whole table.

    The option is set on the *statement*, not the connection:
    ``Connection.execution_options()`` mutates the connection in place, so every
    later statement on it inherited streaming. PostgreSQL then compiled a
    following ``DROP TABLE`` as ``DECLARE ... CURSOR FOR DROP TABLE`` and raised
    a syntax error — which is how mirror key-staging tables survived their own
    cleanup and accumulated in the customer's schema.
    """
    try:
        stmt = statement.execution_options(
            stream_results=True, max_row_buffer=itersize
        )
    except AttributeError:  # a raw string has no execution_options
        stmt = statement
    result = conn.execute(stmt)
    names = [str(k) for k in result.keys()]

    def _rows() -> Iterator[Any]:
        for partition in result.partitions(itersize):
            yield from partition

    return names, _rows()


def iter_select_row_dicts(
    conn: Any,
    statement: Any,
    columns: list[str],
    *,
    itersize: int = READBACK_ITERSIZE,
) -> Iterator[list[dict[str, Any]]]:
    """Yield dict batches from one SELECT. Full-population drain — never OFFSET.

    ``page_clause`` remains the owner for *windowed* preview reads that have a
    stable ORDER BY. A complete scan (SCD2 staging, mirror key walk, active-row
    digest) must be one cursor. Microsoft: OFFSET/FETCH is a new independent
    query per page and requires a unique ORDER BY; without it SQL Server
    errors and Oracle/DB2 reject LIMIT (ORA-03047). OFFSET is also O(n²).

    Dest-engine ``HASH_AGG`` / ``CHECKSUM_AGG`` pushdown is a future
    enhancement of this kernel, not a second path — those aggregates are not
    type-portable (CHECKSUM_AGG ignores NULL; HASH_AGG is Snowflake-only).
    """
    _names, raw = sa_streaming_result(conn, statement, itersize=itersize)
    batch: list[dict[str, Any]] = []
    size = max(1, int(itersize))
    for row in raw:
        mapping = getattr(row, "_mapping", None)
        if mapping is None:
            mapping = dict(zip(columns, row))
        batch.append({c: mapping.get(c) for c in columns})
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def stream_select_checksum(
    conn: Any,
    statement: Any,
    columns: list[str],
    *,
    itersize: int = READBACK_ITERSIZE,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Order-independent checksum of a full SELECT, streamed.

    Same Gate-8 fingerprint kernel as dest read-back
    (``sa_streaming_result`` + ``canonical_checksum_from_iter``). Empty
    population returns ``(0, "")`` — the SCD2/mirror writer contract (blank
    digest, not sha256 of empty). This digest does not by itself close
    Gate-8 or claim ``migration_proven``.
    """
    count = 0

    def _rows() -> Iterator[Any]:
        nonlocal count
        for batch in iter_select_row_dicts(
            conn, statement, columns, itersize=itersize
        ):
            for row in batch:
                count += 1
                yield row

    digest = canonical_checksum_from_iter(
        _rows(),
        columns,
        dest_db_type=dest_db_type,
        dest_types=dest_types,
    )
    return count, (digest if count else "")


# Cap on keys fetched back per batch proof: bounded so a 10M-row append does
# not build a 10M-term IN list, and matched by the writer's stashed id sample.
KEYED_READBACK_ID_CAP: Final[int] = 500

# Text carrier per dialect. The keys arrive from writer meta as strings; an
# uncast bigint/uuid column aborts the read-back, and an aborted read-back is
# not a soft failure — it is silently *no* proof at all.
_KEYED_TEXT_CAST: Final[dict[str, str]] = {
    "mysql": "CHAR",
    "mariadb": "CHAR",
    "sqlserver": "NVARCHAR(4000)",
    "mssql": "NVARCHAR(4000)",
    "oracle": "VARCHAR2(4000)",
    "clickhouse": "String",
    "databricks": "STRING",
}

_KEYED_QUOTE_CHAR: Final[dict[str, str]] = {
    "mysql": "`",
    "mariadb": "`",
    "clickhouse": "`",
    "sqlserver": "[",
    "mssql": "[",
}


# Destinations whose verifier re-reads only ``written_ids``. Anything absent
# here falls back to a whole-table digest, which an append into a populated
# sink cannot compare — the honest verdict is row-count scope, not a mismatch.
KEYED_READBACK_ENGINES: Final[frozenset[str]] = frozenset(
    {
        "sqlite",
        "duckdb",
        "postgresql",
        "redshift",
        "timescaledb",
        "cockroachdb",
        "mysql",
        "mariadb",
        "sqlserver",
        "mssql",
        "azure_sql",
        "oracle",
        "clickhouse",
        "snowflake",
        "databricks",
        "generic_sql",
        "mongodb",
        # Redis addresses every write by ``prefix:identity``, so re-reading one
        # batch's keys is exact. Without it an upsert into a keyspace holding
        # any other key compared whole-prefix digests and reported a correct
        # write as a mismatch.
        "redis",
    }
)


def keyed_readback_scope(
    written_ids: list[str] | None,
    pk_column: str | None,
    *,
    cap: int = KEYED_READBACK_ID_CAP,
) -> tuple[list[str], str]:
    """Normalize ``(written_ids, pk_column)`` into a usable batch scope.

    Returns ``([], "")`` when the batch cannot be keyed, which every caller
    treats as "fingerprint the whole table" — the honest fallback.
    """
    ids = [str(x) for x in (written_ids or []) if x is not None and str(x) != ""][:cap]
    pk = (pk_column or "").strip()
    return (ids, pk) if ids and pk else ([], "")


def keyed_readback_where(
    pk: str, ids: list[str], *, dialect: str, placeholders: list[str]
) -> str:
    """``WHERE CAST(pk AS <text>) IN (...)`` for proving one appended batch.

    Append or upsert into a non-empty table makes whole-table digests
    incomparable — the destination legitimately holds rows this job never
    wrote. Re-scoping the destination digest to the written keys is what turns
    that from a row-count-only verdict back into full-population proof of the
    batch.
    """
    dial = (dialect or "").strip().lower()
    pk_q = quote_sql_identifier(pk, _KEYED_QUOTE_CHAR.get(dial, '"'))
    cast = _KEYED_TEXT_CAST.get(dial, "VARCHAR(4000)")
    return f"WHERE CAST({pk_q} AS {cast}) IN ({','.join(placeholders[: len(ids)])})"


def keyed_readback_sa_clause(
    pk: str, ids: list[str], *, dialect: str
) -> tuple[str, dict[str, str]]:
    """SQLAlchemy flavour of :func:`keyed_readback_where` with bound keys."""
    params = {f"k{i}": v for i, v in enumerate(ids)}
    where = keyed_readback_where(
        pk, ids, dialect=dialect, placeholders=[f":{k}" for k in params]
    )
    return where, params


def verify_postgres_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    try:
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
        )
        from connectors.sql_identifiers import quote_table_ref

        table_ref = quote_table_ref(
            table_name, schema or "public", dialect="postgresql"
        )
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
            count = int(cur.fetchone()[0])
        ids, pk = keyed_readback_scope(written_ids, pk_column)
        with streaming_readback_cursor(conn, engine="postgresql") as cur:
            if ids:
                pk_q = quote_sql_identifier(pk)
                # Ids are text; an uncast bigint/uuid key aborts the read-back.
                cur.execute(
                    f"SELECT * FROM {table_ref} "  # nosec B608
                    f"WHERE CAST({pk_q} AS text) = ANY(%s)",
                    (ids,),
                )
            else:
                cur.execute(f"SELECT * FROM {table_ref}")  # nosec B608
            names, rows = dbapi_streaming_rows(cur)
            columns, projected = project_readback(names, target_columns, rows)
            checksum = canonical_checksum_from_iter(
                projected,
                columns,
                limit=limit,
                dest_db_type="postgresql",
                dest_types=dest_types,
            )
        conn.close()
        return count, checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_pgvector_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Verify pgvector destination by reading source_id/metadata, not the opaque embedding."""
    try:
        from connectors.postgresql_conn import get_connection
        from connectors.sql_identifiers import quote_table_ref

        conn = get_connection(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
        )
        table_ref = quote_table_ref(
            table_name, schema or "public", dialect="postgresql"
        )
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
            count = int(cur.fetchone()[0])
        with streaming_readback_cursor(conn, engine="postgresql") as cur:
            cur.execute(f"SELECT source_id, metadata FROM {table_ref}")  # nosec B608
            names, raw_rows = dbapi_streaming_rows(cur)
            names = names or ["source_id", "metadata"]

            def _source_shaped_rows():
                for raw in raw_rows:
                    rec = dict(zip(names, raw))
                    source_id = rec.get("source_id", "")
                    from services.target_sample_vector import load_pgvector_metadata

                    metadata = load_pgvector_metadata(rec.get("metadata"))
                    # Reconstruct a source-shaped row from metadata; fall back
                    # to source_id for 'id'.
                    row: dict[str, Any] = dict(metadata)
                    if target_columns:
                        for col in target_columns:
                            if col not in row and col.lower() == "id" and source_id:
                                row[col] = source_id
                        row = {k: v for k, v in row.items() if k in target_columns}
                    elif source_id and "id" not in {c.lower() for c in row}:
                        row["id"] = source_id
                    yield row

            checksum = canonical_checksum_from_iter(
                _source_shaped_rows(),
                columns=target_columns or None,
                limit=limit,
            )
        conn.close()
        return count, checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_pinecone_namespace(
    *,
    host: str = "",
    connection_string: str = "",
    username: str = "",
    password: str = "",
    api_key: str = "",
    namespace: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
) -> tuple[int, str]:
    """Independent Pinecone stats + metadata fetch for Gate-8 (embeddings opaque)."""
    try:
        from connectors.pinecone_writer import _headers, _index_url, _requests_session

        index_url = _index_url(host, connection_string)
        key = api_key or password or username or ""
        if not index_url or not key:
            return -1, ""
        session = _requests_session()
        hdrs = _headers(key)
        ns = (namespace or "").strip()
        stats = session.post(
            f"{index_url}/describe_index_stats",
            data=json.dumps({}),
            headers=hdrs,
            timeout=30,
        )
        if stats.status_code not in {200, 201}:
            return -1, ""
        body = stats.json() if stats.content else {}
        namespaces = body.get("namespaces") or {}
        if ns:
            count = int((namespaces.get(ns) or {}).get("vector_count") or 0)
        else:
            count = int(body.get("totalVectorCount") or body.get("total_vector_count") or 0)
            if not count and isinstance(namespaces, dict):
                count = sum(int((v or {}).get("vector_count") or 0) for v in namespaces.values())

        dict_rows: list[dict[str, Any]] = []
        # limit=0 means "full batch" for Gate-8 — still cap fetch for API safety.
        id_cap = int(limit) if limit and int(limit) > 0 else 500
        ids = [str(x) for x in (written_ids or []) if x is not None][:id_cap]
        if ids:
            params = [("ids", i) for i in ids]
            if ns:
                params.append(("namespace", ns))
            # requests supports list of tuples for repeated ids=
            fetch = session.get(
                f"{index_url}/vectors/fetch",
                params=params,
                headers=hdrs,
                timeout=30,
            )
            if fetch.status_code in {200, 201}:
                vectors = (load_http_json(fetch) or {}).get("vectors") or {}
                for vid, payload in vectors.items():
                    meta = payload.get("metadata") if isinstance(payload, dict) else {}
                    if not isinstance(meta, dict):
                        meta = {}
                    dict_rows.append({"id": vid, **meta})
        columns = target_columns or (
            sorted({k for r in dict_rows for k in r}) if dict_rows else ["id"]
        )
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="pinecone",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Pinecone reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_qdrant_collection(
    *,
    host: str = "",
    port: int = 6333,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    collection: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
) -> tuple[int, str]:
    """Independent Qdrant collection count + payload scroll/retrieve for Gate-8."""
    try:
        from connectors.qdrant_writer import _base_url, _headers, _requests_session

        api_key = password or username or ""
        base_url = connection_string.rstrip("/") if connection_string else _base_url(host, port, ssl)
        session = _requests_session()
        hdrs = _headers(api_key)
        info = session.get(f"{base_url}/collections/{collection}", headers=hdrs, timeout=15)
        if info.status_code != 200:
            return -1, ""
        body = info.json() or {}
        result = body.get("result") or body
        count = int(
            (result.get("points_count") if isinstance(result, dict) else None)
            or (result.get("indexed_vectors_count") if isinstance(result, dict) else None)
            or 0
        )
        id_cap = int(limit) if limit and int(limit) > 0 else 500
        ids = [str(x) for x in (written_ids or []) if x is not None][:id_cap]
        dict_rows: list[dict[str, Any]] = []
        if ids:
            retrieve = session.post(
                f"{base_url}/collections/{collection}/points",
                data=json.dumps({
                    "ids": ids,
                    "with_payload": True,
                    "with_vector": False,
                }),
                headers=hdrs,
                timeout=30,
            )
            if retrieve.status_code in {200, 201}:
                points = (load_http_json(retrieve) or {}).get("result") or []
                for pt in points:
                    if not isinstance(pt, dict):
                        continue
                    payload = pt.get("payload") if isinstance(pt.get("payload"), dict) else {}
                    dict_rows.append({"id": pt.get("id"), **payload})
        if not dict_rows:
            scroll = session.post(
                f"{base_url}/collections/{collection}/points/scroll",
                data=json.dumps({
                    "limit": max(1, id_cap if ids else int(limit or 100) or 100),
                    "with_payload": True,
                    "with_vector": False,
                }),
                headers=hdrs,
                timeout=30,
            )
            if scroll.status_code in {200, 201}:
                points = ((load_http_json(scroll) or {}).get("result") or {}).get("points") or []
                for pt in points:
                    if not isinstance(pt, dict):
                        continue
                    payload = pt.get("payload") if isinstance(pt.get("payload"), dict) else {}
                    dict_rows.append({"id": pt.get("id"), **payload})
        columns = target_columns or (
            sorted({k for r in dict_rows for k in r}) if dict_rows else ["id"]
        )
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="qdrant",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Qdrant reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_weaviate_class(
    *,
    host: str = "",
    port: int = 8080,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    api_key: str = "",
    class_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
) -> tuple[int, str]:
    """Independent Weaviate aggregate + object list for Gate-8."""
    try:
        from connectors.weaviate_writer import (
            _base_url,
            _class_name,
            _headers,
            _requests_session,
        )

        key = api_key or password or username or ""
        base_url = _base_url(host, port, ssl, connection_string)
        cls = _class_name(class_name)
        session = _requests_session()
        hdrs = _headers(key)
        id_cap = int(limit) if limit and int(limit) > 0 else 500
        ids = [str(x) for x in (written_ids or []) if x is not None][:id_cap]
        dict_rows: list[dict[str, Any]] = []
        if ids:
            for oid in ids:
                resp = session.get(
                    f"{base_url}/v1/objects/{cls}/{oid}",
                    headers=hdrs,
                    timeout=15,
                )
                if resp.status_code not in {200, 201}:
                    # Older Weaviate: /v1/objects/{uuid}
                    resp = session.get(
                        f"{base_url}/v1/objects/{oid}",
                        headers=hdrs,
                        timeout=15,
                    )
                if resp.status_code not in {200, 201}:
                    continue
                obj = load_http_json(resp) or {}
                if not isinstance(obj, dict):
                    continue
                props = obj.get("properties") if isinstance(obj.get("properties"), dict) else {}
                dict_rows.append({"id": obj.get("id") or oid, **props})
        if not dict_rows:
            agg = session.get(
                f"{base_url}/v1/objects",
                params={"class": cls, "limit": max(1, int(limit or 100) or 100)},
                headers=hdrs,
                timeout=30,
            )
            if agg.status_code not in {200, 201}:
                return -1, ""
            body = load_http_json(agg) or {}
            objects = body.get("objects") or []
            count = int(body.get("totalResults") or len(objects))
            for obj in objects:
                if not isinstance(obj, dict):
                    continue
                props = obj.get("properties") if isinstance(obj.get("properties"), dict) else {}
                dict_rows.append({"id": obj.get("id"), **props})
        else:
            count = len(dict_rows)
            # Prefer class cardinality when list endpoint works.
            try:
                agg = session.get(
                    f"{base_url}/v1/objects",
                    params={"class": cls, "limit": 1},
                    headers=hdrs,
                    timeout=15,
                )
                if agg.status_code in {200, 201}:
                    count = int((agg.json() or {}).get("totalResults") or count)
            except Exception:
                pass
        columns = target_columns or (
            sorted({k for r in dict_rows for k in r}) if dict_rows else ["id"]
        )
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="weaviate",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Weaviate reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_milvus_collection(
    *,
    host: str = "",
    port: int = 19530,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    api_key: str = "",
    database: str = "",
    collection: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
) -> tuple[int, str]:
    """Independent Milvus get_collection_stats + query for Gate-8."""
    try:
        from connectors.milvus_writer import (
            _auth_token,
            _base_url,
            _collection_name,
            _headers,
            _ok_response,
            _requests_session,
        )

        coll = _collection_name(collection)
        db_name = (database or "").strip()
        if db_name.lower() in {"", "test_db", "default", "public"}:
            db_name = ""
        token = _auth_token(api_key=api_key, username=username, password=password)
        base_url = _base_url(host, port, ssl, connection_string)
        session = _requests_session()
        hdrs = _headers(token)
        stats_payload: dict[str, Any] = {"collectionName": coll}
        if db_name:
            stats_payload["dbName"] = db_name
        stats = session.post(
            f"{base_url}/v2/vectordb/collections/get_stats",
            data=json.dumps(stats_payload),
            headers=hdrs,
            timeout=30,
        )
        body = stats.json() if stats.content else {}
        if not _ok_response(body if isinstance(body, dict) else {}, stats.status_code):
            return -1, ""
        data = body.get("data") if isinstance(body, dict) else {}
        count = int(
            (data or {}).get("rowCount")
            or (data or {}).get("row_count")
            or (data or {}).get("row_num")
            or 0
        )
        id_cap = int(limit) if limit and int(limit) > 0 else 500
        ids = [str(x) for x in (written_ids or []) if x is not None][:id_cap]
        query_payload: dict[str, Any] = {
            "collectionName": coll,
            "outputFields": ["id", "content", "source_id", "chunk_index"],
            "limit": max(1, id_cap if ids else int(limit or 100) or 100),
        }
        if ids:
            # Prefer keyed filter when writer stamped identities.
            quoted = ", ".join(json.dumps(i) for i in ids)
            query_payload["filter"] = f"id in [{quoted}]"
        else:
            query_payload["filter"] = ""
        if db_name:
            query_payload["dbName"] = db_name
        query = session.post(
            f"{base_url}/v2/vectordb/entities/query",
            data=json.dumps(query_payload),
            headers=hdrs,
            timeout=30,
        )
        qbody = load_http_json(query) if query.content else {}
        dict_rows: list[dict[str, Any]] = []
        if _ok_response(qbody if isinstance(qbody, dict) else {}, query.status_code):
            rows = qbody.get("data") if isinstance(qbody, dict) else []
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, dict):
                        dict_rows.append({k: v for k, v in row.items() if k != "vector"})
        columns = target_columns or (
            sorted({k for r in dict_rows for k in r}) if dict_rows else ["id"]
        )
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="milvus",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Milvus reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_mysql_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Independent MySQL/MariaDB read-back for Gate-8.

    ``written_ids`` + ``pk_column`` re-scope the digest to this batch's keys
    while ``count`` stays whole-table, so an append into a populated table can
    still prove per-cell fidelity of what it wrote.
    """
    try:
        from connectors.mysql_conn import get_connection

        conn = get_connection(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
        )
        from connectors.sql_identifiers import quote_table_ref

        table_ref = quote_table_ref(table_name, dialect="mysql")
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
            count = int(cur.fetchone()[0])
        ids, pk = keyed_readback_scope(written_ids, pk_column)
        with streaming_readback_cursor(conn, engine="mysql") as cur:
            if ids:
                where = keyed_readback_where(
                    pk, ids, dialect="mysql", placeholders=["%s"] * len(ids)
                )
                cur.execute(f"SELECT * FROM {table_ref} {where}", ids)  # nosec B608
            else:
                cur.execute(f"SELECT * FROM {table_ref}")  # nosec B608
            names, rows = dbapi_streaming_rows(cur)
            columns, projected = project_readback(names, target_columns, rows)
            checksum = canonical_checksum_from_iter(
                projected,
                columns,
                limit=limit,
                dest_db_type="mysql",
                dest_types=dest_types,
            )
        conn.close()
        return count, checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_sqlserver_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    schema: str = "dbo",
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Independent SQL Server / Azure SQL Edge read-back for Gate-8 reconcile.

    ``written_ids`` + ``pk_column`` re-scope the digest to this batch's keys.
    """
    try:
        import pymssql

        from connectors.sql_identifiers import quote_table_ref

        conn = pymssql.connect(
            server=host or "127.0.0.1",
            port=int(port or 1433),
            user=username or "sa",
            password=password or "",
            database=database or "master",
            login_timeout=10,
            timeout=30,
        )
        sch = (schema or "dbo").strip() or "dbo"
        table_ref = quote_table_ref(table_name, schema=sch, dialect="sqlserver")
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
            count = int(cur.fetchone()[0])
            ids, pk = keyed_readback_scope(written_ids, pk_column)
            if ids:
                where = keyed_readback_where(
                    pk, ids, dialect="sqlserver", placeholders=["%s"] * len(ids)
                )
                cur.execute(f"SELECT * FROM {table_ref} {where}", tuple(ids))  # nosec B608
            else:
                cur.execute(f"SELECT * FROM {table_ref}")  # nosec B608
            names = [d[0] for d in cur.description] if cur.description else []
            columns, projected = project_readback(names, target_columns, _iter_fetchmany(cur))
            checksum = canonical_checksum_from_iter(
                projected,
                columns,
                limit=limit,
                dest_db_type="sqlserver",
                dest_types=dest_types,
            )
        finally:
            cur.close()
            conn.close()
        return count, checksum
    except Exception as exc:
        logger.warning("SQL Server reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_oracle_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    schema: str = "",
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Oracle Gate-8 read-back — see :mod:`services.reconciliation_oracle`."""
    from services.reconciliation_oracle import verify_oracle_table as _verify

    return _verify(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        schema=schema,
        table_name=table_name,
        target_columns=target_columns,
        limit=limit,
        dest_types=dest_types,
        written_ids=written_ids,
        pk_column=pk_column,
    )


def verify_bigquery_table(
    *,
    project_id: str,
    dataset_id: str,
    connection_string: str,
    host: str = "",
    port: int = 0,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    try:
        from connectors.bigquery_conn import get_client

        client = get_client(
            project_id=project_id,
            credentials_path=connection_string,
            host=host,
            port=port,
        )
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        table = client.get_table(table_id)
        count = table.num_rows or 0
        field_names = [field.name for field in table.schema] if table.schema else []
        def _row_iter():
            yielded = 0
            for row in client.list_rows(table_id):
                if limit and yielded >= limit:
                    break
                yield list(row.values()) if hasattr(row, "values") else list(row)
                yielded += 1

        columns, projected = project_readback(field_names, target_columns, _row_iter())
        return int(count), canonical_checksum_from_iter(
            projected, columns, limit=limit
        )
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_s3_object(
    *,
    bucket: str,
    key: str,
    host: str,
    port: int,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Gate-8 cell checksum of S3 GET streams. Never JSON-fallback empty.

    Dest COUNT of the same keys is ``destination_row_count``. This digest
    walks records (CSV RFC 4180, JSONL objects, JSON root array, Parquet/
    Avro values) off the GET body. Gzip CSV as UTF-8 JSON garbage is not
    dest=0. XML/Excel/ORC cell walk stays unmeasured. Missing object is
    ``(0, "")``.
    """
    try:
        from services.dest_precount import checksum_object_store

        return checksum_object_store(
            "s3",
            {
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "connection_string": connection_string,
                "ssl": ssl,
                "database": bucket,
            },
            table_name=key,
            columns=target_columns,
            limit=limit,
        )
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_gcs_blob(
    *,
    bucket: str,
    key: str,
    host: str,
    port: int,
    connection_string: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Gate-8 cell checksum of GCS GET streams. Never JSON-fallback empty."""
    try:
        from services.dest_precount import checksum_object_store

        return checksum_object_store(
            "gcs",
            {
                "host": host,
                "port": port,
                "connection_string": connection_string,
                "database": bucket,
            },
            table_name=key,
            columns=target_columns,
            limit=limit,
        )
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_adls_blob(
    *,
    container: str,
    key: str,
    host: str = "",
    port: int = 0,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    service_account: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Gate-8 cell checksum of Azure Blob / ADLS GET streams. Never JSON ``[]``."""
    try:
        from services.dest_precount import checksum_object_store

        return checksum_object_store(
            "adls",
            {
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "connection_string": connection_string,
                "service_account": service_account,
                "database": container,
            },
            table_name=key,
            columns=target_columns,
            limit=limit,
            dest_db_type="adls",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("ADLS reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_hubspot_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent HubSpot CRM read-back for Gate-8 (reverse-ETL honesty)."""
    try:
        from connectors.hubspot import read_object

        cfg = {
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "table": object_name,
            "database": object_name,
        }
        batch = read_object(
            cfg=cfg,
            object=object_name,
            limit=max(int(limit or 500), 1),
        )
        headers = list(batch.headers or target_columns or [])
        rows = list(batch.rows or [])
        # Convert matrix rows to dicts when ReadBatch stores tuples.
        dict_rows: list[Any] = []
        for row in rows:
            if isinstance(row, dict):
                dict_rows.append(row)
            elif headers:
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
            else:
                dict_rows.append(row)
        columns = headers or target_columns or (
            sorted({k for r in dict_rows if isinstance(r, dict) for k in r}) if dict_rows else []
        )
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="hubspot",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("HubSpot reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_salesforce_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Salesforce SOQL read-back for Gate-8 reverse-ETL."""
    try:
        from connectors.salesforce import read_object

        cfg = {
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": object_name,
        }
        batch = read_object(
            cfg=cfg,
            object=object_name,
            limit=max(int(limit or 500), 1),
        )
        headers = list(batch.headers or target_columns or [])
        dict_rows: list[Any] = []
        for row in batch.rows or []:
            if isinstance(row, dict):
                # Strip Salesforce attributes envelope when present.
                d = {k: v for k, v in row.items() if k != "attributes"}
                dict_rows.append(d)
            elif headers:
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
        columns = headers or target_columns or (
            sorted({k for r in dict_rows if isinstance(r, dict) for k in r}) if dict_rows else []
        )
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="salesforce",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Salesforce reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_airtable_table(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    base_id: str,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Airtable read-back — flatten fields for Gate-8 fingerprints."""
    try:
        from connectors.airtable import read_object
        from connectors.saas_typed_schema import flatten_airtable_record

        cfg = {
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string or base_id,
            "api_key": api_key,
            "database": base_id,
            "table": table_name,
            "type": "airtable",
        }
        batch = read_object(
            cfg=cfg,
            object=table_name,
            limit=max(int(limit or 500), 1),
        )
        dict_rows: list[Any] = []
        for row in batch.rows or []:
            if isinstance(row, dict):
                flat, _schema = flatten_airtable_record(row)
                dict_rows.append(flat)
            elif batch.headers:
                headers = list(batch.headers)
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
        columns = target_columns or (
            sorted({k for r in dict_rows if isinstance(r, dict) for k in r}) if dict_rows else []
        )
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="airtable",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Airtable reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def _verify_saas_read_object(
    *,
    driver: str,
    read_object: Any,
    cfg: dict[str, Any],
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    row_transform: Any = None,
) -> tuple[int, str]:
    """Shared CRM/commerce Gate-8 path: call connector read_object → checksum."""
    try:
        batch = read_object(
            cfg=cfg,
            object=object_name,
            limit=max(int(limit or 500), 1),
        )
        headers = list(batch.headers or target_columns or [])
        dict_rows: list[Any] = []
        for row in batch.rows or []:
            if isinstance(row, dict):
                d = row_transform(row) if row_transform else dict(row)
                dict_rows.append(d)
            elif headers:
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
        columns = headers or target_columns or (
            sorted({k for r in dict_rows if isinstance(r, dict) for k in r}) if dict_rows else []
        )
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type=driver,
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("%s reconciliation read-back failed: %s", driver, exc, exc_info=exc)
        return -1, ""


def verify_stripe_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Stripe API read-back for Gate-8 reverse-ETL."""
    from connectors.stripe import read_object

    return _verify_saas_read_object(
        driver="stripe",
        read_object=read_object,
        cfg={
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": object_name,
            "type": "stripe",
        },
        object_name=object_name or "customers",
        target_columns=target_columns,
        limit=limit,
        dest_types=dest_types,
    )


def verify_shopify_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Shopify Admin REST read-back for Gate-8 reverse-ETL."""
    from connectors.shopify import read_object

    return _verify_saas_read_object(
        driver="shopify",
        read_object=read_object,
        cfg={
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": object_name,
            "shop": host,
            "type": "shopify",
        },
        object_name=object_name or "customers",
        target_columns=target_columns,
        limit=limit,
        dest_types=dest_types,
    )


def verify_zendesk_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Zendesk Support API read-back for Gate-8 reverse-ETL."""
    from connectors.zendesk import read_object

    return _verify_saas_read_object(
        driver="zendesk",
        read_object=read_object,
        cfg={
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": object_name,
            "type": "zendesk",
        },
        object_name=object_name or "tickets",
        target_columns=target_columns,
        limit=limit,
        dest_types=dest_types,
    )


def verify_notion_object(
    *,
    host: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    api_key: str = "",
    object_name: str,
    database_id: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Notion database/page read-back for Gate-8 reverse-ETL."""
    from connectors.notion import read_object

    return _verify_saas_read_object(
        driver="notion",
        read_object=read_object,
        cfg={
            "host": host,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "api_key": api_key,
            "table": object_name,
            "database": database_id or object_name,
            "type": "notion",
        },
        object_name=object_name or database_id or "",
        target_columns=target_columns,
        limit=limit,
        dest_types=dest_types,
    )


def verify_kafka_topic(
    *,
    host: str = "",
    port: int = 0,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    topic: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    schema_registry_url: str = "",
) -> tuple[int, str]:
    """Bounded Kafka consume for Gate-8 — sample proof, not topic cardinality."""
    try:
        from connectors.kafka_reader import read_topic_batch

        cfg = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "database": topic,
            "table": topic,
            "group_id": f"dataflow-gate8-verify-{abs(hash(topic)) % 10_000_000}",
            "auto_offset_reset": "earliest",
            "schema_registry_url": schema_registry_url,
        }
        batch, _cursor = read_topic_batch(
            cfg=cfg,
            topic=topic,
            columns=target_columns,
            limit=max(int(limit or 500), 1),
        )
        headers = list(batch.headers or target_columns or [])
        rows = list(batch.rows or [])
        dict_rows: list[Any] = []
        for row in rows:
            if isinstance(row, dict):
                dict_rows.append(row)
            elif headers:
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
        columns = headers or target_columns or []
        return len(dict_rows), canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="kafka",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Kafka reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_databricks_table(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    schema: str = "",
    table_name: str,
    http_path: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Independent Databricks SQL warehouse read-back for Gate-8.

    ``written_ids`` + ``pk_column`` re-scope the digest to this batch's keys.
    """
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import get_sqlalchemy_engine
        from connectors.sql_identifiers import quote_table_ref

        cfg: dict[str, Any] = {
            "type": "databricks",
            "host": host or "",
            "port": int(port or 443),
            "database": database or "",
            "username": username or "",
            "password": password or "",
            "connection_string": connection_string or "",
            "schema": schema or "",
            "http_path": http_path or "",
        }
        engine = get_sqlalchemy_engine(cfg)
        sch = (schema or database or "").strip() or None
        table_ref = quote_table_ref(table_name, schema=sch, dialect="ansi")
        with engine.connect() as conn:
            count = int(
                conn.execute(sa.text(f"SELECT COUNT(*) FROM {table_ref}")).scalar()  # nosec B608
                or 0
            )
            ids, pk = keyed_readback_scope(written_ids, pk_column)
            select = sa.text(f"SELECT * FROM {table_ref}")  # nosec B608
            if ids:
                where, params = keyed_readback_sa_clause(pk, ids, dialect="databricks")
                select = sa.text(
                    f"SELECT * FROM {table_ref} {where}"  # nosec B608
                ).bindparams(**params)
            names, result = sa_streaming_result(conn, select)
            columns, projected = project_readback(names, target_columns, (tuple(row) for row in result))
            checksum = canonical_checksum_from_iter(
                projected,
                columns,
                limit=limit,
                dest_db_type="databricks",
                dest_types=dest_types,
            )
        return count, checksum
    except Exception as exc:
        logger.warning("Databricks reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_sqlite_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
    host: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Reconcile a SQLite target by reading the local file.

    When ``written_ids`` + ``pk_column`` are set (upsert/append batch proof),
    the checksum fingerprints only those keys while ``count`` remains the
    full-table cardinality for operator visibility.
    """
    try:
        import sqlite3

        from connectors.sqlite_common import sqlite_file_path

        path = sqlite_file_path(database, connection_string, host)
        if not path:
            return -1, ""
        from connectors.sql_identifiers import quote_table_ref

        table_ref = quote_table_ref(table_name, dialect="sqlite")
        conn = sqlite3.connect(str(path))
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
        count = cur.fetchone()[0]
        ids, pk = keyed_readback_scope(written_ids, pk_column)
        if ids:
            pk_q = quote_sql_identifier(pk)
            placeholders = ",".join("?" for _ in ids)
            cur.execute(
                f"SELECT * FROM {table_ref} WHERE {pk_q} IN ({placeholders})",  # nosec B608
                ids,
            )
        else:
            cur.execute(f"SELECT * FROM {table_ref}")  # nosec B608
        names = [d[0] for d in cur.description] if cur.description else []
        columns, projected = project_readback(names, target_columns, _iter_fetchmany(cur))
        checksum = canonical_checksum_from_iter(projected, columns, limit=limit)
        conn.close()
        return int(count), checksum
    except sqlite3.OperationalError as exc:
        # Missing table means the target is empty, not that verification is
        # unavailable. Return 0 so reconciliation can surface the mismatch.
        if "no such table" in str(exc).lower():
            return 0, ""
        return -1, ""
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_duckdb_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Reconcile a DuckDB target by reading the local file.

    ``written_ids`` + ``pk_column`` re-scope the digest to this batch's keys.
    """
    try:
        import duckdb

        path = connection_string or database
        if not path:
            return -1, ""
        from connectors.sql_identifiers import quote_table_ref

        table_ref = quote_table_ref(table_name, dialect="duckdb")
        conn = duckdb.connect(str(path))
        count = conn.execute(f"SELECT COUNT(*) FROM {table_ref}").fetchone()[0]  # nosec B608
        ids, pk = keyed_readback_scope(written_ids, pk_column)
        if ids:
            where = keyed_readback_where(
                pk, ids, dialect="duckdb", placeholders=["?"] * len(ids)
            )
            cur = conn.execute(f"SELECT * FROM {table_ref} {where}", ids)  # nosec B608
        else:
            cur = conn.execute(f"SELECT * FROM {table_ref}")  # nosec B608
        names = [d[0] for d in cur.description] if cur.description else []
        columns, projected = project_readback(names, target_columns, _iter_fetchmany(cur))
        checksum = canonical_checksum_from_iter(projected, columns, limit=limit)
        conn.close()
        return int(count), checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_clickhouse_table(
    *,
    host: str = "",
    port: int = 9000,
    database: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    schema: str = "",
    ssl: bool = False,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Independent ClickHouse read-back with ``SELECT … FINAL`` (Airbyte-class).

    ReplacingMergeTree upsert is lazy — Gate-8 without FINAL would fingerprint
    duplicate keys and false-fail checksum reconcile.
    """
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import (
            clickhouse_final_table_sql,
            get_sqlalchemy_engine,
        )
        from connectors.sql_identifiers import quote_table_ref

        cfg: dict[str, Any] = {
            "type": "clickhouse",
            "host": host or "",
            "port": int(port or 9000),
            "database": database or "",
            "username": username or "",
            "password": password or "",
            "connection_string": connection_string or "",
            "schema": schema or "",
            "ssl": bool(ssl),
        }
        engine = get_sqlalchemy_engine(cfg)
        table_ref = quote_table_ref(
            table_name,
            schema=schema or None,
            dialect="clickhouse",
        )
        from_sql = clickhouse_final_table_sql(table_ref)
        with engine.connect() as conn:
            count = int(
                conn.execute(
                    sa.text(f"SELECT count() FROM {from_sql}")  # nosec B608
                ).scalar()
                or 0
            )
            ids, pk = keyed_readback_scope(written_ids, pk_column)
            select = sa.text(f"SELECT * FROM {from_sql}")  # nosec B608
            if ids:
                where, params = keyed_readback_sa_clause(pk, ids, dialect="clickhouse")
                select = sa.text(
                    f"SELECT * FROM {from_sql} {where}"  # nosec B608
                ).bindparams(**params)
            result = conn.execute(select)
            names = list(result.keys())
            columns, projected = project_readback(names, target_columns, (tuple(row) for row in result))
            checksum = canonical_checksum_from_iter(
                projected,
                columns,
                limit=limit,
                dest_db_type="clickhouse",
                dest_types=dest_types,
            )
        return count, checksum
    except Exception as exc:
        logger.warning("ClickHouse reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_generic_sql_table(
    *,
    dest: dict[str, Any],
    schema: str = "",
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    engine_hint: str = "",
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Independent SQLAlchemy read-back for catalog ``generic_sql`` engines.

    Covers Teradata / HANA / Vertica / Informix / Trino / Firebird / Netezza and
    other SQLAlchemy-backed SKUs that share ``get_sqlalchemy_engine``. Returns
    ``(-1, "")`` only when the engine cannot connect or the table is missing —
    never invent a checksum from writer rowcounts (Gate-8 honesty).
    """
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import get_sqlalchemy_engine
        from connectors.sql_identifiers import quote_table_ref

        hint = (
            (engine_hint or dest.get("type") or dest.get("engine") or "generic_sql")
            .strip()
            .lower()
        )
        cfg = dict(dest)
        cfg["type"] = hint if hint and hint != "generic_sql" else (
            str(dest.get("type") or "generic_sql")
        )
        engine = get_sqlalchemy_engine(cfg)
        dialect = (
            getattr(getattr(engine, "dialect", None), "name", None) or hint or "ansi"
        ).lower()
        sch = (schema or dest.get("schema") or "").strip() or None
        table_ref = quote_table_ref(table_name, schema=sch, dialect=dialect)
        with engine.connect() as conn:
            count = int(
                conn.execute(
                    sa.text(f"SELECT COUNT(*) FROM {table_ref}")  # nosec B608
                ).scalar()
                or 0
            )
            ids, pk = keyed_readback_scope(written_ids, pk_column)
            select = sa.text(f"SELECT * FROM {table_ref}")  # nosec B608
            if ids:
                where, params = keyed_readback_sa_clause(pk, ids, dialect=dialect)
                select = sa.text(
                    f"SELECT * FROM {table_ref} {where}"  # nosec B608
                ).bindparams(**params)
            names, result = sa_streaming_result(conn, select)
            columns, projected = project_readback(names, target_columns, (tuple(row) for row in result))
            checksum = canonical_checksum_from_iter(
                projected,
                columns,
                limit=limit,
                dest_db_type=hint or dialect,
                dest_types=dest_types,
            )
        return count, checksum
    except Exception as exc:
        logger.warning(
            "generic_sql reconciliation read-back failed (%s): %s",
            engine_hint or dest.get("type"),
            exc,
            exc_info=exc,
        )
        return -1, ""


def verify_iceberg_table(
    *,
    connection_string: str = "",
    warehouse: str = "",
    table_name: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
) -> tuple[int, str]:
    """Reconcile Iceberg from current-snapshot data files, not ``scan().to_arrow()``.

    Dest COUNT is dest-engine file footers (same leftover MERGE listing).
    Catalog ``SqlCatalog`` / ``scan().count()`` never close filesystem tables
    and never close leftover identity. Unreadable snapshot is unmeasured.
    """
    try:
        from services.dest_precount import destination_row_count, iceberg_target_sample

        cfg = {
            "connection_string": connection_string or warehouse or "",
            "database": warehouse or connection_string or "",
            "warehouse": warehouse or "",
            "host": "",
            "schema": "",
        }
        count = destination_row_count(
            "iceberg", cfg, schema="", table_name=table_name
        )
        if count is None:
            return -1, ""
        cols = [str(c) for c in (target_columns or []) if str(c).strip()]
        if not cols:
            return int(count), ""
        rows = iceberg_target_sample(
            cfg,
            schema="",
            table_name=table_name,
            columns=cols,
            limit=int(limit or 0) or None,
        )
        if rows is None:
            return int(count), ""
        checksum = fingerprint_checksum(_iter_fingerprints(rows, cols))
        return int(count), checksum
    except Exception as exc:
        logger.warning("Iceberg reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_mongodb_collection(
    *,
    host: str = "",
    port: int = 27017,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    database: str = "",
    ssl: bool = False,
    auth_source: str = "",
    table_name: str = "",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Reconcile a MongoDB target by counting and fingerprinting documents.

    When ``written_ids`` + ``pk_column`` are set, the checksum fingerprints only
    those keys while ``count`` remains full-collection cardinality (upsert Gate-8).
    """
    try:
        from connectors.mongodb_common import (
            _mongo_client,
            normalize_mongodb_connection_string,
        )

        conn_str = normalize_mongodb_connection_string(
            connection_string or "",
            database=database,
            host=host,
            port=port,
            username=username,
            password=password,
            ssl=ssl,
            auth_source=auth_source,
        )
        client = _mongo_client(conn_str)
        db = client[database or "test"]
        coll = db[table_name]
        count = coll.count_documents({})
        ids, pk = keyed_readback_scope(written_ids, pk_column)
        query: dict[str, Any] = {}
        if ids:
            # Match string or numeric id forms (CSV upserts often land as int).
            expanded: list[Any] = []
            for raw in ids:
                expanded.append(raw)
                try:
                    if str(int(raw)) == raw:
                        expanded.append(int(raw))
                except (TypeError, ValueError):
                    pass
            query = {pk: {"$in": expanded}}

        def _doc_iter():
            yielded = 0
            for doc in coll.find(query):
                if limit and yielded >= limit:
                    break
                yield doc
                yielded += 1

        columns = target_columns or sorted(
            set(k for doc in coll.find(query).limit(100) for k in doc.keys())
        )
        checksum = canonical_checksum_from_iter(
            _doc_iter(),
            columns,
            limit=limit,
            dest_db_type="mongodb",
            dest_types=dest_types,
        )
        return int(count), checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_redis_prefix(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    prefix: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Reconcile Redis keys under ``prefix:*`` (writer key layout).

    ``written_ids`` scopes the digest to the keys this batch wrote, the same way
    the SQL and warehouse read-backs do. An upsert into a keyspace that already
    holds other keys is otherwise incomparable: the whole-prefix digest includes
    rows the source never sent. Cardinality stays whole-prefix either way.
    """
    try:
        from connectors.redis_reader import _redis_client, redis_json_row

        client = _redis_client(
            {
                "host": host,
                "port": port,
                "database": database,
                "username": username,
                "password": password,
                "connection_string": connection_string,
                "ssl": ssl,
            }
        )
        pattern = f"{prefix}:*" if prefix else "*"
        keys: list[str] = []
        cursor = 0
        while True:
            cursor, batch = client.scan(cursor=cursor, match=pattern, count=500)
            for raw in batch:
                keys.append(raw.decode() if isinstance(raw, bytes) else str(raw))
            if cursor == 0:
                break

        total = len(keys)
        scoped_ids, _pk = keyed_readback_scope(written_ids, pk_column)
        if scoped_ids:
            from connectors.redis_reader import redis_key_for

            wanted = {redis_key_for(prefix, i) for i in scoped_ids}
            keys = [k for k in keys if k in wanted]

        def _row_iter():
            for key in keys:
                yield redis_json_row(client.get(key))

        columns = target_columns or []
        if not columns and keys:
            sample = next(_row_iter(), {})
            columns = sorted(sample.keys()) if sample else ["value"]
        checksum = canonical_checksum_from_iter(_row_iter(), columns, limit=limit)
        return total, checksum
    except Exception as exc:
        logger.warning("Redis reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_elasticsearch_index(
    *,
    host: str = "",
    port: int = 9200,
    username: str = "",
    password: str = "",
    connection_string: str = "",
    ssl: bool = False,
    api_key: str = "",
    index: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Independent Elasticsearch search read-back for Gate-8."""
    try:
        from connectors.elasticsearch_reader import read_index_batch

        cfg = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "connection_string": connection_string,
            "ssl": ssl,
            "api_key": api_key,
        }
        batch, _ = read_index_batch(
            cfg=cfg,
            index=index,
            columns=target_columns,
            limit=max(int(limit or 500), 1),
        )
        headers = list(batch.headers or target_columns or [])
        dict_rows: list[Any] = []
        for row in batch.rows or []:
            if isinstance(row, dict):
                dict_rows.append(row)
            elif headers:
                dict_rows.append(
                    {headers[i]: row[i] if i < len(row) else None for i in range(len(headers))}
                )
        columns = headers or target_columns or (
            sorted({k for r in dict_rows if isinstance(r, dict) for k in r}) if dict_rows else []
        )
        # Prefer index cardinality when available (batch may be LIMIT-capped).
        count = len(dict_rows)
        try:
            from connectors.elasticsearch_reader import _client

            es = _client(cfg)
            try:
                count = int(es.count(index=index).get("count", count))
            finally:
                es.close()
        except Exception:
            pass
        return count, canonical_checksum_from_iter(
            dict_rows,
            columns,
            limit=limit,
            dest_db_type="elasticsearch",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Elasticsearch reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_snowflake_table(
    *,
    host: str,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    warehouse: str,
    table_name: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Independent Snowflake read-back for Gate-8.

    ``written_ids`` + ``pk_column`` re-scope the digest to this batch's keys.
    """
    try:
        from connectors.snowflake_conn import (
            get_connection,
            normalize_account,
            resolve_or_fold_snowflake_table,
            snowflake_qualified_table,
        )

        conn = get_connection(
            account=normalize_account(host),
            username=username,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            connection_string=connection_string,
        )
        from connectors.sql_identifiers import (
            quote_sql_identifier,
            require_safe_identifier,
        )

        with conn.cursor() as cur:
            if warehouse:
                try:
                    wh = require_safe_identifier(warehouse, preserve_case=True)
                    cur.execute(f"USE WAREHOUSE {quote_sql_identifier(wh)}")
                except Exception as exc:
                    logging.getLogger(__name__).warning(
                        "Exception suppressed: %s", exc, exc_info=exc
                    )
            resolved = resolve_or_fold_snowflake_table(
                cur, schema or "PUBLIC", table_name
            )
            qualified_name = snowflake_qualified_table(schema or "PUBLIC", resolved)
            cur.execute(f"SELECT COUNT(*) FROM {qualified_name}")  # nosec B608
            count = int(cur.fetchone()[0])
            ids, pk = keyed_readback_scope(written_ids, pk_column)
            from connectors.sql_identifiers import quote_column_list

            select_list = quote_column_list(target_columns)
            if ids:
                where = keyed_readback_where(
                    pk, ids, dialect="snowflake", placeholders=["%s"] * len(ids)
                )
                cur.execute(
                    f"SELECT {select_list} FROM {qualified_name} {where}", ids
                )  # nosec B608
            else:
                cur.execute(f"SELECT {select_list} FROM {qualified_name}")  # nosec B608
            names = [d[0] for d in cur.description] if cur.description else []
            columns, projected = project_readback(names, target_columns, _iter_fetchmany(cur))
            checksum = canonical_checksum_from_iter(
                projected,
                columns,
                limit=limit,
                dest_db_type="snowflake",
                dest_types=dest_types,
            )
        conn.close()
        return count, checksum
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_dynamodb_table(
    *,
    connection_string: str,
    database: str,
    table_name: str,
    username: str = "local",
    password: str = "local",
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Reconcile a DynamoDB target by Scan count and item fingerprint."""
    try:
        import boto3
        from boto3.dynamodb.types import TypeDeserializer

        endpoint = connection_string or "http://localhost:8000"
        client = boto3.client(
            "dynamodb",
            endpoint_url=endpoint,
            aws_access_key_id=username.strip() or "local",
            aws_secret_access_key=password.strip() or "local",
            region_name="us-east-1",
        )
        paginator = client.get_paginator("scan")
        count = sum(
            page["Count"]
            for page in paginator.paginate(TableName=table_name, Select="COUNT")
        )

        deserializer = TypeDeserializer()

        def _item_iter():
            yielded = 0
            for page in paginator.paginate(TableName=table_name):
                for item in page.get("Items", []):
                    if limit and yielded >= limit:
                        break
                    yield {k: deserializer.deserialize(v) for k, v in item.items()}
                    yielded += 1

        columns = target_columns or []
        return int(count), canonical_checksum_from_iter(
            _item_iter(),
            columns,
            limit=limit,
            dest_db_type="dynamodb",
            dest_types=dest_types,
        )
    except Exception as exc:
        logger.warning("Reconciliation read-back failed: %s", exc, exc_info=exc)
        return -1, ""


def verify_target(
    db_type: str,
    dest: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    fallback_rows: int,
    fallback_checksum: str,
    target_columns: list[str] | None = None,
    limit: int = 0,
    dest_types: dict[str, str] | None = None,
    written_ids: list[str] | None = None,
    pk_column: str | None = None,
) -> tuple[int, str]:
    """Independent destination read-back for Gate-8.

    ``written_ids`` (from writer meta) enables keyed fetch for vector / SaaS
    destinations where full-table scan is unavailable — Fivetran HVR Compare
    class: prove the batch we wrote, not an opaque index count alone.

    For SQL upsert/append, ``written_ids`` + ``pk_column`` fingerprint only the
    batch keys while returning full-table cardinality.
    """
    # Prefer explicit arg; allow dest cfg stash from reconcile_step.
    ids = written_ids
    if ids is None and isinstance(dest.get("written_ids"), list):
        ids = [str(x) for x in dest["written_ids"] if x is not None]
    pk = (pk_column or dest.get("pk_column") or dest.get("gate8_pk_column") or "")
    pk = str(pk).strip() or None

    if db_type == "iceberg":
        count, chk = verify_iceberg_table(
            connection_string=dest.get("connection_string", ""),
            warehouse=dest.get("warehouse", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "mongodb":
        count, chk = verify_mongodb_collection(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 27017),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", ""),
            ssl=bool(dest.get("ssl", False)),
            auth_source=dest.get("auth_source", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
            pk_column=pk,
        )
    elif db_type == "dynamodb":
        from connectors.aws_common import resolve_endpoint_url

        conn_str = resolve_endpoint_url(dest) or "http://localhost:8000"
        count, chk = verify_dynamodb_table(
            connection_string=conn_str,
            database=dest.get("database", ""),
            table_name=table_name,
            username=dest.get("username", "local"),
            password=dest.get("password", "local"),
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "sqlite":
        count, chk = verify_sqlite_table(
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", ""),
            host=dest.get("host", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            written_ids=ids,
            pk_column=pk,
        )
    elif db_type == "duckdb":
        count, chk = verify_duckdb_table(
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            written_ids=ids,
            pk_column=pk,
        )
    elif db_type == "generic_sql":
        # Route local file engines / Oracle / ClickHouse by URL or dest.type.
        # Catalog maps clickhouse→generic_sql driver; endpoint.format stays in type.
        engine_hint = str(dest.get("type") or dest.get("engine") or "").lower()
        conn = (dest.get("connection_string") or dest.get("database") or "").lower()
        if "sqlite" in conn or conn.endswith(".db") or conn.endswith(".sqlite"):
            count, chk = verify_sqlite_table(
                connection_string=dest.get("connection_string", ""),
                database=dest.get("database", ""),
                host=dest.get("host", ""),
                table_name=table_name,
                target_columns=target_columns,
                limit=limit,
                written_ids=ids,
                pk_column=pk,
            )
        elif "duckdb" in conn or conn.endswith(".duckdb") or conn.endswith(".duck"):
            count, chk = verify_duckdb_table(
                connection_string=dest.get("connection_string", ""),
                database=dest.get("database", ""),
                table_name=table_name,
                target_columns=target_columns,
                limit=limit,
                written_ids=ids,
                pk_column=pk,
            )
        elif conn.startswith("oracle") or engine_hint.startswith("oracle"):
            count, chk = verify_oracle_table(
                host=dest.get("host", ""),
                port=int(dest.get("port") or 1521),
                database=dest.get("database", ""),
                username=dest.get("username", ""),
                password=dest.get("password", ""),
                connection_string=dest.get("connection_string", ""),
                schema=schema or dest.get("schema") or "",
                table_name=table_name,
                target_columns=target_columns,
                limit=limit,
                dest_types=dest_types,
                written_ids=ids,
                pk_column=pk,
            )
        elif "clickhouse" in engine_hint or "clickhouse" in conn:
            count, chk = verify_clickhouse_table(
                host=dest.get("host", ""),
                port=int(dest.get("port") or 9000),
                database=dest.get("database", ""),
                username=dest.get("username", ""),
                password=dest.get("password", ""),
                connection_string=dest.get("connection_string", ""),
                schema=schema or dest.get("schema") or "",
                ssl=bool(dest.get("ssl", False)),
                table_name=table_name,
                target_columns=target_columns,
                limit=limit,
                dest_types=dest_types,
                written_ids=ids,
                pk_column=pk,
            )
        else:
            # Teradata / HANA / Vertica / Informix / Trino / Firebird / …
            # — independent SQLAlchemy read-back (never invent from writer counts).
            count, chk = verify_generic_sql_table(
                dest=dest,
                schema=schema or dest.get("schema") or "",
                table_name=table_name,
                target_columns=target_columns,
                limit=limit,
                dest_types=dest_types,
                engine_hint=engine_hint,
                written_ids=ids,
                pk_column=pk,
            )
    elif db_type == "clickhouse":
        count, chk = verify_clickhouse_table(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 9000),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            schema=schema or dest.get("schema") or "",
            ssl=bool(dest.get("ssl", False)),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
            pk_column=pk,
        )
    elif db_type == "pgvector":
        # pgvector tables store an opaque embedding; reconstruct source rows from
        # the JSON metadata and source_id for an honest checksum reconciliation.
        count, chk = verify_pgvector_table(
            host=dest.get("host", ""),
            port=dest.get("port", 5432),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            schema=schema,
            connection_string=dest.get("connection_string", ""),
            ssl=dest.get("ssl", False),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type in ("postgresql", "redshift"):
        # Redshift wire protocol is Postgres; local CI uses the PG emulator.
        count, chk = verify_postgres_table(
            host=dest.get("host", ""),
            port=dest.get("port", 5432),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            schema=schema,
            connection_string=dest.get("connection_string", ""),
            ssl=dest.get("ssl", False)
            if db_type == "postgresql"
            else dest.get("ssl", False),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
            pk_column=pk,
        )
    elif db_type == "redis":
        count, chk = verify_redis_prefix(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 6379),
            database=dest.get("database", "0"),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=bool(dest.get("ssl", False)),
            prefix=table_name,
            target_columns=target_columns,
            limit=limit,
            written_ids=ids,
            pk_column=pk,
        )
    elif db_type in {"elasticsearch", "opensearch", "elastic"}:
        count, chk = verify_elasticsearch_index(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 9200),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=bool(dest.get("ssl", False)),
            api_key=str(dest.get("api_key") or dest.get("service_account") or ""),
            index=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "snowflake":
        count, chk = verify_snowflake_table(
            host=dest.get("host", ""),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            schema=schema,
            connection_string=dest.get("connection_string", ""),
            warehouse=dest.get("warehouse", ""),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
            pk_column=pk,
        )
    elif db_type == "mysql":
        count, chk = verify_mysql_table(
            host=dest.get("host", ""),
            port=int(dest.get("port", 3306)),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=dest.get("ssl", False),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
            pk_column=pk,
        )
    elif db_type in {"sqlserver", "mssql", "azure_sql"}:
        count, chk = verify_sqlserver_table(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 1433),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            schema=schema or dest.get("schema") or "dbo",
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
            pk_column=pk,
        )
    elif db_type in {
        "oracle",
        "oracledb",
        "oracle_db",
        "oracle_autonomous",
        "oracle_autonomous_warehouse",
        "amazon_rds_oracle",
    }:
        count, chk = verify_oracle_table(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 1521),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            schema=schema or dest.get("schema") or "",
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
            pk_column=pk,
        )
    elif db_type == "bigquery":
        count, chk = verify_bigquery_table(
            project_id=dest.get("database", ""),
            dataset_id=schema,
            connection_string=dest.get("connection_string", ""),
            host=dest.get("host", ""),
            port=int(dest.get("port", 0) or 0),
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "s3":
        count, chk = verify_s3_object(
            bucket=dest.get("database", ""),
            key=table_name,
            host=dest.get("host", ""),
            port=int(dest.get("port", 0)),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=bool(dest.get("ssl", False)),
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type == "gcs":
        count, chk = verify_gcs_blob(
            bucket=dest.get("database", ""),
            key=table_name,
            host=dest.get("host", ""),
            port=int(dest.get("port", 0)),
            connection_string=dest.get("connection_string", ""),
            target_columns=target_columns,
            limit=limit,
        )
    elif db_type in {
        "adls",
        "azure_blob_storage",
        "azure_data_lake",
        "azure_data_lake_storage",
    }:
        count, chk = verify_adls_blob(
            container=dest.get("database", "") or schema or "",
            key=table_name,
            host=dest.get("host", ""),
            port=int(dest.get("port") or 0),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            service_account=dest.get("service_account", ""),
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "sftp":
        from connectors.sftp_common import host_key_settings

        count, chk = verify_sftp_object(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 22),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            database=dest.get("database", "") or schema or "",
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            **host_key_settings(dest),
        )
    elif db_type in {
        "databricks",
        "databricks_sql",
        "delta",
        "delta_lake",
        "unity_catalog",
        "spark",
    }:
        count, chk = verify_databricks_table(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 443),
            database=dest.get("database", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            schema=schema or dest.get("schema") or "",
            table_name=table_name,
            http_path=str(dest.get("http_path") or dest.get("warehouse") or ""),
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
            pk_column=pk,
        )
    elif db_type == "hubspot":
        count, chk = verify_hubspot_object(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            object_name=table_name or dest.get("table") or "contacts",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "salesforce":
        count, chk = verify_salesforce_object(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            api_key=str(dest.get("api_key") or ""),
            object_name=table_name or dest.get("table") or "Account",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "airtable":
        count, chk = verify_airtable_table(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            api_key=str(dest.get("api_key") or ""),
            base_id=str(dest.get("database") or schema or ""),
            table_name=table_name or dest.get("table") or "",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "stripe":
        count, chk = verify_stripe_object(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            api_key=str(dest.get("api_key") or ""),
            object_name=table_name or dest.get("table") or "customers",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "shopify":
        count, chk = verify_shopify_object(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            api_key=str(dest.get("api_key") or ""),
            object_name=table_name or dest.get("table") or "customers",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "zendesk":
        count, chk = verify_zendesk_object(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            api_key=str(dest.get("api_key") or ""),
            object_name=table_name or dest.get("table") or "tickets",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "notion":
        count, chk = verify_notion_object(
            host=dest.get("host", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            api_key=str(dest.get("api_key") or ""),
            object_name=table_name or dest.get("table") or "",
            database_id=str(dest.get("database") or schema or ""),
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
        )
    elif db_type == "kafka":
        count, chk = verify_kafka_topic(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 9092),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            topic=table_name or dest.get("table") or dest.get("database") or "",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            schema_registry_url=str(
                dest.get("schema_registry_url") or dest.get("registry_url") or ""
            ),
        )
    elif db_type == "pinecone":
        count, chk = verify_pinecone_namespace(
            host=dest.get("host", ""),
            connection_string=dest.get("connection_string", ""),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            api_key=str(dest.get("api_key") or ""),
            namespace=table_name or dest.get("schema") or "",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
        )
    elif db_type == "qdrant":
        count, chk = verify_qdrant_collection(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 6333),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=bool(dest.get("ssl", False)),
            collection=table_name or dest.get("database") or "dataflow_vectors",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
        )
    elif db_type == "weaviate":
        count, chk = verify_weaviate_class(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 8080),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=bool(dest.get("ssl", False)),
            api_key=str(dest.get("api_key") or ""),
            class_name=table_name or dest.get("database") or "DataflowChunk",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
        )
    elif db_type == "milvus":
        count, chk = verify_milvus_collection(
            host=dest.get("host", ""),
            port=int(dest.get("port") or 19530),
            username=dest.get("username", ""),
            password=dest.get("password", ""),
            connection_string=dest.get("connection_string", ""),
            ssl=bool(dest.get("ssl", False)),
            api_key=str(dest.get("api_key") or ""),
            database=dest.get("database", "") or schema or "",
            collection=table_name or "dataflow_chunks",
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            written_ids=ids,
        )
    elif db_type in {
        "teradata",
        "teradata_vantage",
        "vertica",
        "hana",
        "sap_hana",
        "sap_bw_4hana",
        "informix",
        "firebird",
        "netezza",
        "trino",
        "presto",
        "athena",
        "amazon_athena",
        "hive",
        "impala",
        "sybase",
        "sybase_ase",
        "sap_ase",
        "sap_iq",
    }:
        # Catalog id may arrive before resolve_driver_type → generic_sql.
        count, chk = verify_generic_sql_table(
            dest=dest,
            schema=schema or dest.get("schema") or "",
            table_name=table_name,
            target_columns=target_columns,
            limit=limit,
            dest_types=dest_types,
            engine_hint=db_type,
            written_ids=ids,
            pk_column=pk,
        )
    else:
        count, chk = -1, ""

    if count >= 0:
        return count, chk
    return fallback_rows, fallback_checksum


def fingerprint_for_reconcile(
    value: Any,
    *,
    ddl_type: str = "",
    engine: str = "",
    transform: str | None = None,
) -> str:
    """Canonical Gate-8 fingerprint: transform → destination bind → normalize_cell.

    Source samples and destination read-back must share this path so Mongo
    ``\"true\"`` / MySQL ``0`` / Postgres ``False`` compare as equal.
    """
    ddl_type = instant_date_carrier(engine, ddl_type)
    wire: Any = value
    if is_missing_sentinel(value):
        # An absent field is NULL to every SQL destination — that is what the
        # writer stores and what the read-back returns. Only the ``Missing``
        # *object* reached this path untranslated (its string spelling was
        # already handled downstream), and ``cell_to_string`` renders it as an
        # empty string. That made a sparse Mongo/DynamoDB/Redis document
        # fingerprint as ``''`` against a destination NULL, failing Gate-8 on a
        # correct transfer — and, in the other direction, matching a destination
        # that really did store an empty string.
        wire = None
        value = None
    tname = (transform or "").strip().lower()
    if tname and tname not in {"", "none", "identity", "passthrough"}:
        cell = cell_to_string(value, preserve_sql_null=True) if value is not None else None
        if value is None:
            converted, err = None, None
        else:
            converted, err = apply_transform(
                cell if cell is not None else "", transform or "none"
            )
        if err:
            # Quarantine / coerce_null write path stores SQL NULL for failed cells.
            # Fingerprint must match that — not the raw bad wire — or Gate-8
            # false-fails every quarantined coercion as a sample mismatch.
            wire = None
        else:
            wire = converted
    elif value is not None and not isinstance(value, (str, int, float, bool, bytes)):
        wire = cell_to_string(value, preserve_sql_null=True)

    if ddl_type:
        # Dest read-back is storage-canonical. Re-applying the transfer locale
        # turns EU ``1.234`` (from ``1,234``) into 1234 and false-fails Gate-8.
        token = set_active_number_locale("")
        try:
            try:
                wire = normalize_sql_bind_value(wire, ddl_type, engine=engine)
            except Exception:
                pass
        finally:
            reset_active_number_locale(token)
        # Fingerprint the instant at the granularity the carrier keeps, or a
        # declared narrowing (Snowflake TIMESTAMP → MySQL DATETIME) reports as
        # a whole-column checksum mismatch with no column named.
        wire = quantize_instant_for_carrier(wire, ddl_type=ddl_type, engine=engine)
    return normalize_cell(wire, ddl_type=ddl_type, engine=engine)


def normalize_cell(value: Any, *, ddl_type: str = "", engine: str = "") -> str:
    """Canonical cell text for Gate-8 checksums.

    CHAR/NCHAR blank-pad is a storage artifact (Oracle/SQL Server/MySQL) — rtrim
    spaces so faithful CHAR→VARCHAR migrations do not false-fail (wmk/Airbyte
    class). VARCHAR/TEXT preserve trailing spaces when ``ddl_type`` is known so
    intentional trailing whitespace is not silently equated away.

    When ``engine`` is Oracle, zero-length strings fingerprint as NULL (Oracle
    VARCHAR2 semantics / HVR write-location compare rules). Postgres/MySQL keep
    NULL and ``""`` distinct.
    """
    if value is None:
        # Distinct from empty string — SQL/Dynamo NULL must not checksum as "".
        return _NULL_SENTINEL
    # Dense write materializes absent schemaless fields as SQL NULL; fingerprint
    # must match. Sparse CDC sample_compare skips DF_MISSING columns (omit-from-SET).
    if is_missing_sentinel(value):
        return _NULL_SENTINEL
    if isinstance(value, str) and value.strip().lower() in {
        "__df_sql_null__",
        "__df_ddb_null__",
        "__df_missing__",
    }:
        return _NULL_SENTINEL
    # Oracle write-location: '' is stored/read as NULL — equate before other paths.
    if destination_empty_string_is_null(engine) and isinstance(value, str) and value == "":
        return _NULL_SENTINEL
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, _datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        # Canonical form is UTC wall-clock without a Z marker so TIMESTAMPTZ
        # sources match NTZ sinks that stored the same UTC components, while
        # offset-aware values still differ from a different naive wall clock.
        if value.microsecond:
            return value.strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0").rstrip(".")
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, _date):
        return _datetime.combine(value, _time.min).strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, float):
        # Pass the float through — str(float) keeps IEEE residue and false-fails
        # Gate-8 vs DECIMAL sinks (106.60000000000001 vs 106.6).
        return _canonicalize_number(value) or "nan"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return _canonicalize_number(value) or "nan"
    if isinstance(value, (bytes, bytearray, memoryview)):
        # Bytes may be raw payload or a base64-encoded string stored as bytes
        # (common in emulators). When the bytes are a valid base64 string,
        # decode and re-encode so the canonical checksum matches the original
        # encoded text; otherwise base64-encode the raw bytes.
        try:
            decoded = base64.b64decode(value, validate=True)
            re_encoded = base64.b64encode(decoded)
            if re_encoded == value:
                return re_encoded.decode("ascii")
        except Exception:
            logger.debug("Raw bytes are not valid base64; encoding as base64 for checksum")
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, (dict, list, tuple, set, frozenset)):
        return json.dumps(value, sort_keys=True, default=json_default)
    raw_text = str(value)
    if ddl_type:
        try:
            plan = _text_fold_plan(ddl_type)
            if plan.rstrip_blank_pad:
                # Blank-pad only — do not strip leading spaces (rare but significant).
                text = raw_text.rstrip(" ")
            elif plan.keep_trailing_spaces:
                # VARCHAR/TEXT: preserve trailing spaces (significant payload).
                text = raw_text
            else:
                text = raw_text.strip()
            # Collation equality must match the destination engine (CI/AI/WI/KI/VSS).
            if plan.fold_width:
                from services.type_system import fold_width_forms

                text = fold_width_forms(text)
            if plan.fold_kana:
                from services.type_system import fold_kana

                text = fold_kana(text)
            if plan.fold_variation:
                from services.type_system import fold_variation_selectors

                text = fold_variation_selectors(text)
            if plan.fold_accent:
                from services.type_system import fold_diacritics

                text = fold_diacritics(text)
            if plan.casefold:
                text = text.casefold()
            # UUID / UNIQUEIDENTIFIER / CHAR(36) UUID carriers — canonicalize
            # braces / 32-hex / case so source wire and dest read-back match
            # (Fivetran HVR compare class: destination storage rules win).
            if plan.uuid_carrier:
                try:
                    from connectors.sql_bind import coerce_uuid_wire

                    text = coerce_uuid_wire(text) or text
                except ValueError:
                    if _UUID_RE.match(text):
                        text = text.lower()
        except Exception:
            text = raw_text.strip()
    else:
        text = raw_text.strip()
    # Boolean and empty fast paths.
    if not text:
        return ""
    lowered = text.lower()
    # Align with transform_engine strict boolean tokens only. Status enums
    # ("active"/"enabled"/…) must NOT collide with true/false in checksums —
    # that falsely claimed 100% fidelity when status strings met bool columns.
    if lowered in _STRICT_BOOL_TRUE:
        return "1"
    if lowered in _STRICT_BOOL_FALSE:
        return "0"
    # Numeric fast path: write-path bind only. Auto-ambiguous ``1,234`` /
    # ``1.234`` / ``1.000`` stay opaque text — ``Decimal(text)`` is a second
    # algorithm and invented ``1.000`` → ``1``. Locale money the write path
    # binds still folds so Gate-8 matches the dest DECIMAL.
    if _looks_like_numeric_wire(text):
        canonical = _canonicalize_number(text)
        if canonical is not None:
            return canonical
        return text
    # JSON payloads (e.g. jsonb).
    if text.startswith(("{", "[")):
        try:
            parsed = json_loads_exact(text)
            if isinstance(parsed, (dict, list)):
                return json.dumps(parsed, sort_keys=True, default=json_default)
        except (json.JSONDecodeError, TypeError):
            pass
    # Date/time normalization: cheap heuristic first to avoid running the date
    # regex on every non-date string.
    if (
        text[0].isdigit()
        and len(text) >= 8
        and _DATE_LIKE_CHARS.intersection(text)
        and _DATE_LIKE_RE.search(text)
    ):
        dtm = _parse_datetime(text)
        if dtm:
            # Fold offsets/Z to UTC wall-clock; keep naive strings as wall-clock.
            # Do not keep a trailing Z — that falsely fails NTZ sink readback
            # against TIMESTAMPTZ sources that share the same UTC components.
            return _checksum_datetime_utc_wall(
                dtm if isinstance(dtm, str) else str(dtm), original=text
            )
        dt = _parse_date(text)
        if dt:
            return f"{dt}T00:00:00"
    return text


def _checksum_datetime_utc_wall(iso_text: str, *, original: str | None = None) -> str:
    """Canonical UTC wall-clock for checksum equality (no Z marker).

    Aware/offset/Z strings fold to UTC components. Naive wall-clock strings
    stay as-is. That equates TIMESTAMPTZ→DATETIME UTC storage without equating
    ``12:00+05:30`` (06:30 UTC) to naive ``12:00``.
    """
    text = (iso_text or "").strip()
    if not text:
        return ""
    src = (original or text).strip()
    aware_or_epoch = bool(
        src.endswith(("Z", "z")) or re.search(r"[+-]\d{2}:?\d{2}\s*$", src)
    )
    try:
        from services.transform_engine import _EPOCH_MS_RE, _EPOCH_S_RE

        aware_or_epoch = aware_or_epoch or bool(
            _EPOCH_MS_RE.match(src) or _EPOCH_S_RE.match(src)
        )
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    try:
        iso = text[:-1] + "+00:00" if text.endswith("Z") else text
        obj = _datetime.fromisoformat(iso)
        if obj.tzinfo is not None or aware_or_epoch:
            if obj.tzinfo is None:
                obj = obj.replace(tzinfo=timezone.utc)
            else:
                obj = obj.astimezone(timezone.utc)
            obj = obj.replace(tzinfo=None)
        if obj.microsecond:
            return obj.strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0").rstrip(".")
        return obj.strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        # Fall back: strip invented trailing Z from transform parser.
        if text.endswith("Z") and not aware_or_epoch:
            return text[:-1]
        if text.endswith("Z"):
            return text[:-1]  # still wall-clock form for checksum
        return text


def _checksum_datetime_utc_z(iso_text: str) -> str:
    """Backward-compatible alias — checksums use wall-clock UTC, not Z."""
    return _checksum_datetime_utc_wall(iso_text)


def _decimal_from_ieee_float(value: float) -> Decimal | None:
    """Collapse IEEE binary residue (Excel ``106.60000000000001`` → ``106.6``).

    Double has ~15–17 significant digits; formatting with 15 significant figures
    matches Airbyte/Fivetran-class compare and avoids Gate-8 false fails when
    DECIMAL sinks store the human value.
    """
    import math

    if math.isnan(value):
        return None
    if math.isinf(value):
        return Decimal("Infinity") if value > 0 else Decimal("-Infinity")
    return Decimal(format(value, ".15g"))


def _normalize_exact(d: Decimal) -> Decimal:
    """Strip trailing zeros without the 28-digit rounding of the default context.

    ``Decimal.normalize`` honours the ambient context precision, so a value
    carrying more than 28 significant digits is quietly rounded. Inside a
    checksum that is a correctness bug in both directions: it can fail a clean
    transfer, and it can map two genuinely different decimals onto one digest,
    reporting corrupted data as verified.
    """
    if not d.is_finite():
        return d
    with localcontext() as ctx:
        ctx.prec = max(len(d.as_tuple().digits), 28)
        return d.normalize()


def _is_exact_double(d: Decimal) -> bool:
    """True when ``d`` is precisely the value of some IEEE double.

    This is the licence to round a mantissa down to 15 significant digits. When
    it holds, the long tail is the double's own decimal expansion and dropping it
    recovers the human value (``106.60000000000001`` → ``106.6``). When it fails,
    the digits carry information no double can hold — ``12345678901234567890.123``
    or ``-999999999999999999.999999`` — and rounding would corrupt the very value
    Gate-8 is comparing, either failing a clean transfer or, worse, letting two
    genuinely different decimals collapse onto one checksum.
    """
    import math

    try:
        f = float(d)
    except (OverflowError, ValueError):
        return False
    if not math.isfinite(f):
        return False
    try:
        return Decimal(repr(f)) == _normalize_exact(d)
    except (InvalidOperation, Overflow, ValueError):
        return False


def _canonicalize_number(value: Any) -> str | None:
    """Return a canonical string for numeric values so 9.5 == 9.5000000000.

    Strings go through ``decimal_wire_value`` — the same parser the write path
    binds. Auto-ambiguous ``1,234`` / ``1.234`` / ``1.000`` return ``None`` so
    Gate-8 cannot invent ``1.000`` → ``1``. Locale money and both-separator
    forms still fold. IEEE residue still collapses when information-free.
    """
    try:
        if isinstance(value, float):
            d = _decimal_from_ieee_float(value)
            if d is None:
                return None
        elif isinstance(value, Decimal):
            d = value
            # Decimal(float(...)) keeps binary noise — collapse long mantissas.
            if d.is_finite():
                digits = d.as_tuple().digits
                exp = d.as_tuple().exponent
                if (
                    len(digits) > 15 or (isinstance(exp, int) and exp < -12)
                ) and _is_exact_double(d):
                    try:
                        d = _decimal_from_ieee_float(float(d)) or d
                    except (OverflowError, ValueError):
                        pass
        else:
            text = str(value).strip()
            if not text:
                return None
            # Checksum dest text is already storage-canonical. Re-applying the
            # transfer locale turns EU ``1.234`` (from ``1,234``) into 1234.
            token = set_active_number_locale("")
            try:
                parsed = decimal_wire_value(text)
            finally:
                reset_active_number_locale(token)
            if parsed is None:
                # Write path refused (Auto ``1,234`` / ``1.234`` / ``1.000``).
                # Decimal(text) invented a different number (``1.000`` → ``1``).
                return None
            d = parsed
            # String form of float residue (common from Excel/CSV readers).
            if d.is_finite() and ("." in text or "e" in text.lower() or "," in text):
                head = text.split("e")[0].split("E")[0]
                frac = head.split(".")[-1] if "." in head else ""
                if len(frac.rstrip("0")) > 12 and _is_exact_double(d):
                    try:
                        d = _decimal_from_ieee_float(float(d)) or d
                    except (OverflowError, ValueError):
                        pass
        if d.is_nan():
            return None
        from services.value_serializer import safe_decimal_text

        s = safe_decimal_text(_normalize_exact(d) if d.is_finite() else d)
        if s is None:
            return None
        if "." in s and "e" not in s.lower():
            s = s.rstrip("0").rstrip(".")
        return s if s else "0"
    except (InvalidOperation, Overflow, TypeError, ValueError):
        return None


def build_reconciliation_proof(
    source_records: list[dict[str, Any]],
    target_records: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
    *,
    primary_key: str | None = None,
    sample_size: int = 50,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build a deterministic proof object for row-level transfer verification.

    The proof is based on exact primary-key matching and normalized mapped-value
    comparison across a bounded sample. It returns a score suitable for a
    preflight/reconciliation gate, not a legal audit guarantee.

    When ``dest_db_type`` / ``dest_types`` are set, sample compare uses the same
    write-path fingerprint as live Gate-8 (bool/JSON bind parity).
    """
    if not source_records and not target_records:
        return {
            "passed": True,
            "matched_key_count": 0,
            "missing_key_count": 0,
            "row_fidelity_score": 1.0,
            "sample_compare": {"passed": True, "compared": 0, "mismatches": []},
        }

    key_col = primary_key
    key_provenance = "explicit" if primary_key else "missing"
    if not key_col:
        # Do not invent "id" — positional / guessed identity produces false
        # high row-fidelity scores (Airbyte-class honesty gap).
        return {
            "passed": False,
            "matched_key_count": 0,
            "missing_key_count": len(source_records),
            "extra_key_count": 0,
            "row_fidelity_score": 0.0,
            "sample_compare": {
                "passed": False,
                "compared": 0,
                "mismatches": [],
                "alignment": "unproven_identity",
            },
            "identity": {
                "column": None,
                "proven": False,
                "reason": "primary_key required for key-aligned fidelity proof",
            },
            "verification_mode": "unproven_identity",
        }

    source_keys = {
        normalize_cell(row.get(key_col))
        for row in source_records
        if row.get(key_col) is not None
    }
    target_keys = {
        normalize_cell(row.get(key_col))
        for row in target_records
        if row.get(key_col) is not None
    }
    # Null / blank keys cannot prove uniqueness — fail closed on fidelity claims.
    source_null_keys = sum(
        1
        for row in source_records
        if row.get(key_col) is None or normalize_cell(row.get(key_col)) == ""
    )
    target_null_keys = sum(
        1
        for row in target_records
        if row.get(key_col) is None or normalize_cell(row.get(key_col)) == ""
    )
    if (
        source_null_keys
        or target_null_keys
        or len(source_keys) < max(1, len(source_records) - source_null_keys)
    ):
        # Duplicate or null identity → positional comparison only, never high confidence.
        sample_compare = sample_compare_rows(
            source_records,
            target_records,
            mappings,
            sample_size=sample_size,
            sort_key=None,
            dest_db_type=dest_db_type,
            dest_types=dest_types,
        )
        sample_compare = {
            **sample_compare,
            "alignment": "positional_only",
            "identity_warning": "weak or non-unique primary key — fidelity is sample/positional only",
        }
        return {
            "passed": False,
            "matched_key_count": 0,
            "missing_key_count": len(source_records),
            "extra_key_count": 0,
            "row_fidelity_score": 0.0,
            "sample_compare": sample_compare,
            "identity": {
                "column": key_col,
                "proven": False,
                "reason": "null or duplicate identity values — refuse key-aligned proof",
            },
            "verification_mode": "positional_only",
        }

    matched_keys = source_keys & target_keys
    missing_keys = source_keys - target_keys
    extra_keys = target_keys - source_keys

    sample_compare = sample_compare_rows(
        source_records,
        target_records,
        mappings,
        sample_size=sample_size,
        sort_key=key_col,
        dest_db_type=dest_db_type,
        dest_types=dest_types,
    )

    matched_key_count = len(matched_keys)
    missing_key_count = len(missing_keys)
    extra_key_count = len(extra_keys)
    total_keys = max(len(source_keys), 1)
    row_fidelity_score = round(
        max(
            0.0,
            1.0
            - (missing_key_count / total_keys)
            - (extra_key_count / total_keys) * 0.25,
        ),
        4,
    )

    passed = (
        missing_key_count == 0
        and extra_key_count == 0
        and sample_compare.get("passed", True)
        and row_fidelity_score >= 0.95
    )

    return {
        "passed": passed,
        "matched_key_count": matched_key_count,
        "missing_key_count": missing_key_count,
        "extra_key_count": extra_key_count,
        "row_fidelity_score": row_fidelity_score,
        # Stamp alignment on every branch so the UI can distinguish keyed proof
        # from positional comparison without inferring it from absence.
        "sample_compare": {**sample_compare, "alignment": "key_aligned"},
        "identity": {"column": key_col, "proven": True, "provenance": key_provenance},
        "verification_mode": "key_aligned",
    }





# Sample selection lives in its own module (size budget); re-exported because
# this module is the reconciliation surface callers import from.
from services.sample_strategy import (  # noqa: E402,F401 — re-export
    _auto_stratify_source_column,
    _bucket_member_order,
    _stratified_sample_indices,
)


def sample_compare_rows(
    source_records: list[dict[str, Any]],
    target_rows: list[dict[str, Any]] | list[tuple[Any, ...]] | list[list[Any]],
    mappings: list[dict[str, Any]],
    *,
    target_columns: list[str] | None = None,
    sample_size: int = 50,
    sort_key: str | None = None,
    dest_db_type: str = "",
    dest_types: dict[str, str] | None = None,
    stratify_by: str | None = None,
    rows_are_paired: bool = False,
) -> dict[str, Any]:
    """
    Compare mapped column values between source records and destination read-back.
    Rows are aligned by a unique key, so upserts and out-of-order writes compare
    correctly. Without one the comparison is declined rather than guessed.

    ``rows_are_paired`` is for the caller that can vouch the two collections are
    the same rows in the same order — the row at index *i* on each side really is
    the same row. Gate-8 cannot: its source rows are the batch the pass held and
    its destination rows are the first N of the table by ``ORDER BY 1``, two
    independent draws. Pairing those by index reports unrelated rows as
    corruption, which is why it is opt-in and off by default.

    When ``dest_db_type`` / ``dest_types`` are provided, both sides are fingerprinted
    through the same write-path bind helpers as MySQL/Postgres/Snowflake writers
    (``fingerprint_for_reconcile``) so Gate-8 does not false-fail on bool/JSON wire.
    """
    if not source_records or not target_rows or not mappings:
        return {"passed": True, "compared": 0, "mismatches": [], "skipped": True}

    dest_types = dest_types or {}
    eng = (dest_db_type or "").strip().lower()

    def _as_dict(tgt_raw: Any) -> dict[str, Any] | None:
        if isinstance(tgt_raw, dict):
            return tgt_raw
        if target_columns and isinstance(tgt_raw, (list, tuple)):
            return {
                col: tgt_raw[i] if i < len(tgt_raw) else None
                for i, col in enumerate(target_columns)
            }
        return None

    target_dicts = [d for d in (_as_dict(t) for t in target_rows) if d is not None]

    # sort_key is a *target* column (e.g. id). Source rows may still use the
    # pre-map name (rec_id → id) — resolve via mappings so key alignment works.
    source_sort_key = sort_key
    if sort_key and mappings:
        sk = sort_key.lower()
        for m in mappings:
            if str(m.get("target") or "").lower() == sk and m.get("source"):
                source_sort_key = str(m["source"])
                break

    target_by_key: dict[str, dict[str, Any]] = {}
    duplicate_alignment_key = ""
    if sort_key:
        for d in target_dicts:
            key = normalize_cell(d.get(sort_key))
            if not key:
                continue
            if key in target_by_key:
                # Two destination rows answer to the same key, so the key does
                # not identify a row and cannot align one. Keeping the first and
                # comparing every later row against it reports the difference
                # between two *different rows* as corruption.
                duplicate_alignment_key = sort_key
                break
            target_by_key[key] = d
        if not duplicate_alignment_key:
            seen_source_keys: set[str] = set()
            for rec in source_records:
                if not isinstance(rec, dict):
                    continue
                key = normalize_cell(
                    rec.get(source_sort_key) if source_sort_key else None
                )
                if not key:
                    continue
                if key in seen_source_keys:
                    duplicate_alignment_key = sort_key
                    break
                seen_source_keys.add(key)

    if duplicate_alignment_key:
        # Nothing here is evidence of a bad write, so nothing here may fail the
        # transfer. ``_sort_key_for_columns`` falls back to the first mapped
        # column when no identity column exists, which for an ordinary export —
        # a region, a status, a date — repeats on almost every row. Declining is
        # the honest answer: the sample proved nothing, and says so.
        return {
            "passed": True,
            "compared": 0,
            "mismatches": [],
            "skipped": True,
            "alignment": "declined",
            "reason": (
                f"No unique identity key for read-back alignment: "
                f"`{duplicate_alignment_key}` repeats, so it cannot say which "
                "destination row corresponds to which source row. Map a primary "
                "key to enable per-row sample compare."
            ),
        }

    def _fingerprint(raw: Any, *, transform: str | None, tgt_col: str) -> str:
        ddl = (
            dest_types.get(tgt_col)
            or dest_types.get(tgt_col.lower())
            or ""
        )
        if not ddl:
            for m in mappings:
                if str(m.get("target") or "") == tgt_col:
                    ddl = str(m.get("target_type") or m.get("inferredType") or "")
                    break
        if ddl and typed_cast_incompatible_with_text_sink(
            transform or "", normalize_logical_type(ddl)
        ):
            # A text carrier holds whichever of the two the write produced: the
            # converted value when the cast succeeded ('$1,000.00' → '1000.00'),
            # or the token verbatim when it failed ('Y' against boolean, which
            # otherwise made Gate-8 report corruption on a row that landed
            # correctly). Only the failing case may retire the cast.
            try:
                _, cast_err = apply_transform(raw, transform or "")
            except Exception:
                cast_err = "transform_failed"
            if cast_err:
                transform = None
        if eng and ddl:
            try:
                return fingerprint_for_reconcile(
                    raw, ddl_type=ddl, engine=eng, transform=transform
                )
            except Exception:
                pass
        if transform:
            try:
                converted, err = apply_transform(raw, transform)
            except Exception:
                converted, err = raw, "transform_failed"
            if err:
                converted = None
        else:
            converted = raw
        return normalize_cell(converted)

    def _row_key(rec: Any, *, source_side: bool = False) -> Any:
        if isinstance(rec, dict):
            key = source_sort_key if source_side else sort_key
            val = rec.get(key) if key else None
            if val is None and source_side and sort_key and sort_key != source_sort_key:
                val = rec.get(sort_key)
            return val or (rec.get(target_columns[0]) if target_columns else None)
        return rec

    def _sortable(value: Any) -> tuple:
        """Return a stable, type-safe sort key that handles bson.Decimal128."""
        if value is None:
            return (0, Decimal(0))
        if value.__class__.__name__ == "Decimal128":
            try:
                return (0, value.to_decimal())
            except Exception:
                return (1, str(value))
        if isinstance(value, Decimal):
            return (0, value)
        if isinstance(value, bool):
            return (0, Decimal(int(value)))
        if isinstance(value, (int, float)):
            try:
                return (0, Decimal(value))
            except Exception:
                return (1, str(value))
        text = str(value).strip()
        parsed = decimal_wire_value(text)
        if parsed is not None:
            return (0, parsed)
        return (1, text.lower())

    import hashlib

    dict_records = [r for r in source_records if isinstance(r, dict)]
    auto_selected = False
    # Auto-stratify: skewed low-cardinality mapped column (never the PK).
    strat_col = (stratify_by or "").strip() or None
    if not strat_col and len(dict_records) > sample_size:
        strat_col = _auto_stratify_source_column(
            dict_records,
            mappings,
            sort_key=sort_key,
            source_sort_key=source_sort_key,
        )
        auto_selected = bool(strat_col)

    if strat_col and dict_records:
        idxs = _stratified_sample_indices(
            dict_records,
            stratify_col=strat_col,
            sample_size=sample_size,
            seed=f"{sort_key or ''}:{strat_col}",
        )
        source_sorted = [dict_records[i] for i in idxs if i < len(dict_records)]
        method = "stratified"
    else:
        source_sorted = sorted(
            dict_records or source_records,
            key=lambda r: _sortable(_row_key(r, source_side=True)),
        )[:sample_size]
        method = "keyed_sorted" if sort_key else "positional_sorted"
        strat_col = None
        auto_selected = False

    # Deterministic seed for auditor replay — sorted PK (or positional index) set.
    pk_values: list[str] = []
    for src in source_sorted:
        key_raw = _row_key(src, source_side=True)
        pk_values.append(normalize_cell(key_raw) if key_raw is not None else "")
    seed_canon = json.dumps(
        {
            "method": method,
            "size": len(source_sorted),
            "sort_key": sort_key or "",
            "source_sort_key": source_sort_key or "",
            "stratify_by": strat_col or "",
            "auto_selected": auto_selected,
            "pk_values": pk_values,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    sample_seed = {
        "method": method,
        "size": len(source_sorted),
        "sort_key": sort_key or "",
        "source_sort_key": source_sort_key or "",
        "stratify_by": strat_col or "",
        "auto_selected": auto_selected,
        "coverage": "sample",
        "population_proof": False,
        "note": (
            "Stratified/keyed sample improves category coverage within the "
            "read-back sample only — not population proof."
            if method == "stratified"
            else "Keyed/positional sample — not population proof."
        ),
        "pk_values": pk_values[:sample_size],
        "content_sha256": hashlib.sha256(seed_canon.encode("utf-8")).hexdigest(),
    }

    if not (sort_key and target_by_key) and not rows_are_paired:
        # Without a key there is nothing to join on, and the two sides are not
        # the same draw: the source rows are the batch this pass held, while the
        # destination read is the first N of the whole table by ``ORDER BY 1``.
        # Lining those up by index compares unrelated rows and calls the
        # difference corruption. A sample that cannot be aligned proves nothing,
        # which is a different statement from the data being wrong.
        return {
            "passed": True,
            "compared": 0,
            "mismatches": [],
            "skipped": True,
            "alignment": "declined",
            "sample_seed": sample_seed,
            "reason": (
                "No identity key to align the read-back sample: source rows and "
                "the destination read are drawn independently, so position does "
                "not pair them. Map a primary key to enable per-row compare."
            ),
        }

    mismatches: list[dict[str, str]] = []
    compared = 0
    rows_compared = 0
    keyed = bool(sort_key and target_by_key)
    # Only reachable when the caller vouched for pairing; see ``rows_are_paired``.
    target_paired = list(target_dicts) if not keyed else []

    def _result(*, passed: bool) -> dict[str, Any]:
        # ``compared`` counts *cells*, and every operator-facing line rendered it
        # as "compared N row(s)" — a 500-cell, 50-row sample read as ten times
        # the evidence it was. Both denominators are reported so a match
        # percentage can name the population it is a percentage of.
        return {
            "passed": passed,
            "compared": compared,
            "cells_compared": compared,
            "rows_compared": rows_compared,
            "mismatches": mismatches,
            "sample_seed": sample_seed,
            "alignment": "keyed" if keyed else "paired_by_caller",
        }

    for idx, src in enumerate(source_sorted):
        if keyed:
            key = normalize_cell(src.get(source_sort_key) if source_sort_key else None)
            if not key and sort_key:
                key = normalize_cell(src.get(sort_key))
            tgt = target_by_key.get(key) if key else None
            if tgt is None:
                # This key is not in the destination read-back window. That is a
                # scope miss, not a missing row, and falling back to the row at
                # the same index would compare two unrelated rows.
                continue
        else:
            tgt = target_paired[idx] if idx < len(target_paired) else None
        if tgt is None:
            continue

        # Case-insensitive target lookup — MySQL/Snowflake cursors may fold names.
        tgt_keys = {str(k).lower(): k for k in tgt.keys()}
        rows_compared += 1

        for m in mappings:
            src_col = str(m.get("source") or "")
            tgt_col = str(m.get("target") or "")
            if not src_col or not tgt_col:
                continue
            physical_tgt = tgt_keys.get(tgt_col.lower())
            if physical_tgt is None:
                # Column absent from read-back sample — do not invent NULL mismatch.
                continue
            transform = m.get("transform")
            if not transform or str(transform).strip().lower() in {
                "",
                "none",
                "identity",
                "passthrough",
            }:
                # Mirror writer_common.resolve_transform so quarantine coerces
                # (integer/date/…) fingerprint as NULL like the write path.
                try:
                    from services.transform_resolver import resolve_transform

                    transform = resolve_transform(
                        m,
                        column_types={},
                        dest_types=dest_types,
                    )
                except Exception:
                    transform = m.get("transform")
            # Sparse CDC / STOP_COLUMN / coerce_null: source DF_MISSING means
            # omit-from-SET — skip compare (do not fingerprint as NULL).
            # Destination DF_MISSING is a leak and must still mismatch.
            from services.value_serializer import is_missing_sentinel

            raw_src = src.get(src_col) if isinstance(src, dict) else None
            if is_missing_sentinel(raw_src):
                continue
            raw_tgt = tgt.get(physical_tgt)
            src_val = _fingerprint(
                raw_src, transform=transform, tgt_col=tgt_col
            )
            # Destination already applied bind at write — fingerprint without
            # re-transform so read-back bools/JSON match source write-path form.
            tgt_val = _fingerprint(
                raw_tgt, transform=None, tgt_col=tgt_col
            )
            compared += 1
            if src_val != tgt_val:
                mismatches.append(
                    {
                        "row": str(idx),
                        "source": src_col,
                        "target": tgt_col,
                        "source_value": src_val[:120],
                        "target_value": tgt_val[:120],
                    }
                )
                if len(mismatches) >= 10:
                    return _result(passed=False)

    return _result(passed=len(mismatches) == 0)


class TargetSampleUnavailable(RuntimeError):
    """Destination sample could not be read — distinct from an empty table.

    Callers that treat ``[]`` as "nothing is there" (CDC delete proof, Gate-8
    fidelity) must catch this and fail the gate. Swallowing the error as an
    empty list is what made a missing SELECT grant report delete proof as
    passed while the rows were still live.
    """


def read_target_sample(
    db_type: str,
    dest: dict[str, Any],
    *,
    schema: str,
    table_name: str,
    columns: list[str] | None = None,
    limit: int = 50,
    sort_key: str | None = None,
    key_values: list[Any] | None = None,
) -> list[dict[str, Any]]:
    """Read a small ordered sample from the destination for value reconciliation.

    Implemented in :mod:`services.target_sample`; kept here as the stable entry
    point Gate-8 and the transfer engine already import.
    """
    from services.target_sample import read_target_sample as _read

    return _read(
        db_type,
        dest,
        schema=schema,
        table_name=table_name,
        columns=columns,
        limit=limit,
        sort_key=sort_key,
        key_values=key_values,
    )
